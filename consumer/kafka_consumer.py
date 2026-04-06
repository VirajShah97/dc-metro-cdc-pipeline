"""
Kafka consumer for dc-metro-cdc-pipeline.

Reads Debezium CDC messages from:
  - metro.public.train_predictions
  - metro.public.train_positions

Applies schema validation, anomaly flagging (predictions only), and
deduplication, then writes NDJSON batches to S3 (production) or local
filesystem (development).

Batch interval: 5 minutes. Dead letter records written separately.
"""

import json
import os
import time
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer
import boto3
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'metro-consumer-group')
S3_BUCKET = os.getenv('S3_BUCKET', '')
OUTPUT_MODE = os.getenv('OUTPUT_MODE', 'local')
LOCAL_OUTPUT_DIR = os.getenv(
    'LOCAL_OUTPUT_DIR',
    os.path.join(os.path.dirname(__file__), '..', 'output')
)
BATCH_INTERVAL_SECONDS = int(os.getenv('BATCH_INTERVAL_SECONDS', '300'))

PREDICTIONS_TOPIC = 'metro.public.train_predictions'
POSITIONS_TOPIC = 'metro.public.train_positions'

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Static lookups
# ---------------------------------------------------------------------------

STATION_CODES_PATH = os.path.join(os.path.dirname(__file__), 'station_codes.json')
with open(STATION_CODES_PATH, 'r') as f:
    STATION_CODES = json.load(f)

VALID_LINE_CODES = {'RD', 'BL', 'OR', 'SV', 'GR', 'YL'}
VALID_CAR_COUNTS = {'4', '6', '8'}
VALID_MINUTES_SPECIAL = {'ARR', 'BRD'}


# ---------------------------------------------------------------------------
# Processing functions
# ---------------------------------------------------------------------------

def extract_record(message_value):
    payload = message_value
    if 'payload' in message_value:
        payload = message_value['payload']
    op = payload.get('op')
    if op == 'd' or payload.get('after') is None:
        return None, op
    return payload['after'], op


def validate_predictions_schema(record):
    required_fields = ['station_code', 'line_code', 'minutes', 'ingested_at']
    missing = [f for f in required_fields if f not in record or record[f] is None]
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"
    return True, None


def validate_positions_schema(record):
    required_fields = ['train_id', 'circuit_id', 'line_code', 'ingested_at']
    missing = [f for f in required_fields if f not in record or record[f] is None]
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"
    return True, None


def flag_anomalies(record):
    reasons = []
    minutes_val = str(record.get('minutes', ''))
    if minutes_val not in VALID_MINUTES_SPECIAL:
        try:
            int(minutes_val)
        except (ValueError, TypeError):
            reasons.append(f"invalid minutes: {minutes_val}")
    car_count = str(record.get('car_count', '') or '')
    if car_count and car_count not in VALID_CAR_COUNTS:
        reasons.append(f"unexpected car_count: {car_count}")
    line_code = record.get('line_code', '')
    if line_code and line_code not in VALID_LINE_CODES:
        reasons.append(f"unknown line_code: {line_code}")
    station_code = record.get('station_code', '')
    if station_code not in STATION_CODES:
        reasons.append(f"unknown station_code: {station_code}")
    return len(reasons) > 0, reasons


def deduplicate_predictions(batch):
    seen = {}
    for record in batch:
        key = (
            record.get('station_code', ''),
            record.get('destination_code', ''),
            record.get('platform_group', ''),
            record.get('ingested_at', '')
        )
        seen[key] = record
    return list(seen.values())


def deduplicate_positions(batch):
    seen = {}
    for record in batch:
        key = (
            record.get('train_id', ''),
            record.get('circuit_id', ''),
            record.get('ingested_at', '')
        )
        seen[key] = record
    return list(seen.values())


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _write_output(path, content):
    if OUTPUT_MODE == 's3':
        s3 = boto3.client('s3')
        s3.put_object(Bucket=S3_BUCKET, Key=path, Body=content.encode('utf-8'))
    else:
        full_path = os.path.join(LOCAL_OUTPUT_DIR, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w') as f:
            f.write(content)


def write_batch(predictions, positions, dead_letters):
    now = datetime.now(timezone.utc)
    date_path = now.strftime('%Y-%m-%d')
    timestamp = now.strftime('%H-%M-%S')

    if predictions:
        path = f"raw/predictions/{date_path}/predictions_{timestamp}.json"
        _write_output(path, '\n'.join(json.dumps(r) for r in predictions))
        logger.info(f"Wrote {len(predictions)} prediction records to {path}")

    if positions:
        path = f"raw/positions/{date_path}/positions_{timestamp}.json"
        _write_output(path, '\n'.join(json.dumps(r) for r in positions))
        logger.info(f"Wrote {len(positions)} position records to {path}")

    if dead_letters:
        dl_path = f"dead-letter/{date_path}/failed_{timestamp}.json"
        _write_output(dl_path, '\n'.join(json.dumps(d) for d in dead_letters))
        logger.warning(f"Wrote {len(dead_letters)} dead letters to {dl_path}")


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def run_consumer():
    logger.info(f"Starting consumer — broker: {KAFKA_BROKER}, topics: {PREDICTIONS_TOPIC}, {POSITIONS_TOPIC}")
    logger.info(f"Output mode: {OUTPUT_MODE}, batch interval: {BATCH_INTERVAL_SECONDS}s")

    consumer = KafkaConsumer(
        PREDICTIONS_TOPIC,
        POSITIONS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    predictions_batch = []
    positions_batch = []
    dead_letters = []
    last_flush = time.time()

    logger.info("Consumer connected. Listening for messages...")

    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)

            for tp, records in messages.items():
                topic = tp.topic
                for message in records:
                    try:
                        after, op = extract_record(message.value)
                        if after is None:
                            continue

                        if topic == PREDICTIONS_TOPIC:
                            valid, error = validate_predictions_schema(after)
                            if not valid:
                                dead_letters.append({'topic': topic, 'original': after, 'error': error, 'timestamp': datetime.now(timezone.utc).isoformat()})
                                continue
                            is_anomaly, reasons = flag_anomalies(after)
                            after['is_anomaly'] = is_anomaly
                            after['anomaly_reasons'] = reasons if is_anomaly else []
                            predictions_batch.append(after)

                        elif topic == POSITIONS_TOPIC:
                            valid, error = validate_positions_schema(after)
                            if not valid:
                                dead_letters.append({'topic': topic, 'original': after, 'error': error, 'timestamp': datetime.now(timezone.utc).isoformat()})
                                continue
                            positions_batch.append(after)

                    except Exception as e:
                        logger.error(f"Error processing message at offset {message.offset} on {topic}: {e}")
                        dead_letters.append({'topic': topic, 'original': str(message.value)[:500], 'error': str(e), 'timestamp': datetime.now(timezone.utc).isoformat()})

            elapsed = time.time() - last_flush
            if elapsed >= BATCH_INTERVAL_SECONDS and (predictions_batch or positions_batch or dead_letters):
                deduped_predictions = deduplicate_predictions(predictions_batch)
                deduped_positions = deduplicate_positions(positions_batch)
                pred_dupes = len(predictions_batch) - len(deduped_predictions)
                pos_dupes = len(positions_batch) - len(deduped_positions)
                logger.info(
                    f"Batch flush — predictions: {len(predictions_batch)} raw, {pred_dupes} dupes removed, "
                    f"{len(deduped_predictions)} written | positions: {len(positions_batch)} raw, "
                    f"{pos_dupes} dupes removed, {len(deduped_positions)} written | "
                    f"{len(dead_letters)} dead letters"
                )
                write_batch(deduped_predictions, deduped_positions, dead_letters)
                predictions_batch = []
                positions_batch = []
                dead_letters = []
                last_flush = time.time()

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        if predictions_batch or positions_batch or dead_letters:
            deduped_predictions = deduplicate_predictions(predictions_batch)
            deduped_positions = deduplicate_positions(positions_batch)
            write_batch(deduped_predictions, deduped_positions, dead_letters)
            logger.info(f"Final flush: {len(deduped_predictions)} predictions, {len(deduped_positions)} positions, {len(dead_letters)} dead letters")
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == '__main__':
    run_consumer()