"""
Kafka consumer for dc-metro-cdc-pipeline.

Reads Debezium CDC messages from the metro.public.train_predictions topic,
applies schema validation, anomaly flagging, and deduplication, then writes
NDJSON batches to S3 (production) or local filesystem (development).

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
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'metro.public.train_predictions')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'metro-consumer-group')
S3_BUCKET = os.getenv('S3_BUCKET', '')
OUTPUT_MODE = os.getenv('OUTPUT_MODE', 'local')  # 'local' or 's3'
LOCAL_OUTPUT_DIR = os.getenv(
    'LOCAL_OUTPUT_DIR',
    os.path.join(os.path.dirname(__file__), '..', 'output')
)
BATCH_INTERVAL_SECONDS = int(os.getenv('BATCH_INTERVAL_SECONDS', '300'))

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
    """
    Extract the 'after' record from a Debezium CDC message.

    Debezium envelope structure:
        {"schema": {...}, "payload": {"before": ..., "after": {...}, "op": "c"}}

    Returns (after_dict, op_string) or (None, op_string) for deletes/tombstones.
    """
    payload = message_value
    if 'payload' in message_value:
        payload = message_value['payload']

    op = payload.get('op')

    # d = delete (no after), tombstone messages have no payload at all
    if op == 'd' or payload.get('after') is None:
        return None, op

    return payload['after'], op


def validate_schema(record):
    """
    Validate that required fields exist and are non-null in the CDC record.

    Required: station_code, line_code, minutes, ingested_at.
    Returns (is_valid, error_message).
    """
    required_fields = ['station_code', 'line_code', 'minutes', 'ingested_at']
    missing = [f for f in required_fields if f not in record or record[f] is None]

    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"
    return True, None


def flag_anomalies(record):
    """
    Check for anomalous values. Record passes through regardless —
    is_anomaly flag is set for downstream filtering.

    Anomaly conditions:
    - minutes is not a number and not ARR/BRD
    - car_count not in (4, 6, 8)
    - line_code not in (RD, BL, OR, SV, GR, YL)
    - station_code not in known station set
    """
    reasons = []

    # minutes check
    minutes_val = str(record.get('minutes', ''))
    if minutes_val not in VALID_MINUTES_SPECIAL:
        try:
            int(minutes_val)
        except (ValueError, TypeError):
            reasons.append(f"invalid minutes: {minutes_val}")

    # car_count check — can be null/empty for some predictions
    car_count = str(record.get('car_count', '') or '')
    if car_count and car_count not in VALID_CAR_COUNTS:
        reasons.append(f"unexpected car_count: {car_count}")

    # line_code check
    line_code = record.get('line_code', '')
    if line_code and line_code not in VALID_LINE_CODES:
        reasons.append(f"unknown line_code: {line_code}")

    # station_code check
    station_code = record.get('station_code', '')
    if station_code not in STATION_CODES:
        reasons.append(f"unknown station_code: {station_code}")

    return len(reasons) > 0, reasons



def deduplicate_batch(batch):
    """
    Deduplicate within a batch using composite key:
    station_code + destination_code + platform_group + ingested_at.

    Last write wins — later messages in the batch overwrite earlier ones
    with the same key.
    """
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


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _write_output(path, content):
    """Write content string to S3 or local filesystem."""
    if OUTPUT_MODE == 's3':
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=path,
            Body=content.encode('utf-8')
        )
    else:
        full_path = os.path.join(LOCAL_OUTPUT_DIR, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w') as f:
            f.write(content)


def write_batch(records, dead_letters):
    """Flush records and dead letters to NDJSON files."""
    now = datetime.now(timezone.utc)
    date_path = now.strftime('%Y-%m-%d')
    timestamp = now.strftime('%H-%M-%S')

    if records:
        predictions_path = f"raw/predictions/{date_path}/predictions_{timestamp}.json"
        ndjson = '\n'.join(json.dumps(r) for r in records)
        _write_output(predictions_path, ndjson)
        logger.info(f"Wrote {len(records)} records to {predictions_path}")

    if dead_letters:
        dl_path = f"dead-letter/{date_path}/failed_{timestamp}.json"
        ndjson = '\n'.join(json.dumps(d) for d in dead_letters)
        _write_output(dl_path, ndjson)
        logger.warning(f"Wrote {len(dead_letters)} dead letters to {dl_path}")


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def run_consumer():
    """Connect to Kafka and process messages in batched intervals."""
    logger.info(f"Starting consumer — broker: {KAFKA_BROKER}, topic: {KAFKA_TOPIC}")
    logger.info(f"Output mode: {OUTPUT_MODE}, batch interval: {BATCH_INTERVAL_SECONDS}s")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    batch = []
    dead_letters = []
    last_flush = time.time()

    logger.info("Consumer connected. Listening for messages...")

    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)

            for tp, records in messages.items():
                for message in records:
                    try:
                        after, op = extract_record(message.value)

                        # Skip deletes and tombstones
                        if after is None:
                            continue

                        # 1. Schema validation
                        valid, error = validate_schema(after)
                        if not valid:
                            dead_letters.append({
                                'original': after,
                                'error': error,
                                'timestamp': datetime.now(timezone.utc).isoformat()
                            })
                            continue

                        # 2. Anomaly flagging (pass-through)
                        is_anomaly, reasons = flag_anomalies(after)
                        after['is_anomaly'] = is_anomaly
                        after['anomaly_reasons'] = reasons if is_anomaly else []

                        batch.append(after)

                    except Exception as e:
                        logger.error(f"Error processing message at offset {message.offset}: {e}")
                        dead_letters.append({
                            'original': str(message.value)[:500],
                            'error': str(e),
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })

            # Flush on interval
            elapsed = time.time() - last_flush
            if elapsed >= BATCH_INTERVAL_SECONDS and (batch or dead_letters):
                deduped = deduplicate_batch(batch)
                dupes_removed = len(batch) - len(deduped)
                logger.info(
                    f"Batch flush: {len(batch)} raw, {dupes_removed} dupes removed, "
                    f"{len(deduped)} written, {len(dead_letters)} dead letters"
                )
                write_batch(deduped, dead_letters)
                batch = []
                dead_letters = []
                last_flush = time.time()

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        if batch or dead_letters:
            deduped = deduplicate_batch(batch)
            write_batch(deduped, dead_letters)
            logger.info(f"Final flush: {len(deduped)} records, {len(dead_letters)} dead letters")
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == '__main__':
    run_consumer()