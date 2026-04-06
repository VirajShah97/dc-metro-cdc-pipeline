import os
import time
import logging
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

WMATA_API_KEY       = os.getenv("WMATA_API_KEY")
WMATA_PREDICTIONS_URL = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
WMATA_POSITIONS_URL   = "https://api.wmata.com/TrainPositions/TrainPositions?contentType=json"
POLL_INTERVAL       = int(os.getenv("POLL_INTERVAL", 30))
MAX_RETRIES         = 4
BASE_BACKOFF        = 2

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "metro_db")
DB_USER     = os.getenv("DB_USER", "metro_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "metro_password")

INSERT_PREDICTIONS_SQL = """
    INSERT INTO train_predictions (
        station_code, station_name, line_code, car_count,
        destination_code, destination_name, platform_group, minutes
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_POSITIONS_SQL = """
    INSERT INTO train_positions (
        train_id, car_count, direction_num, circuit_id,
        destination_station_code, line_code, seconds_at_location, service_type
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def _get_with_retries(url: str) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code < 500:
                raise
            wait = BASE_BACKOFF ** attempt
            logger.warning(f"Server error on attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {wait}s. Error: {e}")
            time.sleep(wait)
        except requests.exceptions.RequestException as e:
            wait = BASE_BACKOFF ** attempt
            logger.warning(f"Request failed on attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {wait}s. Error: {e}")
            time.sleep(wait)
    raise RuntimeError(f"All {MAX_RETRIES} attempts failed for {url}")

def fetch_predictions() -> list[dict]:
    data = _get_with_retries(f"{WMATA_PREDICTIONS_URL}?api_key={WMATA_API_KEY}")
    return data.get("Trains", [])

def fetch_positions() -> list[dict]:
    data = _get_with_retries(f"{WMATA_POSITIONS_URL}&api_key={WMATA_API_KEY}")
    return data.get("TrainPositions", [])

def insert_predictions(conn, trains: list[dict]) -> int:
    rows = [
        (
            train.get("LocationCode"),
            train.get("LocationName"),
            train.get("Line"),
            train.get("Car"),
            train.get("DestinationCode"),
            train.get("DestinationName"),
            train.get("Group"),
            train.get("Min"),
        )
        for train in trains
    ]
    with conn.cursor() as cur:
        cur.executemany(INSERT_PREDICTIONS_SQL, rows)
    conn.commit()
    return len(rows)

def insert_positions(conn, positions: list[dict]) -> int:
    rows = [
        (
            pos.get("TrainId"),
            pos.get("CarCount"),
            pos.get("DirectionNum"),
            pos.get("CircuitId"),
            pos.get("DestinationStationCode"),
            pos.get("LineCode"),
            pos.get("SecondsAtLocation"),
            pos.get("ServiceType"),
        )
        for pos in positions
    ]
    with conn.cursor() as cur:
        cur.executemany(INSERT_POSITIONS_SQL, rows)
    conn.commit()
    return len(rows)

def main():
    logger.info("Starting WMATA ingestion. Polling every %ds.", POLL_INTERVAL)
    conn = get_db_connection()
    logger.info("Connected to Postgres.")
    try:
        while True:
            try:
                trains = fetch_predictions()
                pred_count = insert_predictions(conn, trains)
                logger.info("Inserted %d predictions.", pred_count)

                positions = fetch_positions()
                pos_count = insert_positions(conn, positions)
                logger.info("Inserted %d train positions.", pos_count)

            except RuntimeError as e:
                logger.error("Skipping cycle after exhausting retries: %s", e)
            except Exception as e:
                logger.error("Unexpected error in cycle: %s", e)
                conn = get_db_connection()
            time.sleep(POLL_INTERVAL)
    finally:
        conn.close()
        logger.info("Postgres connection closed.")

if __name__ == "__main__":
    main()