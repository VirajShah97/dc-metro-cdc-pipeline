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

WMATA_API_KEY   = os.getenv("WMATA_API_KEY")
WMATA_URL       = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", 30))
MAX_RETRIES     = 4
BASE_BACKOFF    = 2

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "metro_db")
DB_USER     = os.getenv("DB_USER", "metro_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "metro_password")

INSERT_SQL = """
    INSERT INTO train_predictions (
        station_code, station_name, line_code, car_count,
        destination_code, destination_name, platform_group, minutes
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


def _get_with_retries(url: str, headers: dict) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=10)
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
    headers = {}
    data = _get_with_retries(f"{WMATA_URL}?api_key={WMATA_API_KEY}", headers)
    return data.get("Trains", [])


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
        cur.executemany(INSERT_SQL, rows)
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
                count = insert_predictions(conn, trains)
                logger.info("Inserted %d predictions.", count)
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