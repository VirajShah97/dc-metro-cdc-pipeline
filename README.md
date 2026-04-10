# dc-metro-cdc-pipeline

A near-real-time CDC pipeline that captures live DC Metro train predictions and measures how reliable they actually are.

## Why

DC Metro is my primary way of getting around. When the app says 1 minute and the train doesn't show for another 8, that's not just annoying — it affects real plans. I built this to stop guessing and start measuring.

**Business question:** Which DC Metro stations have the least reliable predictions, and when does reliability degrade most?

---

## Architecture

```
WMATA API → PostgreSQL (EC2) → Debezium → Kafka → S3 → Snowpipe → Snowflake → dbt → Tableau
```

| Component | Role |
|---|---|
| WMATA API | Source — GetPredictions and TrainPositions endpoints polled every 30 seconds |
| PostgreSQL | Landing table for raw API responses; WAL enables CDC |
| Debezium | Captures every row change via PostgreSQL WAL tailing |
| Kafka | Event stream — 2 topics: `metro.public.train_predictions`, `metro.public.train_positions` |
| Python Consumer | Consumes Kafka topics, batches records, writes NDJSON to S3 every 5 minutes |
| Amazon S3 | Immutable raw storage — `raw/predictions/` and `raw/positions/` |
| Snowpipe | Auto-ingests files from S3 into Snowflake on arrival via SQS event notification |
| Snowflake | Data warehouse — `DC_METRO.RAW`, `DC_METRO.STAGING`, `DC_METRO.MARTS` |
| dbt Core | Staging models (dedup, casting, timezone normalization) → mart models (volatility metric) |
| Tableau | Dashboard — system-wide health, station ranking, station detail map |

**Infrastructure:** EC2 t3.small running the full CDC stack (PostgreSQL, Zookeeper, Kafka, Debezium) via Docker Compose. EC2 was the right fit — the CDC stack is tightly coupled and stateful, making Fargate a poor fit without MSK + RDS.

---

## Key Design Decisions

**Why PostgreSQL before Debezium?**
Debezium tails database transaction logs — it cannot connect directly to an API. PostgreSQL provides the WAL that CDC requires.

**Why Kafka?**
Decouples ingestion from consumption. The consumer can restart without losing events (within the 24-hour retention window).

**Why not write directly from consumer to Snowflake?**
S3 as an intermediary provides replayability, decouples the consumer from the warehouse, and lets Snowpipe handle loading asynchronously.

**Why EC2 over Fargate?**
The CDC stack (PostgreSQL, Zookeeper, Kafka, Debezium) is tightly coupled and stateful. Fargate is better suited for stateless isolated workloads.

**At-least-once delivery:**
Kafka guarantees at-least-once delivery. Consumer restarts can produce duplicate records in S3. Deduplication happens in the dbt staging layer using `QUALIFY ROW_NUMBER()` — raw data stays immutable and replayable.

---

## Volatility Metric

Prediction volatility is defined as a prediction jumping upward between consecutive polls for the same `station_code + line_code + destination_code + platform_group`, measured using `LAG()` in dbt.

A smooth countdown (5 → 4 → 3 → ARR) is not volatile. A jump (3 → 5 → 2) is volatile (`is_volatile = TRUE`).

**Finding:** Evening rush predictions are ~28% more volatile than off-peak. If you're sprinting down the escalator at 6 PM because the board says 1 minute — the data says you're probably fine.

---

## Repo Structure

```
dc-metro-cdc-pipeline/
├── ingestion/
│   └── wmata_ingestion.py       # Polls WMATA API every 30s, inserts to PostgreSQL
├── consumer/
│   └── kafka_consumer.py        # Consumes Kafka, batches and writes to S3
├── infrastructure/
│   ├── docker-compose.yml       # PostgreSQL, Zookeeper, Kafka, Debezium
│   ├── postgres_setup.sql       # Table definitions + REPLICA IDENTITY FULL
│   ├── debezium_connector.json  # Debezium connector config
│   └── snowflake_setup.sql      # Snowflake warehouse, database, stage, pipe setup
├── dc_metro_dbt/
│   ├── models/
│   │   ├── staging/             # stg__predictions.sql — dedup, casting, timezone fix
│   │   └── marts/               # mart__predictions.sql — volatility metric
│   └── seeds/
│       └── dim_stations.csv     # 102 DC Metro stations with lat/lon
└── .env.example                 # Environment variable template
```

---

## Prerequisites

- AWS account (EC2, S3, SQS, IAM)
- Snowflake account
- WMATA API key — [register here](https://developer.wmata.com/) (free, Default Tier: 10 calls/sec, 50k calls/day)
- Docker and Docker Compose
- Python 3.9+
- dbt Core with `dbt-snowflake` adapter

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/VirajShah97/dc-metro-cdc-pipeline.git
cd dc-metro-cdc-pipeline
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your values:

```
WMATA_API_KEY=your_key
DB_HOST=localhost
DB_PORT=5433
DB_NAME=metro_db
DB_USER=metro_user
DB_PASSWORD=metro_password
KAFKA_BROKER=localhost:9092
S3_BUCKET=your-bucket-name
AWS_REGION=us-east-1
```

### 3. Start the CDC stack

```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

Wait ~60 seconds for Kafka and Debezium to initialize.

### 4. Apply the PostgreSQL schema

```bash
cat infrastructure/postgres_setup.sql | docker exec -i metro_postgres psql -U metro_user -d metro_db
```

### 5. Register the Debezium connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @infrastructure/debezium_connector.json
```

### 6. Set up Snowflake

Run `infrastructure/snowflake_setup.sql` in your Snowflake account. Replace placeholder values for your S3 bucket, IAM role ARN, and SQS ARN.

### 7. Start ingestion and consumer

```bash
nohup python3 ingestion/wmata_ingestion.py > logs/ingestion.log 2>&1 &
nohup python3 consumer/kafka_consumer.py > logs/consumer.log 2>&1 &
```

### 8. Run dbt

```bash
cd dc_metro_dbt
dbt run
dbt test
```

---

## Notes

- EC2 public IP changes on every stop/start. Update the security group SSH inbound rule each time.
- `docker compose` (v2 plugin) is required — `docker-compose` (v1) is broken on newer Docker versions.
- Raw S3 data is immutable. Never modify files in `raw/` — all transformation happens downstream in dbt.
- The pipeline runs continuously. dbt runs on an hourly cron schedule on EC2.