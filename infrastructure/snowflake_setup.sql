-- =============================================================================
-- DC Metro CDC Pipeline — Snowflake Setup
-- =============================================================================
-- Run these statements in order in a Snowflake worksheet.
-- Replace all <PLACEHOLDER> values with your actual credentials/identifiers.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Warehouse
-- -----------------------------------------------------------------------------

CREATE WAREHOUSE DC_METRO_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- -----------------------------------------------------------------------------
-- 2. Database and Schema
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS DC_METRO;
CREATE SCHEMA IF NOT EXISTS DC_METRO.RAW;

-- STAGING and MARTS schemas are created automatically by dbt on first run.

-- -----------------------------------------------------------------------------
-- 3. Raw Table
-- -----------------------------------------------------------------------------
-- Single VARIANT column for schema-flexible NDJSON ingestion.
-- loaded_at captures when Snowpipe loaded the record, not when the
-- prediction was observed (that's ingested_at inside the VARIANT payload).

CREATE TABLE DC_METRO.RAW.PREDICTIONS (
    raw_data VARIANT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- 4. File Format
-- -----------------------------------------------------------------------------

CREATE FILE FORMAT DC_METRO.RAW.NDJSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE;

-- -----------------------------------------------------------------------------
-- 5. Storage Integration
-- -----------------------------------------------------------------------------
-- Connects Snowflake to S3 via IAM role assumption. No hardcoded credentials.
--
-- [ACTION REQUIRED] After running this statement:
--   1. Run DESC INTEGRATION to get STORAGE_AWS_IAM_USER_ARN and
--      STORAGE_AWS_EXTERNAL_ID.
--   2. Update the trust policy on your IAM role in AWS with these values.
--      If reusing an existing role (e.g. from another project), add the new
--      external ID to the StringEquals condition as an array value alongside
--      the existing one.

CREATE STORAGE INTEGRATION S3_METRO_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://<S3_BUCKET_NAME>/')
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/<IAM_ROLE_NAME>';

-- Retrieve IAM user ARN and external ID for trust policy update
DESC INTEGRATION S3_METRO_INTEGRATION;

-- -----------------------------------------------------------------------------
-- 6. Stage
-- -----------------------------------------------------------------------------

CREATE STAGE DC_METRO.RAW.S3_PREDICTIONS_STAGE
    URL = 's3://<S3_BUCKET_NAME>/raw/predictions/'
    STORAGE_INTEGRATION = S3_METRO_INTEGRATION
    FILE_FORMAT = DC_METRO.RAW.NDJSON_FORMAT;

-- Verify Snowflake can read S3 files
LIST @DC_METRO.RAW.S3_PREDICTIONS_STAGE;

-- -----------------------------------------------------------------------------
-- 7. Snowpipe
-- -----------------------------------------------------------------------------
-- AUTO_INGEST = TRUE enables event-driven loading via S3 → SQS notification.
--
-- [ACTION REQUIRED] After creating the pipe:
--   1. Run SHOW PIPES to get the notification_channel (SQS ARN).
--   2. In AWS S3 bucket properties, create an event notification:
--        Name:       metro-predictions-snowpipe
--        Prefix:     raw/predictions/
--        Event type: s3:ObjectCreated:*
--        Destination: SQS queue (paste the ARN from SHOW PIPES)

CREATE PIPE DC_METRO.RAW.PREDICTIONS_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO DC_METRO.RAW.PREDICTIONS (raw_data)
    FROM @DC_METRO.RAW.S3_PREDICTIONS_STAGE;

-- Get the SQS ARN for S3 event notification setup
SHOW PIPES IN DC_METRO.RAW;

-- -----------------------------------------------------------------------------
-- 8. Backfill and Diagnostics
-- -----------------------------------------------------------------------------

-- Backfill files that landed before the S3 event notification was configured
ALTER PIPE DC_METRO.RAW.PREDICTIONS_PIPE REFRESH;

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('DC_METRO.RAW.PREDICTIONS_PIPE');

-- Verify data loaded
SELECT COUNT(*) FROM DC_METRO.RAW.PREDICTIONS;

-- Check load history for troubleshooting
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'DC_METRO.RAW.PREDICTIONS',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
));

-- -----------------------------------------------------------------------------
-- 9. Positions Table
-- -----------------------------------------------------------------------------

CREATE TABLE DC_METRO.RAW.POSITIONS (
    RAW_DATA VARIANT,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- 10. Positions Stage
-- -----------------------------------------------------------------------------
-- Reuses the same storage integration but points to raw/positions/ prefix.

CREATE STAGE DC_METRO.RAW.S3_POSITIONS_STAGE
    URL = 's3://<S3_BUCKET_NAME>/raw/positions/'
    STORAGE_INTEGRATION = S3_METRO_INTEGRATION
    FILE_FORMAT = DC_METRO.RAW.NDJSON_FORMAT;

-- -----------------------------------------------------------------------------
-- 11. Positions Snowpipe
-- -----------------------------------------------------------------------------
-- [ACTION REQUIRED] After creating the pipe:
--   1. Run SHOW PIPES to get the notification_channel (SQS ARN) for POSITIONS_PIPE.
--   2. In AWS S3 bucket properties, create a second event notification:
--        Name:       metro-positions-snowpipe
--        Prefix:     raw/positions/
--        Event type: s3:ObjectCreated:*
--        Destination: SQS queue (paste the ARN from SHOW PIPES)

CREATE OR REPLACE PIPE DC_METRO.RAW.POSITIONS_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO DC_METRO.RAW.POSITIONS (RAW_DATA)
    FROM (SELECT $1 FROM @DC_METRO.RAW.S3_POSITIONS_STAGE/raw/positions/)
    FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = FALSE);

-- Get SQS ARN for S3 event notification setup
SHOW PIPES IN DC_METRO.RAW;

-- Backfill files already in S3
ALTER PIPE DC_METRO.RAW.POSITIONS_PIPE REFRESH;

-- Verify
SELECT SYSTEM$PIPE_STATUS('DC_METRO.RAW.POSITIONS_PIPE');
SELECT COUNT(*) FROM DC_METRO.RAW.POSITIONS;