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
-- 3. Raw Tables
-- -----------------------------------------------------------------------------

CREATE TABLE DC_METRO.RAW.PREDICTIONS (
    raw_data VARIANT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE DC_METRO.RAW.POSITIONS (
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
-- [ACTION REQUIRED] After running this statement:
--   1. Run DESC INTEGRATION to get STORAGE_AWS_IAM_USER_ARN and
--      STORAGE_AWS_EXTERNAL_ID.
--   2. Update the trust policy on your IAM role in AWS with these values.

CREATE STORAGE INTEGRATION S3_METRO_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://<S3_BUCKET_NAME>/')
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/<IAM_ROLE_NAME>';

DESC INTEGRATION S3_METRO_INTEGRATION;

-- -----------------------------------------------------------------------------
-- 6. Stages
-- -----------------------------------------------------------------------------

CREATE STAGE DC_METRO.RAW.S3_PREDICTIONS_STAGE
    URL = 's3://<S3_BUCKET_NAME>/raw/predictions/'
    STORAGE_INTEGRATION = S3_METRO_INTEGRATION
    FILE_FORMAT = DC_METRO.RAW.NDJSON_FORMAT;

CREATE STAGE DC_METRO.RAW.S3_POSITIONS_STAGE
    URL = 's3://<S3_BUCKET_NAME>/raw/positions/'
    STORAGE_INTEGRATION = S3_METRO_INTEGRATION
    FILE_FORMAT = DC_METRO.RAW.NDJSON_FORMAT;

-- Verify Snowflake can read S3 files
LIST @DC_METRO.RAW.S3_PREDICTIONS_STAGE;
LIST @DC_METRO.RAW.S3_POSITIONS_STAGE;

-- -----------------------------------------------------------------------------
-- 7. Snowpipes
-- -----------------------------------------------------------------------------
-- [ACTION REQUIRED] After creating each pipe:
--   1. Run SHOW PIPES to get the notification_channel (SQS ARN).
--   2. In AWS S3 bucket properties, create an event notification per pipe:
--
--   Predictions pipe:
--     Name:       metro-predictions-snowpipe
--     Prefix:     raw/predictions/
--     Event type: s3:ObjectCreated:*
--     Destination: SQS ARN from SHOW PIPES for PREDICTIONS_PIPE
--
--   Positions pipe:
--     Name:       metro-positions-snowpipe
--     Prefix:     raw/positions/
--     Event type: s3:ObjectCreated:*
--     Destination: SQS ARN from SHOW PIPES for POSITIONS_PIPE

CREATE PIPE DC_METRO.RAW.PREDICTIONS_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO DC_METRO.RAW.PREDICTIONS (raw_data)
    FROM @DC_METRO.RAW.S3_PREDICTIONS_STAGE;

CREATE OR REPLACE PIPE DC_METRO.RAW.POSITIONS_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO DC_METRO.RAW.POSITIONS (RAW_DATA)
    FROM (SELECT $1 FROM @DC_METRO.RAW.S3_POSITIONS_STAGE)
    FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = FALSE);

SHOW PIPES IN DC_METRO.RAW;

-- -----------------------------------------------------------------------------
-- 8. Backfill and Diagnostics
-- -----------------------------------------------------------------------------

ALTER PIPE DC_METRO.RAW.PREDICTIONS_PIPE REFRESH;
ALTER PIPE DC_METRO.RAW.POSITIONS_PIPE REFRESH;

SELECT SYSTEM$PIPE_STATUS('DC_METRO.RAW.PREDICTIONS_PIPE');
SELECT SYSTEM$PIPE_STATUS('DC_METRO.RAW.POSITIONS_PIPE');

SELECT COUNT(*) FROM DC_METRO.RAW.PREDICTIONS;
SELECT COUNT(*) FROM DC_METRO.RAW.POSITIONS;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'PREDICTIONS',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
));

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'POSITIONS',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
));