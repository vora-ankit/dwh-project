-- Load any new Parquet files from Azure Blob into the three raw tables.
-- Safe to run repeatedly: Snowflake remembers which files it already loaded (64 days),
-- so a rerun with no new files loads 0 rows. This is the statement set the Airflow
-- load task will run later.
--
-- Replace <your_warehouse> before running.
-- Do NOT confuse this with rebuild_raw.sql, which DROPS the raw tables.

USE ROLE SYSADMIN;
USE WAREHOUSE <your_warehouse>;
USE SCHEMA CLAIMS_DWH.RAW;

COPY INTO CLAIMS_DWH.RAW.CLAIMS
  FROM @CLAIMS_DWH.RAW.CLAIMS_EXT_STAGE
  PATTERN = '.*[.]parquet'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  INCLUDE_METADATA = (
    _SOURCE_FILE = METADATA$FILENAME,
    _FILE_ROW    = METADATA$FILE_ROW_NUMBER,
    _LOADED_AT   = METADATA$START_SCAN_TIME)
  ON_ERROR = ABORT_STATEMENT;

COPY INTO CLAIMS_DWH.RAW.PROVIDERS
  FROM @CLAIMS_DWH.RAW.PROVIDERS_EXT_STAGE
  PATTERN = '.*[.]parquet'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  INCLUDE_METADATA = (
    _SOURCE_FILE = METADATA$FILENAME,
    _FILE_ROW    = METADATA$FILE_ROW_NUMBER,
    _LOADED_AT   = METADATA$START_SCAN_TIME)
  ON_ERROR = ABORT_STATEMENT;

COPY INTO CLAIMS_DWH.RAW.MEMBERS
  FROM @CLAIMS_DWH.RAW.MEMBERS_EXT_STAGE
  PATTERN = '.*[.]parquet'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  INCLUDE_METADATA = (
    _SOURCE_FILE = METADATA$FILENAME,
    _FILE_ROW    = METADATA$FILE_ROW_NUMBER,
    _LOADED_AT   = METADATA$START_SCAN_TIME)
  ON_ERROR = ABORT_STATEMENT;

-- Claim key lists (deletes detection). Needs snowflake_keys_setup.sql to have been run once.
COPY INTO CLAIMS_DWH.RAW.CLAIM_KEYS
  FROM @CLAIMS_DWH.RAW.CLAIM_KEYS_EXT_STAGE
  PATTERN = '.*[.]parquet'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  INCLUDE_METADATA = (
    _SOURCE_FILE = METADATA$FILENAME,
    _LOADED_AT   = METADATA$START_SCAN_TIME)
  ON_ERROR = ABORT_STATEMENT;

-- Check: total rows and the most recent load per table
SELECT 'claims' AS table_name, COUNT(*) AS total_rows, MAX(_LOADED_AT) AS last_loaded FROM CLAIMS_DWH.RAW.CLAIMS
UNION ALL SELECT 'providers', COUNT(*), MAX(_LOADED_AT) FROM CLAIMS_DWH.RAW.PROVIDERS
UNION ALL SELECT 'members',   COUNT(*), MAX(_LOADED_AT) FROM CLAIMS_DWH.RAW.MEMBERS;
