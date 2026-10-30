-- One-time Snowflake setup for the claim key list (deletes detection).
-- Run AFTER `python extract_keys.py` has written at least one file to Azure Blob.
-- Replace <your_warehouse> and <storageaccount> first. Safe to rerun.

USE ROLE SYSADMIN;
USE WAREHOUSE <your_warehouse>;
USE SCHEMA CLAIMS_DWH.RAW;

-- 1. Stage for the key files. The folder is claims_keys/, which is NOT inside claims/,
--    so the existing claims stage does not pick these files up by accident.
CREATE OR REPLACE STAGE CLAIMS_DWH.RAW.CLAIM_KEYS_EXT_STAGE
  URL = 'azure://<storageaccount>.blob.core.windows.net/raw/claims_keys/'
  STORAGE_INTEGRATION = AZURE_RAW_INT
  FILE_FORMAT = CLAIMS_DWH.RAW.PARQUET_FF;

LIST @CLAIMS_DWH.RAW.CLAIM_KEYS_EXT_STAGE;   -- check: keys.parquet files are listed

-- 2. Raw table (columns CLAIMID and CAPTURED_AT, inferred from the file)
CREATE TABLE IF NOT EXISTS CLAIMS_DWH.RAW.CLAIM_KEYS
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID)
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@CLAIMS_DWH.RAW.CLAIM_KEYS_EXT_STAGE',
      FILE_FORMAT => 'CLAIMS_DWH.RAW.PARQUET_FF',
      IGNORE_CASE => TRUE)));

ALTER TABLE CLAIMS_DWH.RAW.CLAIM_KEYS ADD COLUMN IF NOT EXISTS _SOURCE_FILE VARCHAR;
ALTER TABLE CLAIMS_DWH.RAW.CLAIM_KEYS ADD COLUMN IF NOT EXISTS _LOADED_AT TIMESTAMP_LTZ;

-- 3. Load
COPY INTO CLAIMS_DWH.RAW.CLAIM_KEYS
  FROM @CLAIMS_DWH.RAW.CLAIM_KEYS_EXT_STAGE
  PATTERN = '.*[.]parquet'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  INCLUDE_METADATA = (
    _SOURCE_FILE = METADATA$FILENAME,
    _LOADED_AT   = METADATA$START_SCAN_TIME)
  ON_ERROR = ABORT_STATEMENT;

-- 4. Check: CAPTURED_AT should be a timestamp (not a huge number), and the key count should
--    equal the number of claims currently in the source.
DESC TABLE CLAIMS_DWH.RAW.CLAIM_KEYS;
SELECT CAPTURED_AT, COUNT(*) AS keys FROM CLAIMS_DWH.RAW.CLAIM_KEYS GROUP BY 1 ORDER BY 1;
