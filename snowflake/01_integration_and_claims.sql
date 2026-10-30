-- 01: one-time Snowflake setup for the claims raw layer.
-- Run in a Snowsight worksheet. Replace the placeholders first:
--   <your Entra tenant ID>   Microsoft Entra ID -> Overview -> Tenant ID
--   <storageaccount>         Azure storage account name
--   <your_warehouse>         X-Small warehouse
-- Then run 02_providers_members.sql and 03_claim_keys.sql.

-- 1. Storage integration (needs ACCOUNTADMIN once)
USE ROLE ACCOUNTADMIN;
CREATE STORAGE INTEGRATION IF NOT EXISTS AZURE_RAW_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<your Entra tenant ID>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://<storageaccount>.blob.core.windows.net/raw/');

DESC STORAGE INTEGRATION AZURE_RAW_INT;
-- Open AZURE_CONSENT_URL and accept. Then in Azure, on the storage account:
-- Access control (IAM) -> Add role assignment -> Storage Blob Data Reader ->
-- the app named in AZURE_MULTI_TENANT_APP_NAME (part before the underscore).

GRANT USAGE ON INTEGRATION AZURE_RAW_INT TO ROLE SYSADMIN;

-- 2. Database, schema, file format, claims stage
USE ROLE SYSADMIN;
USE WAREHOUSE <your_warehouse>;

CREATE DATABASE IF NOT EXISTS CLAIMS_DWH;
CREATE SCHEMA IF NOT EXISTS CLAIMS_DWH.RAW;

-- USE_LOGICAL_TYPE = TRUE: read Parquet timestamps as timestamps, not raw integers
CREATE FILE FORMAT IF NOT EXISTS CLAIMS_DWH.RAW.PARQUET_FF
  TYPE = PARQUET
  USE_LOGICAL_TYPE = TRUE;

CREATE OR REPLACE STAGE CLAIMS_DWH.RAW.CLAIMS_EXT_STAGE
  URL = 'azure://<storageaccount>.blob.core.windows.net/raw/claims/'
  STORAGE_INTEGRATION = AZURE_RAW_INT
  FILE_FORMAT = CLAIMS_DWH.RAW.PARQUET_FF;

LIST @CLAIMS_DWH.RAW.CLAIMS_EXT_STAGE;   -- check: Parquet files listed (run an extract first)

-- 3. Raw claims table, columns inferred from the files, plus lineage columns
CREATE TABLE IF NOT EXISTS CLAIMS_DWH.RAW.CLAIMS
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID)
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@CLAIMS_DWH.RAW.CLAIMS_EXT_STAGE',
      FILE_FORMAT => 'CLAIMS_DWH.RAW.PARQUET_FF',
      IGNORE_CASE => TRUE)));

ALTER TABLE CLAIMS_DWH.RAW.CLAIMS ADD COLUMN IF NOT EXISTS _SOURCE_FILE VARCHAR;
ALTER TABLE CLAIMS_DWH.RAW.CLAIMS ADD COLUMN IF NOT EXISTS _FILE_ROW NUMBER;
ALTER TABLE CLAIMS_DWH.RAW.CLAIMS ADD COLUMN IF NOT EXISTS _LOADED_AT TIMESTAMP_LTZ;
-- Loading is done by load_raw.sql (or the Airflow load_raw task).
