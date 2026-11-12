-- 04: add the ROW_VERSION column to RAW.CLAIM_KEYS. Run ONCE, BEFORE the first pipeline run that uses
-- the new extract_keys.py.
--
-- Why this is needed: COPY INTO ... MATCH_BY_COLUMN_NAME only loads columns the table already has.
-- Without this column, the new row_version values in the key files would be silently dropped
-- (the schema-change problem from Lesson 2 / backlog item 9).
--
-- Older captures will have ROW_VERSION = NULL; the reconciliation test skips those.
USE ROLE SYSADMIN;
ALTER TABLE CLAIMS_DWH.RAW.CLAIM_KEYS ADD COLUMN IF NOT EXISTS ROW_VERSION VARCHAR;

DESC TABLE CLAIMS_DWH.RAW.CLAIM_KEYS;   -- check: ROW_VERSION is listed
