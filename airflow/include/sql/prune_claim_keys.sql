-- Key-list retention: keep only the last 7 days of key captures in RAW.CLAIM_KEYS.
-- Runs after a successful dbt_build, so the capture dbt just used is never touched.
--
-- The cutoff is relative to the NEWEST capture, not to today. If the pipeline stops for two weeks,
-- this still keeps the latest capture (and the 7 days before it) instead of deleting everything.
--
-- DELETE, never TRUNCATE: TRUNCATE also clears the table's load history, so the next COPY INTO
-- would reload every old key file still in Blob.
DELETE FROM CLAIMS_DWH.RAW.CLAIM_KEYS
WHERE captured_at < (SELECT MAX(captured_at) FROM CLAIMS_DWH.RAW.CLAIM_KEYS) - INTERVAL '7 days';
