"""Incremental extract of several source tables to Parquet in Azure Blob Storage.

Usage (venv active, project folder):
    python extract_tables.py                       # claims, providers and members
    python extract_tables.py providers members     # only these
    python extract_tables.py --full providers      # ignore the saved watermark for these

Needs extract_claims.py in the same folder: connection, type mapping and Parquet conversion
are reused from there so the logic lives in one place. The watermark approach is the one
described in that file (rowversion column, MIN_ACTIVE_ROWVERSION upper bound, watermark saved
only after every file for the table uploaded).

Each table gets its own folder and its own watermark in the container:
    claims/load_date=.../...       _state/claims_watermark.json     (same as before)
    providers/load_date=.../...    _state/providers_watermark.json
    members/load_date=.../...      _state/members_watermark.json
A failure on one table does not stop the others. The script exits with an error at the end
if any table failed, so a scheduler such as Airflow marks the run as failed.

To add a table: add an entry to TABLES, then create a stage and raw table for it in Snowflake.
"""
import argparse
import io
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import pyarrow.parquet as pq
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from extract_claims import ZERO_WATERMARK, connect, rows_to_table

load_dotenv()

TABLES = {
    "claims":    {"table": "dbo.Claims",    "watermark_col": "LastActiveRowVersionNumber"},
    "providers": {"table": "dbo.Providers", "watermark_col": "RowVersionNumber"},
    "members":   {"table": "dbo.Members",   "watermark_col": "RowVersionNumber"},
}


def state_blob(name):
    return f"_state/{name}_watermark.json"


def read_watermark(container, name):
    try:
        return json.loads(container.download_blob(state_blob(name)).readall())["watermark"]
    except ResourceNotFoundError:
        return ZERO_WATERMARK


def write_watermark(container, name, watermark):
    payload = {"watermark": watermark, "updated_at": datetime.now(timezone.utc).isoformat()}
    container.upload_blob(state_blob(name), json.dumps(payload), overwrite=True)


def extract_table(conn, container, name, run_ts, run_id, full, chunk_rows):
    """Extract one table. Returns the number of rows written."""
    cfg = TABLES[name]
    wm_col = cfg["watermark_col"]
    watermark = ZERO_WATERMARK if full else read_watermark(container, name)
    print(f"[{name}] extracting rows with rowversion after {watermark}")

    query = (
        f"SELECT * FROM {cfg['table']} "
        f"WHERE {wm_col} > ? AND {wm_col} < MIN_ACTIVE_ROWVERSION() "
        f"ORDER BY {wm_col}"
    )
    prefix = f"{name}/load_date={run_ts:%Y-%m-%d}/{name}_{run_id}"
    cur = conn.cursor()
    cur.execute(query, bytes.fromhex(watermark))
    description = cur.description
    wm_index = [col[0] for col in description].index(wm_col)

    total, part, new_watermark = 0, 0, watermark
    while True:
        rows = cur.fetchmany(chunk_rows)
        if not rows:
            break
        part += 1
        buffer = io.BytesIO()
        pq.write_table(rows_to_table(description, rows), buffer, compression="snappy")
        buffer.seek(0)
        blob_name = f"{prefix}_part{part:04d}.parquet"
        container.upload_blob(blob_name, buffer, overwrite=False)
        total += len(rows)
        new_watermark = rows[-1][wm_index].hex()
        print(f"[{name}]   uploaded {blob_name} ({len(rows)} rows)")

    if total == 0:
        print(f"[{name}] no new or changed rows")
        return 0
    write_watermark(container, name, new_watermark)
    print(f"[{name}] done: {total} rows in {part} file(s), new watermark {new_watermark}")
    return total


def main():
    parser = argparse.ArgumentParser(description="Extract source tables to Azure Blob as Parquet")
    parser.add_argument("tables", nargs="*", help=f"any of: {', '.join(TABLES)} (default: all)")
    parser.add_argument("--full", action="store_true", help="ignore the saved watermark")
    parser.add_argument("--chunk-rows", type=int, default=50000, help="rows per Parquet file")
    args = parser.parse_args()

    names = args.tables or list(TABLES)
    unknown = [n for n in names if n not in TABLES]
    if unknown:
        raise SystemExit(f"Unknown table(s): {', '.join(unknown)}. Choose from: {', '.join(TABLES)}")

    conn_string = os.environ.get("AZ_STORAGE_CONNECTION_STRING")
    if not conn_string:
        raise SystemExit("Missing AZ_STORAGE_CONNECTION_STRING in .env")
    service = BlobServiceClient.from_connection_string(conn_string)
    container = service.get_container_client(os.environ.get("AZ_CONTAINER", "raw"))

    run_ts = datetime.now(timezone.utc)
    run_id = f"{run_ts:%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    failed = []

    with connect() as conn:
        for name in names:
            try:
                extract_table(conn, container, name, run_ts, run_id, args.full, args.chunk_rows)
            except Exception as exc:  # keep going so one bad table does not block the rest
                print(f"[{name}] FAILED: {exc}")
                failed.append(name)

    if failed:
        print(f"Failed tables: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
