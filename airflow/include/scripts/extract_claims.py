"""Incremental extract: Azure SQL dbo.Claims -> Parquet files in Azure Blob Storage.

Usage (venv active, run from the project folder):
    python extract_claims.py              # extract rows changed since the last run
    python extract_claims.py --full       # ignore the saved watermark, extract everything

.env needs (in addition to the AZSQL_* values you already have):
    AZ_STORAGE_CONNECTION_STRING=...      # storage account -> Access keys -> connection string
    AZ_CONTAINER=raw                      # optional, defaults to "raw"

Connects with the read-only pipeline_user (AZSQL_USER / AZSQL_PASSWORD).

How it works
  * Rows are selected by the table's rowversion column, which changes on every insert and
    update. The highest value read is saved as the "watermark" in the blob container
    (_state/claims_watermark.json), so the next run only picks up newer rows. Keeping the
    watermark in Blob (not on your laptop) means Airflow can run this from anywhere later.
  * The upper bound MIN_ACTIVE_ROWVERSION() skips rows from transactions that have not
    committed yet. Without it, a row that commits late could get a rowversion lower than
    the watermark and be missed forever.
  * Each chunk becomes one Parquet file under
        claims/load_date=YYYY-MM-DD/claims_<run timestamp>_<run id>_partNNNN.parquet
  * The watermark is only saved after every file has uploaded. If a run fails halfway, the
    next run re-extracts those rows, so raw can contain the same ClaimID more than once.
    Your dbt staging model should keep the latest row per ClaimID (by rowversion).

Known gap: deleted rows are invisible to a watermark load. Handle those separately
(for example, a periodic snapshot of all ClaimIDs compared against the warehouse).
"""
import argparse
import io
import json
import os
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

TABLE = "dbo.Claims"
WATERMARK_COL = "LastActiveRowVersionNumber"
BLOB_PREFIX = "claims"
STATE_BLOB = "_state/claims_watermark.json"
ZERO_WATERMARK = "0" * 16  # rowversion is 8 bytes = 16 hex characters

QUERY = f"""
SELECT * FROM {TABLE}
WHERE {WATERMARK_COL} > ?
  AND {WATERMARK_COL} < MIN_ACTIVE_ROWVERSION()
ORDER BY {WATERMARK_COL}
"""


# ---------------------------------------------------------------- source database
def connect(max_attempts=6, base_delay=10):
    """Connect with retries; a paused serverless DB needs time to wake."""
    required = ["AZSQL_SERVER", "AZSQL_DATABASE", "AZSQL_USER", "AZSQL_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"Missing values in .env: {', '.join(missing)}")

    password = os.environ["AZSQL_PASSWORD"].replace("}", "}}")
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=tcp:{os.environ['AZSQL_SERVER']},1433;"
        f"DATABASE={os.environ['AZSQL_DATABASE']};"
        f"UID={os.environ['AZSQL_USER']};"
        f"PWD={{{password}}};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    for attempt in range(1, max_attempts + 1):
        try:
            return pyodbc.connect(conn_str, timeout=60)
        except pyodbc.Error as exc:
            if attempt == max_attempts:
                raise
            wait = base_delay * attempt
            print(f"Connect attempt {attempt} failed ({str(exc)[:100]}...). Retrying in {wait}s")
            time.sleep(wait)


# ---------------------------------------------------------------- rows -> Parquet
def arrow_type(type_code, precision, scale):
    """Map the Python type pyodbc reports for a column to an explicit Arrow type.

    Explicit types matter: a column that is NULL for every row in a chunk would otherwise
    be written as Arrow's 'null' type, which loads badly into Snowflake.
    """
    if type_code is bool:
        return pa.bool_()
    if type_code is int:
        return pa.int64()
    if type_code is float:
        return pa.float64()
    if type_code is Decimal:
        return pa.decimal128(precision or 38, scale or 0)
    if type_code is datetime:
        return pa.timestamp("us")
    if type_code is date:
        return pa.date32()
    return pa.string()  # str, and bytes (rowversion), which is stored as a hex string


def rows_to_table(description, rows):
    columns = list(zip(*rows))
    arrays, fields = [], []
    for (name, type_code, _display, _internal, precision, scale, _null_ok), values in zip(description, columns):
        # Binary columns (the rowversion) are stored as hex strings. pyodbc reports their
        # type as bytearray but returns bytes, so convert by the value's own type.
        values = [v.hex() if isinstance(v, (bytes, bytearray)) else v for v in values]
        typ = arrow_type(type_code, precision, scale)
        arrays.append(pa.array(values, type=typ))
        fields.append(pa.field(name, typ))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


# ---------------------------------------------------------------- watermark state
def read_watermark(container):
    try:
        data = container.download_blob(STATE_BLOB).readall()
        return json.loads(data)["watermark"]
    except ResourceNotFoundError:
        return ZERO_WATERMARK


def write_watermark(container, watermark):
    payload = {"watermark": watermark, "updated_at": datetime.now(timezone.utc).isoformat()}
    container.upload_blob(STATE_BLOB, json.dumps(payload), overwrite=True)


# ---------------------------------------------------------------- main
def extract(args):
    conn_string = os.environ.get("AZ_STORAGE_CONNECTION_STRING")
    if not conn_string:
        raise SystemExit("Missing AZ_STORAGE_CONNECTION_STRING in .env")
    service = BlobServiceClient.from_connection_string(conn_string)
    container = service.get_container_client(os.environ.get("AZ_CONTAINER", "raw"))

    watermark = ZERO_WATERMARK if args.full else read_watermark(container)
    print(f"Extracting rows with rowversion after {watermark}")

    run_ts = datetime.now(timezone.utc)
    run_id = f"{run_ts:%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"  # unique even if two runs start in the same second
    prefix = f"{BLOB_PREFIX}/load_date={run_ts:%Y-%m-%d}/claims_{run_id}"
    total, part, new_watermark = 0, 0, watermark

    with connect() as conn:
        cur = conn.cursor()
        cur.execute(QUERY, bytes.fromhex(watermark))
        description = cur.description
        wm_index = [col[0] for col in description].index(WATERMARK_COL)

        while True:
            rows = cur.fetchmany(args.chunk_rows)
            if not rows:
                break
            part += 1
            table = rows_to_table(description, rows)

            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)
            blob_name = f"{prefix}_part{part:04d}.parquet"
            container.upload_blob(blob_name, buffer, overwrite=False)

            total += len(rows)
            new_watermark = rows[-1][wm_index].hex()
            print(f"  uploaded {blob_name} ({len(rows)} rows)")

    if total == 0:
        print("No new or changed rows.")
        return

    write_watermark(container, new_watermark)
    print(f"Done: {total} rows in {part} file(s). New watermark {new_watermark}")


def main():
    parser = argparse.ArgumentParser(description="Extract dbo.Claims to Azure Blob as Parquet")
    parser.add_argument("--full", action="store_true", help="ignore the saved watermark")
    parser.add_argument("--chunk-rows", type=int, default=50000, help="rows per Parquet file")
    extract(parser.parse_args())


if __name__ == "__main__":
    main()
