"""Capture the full list of current primary keys from the source and write it to Azure Blob.

Why: a rowversion watermark load never sees deleted rows. Comparing the latest key list with
the warehouse is how deletes are detected (see the is_deleted flag in fct_claims).

Usage (venv active, project folder):
    python extract_keys.py                 # claims
    python extract_keys.py claims members  # several

RUN THIS AFTER extract_tables.py, never before. If keys were captured first, a row inserted in
between would be in the warehouse but missing from the key list and be flagged as deleted by
mistake. Captured afterwards, the worst case is a key whose row has not been extracted yet,
which is harmless.

Each run writes one file:  <name>_keys/captured_at=YYYYMMDDTHHMMSS/keys.parquet
with columns <key> (int) and captured_at (UTC timestamp). Older captures are kept; dbt only
reads the newest. At scale you would expire old captures with a storage lifecycle rule.

Safety: an EMPTY key list is refused. If the source query ever returned nothing because of an
outage or a permissions change, writing it would mark every row as deleted.
"""
import argparse
import io
import os
import sys
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from extract_claims import connect

load_dotenv()

# name -> (source table, key column). Snowflake and dbt are only set up for claims so far.
KEY_TABLES = {
    "claims":    ("dbo.Claims",    "ClaimID"),
    "providers": ("dbo.Providers", "ProviderID"),
    "members":   ("dbo.Members",   "MemberID"),
}


def capture(conn, container, name, captured_at):
    """Write the current key list for one table. Returns the number of keys."""
    table, key = KEY_TABLES[name]
    cur = conn.cursor()
    cur.execute(f"SELECT {key} FROM {table}")
    ids = []
    while True:
        rows = cur.fetchmany(100000)
        if not rows:
            break
        ids.extend(row[0] for row in rows)

    if not ids:
        raise RuntimeError(f"{table} returned no keys; refusing to write an empty key list")

    arrow_table = pa.table({
        key.lower(): pa.array(ids, type=pa.int64()),
        "captured_at": pa.array([captured_at] * len(ids), type=pa.timestamp("us")),
    })
    blob_name = f"{name}_keys/captured_at={captured_at:%Y%m%dT%H%M%S}/keys.parquet"
    buffer = io.BytesIO()
    pq.write_table(arrow_table, buffer, compression="snappy")
    buffer.seek(0)
    container.upload_blob(blob_name, buffer, overwrite=False)
    print(f"[{name}] captured {len(ids)} keys -> {blob_name}")
    return len(ids)


def main():
    parser = argparse.ArgumentParser(description="Capture current primary keys to Azure Blob")
    parser.add_argument("tables", nargs="*", help=f"any of: {', '.join(KEY_TABLES)} (default: claims)")
    args = parser.parse_args()

    names = args.tables or ["claims"]
    unknown = [n for n in names if n not in KEY_TABLES]
    if unknown:
        raise SystemExit(f"Unknown table(s): {', '.join(unknown)}. Choose from: {', '.join(KEY_TABLES)}")

    conn_string = os.environ.get("AZ_STORAGE_CONNECTION_STRING")
    if not conn_string:
        raise SystemExit("Missing AZ_STORAGE_CONNECTION_STRING in .env")
    service = BlobServiceClient.from_connection_string(conn_string)
    container = service.get_container_client(os.environ.get("AZ_CONTAINER", "raw"))

    captured_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    failed = []
    with connect() as conn:
        for name in names:
            try:
                capture(conn, container, name, captured_at)
            except Exception as exc:
                print(f"[{name}] FAILED: {exc}")
                failed.append(name)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
