"""Schema-change check: compare source table columns (Azure SQL) with raw table columns (Snowflake).

Runs in the DAG AFTER the extracts and BEFORE load_raw:
  * A column in the source but NOT in the raw table -> FAIL (exit 1).
    COPY INTO ... MATCH_BY_COLUMN_NAME would silently drop it. Failing here leaves the new Parquet
    files unloaded in Blob. Fix: ALTER TABLE CLAIMS_DWH.RAW.<TABLE> ADD COLUMN <name> <type>;
    then rerun from load_raw: COPY INTO loads the waiting files WITH the new column. Nothing is lost.
  * A column in the raw table but no longer in the source -> WARNING only.
    COPY INTO fills it with NULL. Check whether any dbt model still relies on it.
  * Type changes are not compared: SQL Server and Snowflake type names differ, and a real type
    conflict makes COPY INTO fail loudly anyway.

Pipeline-only columns in raw (_SOURCE_FILE, _FILE_ROW, _LOADED_AT) are ignored.

Usage:  python check_schema.py            (all tables in extract_tables.TABLES)
        python check_schema.py claims     (selected tables)
Needs the AZSQL_* and SNOWFLAKE_* environment variables (Airflow's .env has both).
"""
import argparse
import os
import sys

from dotenv import load_dotenv

load_dotenv()

# source table -> raw table, same names as the extract uses
TABLES = {
    "claims":    ("Claims",    "CLAIMS"),
    "providers": ("Providers", "PROVIDERS"),
    "members":   ("Members",   "MEMBERS"),
}
PIPELINE_COLUMNS = {"_SOURCE_FILE", "_FILE_ROW", "_LOADED_AT"}


def compare_columns(source_columns, raw_columns):
    """Return (added, removed): column names in the source but not raw, and in raw but not the source.

    Names are compared case-insensitively (Snowflake stores them upper case); pipeline columns are ignored.
    """
    source = {c.upper() for c in source_columns}
    raw = {c.upper() for c in raw_columns} - PIPELINE_COLUMNS
    return sorted(source - raw), sorted(raw - source)


def source_columns(azure_conn, table):
    cur = azure_conn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?",
        table,
    )
    return [row[0] for row in cur.fetchall()]


def raw_columns(snowflake_conn, table):
    cur = snowflake_conn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME FROM CLAIMS_DWH.INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = %s",
        (table,),
    )
    return [row[0] for row in cur.fetchall()]


def snowflake_connect():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="CLAIMS_DWH",
    )


def check(names, azure_conn, snowflake_conn):
    """Print a report for each table. Returns True if the load can go ahead."""
    ok = True
    for name in names:
        src_table, raw_table = TABLES[name]
        src = source_columns(azure_conn, src_table)
        raw = raw_columns(snowflake_conn, raw_table)
        if not src:
            print(f"[{name}] FAIL: dbo.{src_table} has no visible columns (missing table or no permission)")
            ok = False
            continue
        if not raw:
            print(f"[{name}] FAIL: RAW.{raw_table} not found in Snowflake")
            ok = False
            continue
        added, removed = compare_columns(src, raw)
        if added:
            ok = False
            print(f"[{name}] FAIL: new source column(s) missing from RAW.{raw_table}: {', '.join(added)}")
            for col in added:
                print(f"         fix: ALTER TABLE CLAIMS_DWH.RAW.{raw_table} ADD COLUMN {col} <type>;")
        if removed:
            print(f"[{name}] WARNING: column(s) no longer in dbo.{src_table}, will load as NULL: {', '.join(removed)}")
        if not added and not removed:
            print(f"[{name}] OK: {len(src)} source columns all present in RAW.{raw_table}")
    return ok


def main():
    parser = argparse.ArgumentParser(description="Compare source and raw table columns before loading")
    parser.add_argument("tables", nargs="*", help=f"any of: {', '.join(TABLES)} (default: all)")
    args = parser.parse_args()
    names = args.tables or list(TABLES)
    unknown = [n for n in names if n not in TABLES]
    if unknown:
        raise SystemExit(f"Unknown table(s): {', '.join(unknown)}. Choose from: {', '.join(TABLES)}")

    from extract_claims import connect   # Azure SQL connection with wake-up retries
    with connect() as azure_conn:
        sf = snowflake_connect()
        try:
            ok = check(names, azure_conn, sf)
        finally:
            sf.close()
    if not ok:
        print("Schema check failed: load_raw will not run. New files stay in Blob until the raw table is fixed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
