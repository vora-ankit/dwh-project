"""Claims pipeline: Azure SQL -> Blob (Parquet) -> Snowflake RAW -> dbt.

Task order (each step is safe to rerun, which is what makes retries safe):
    extract_rows       new and changed rows since each table's watermark   (extract_tables.py)
    extract_keys       full list of current claim IDs, AFTER the rows       (extract_keys.py)
    check_schema       stop before loading if the source gained a column    (check_schema.py)
    load_raw           COPY INTO the RAW tables; already-loaded files skip  (include/sql/load_raw.sql)
    source_freshness   did the load land? claim_keys must be recent         (dbt source freshness)
    dbt_build          snapshots, models and tests in dependency order      (dbt build)
    prune_claim_keys   keep 7 days of key captures in RAW.CLAIM_KEYS        (include/sql/prune_claim_keys.sql)

Credentials come from environment variables set in the Astro project's .env file.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

AIRFLOW_HOME = Path("/usr/local/airflow")
SCRIPTS = AIRFLOW_HOME / "include" / "scripts"
SQL_DIR = AIRFLOW_HOME / "include" / "sql"
DBT_DIR = AIRFLOW_HOME / "include" / "dbt"
DBT_PROJECT = DBT_DIR / "claims_dbt"
DBT = AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"

default_args = {
    "owner": "ankit",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def run_snowflake_sql(sql_file):
    """Run every statement in a .sql file against Snowflake and print the results to the task log."""
    import snowflake.connector

    sql = (SQL_DIR / sql_file).read_text(encoding="utf-8")
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="CLAIMS_DWH",
        schema="RAW",
    )
    try:
        for cursor in conn.execute_string(sql):
            print(f"statement affected {cursor.rowcount} row(s)")
            for row in cursor:
                print(row)
    finally:
        conn.close()


@dag(
    dag_id="claims_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,          # do not run once for every missed day since start_date
    max_active_runs=1,      # two overlapping runs would race on the same watermarks
    default_args=default_args,
    tags=["claims"],
)
def claims_pipeline():
    # The serverless Azure SQL database may be paused. The scripts already retry the
    # connection; these task retries cover anything longer, such as a slow resume.
    extract_rows = BashOperator(
        task_id="extract_rows",
        bash_command=f"cd {SCRIPTS} && python extract_tables.py",
        retries=3,
        retry_delay=timedelta(minutes=3),
    )

    extract_keys = BashOperator(
        task_id="extract_keys",
        bash_command=f"cd {SCRIPTS} && python extract_keys.py",
        retries=3,
        retry_delay=timedelta(minutes=3),
    )

    # A new source column would be silently dropped by COPY INTO. Failing here leaves the new files
    # unloaded in Blob; after ALTER TABLE ... ADD COLUMN, clearing this task loads them with the column.
    check_schema = BashOperator(
        task_id="check_schema",
        bash_command=f"cd {SCRIPTS} && python check_schema.py",
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def load_raw():
        """COPY INTO the raw tables. Snowflake skips files it already loaded."""
        run_snowflake_sql("load_raw.sql")

    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"cd {DBT_PROJECT} && {DBT} source freshness --profiles-dir {DBT_DIR}",
        retries=0,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT} && {DBT} build --profiles-dir {DBT_DIR}",
        retries=0,   # a failing test should stop the run and be looked at, not retried
    )

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def prune_claim_keys():
        """Keep 7 days of key captures. Runs only after a successful dbt_build."""
        run_snowflake_sql("prune_claim_keys.sql")

    (extract_rows >> extract_keys >> check_schema >> load_raw()
     >> source_freshness >> dbt_build >> prune_claim_keys())


claims_pipeline()
