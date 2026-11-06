"""Claims pipeline: Azure SQL -> Blob (Parquet) -> Snowflake RAW -> dbt.

Task order (each step is safe to rerun, which is what makes retries safe):
    extract_rows   new and changed rows since each table's watermark   (extract_tables.py)
    extract_keys   full list of current claim IDs, AFTER the rows       (extract_keys.py)
    load_raw       COPY INTO the RAW tables; already-loaded files skip  (include/sql/load_raw.sql)
    dbt_build      snapshots, models and tests in dependency order      (dbt build)

Credentials come from environment variables set in the Astro project's .env file.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

AIRFLOW_HOME = Path("/usr/local/airflow")
SCRIPTS = AIRFLOW_HOME / "include" / "scripts"
LOAD_SQL = AIRFLOW_HOME / "include" / "sql" / "load_raw.sql"
DBT_DIR = AIRFLOW_HOME / "include" / "dbt"
DBT_PROJECT = DBT_DIR / "claims_dbt"
DBT = AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"

default_args = {
    "owner": "ankit",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="claims_pipeline",
    schedule=None,
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

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def load_raw():
        """Run the COPY INTO statements. Snowflake skips files it already loaded."""
        import snowflake.connector

        sql = LOAD_SQL.read_text(encoding="utf-8")
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
                for row in cursor:
                    print(row)   # COPY results and the final row counts appear in the task log
        finally:
            conn.close()

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_PROJECT} && {DBT} build --profiles-dir {DBT_DIR}",
        retries=0,   # a failing test should stop the run and be looked at, not retried
    )

    extract_rows >> extract_keys >> load_raw() >> dbt_build


claims_pipeline()
