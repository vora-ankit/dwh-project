"""Independent morning check: did the claims pipeline actually run?

The source_freshness task inside claims_pipeline can't notice the pipeline NOT running, because then it
doesn't run either. This separate DAG runs `dbt source freshness` on its own schedule. claim_keys gets a new
capture on every pipeline run and errors after 30 hours, so if the nightly run didn't happen (or didn't
load), this task fails and the failure callback posts to Slack.

Schedule: 12:00 UTC (7:00 New York in winter, 8:00 in summer), well after the nightly 00:00 UTC run.
Pause this DAG whenever you pause claims_pipeline, or it will (correctly) alert every morning.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from alerting import task_failure_alert

DBT_DIR = Path("/usr/local/airflow/include/dbt")
DBT_PROJECT = DBT_DIR / "claims_dbt"
DBT = Path("/usr/local/airflow/dbt_venv/bin/dbt")


@dag(
    dag_id="claims_freshness_monitor",
    schedule="0 12 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "ankit", "retries": 1, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": task_failure_alert},
    tags=["claims", "monitoring"],
)
def claims_freshness_monitor():
    BashOperator(
        task_id="check_raw_freshness",
        bash_command=f"cd {DBT_PROJECT} && {DBT} source freshness --profiles-dir {DBT_DIR}",
    )


claims_freshness_monitor()
