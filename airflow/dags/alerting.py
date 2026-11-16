"""Slack alerting for the claims pipeline.

Lives in the dags folder so every DAG can `from alerting import ...` (Airflow puts the dags folder on the
Python path). It defines no DAG itself.

The webhook URL comes from the SLACK_WEBHOOK_URL environment variable (airflow\\.env). If it's missing,
alerts are printed to the task log instead of failing anything: alerting must never break the pipeline.
"""
import json
import os
import urllib.request


def send_slack(text):
    """Post a message to Slack. Returns True if it was sent."""
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        print(f"[alerting] SLACK_WEBHOOK_URL not set; would have sent:\n{text}")
        return False
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status == 200
    except Exception as exc:  # never let an alerting problem mask the real failure
        print(f"[alerting] could not reach Slack: {exc}")
        return False


def task_failure_alert(context):
    """on_failure_callback: runs once, after a task has used up all its retries."""
    ti = context.get("ti") or context.get("task_instance")
    dag_id = getattr(ti, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    run_id = getattr(ti, "run_id", None) or getattr(context.get("dag_run"), "run_id", "?")
    tries = getattr(ti, "try_number", "?")
    error = str(context.get("exception") or "see the task log")
    if len(error) > 500:
        error = error[:500] + "..."
    lines = [
        f":red_circle: *{dag_id}* failed at task `{task_id}`",
        f"Run: `{run_id}` (attempt {tries})",
        f"Error: {error}",
    ]
    log_url = getattr(ti, "log_url", None)
    if log_url:
        lines.append(f"Log: {log_url}")
    send_slack("\n".join(lines))


def parse_dbt_warnings(run_results_path, sources_path):
    """Read dbt's artifacts and return a list of human-readable warnings."""
    warnings = []
    if os.path.exists(run_results_path):
        with open(run_results_path, encoding="utf-8") as f:
            for result in json.load(f).get("results", []):
                if result.get("status") == "warn":
                    name = result.get("unique_id", "?").split(".")[-1]
                    detail = result.get("message") or f"{result.get('failures', '?')} row(s)"
                    warnings.append(f"dbt test `{name}`: {detail}")
    if os.path.exists(sources_path):
        with open(sources_path, encoding="utf-8") as f:
            for result in json.load(f).get("results", []):
                if result.get("status") == "warn":
                    name = ".".join(result.get("unique_id", "?").split(".")[-2:])
                    age_h = (result.get("max_loaded_at_time_ago_in_s") or 0) / 3600
                    warnings.append(f"freshness `{name}`: last load {age_h:.1f} hours ago")
    return warnings


def parse_schema_warnings(xcom_line):
    """check_schema prints a last line 'SCHEMA_WARNINGS: [...]', which Airflow keeps as the task's XCom."""
    prefix = "SCHEMA_WARNINGS: "
    if not xcom_line or not str(xcom_line).startswith(prefix):
        return []
    try:
        return [f"schema check: {w}" for w in json.loads(str(xcom_line)[len(prefix):])]
    except ValueError:
        return []


def warnings_message(dag_id, run_id, warnings):
    lines = [f":large_yellow_circle: *{dag_id}* succeeded with {len(warnings)} warning(s)", f"Run: `{run_id}`"]
    lines += [f"• {w}" for w in warnings[:20]]
    if len(warnings) > 20:
        lines.append(f"...and {len(warnings) - 20} more")
    return "\n".join(lines)
