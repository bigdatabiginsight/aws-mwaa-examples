"""
Option 1 demo: external monitor DAG that polls the Airflow REST API for
long-running tasks (task-level SLA alerting on MWAA, Airflow 3.x).

This is the "primary" task-level workaround. A separate,
frequently scheduled DAG asks the Airflow REST API which task instances are
currently *running*, works out how long each has been running, and raises an
alert for any task that has exceeded its threshold. The monitored tasks keep
running uninterrupted — this is detection/alerting, not a kill switch.

Why this and not direct metadata-DB access?
    On Airflow 3.x, tasks may no longer read the metadata DB directly, so the
    REST API is the supported way to introspect other DAGs/tasks.

How it talks to the REST API (MWAA)
    It uses the MWAA control-plane call ``mwaa:InvokeRestApi`` via boto3, which
    is authenticated with AWS credentials (SigV4/IAM) — no webserver token to
    manage, and the webserver stays private. The MWAA execution role needs:
        airflow:InvokeRestApi  (and airflow:CreateWebLoginToken)
    on arn:aws:airflow:<region>:<acct>:role/<env-name>/<airflow-role>.

    Note the InvokeRestApi limits: 10s per call and 6MB response. We keep the
    response tiny by asking the API to pre-filter to only tasks that have
    ALREADY breached (state=running AND start_date <= now - threshold).

Config (Airflow Variables, all optional except the env name on real MWAA):
    sla_monitor_env_name        MWAA environment name (required on MWAA)
    sla_monitor_threshold_min   minutes a task may run before it's flagged (default 15)
    sla_monitor_dag_pattern     only watch DAGs whose id matches this substring (default "" = all)
    sla_monitor_exclude_self    "true"/"false" — skip the monitor's own DAG (default "true")

Per-DAG / per-task overrides:
    Set Variable ``sla_monitor_thresholds`` to a JSON object mapping
    "dag_id" or "dag_id.task_id" to a minute threshold, e.g.:
        {"critical_etl": 10, "critical_etl.load_step": 5}
    Anything not listed falls back to sla_monitor_threshold_min.

Schedule it every few minutes (default */5). Requires Airflow >= 3 on MWAA.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

# Shared alert sink (SNS or log) from the plugins folder.
from deadline_callbacks import send_sla_alert

log = logging.getLogger("mwaa.sla_monitor")

MONITOR_DAG_ID = "sla_rest_api_monitor"
RUNNING_STATES = ["running"]
# Safety cap so a single InvokeRestApi response stays well under the 6MB limit.
PAGE_LIMIT = 100


def _get_var(key: str, default):
    # Airflow 3 Task SDK (airflow.sdk) uses `default`, not the legacy `default_var`.
    return Variable.get(key, default=default)


def _threshold_for(dag_id: str, task_id: str, default_min: float, overrides: dict) -> float:
    """Resolve the most specific threshold: dag_id.task_id > dag_id > global."""
    if f"{dag_id}.{task_id}" in overrides:
        return float(overrides[f"{dag_id}.{task_id}"])
    if dag_id in overrides:
        return float(overrides[dag_id])
    return default_min


def _call_rest_api(path: str, query: dict) -> dict:
    """Call the Airflow REST API through MWAA's InvokeRestApi (IAM auth)."""
    import boto3

    env_name = _get_var("sla_monitor_env_name", "")
    if not env_name:
        raise ValueError(
            "Airflow Variable 'sla_monitor_env_name' is not set. On MWAA this "
            "must be your environment name so InvokeRestApi can reach the API."
        )
    client = boto3.client("mwaa")
    resp = client.invoke_rest_api(
        Name=env_name,
        Path=path,
        Method="GET",
        QueryParameters=query,
    )
    return resp["RestApiResponse"]


def find_breaching_tasks(**_) -> None:
    """Query running task instances and alert on any over their threshold."""
    now = datetime.now(timezone.utc)
    default_min = float(_get_var("sla_monitor_threshold_min", 15))
    dag_pattern = str(_get_var("sla_monitor_dag_pattern", ""))
    exclude_self = str(_get_var("sla_monitor_exclude_self", "true")).lower() == "true"
    try:
        overrides = json.loads(_get_var("sla_monitor_thresholds", "{}")) or {}
    except (TypeError, ValueError):
        log.warning("sla_monitor_thresholds is not valid JSON; ignoring overrides.")
        overrides = {}

    # Server-side pre-filter to the GLOBAL default threshold so the response is
    # small. Per-task overrides tighter than the global are re-checked below;
    # if an override is LOOSER than the global we'd otherwise over-alert, so we
    # filter at the smallest threshold in play.
    smallest_min = min([default_min, *[float(v) for v in overrides.values()]]) if overrides else default_min
    cutoff = now - timedelta(minutes=smallest_min)

    # Wildcards ("~") list task instances across ALL dags and dag runs.
    query = {
        "state": RUNNING_STATES,
        "start_date_lte": cutoff.isoformat(),
        "limit": PAGE_LIMIT,
        "order_by": "start_date",
    }
    if dag_pattern:
        query["dag_id_pattern"] = dag_pattern

    breaches = []
    offset = 0
    while True:
        query["offset"] = offset
        payload = _call_rest_api("/dags/~/dagRuns/~/taskInstances", query)
        tis = payload.get("task_instances", []) if isinstance(payload, dict) else []
        if not tis:
            break
        for ti in tis:
            dag_id = ti.get("dag_id", "")
            task_id = ti.get("task_id", "")
            if exclude_self and dag_id == MONITOR_DAG_ID:
                continue
            start = ti.get("start_date")
            if not start:
                continue
            started = datetime.fromisoformat(start.replace("Z", "+00:00"))
            running_min = (now - started).total_seconds() / 60.0
            threshold = _threshold_for(dag_id, task_id, default_min, overrides)
            if running_min >= threshold:
                breaches.append(
                    {
                        "dag_id": dag_id,
                        "task_id": task_id,
                        "run_id": ti.get("dag_run_id", ""),
                        "running_min": round(running_min, 1),
                        "threshold_min": threshold,
                        "started": start,
                        "hostname": ti.get("hostname", ""),
                    }
                )
        if len(tis) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT

    if not breaches:
        log.info("SLA monitor: no running tasks over threshold.")
        return

    lines = [
        f"  • {b['dag_id']}.{b['task_id']} (run {b['run_id']}) — running "
        f"{b['running_min']} min, threshold {b['threshold_min']} min, "
        f"started {b['started']}"
        for b in breaches
    ]
    message = (
        f"MWAA task-level SLA breach: {len(breaches)} task(s) running longer "
        f"than their threshold as of {now.isoformat()}:\n" + "\n".join(lines)
    )
    log.warning(message)
    send_sla_alert(message, subject=f"MWAA SLA breach: {len(breaches)} task(s) long-running")


with DAG(
    dag_id=MONITOR_DAG_ID,
    description="Option 1: REST API monitor for long-running tasks (task-level SLA alerting)",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # never overlap monitor runs
    tags=["sla", "rest-api-monitor", "mwaa-demo"],
):
    PythonOperator(
        task_id="find_breaching_tasks",
        python_callable=find_breaching_tasks,
    )
