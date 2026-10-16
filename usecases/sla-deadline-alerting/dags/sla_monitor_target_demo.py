"""
Target DAG for the Option 1 REST API monitor demo.

Trigger this DAG, then let `sla_rest_api_monitor` run on its 5-minute schedule.
The `long_running_step` task sleeps longer than the monitor's threshold, so the
monitor will detect it as a running task over its SLA and raise an alert — while
this task keeps running to completion (soft alert, not a kill).

Tunable: Airflow Variable `sla_target_sleep_min` (default 20 minutes). Make sure
it is comfortably larger than `sla_monitor_threshold_min` (default 15) so the
monitor has a window to catch it between its scheduled runs.
"""

from __future__ import annotations

import time
from datetime import timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

SLEEP_MIN = float(Variable.get("sla_target_sleep_min", default=20))


def _long_running(**_) -> None:
    seconds = int(SLEEP_MIN * 60)
    print(f"Target task sleeping {SLEEP_MIN} min ({seconds}s) to exceed the SLA threshold.")
    time.sleep(seconds)
    print("Target task finished.")


with DAG(
    dag_id="sla_monitor_target_demo",
    description="Long-running DAG for the Option 1 REST API monitor to catch",
    schedule=None,  # trigger manually for the demo
    catchup=False,
    tags=["sla", "rest-api-monitor", "mwaa-demo"],
    # Per-task override example the monitor will honour via sla_monitor_thresholds.
    default_args={"retries": 0},
):
    PythonOperator(
        task_id="long_running_step",
        python_callable=_long_running,
        execution_timeout=timedelta(hours=2),  # generous; we are NOT relying on timeout
    )
