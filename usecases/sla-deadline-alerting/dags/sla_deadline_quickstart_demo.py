"""
Quick-start deadline alert — fires on the FIRST run (no history needed).

AVERAGE_RUNTIME needs several completed runs before it can arm a deadline. This
companion DAG uses DeadlineReference.DAGRUN_QUEUED_AT so you can demonstrate a
deadline breach immediately — handy for a live walkthrough before the
AVERAGE_RUNTIME baseline has been built up.

Behaviour: the task sleeps 3 minutes but the deadline is 1 minute after the run
is queued, so the alert fires ~2 minutes before the task finishes. The task is
NOT killed — it runs to completion, proving this is a soft SLA alert rather than
a hard timeout.

Requires Apache Airflow >= 3.2 (MWAA 3.2.1+).
"""

from __future__ import annotations

import time
from datetime import timedelta

from airflow.sdk import (
    DAG,
    SyncCallback,
    DeadlineAlert,
    DeadlineReference,
)
from airflow.providers.standard.operators.python import PythonOperator

DEADLINE_MINUTES = 1
WORK_MINUTES = 3


def _slow_work(**_: object) -> None:
    print(f"Sleeping {WORK_MINUTES} min; deadline is {DEADLINE_MINUTES} min after queue.")
    time.sleep(WORK_MINUTES * 60)
    print("Work complete (deadline alert should already have fired).")


with DAG(
    dag_id="sla_deadline_quickstart_demo",
    description="Option 2: instant deadline alert (DAGRUN_QUEUED_AT)",
    schedule=None,
    catchup=False,
    tags=["sla", "deadline-alerts", "mwaa-demo"],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=DEADLINE_MINUTES),
        # Sync callback runs on the worker/executor — no Triggerer restart needed.
        callback=SyncCallback(
            "deadline_callbacks.notify_sla_breach_sync",
            kwargs={
                "severity": "medium",
                "alert_type": "queued_deadline_exceeded",
            },
        ),
    ),
):
    PythonOperator(
        task_id="simulated_work",
        python_callable=_slow_work,
    )
