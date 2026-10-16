"""
Option 2 demo: DAG-level Deadline Alerts with AVERAGE_RUNTIME (Airflow 3.2+).

This is the "complementary, zero-infrastructure" DAG-level option. Instead of a
separate monitor DAG polling the REST API,
Airflow itself watches each run and fires a callback when the run exceeds its
own historical average duration plus a grace interval.

How the demo proves it works
----------------------------
AVERAGE_RUNTIME needs history: it only arms a deadline once at least
``min_runs`` completed runs exist. So you run the DAG in two phases:

  Phase 1 - BASELINE: leave the Airflow Variable ``deadline_demo_slow`` unset
            (or "false"). Each run sleeps ~30s. Trigger it min_runs times
            (default 5 here) to build a ~30s historical average.

  Phase 2 - BREACH:   set Variable ``deadline_demo_slow`` = "true". The task now
            sleeps 10 minutes. Average (~30s) + interval (2 min) is ~2.5 min, so
            well before the task finishes the scheduler fires the deadline
            callback and you get the alert — while the task keeps running
            uninterrupted (unlike execution_timeout, which would kill it).

Tunables (via Airflow Variables, all optional):
  deadline_demo_slow        "true"/"false"  - switch between baseline and breach
  deadline_demo_fast_sec    int seconds     - baseline sleep (default 30)
  deadline_demo_slow_sec    int seconds     - breach sleep   (default 600)
  deadline_demo_interval_min int minutes    - grace on top of average (default 2)

Requires Apache Airflow >= 3.2 (MWAA 3.2.1+). The Deadline Alerts feature is
experimental and its API may change in future Airflow releases.
"""

from __future__ import annotations

import time
from datetime import timedelta

from airflow.sdk import (
    DAG,
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
    Variable,
)
from airflow.providers.standard.operators.python import PythonOperator

# --- tunables -------------------------------------------------------------
FAST_SECONDS = int(Variable.get("deadline_demo_fast_sec", default=30))
SLOW_SECONDS = int(Variable.get("deadline_demo_slow_sec", default=600))
INTERVAL_MIN = int(Variable.get("deadline_demo_interval_min", default=2))
MIN_RUNS = 5
MAX_RUNS = 10


def _do_work(**_: object) -> None:
    """Sleep for a fast or slow duration depending on the demo switch."""
    slow = str(Variable.get("deadline_demo_slow", default="false")).lower() == "true"
    seconds = SLOW_SECONDS if slow else FAST_SECONDS
    mode = "SLOW (expected to breach deadline)" if slow else "FAST (baseline)"
    print(f"Running in {mode} mode — sleeping {seconds}s to simulate work.")
    time.sleep(seconds)
    print("Work complete.")


with DAG(
    dag_id="sla_deadline_average_runtime_demo",
    description="Option 2: DAG-level deadline alert using AVERAGE_RUNTIME",
    schedule=None,  # manual trigger — you drive the baseline/breach phases
    catchup=False,
    tags=["sla", "deadline-alerts", "mwaa-demo"],
    # The deadline is: run_start + historical_average_runtime + interval.
    # If fewer than MIN_RUNS completed runs exist, NO deadline is armed
    # (that is expected during Phase 1 — build history first).
    deadline=DeadlineAlert(
        reference=DeadlineReference.AVERAGE_RUNTIME(
            max_runs=MAX_RUNS,
            min_runs=MIN_RUNS,
        ),
        interval=timedelta(minutes=INTERVAL_MIN),
        callback=AsyncCallback(
            # Resolve by dotted path so the Triggerer imports it from plugins/.
            "deadline_callbacks.notify_sla_breach_async",
            kwargs={
                "severity": "high",
                "alert_type": "average_runtime_exceeded",
            },
        ),
    ),
):
    PythonOperator(
        task_id="simulated_work",
        python_callable=_do_work,
    )
