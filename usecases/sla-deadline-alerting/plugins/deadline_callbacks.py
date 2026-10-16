"""
Custom deadline-alert callbacks for the Airflow 3.2 "Deadline Alerts" demo.

This file MUST live in the Airflow ``plugins/`` folder so that:
  * Async callbacks are importable by the **Triggerer** (restart the Triggerer
    after adding/changing this file).
  * Sync callbacks are importable by the **worker/executor**.

Nested callables are NOT supported — every callback here is a top-level
function. See:
https://airflow.apache.org/docs/apache-airflow/3.2.2/howto/deadline-alerts.html

Each callback receives Airflow's simplified deadline context under
``kwargs["context"]`` plus any kwargs you set on the Callback object. We keep
the external dependency surface minimal: SNS publishing uses boto3 (already
present on MWAA workers), and everything degrades to a log line if the optional
target isn't configured, so the demo runs even without Slack/SNS wired up.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

log = logging.getLogger("mwaa.deadline_alerts")


def _extract(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Pull the useful bits out of the deadline callback context.

    The context Airflow passes to a deadline callback is a *simplified* version
    of the task context (see the "Templating and Context" section of the docs):
    it carries the Dag Run REST payload plus the computed deadline time. We read
    defensively because the exact shape is still experimental in 3.2.
    """
    context = kwargs.get("context", {}) or {}
    dag_run = context.get("dag_run", {}) or {}
    deadline = context.get("deadline", {}) or {}

    return {
        "dag_id": dag_run.get("dag_id") or context.get("dag_id", "<unknown>"),
        "run_id": dag_run.get("run_id") or context.get("run_id", "<unknown>"),
        "logical_date": str(dag_run.get("logical_date", "")),
        "state": dag_run.get("state", ""),
        "deadline_time": str(
            deadline.get("deadline_time") or context.get("deadline_time", "")
        ),
        "severity": kwargs.get("severity", "warning"),
        "alert_type": kwargs.get("alert_type", "deadline_exceeded"),
    }


def _build_message(info: dict[str, Any]) -> str:
    return (
        f"[{info['severity'].upper()}] MWAA SLA/deadline breach\n"
        f"  DAG:           {info['dag_id']}\n"
        f"  Run:           {info['run_id']}\n"
        f"  Logical date:  {info['logical_date']}\n"
        f"  Deadline time: {info['deadline_time']}\n"
        f"  Alert type:    {info['alert_type']}\n"
        f"  State at alert:{info['state']}\n"
        f"The DAG run exceeded its historical AVERAGE_RUNTIME (+ interval) and "
        f"has not finished. Investigate before the business SLA is missed."
    )


def _publish_sns(message: str, subject: str) -> bool:
    """Publish to SNS if DEADLINE_ALERT_SNS_TOPIC_ARN is set. Returns True on send."""
    topic_arn = os.environ.get("DEADLINE_ALERT_SNS_TOPIC_ARN")
    if not topic_arn:
        return False
    try:
        import boto3  # available on MWAA workers

        boto3.client("sns").publish(
            TopicArn=topic_arn,
            Subject=subject[:100],  # SNS subject hard limit
            Message=message,
        )
        log.info("Deadline alert published to SNS topic %s", topic_arn)
        return True
    except Exception:  # noqa: BLE001 - never let alerting break the scheduler loop
        log.exception("Failed to publish deadline alert to SNS")
        return False


def notify_sla_breach_sync(**kwargs: Any) -> None:
    """Synchronous deadline callback — runs on the worker/executor.

    Wire this with ``SyncCallback(notify_sla_breach_sync, kwargs={...})``.
    Sends to SNS when DEADLINE_ALERT_SNS_TOPIC_ARN is configured, otherwise logs.
    """
    info = _extract(kwargs)
    message = _build_message(info)
    subject = f"MWAA deadline breach: {info['dag_id']}"

    log.warning("DEADLINE ALERT (sync)\n%s", message)
    if not _publish_sns(message, subject):
        log.warning(
            "DEADLINE_ALERT_SNS_TOPIC_ARN not set — alert logged only. "
            "Context: %s",
            json.dumps(info),
        )


async def notify_sla_breach_async(**kwargs: Any) -> None:
    """Asynchronous deadline callback — runs in the Triggerer.

    Wire this with ``AsyncCallback(notify_sla_breach_async, kwargs={...})``.
    The Triggerer must be restarted whenever this file changes.
    """
    info = _extract(kwargs)
    message = _build_message(info)
    subject = f"MWAA deadline breach: {info['dag_id']}"

    log.warning("DEADLINE ALERT (async)\n%s", message)
    # boto3 is synchronous; for a demo a direct call from the Triggerer is fine.
    # In production prefer an async SNS client or hand off to a queue.
    if not _publish_sns(message, subject):
        log.warning(
            "DEADLINE_ALERT_SNS_TOPIC_ARN not set — alert logged only. "
            "Context: %s",
            json.dumps(info),
        )


# ---------------------------------------------------------------------------
# Shared alert entry point — used by the Option 1 monitor DAG (REST API poller)
# so both options route alerts through the same SNS/log path.
# ---------------------------------------------------------------------------
def send_sla_alert(message: str, subject: str = "MWAA SLA breach") -> None:
    """Send a prebuilt alert message to SNS if configured, otherwise log it.

    This is a plain function (not a deadline callback) that the monitor DAG
    calls directly once it has detected one or more long-running tasks.
    """
    log.warning("SLA ALERT\n%s", message)
    if not _publish_sns(message, subject):
        log.warning(
            "DEADLINE_ALERT_SNS_TOPIC_ARN not set — alert logged only."
        )
