# MWAA SLA Alerting — Option 1 & Option 2 Demos

> ⚠️ **Illustrative example — not production-ready.** This code is a
> proof-of-concept to demonstrate two patterns for SLA/deadline alerting on
> Amazon MWAA. It is provided as-is to illustrate the approach; review, harden,
> and test it against your own requirements before any production use. Thresholds
> and intervals are tuned short for fast demos, not real SLAs.

Two approaches to alerting when an Airflow task or DAG runs longer than expected
on MWAA:

- **Option 1 — REST API monitor DAG (task-level).** A separate scheduled DAG
  polls the Airflow REST API for running tasks, works out how long each has
  been running, and alerts on any over its threshold. Needs no new feature;
  gives **task-level** granularity.
- **Option 2 — native Deadline Alerts with `AVERAGE_RUNTIME` (DAG-level).**
  Airflow 3.2's built-in feature; alerts when a run exceeds its own historical
  average duration. **Zero extra infrastructure**, but **DAG-run** granularity.

Both are stop-gaps until native **task-level** deadline alerting
(Apache Airflow roadmap item **AIP-86**) ships. In neither case is the running
task killed — these are *soft* SLA alerts. See
[**"Which option should I use?"**](#which-option-should-i-use) below.

> **Requires Apache Airflow 3.x on MWAA.** Option 2 specifically needs
> **Airflow 3.2.1 or later**. Validated on Airflow 3.2.1. Deadline Alerts are
> flagged **experimental** by Apache; the API may change in a future Airflow
> release.
>
> **Note on the `AVERAGE_RUNTIME` variant:** the rolling average can be skewed
> by a single outlier run, which may drift the alert threshold over time. Treat
> the average-runtime logic as needing further validation for your workloads
> (e.g. seeding a representative baseline, handling outliers) before relying on
> it. The fixed-reference deadline (quickstart) and the Option 1 monitor are the
> more deterministic choices.

---

## What's in here

```
deadline-alerts-demo/
├── dags/
│   ├── sla_rest_api_monitor_dag.py            # Option 1: REST API monitor (task-level)
│   ├── sla_monitor_target_demo.py             # Option 1: long-running DAG to be caught
│   ├── sla_deadline_average_runtime_demo.py   # Option 2: AVERAGE_RUNTIME deadline alert
│   └── sla_deadline_quickstart_demo.py        # Option 2: fires on the FIRST run (no history)
├── plugins/
│   └── deadline_callbacks.py                  # shared alert sink (SNS / log) + deadline callbacks
├── requirements.txt                           # MWAA extra deps (Slack optional)
└── README.md
```

---

# Option 2 — Native Deadline Alerts (`AVERAGE_RUNTIME`)

## How Deadline Alerts work (one paragraph)

You attach a `deadline=DeadlineAlert(...)` to the DAG. When a run **starts**,
Airflow computes a deadline = `reference + interval` and stores it. The
scheduler loop then checks every few seconds; the moment the deadline passes
**and the run is still going**, it fires your callback. Crucially the task is
**not killed** — this is a *soft* SLA alert, unlike `execution_timeout`.

For `AVERAGE_RUNTIME` specifically:

```
deadline = run_start + average_of_last_N_runtimes + interval
```

`AVERAGE_RUNTIME` only arms a deadline once enough completed runs exist
(`min_runs`). Until then, no deadline is created — which is why the demo has a
baseline phase.

---

## Running the demo on MWAA (Airflow 3.2.1+)

1. **Upload the files to your environment's S3 bucket:**
   - `dags/*.py`  → `s3://<your-mwaa-bucket>/dags/`
   - `plugins/deadline_callbacks.py` → zip the `plugins/` contents into
     `plugins.zip` and upload to `s3://<your-mwaa-bucket>/plugins.zip`, then set
     it as the environment's **Plugins file**.
     ```bash
     cd plugins && zip ../plugins.zip deadline_callbacks.py && cd ..
     # upload plugins.zip to S3 and point the MWAA env at it
     ```
     This same `plugins.zip` also serves Option 1 (it contains the shared
     `send_sla_alert` sink the monitor imports).
   - (Optional) upload `requirements.txt` and set it as the **Requirements file**
     only if you switch to the Slack notifier.

2. **(Optional) Wire up SNS** so alerts page the team instead of just logging.
   Add an Airflow environment variable on the MWAA environment:
   ```
   DEADLINE_ALERT_SNS_TOPIC_ARN = arn:aws:sns:<region>:<acct>:<topic>
   ```
   The MWAA **execution role** needs `sns:Publish` on that topic. If this var is
   unset the callbacks simply log the alert (fine for a first demo).

3. **Restart the Triggerer** after uploading `plugins.zip`. The async callback
   used by the `AVERAGE_RUNTIME` DAG runs in the Triggerer and is imported from
   the plugin file — it only picks up changes on restart. (Updating the
   environment / requirements triggers this automatically.)

4. **Phase 1 — build the baseline** (`sla_deadline_average_runtime_demo`):
   - Leave Airflow Variable `deadline_demo_slow` unset or `"false"`.
   - Trigger the DAG **5 times** (that's `min_runs`). Each run sleeps ~30s and
     completes normally. No alert — you're establishing the historical average.

5. **Phase 2 — breach the deadline:**
   - Set Airflow Variable `deadline_demo_slow = "true"`.
   - Trigger the DAG once. The task now sleeps 10 minutes, but the deadline
     (~30s average + 2 min interval ≈ 2.5 min) passes first → the callback
     fires and you get the alert, while the task keeps running.
   - Check the alert in the **Triggerer logs** (and in SNS if configured).

### Want an instant demo (no baseline)?

Trigger **`sla_deadline_quickstart_demo`** instead. It uses
`DAGRUN_QUEUED_AT + 1 min` so it alerts on the very first run (~2 min before the
3-minute task finishes). Its callback is a **sync** callback that runs on the
worker — no Triggerer restart needed. Great for a live screen-share.

---

## Tuning (Airflow Variables, all optional)

| Variable                      | Default | Effect                                            |
|-------------------------------|---------|---------------------------------------------------|
| `deadline_demo_slow`          | `false` | `true` = breach mode, `false` = baseline mode     |
| `deadline_demo_fast_sec`      | `30`    | Baseline run duration (seconds)                   |
| `deadline_demo_slow_sec`      | `600`   | Breach run duration (seconds)                     |
| `deadline_demo_interval_min`  | `2`     | Grace added on top of the historical average      |

In `sla_deadline_average_runtime_demo.py` the reference is
`AVERAGE_RUNTIME(max_runs=10, min_runs=5)` — analyse up to the last 10 runs,
require at least 5 completed runs before arming a deadline.

---

## Pointing the alert at Slack instead of SNS

The callbacks in `plugins/deadline_callbacks.py` publish to SNS (or log). If you
prefer Slack, swap the callback for Airflow's built-in notifier — no plugin
needed:

```python
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.sdk import AsyncCallback

callback = AsyncCallback(
    SlackWebhookNotifier,
    kwargs={"text": "🚨 DAG {{ dag_run.dag_id }} missed its deadline at {{ deadline.deadline_time }}"},
)
```

Set a `slack_default` connection (webhook token) and add
`apache-airflow-providers-slack` via the `requirements.txt`. Notifiers run Jinja
templating against the simplified deadline context (DAG-run fields +
`deadline.deadline_time`).

---

## Run it locally first (optional sanity check)

Pin the same Airflow version your MWAA environment runs (**3.2.1** here):

```bash
uv venv --python 3.12 .venv
VIRTUAL_ENV=.venv uv pip install "apache-airflow==3.2.1" \
  apache-airflow-providers-standard apache-airflow-providers-slack \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export PYTHONPATH="$PWD/plugins"
.venv/bin/airflow dags list          # both demo DAGs should appear, no import errors
```

(Both DAGs were validated this way on **Airflow 3.2.1** — they load in a 3.2.1
DagBag with the `DeadlineAlert` objects constructed and zero import errors.
The demo uses no 3.2.2-only API: Deadline Alerts and `AVERAGE_RUNTIME` shipped
in 3.1, and the `airflow.sdk` imports plus `SyncCallback` shipped in 3.2.0, so
it runs identically on 3.2.1.)

---

---

# Option 1 — REST API Monitor DAG (task-level)

A separate DAG (`sla_rest_api_monitor`) runs every 5 minutes, asks the Airflow
REST API which task instances are currently **running**, computes how long each
has been running, and raises an alert for any over its threshold. The monitored
tasks keep running — this is detection, not a kill. It gives **task-level**
granularity, which Option 2 cannot.

On Airflow 3.x this REST-API approach is the *supported* way to introspect other
DAGs/tasks, because tasks may no longer read the metadata DB directly.

### How it reaches the REST API on MWAA

The monitor uses the MWAA control-plane call **`InvokeRestApi`** via boto3,
authenticated with **AWS credentials (SigV4/IAM)** — no webserver token to
manage, and the webserver stays private. It queries the cross-DAG endpoint:

```
GET /api/v2/dags/~/dagRuns/~/taskInstances?state=running&start_date_lte=<now-threshold>
```

Asking the API to pre-filter to tasks that have *already* breached keeps the
response small — important because `InvokeRestApi` has a **10-second timeout**
and a **6 MB response cap**.

### IAM: grant the MWAA execution role REST access

Attach this to the environment's **execution role** (set `{airflow-role}` to
`Op` or `Admin` so it can read task instances):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowMwaaRestApiAccess",
    "Effect": "Allow",
    "Action": ["airflow:InvokeRestApi", "airflow:CreateWebLoginToken"],
    "Resource": "arn:aws:airflow:<region>:<account-id>:role/<env-name>/<airflow-role>"
  }]
}
```

(If you also wire SNS, add `sns:Publish` on your topic — same as Option 2.)

### Running the Option 1 demo on MWAA

1. Upload all `dags/*.py` to `s3://<bucket>/dags/` and the `plugins.zip` (same
   zip as Option 2 — it also contains the shared alert sink). Attach the IAM
   policy above.
2. Set the Airflow Variable the monitor needs:
   ```
   sla_monitor_env_name = <your MWAA environment name>
   ```
   (Optional) `sla_monitor_threshold_min` (default 15), `sla_monitor_dag_pattern`,
   `sla_monitor_thresholds` (JSON per-dag/per-task overrides),
   `sla_monitor_exclude_self` (default true).
3. Lower the threshold for a quick demo, e.g. `sla_monitor_threshold_min = 2`,
   and set `sla_target_sleep_min = 5`.
4. Trigger **`sla_monitor_target_demo`**. Within ~5 min the monitor's next run
   detects the long-running `long_running_step` task and alerts (Triggerer/worker
   logs, and SNS if configured). The target task keeps running to completion.

### Monitor configuration (Airflow Variables)

| Variable                    | Default | Effect                                                   |
|-----------------------------|---------|----------------------------------------------------------|
| `sla_monitor_env_name`      | —       | **Required on MWAA.** Environment name for InvokeRestApi |
| `sla_monitor_threshold_min` | `15`    | Global minutes a task may run before it's flagged        |
| `sla_monitor_dag_pattern`   | `""`    | Only watch DAGs whose id matches this substring          |
| `sla_monitor_exclude_self`  | `true`  | Skip the monitor's own DAG                               |
| `sla_monitor_thresholds`    | `{}`    | JSON overrides: `{"dag_id": 10, "dag_id.task_id": 5}`    |
| `sla_target_sleep_min`      | `20`    | (target DAG) how long the demo task sleeps               |

The breach-detection logic was unit-tested with a mocked REST response: it
correctly flags tasks over the global threshold **and** per-task overrides,
skips under-threshold tasks, paginates, and excludes itself.

> Note: Option 1 polls on a schedule, so detection latency is up to one monitor
> interval (5 min by default). Tighten the schedule for faster detection, at the
> cost of more REST calls.

---

## Which option should I use?

| | **Option 1 — REST monitor** | **Option 2 — Deadline Alerts** | Option 3 — `execution_timeout` |
|---|---|---|---|
| Granularity | **Task-level** | DAG-run level | Task-level |
| Extra infra | A scheduled monitor DAG + IAM | **None (native)** | None |
| Kills the task? | No | No | **Yes** (hard timeout) |
| Detection latency | Up to one poll interval | Within `scheduler_heartbeat_sec` | At the timeout |
| Needs run history? | No | Yes (`AVERAGE_RUNTIME` only) | No |
| Min Airflow version | 3.x | **3.2.1** | any |
| In this demo | ✅ | ✅ | — (one-liner below) |

Rules of thumb:
- Need **per-task** alerting now, across many DAGs → **Option 1**.
- Want **zero infrastructure** and DAG-level is enough → **Option 2**.
- Use the two together: Option 2 as the native safety net, Option 1 where you
  need task-level visibility.
- **Option 3** (`execution_timeout=timedelta(...)` + `on_failure_callback`) is a
  one-line backstop, but it **kills** the task at the threshold — use it only
  where termination is acceptable.

Native task-level deadline alerting (**AIP-86**) is the longer-term fix; both
demos are the bridge until it ships.
```
