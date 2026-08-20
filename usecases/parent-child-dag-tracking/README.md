# MWAA Parent/Child DAG Tracking — Examples

> ⚠️ **Proof-of-concept (POC) code — for demonstration and educational purposes only.**
> This was built to illustrate patterns and is not production-ready as-is. Test, secure,
> and optimize it against your own standards before any production use. Provided as-is,
> without warranty — see the repository [LICENSE](../../LICENSE).

Two working examples showing how a **parent DAG** can trigger multiple **child DAGs**
and reliably track whether each child has executed, on **Amazon MWAA (Apache Airflow 2.10.3)**.

This was built to address a common source of "unexpected behaviour" when orchestrating
child DAGs from a parent: the parent appears to run, but child DAGs stay queued and
never complete. See [Common Pitfalls](#common-pitfalls) below.

## Two approaches

| | Set A — Variables | Set B — XCom |
|---|---|---|
| Folder | `set_a_variables/` | `set_b_xcom/` |
| Mechanism | Airflow **Variables** (persistent key-value store) | **XCom** (scoped to the DAG run) |
| Persists across runs | Yes | No — cleared with DAG run retention |
| Race conditions | Possible on concurrent writes | Isolated per task instance |
| Best for | "Did this child run at all today?" | "Did this child run in *this* parent run?" |
| Where to inspect | Airflow UI → Admin → Variables | Airflow UI → Task Instance → XCom tab |

Each set contains 1 parent DAG + 6 child DAGs. Every child simulates a distinct
pipeline stage (extraction, transformation, validation, load, notifications, cleanup),
does some work, then reports its status back to the parent.

### Set A — Variables (`set_a_variables/`)
- `parent_dag_variables.py` — initializes a JSON tracker Variable, triggers the 6 children, then reads the tracker to confirm completion.
- `child_var_1.py` … `child_var_6.py` — each updates the shared `child_dag_tracker` Variable on success.

### Set B — XCom (`set_b_xcom/`)
- `parent_dag_xcom.py` — triggers the 6 children, then pulls each child's XCom to confirm completion.
- `child_xcom_1.py` … `child_xcom_6.py` — each pushes an `child_execution_result` XCom on success.

## Deployment (MWAA)

MWAA discovers DAGs from the root of the S3 DAGs bucket, so upload the `.py` files flat:

```bash
aws s3 cp set_a_variables/ s3://<your-mwaa-bucket>/dags/ --recursive
aws s3 cp set_b_xcom/      s3://<your-mwaa-bucket>/dags/ --recursive
```

No `requirements.txt` changes are needed — these use only built-in Airflow 2.x
modules and the Python standard library.

## Running

1. Confirm all child DAGs are **unpaused** (they ship with `is_paused_upon_creation=False`, so on a fresh environment they land unpaused automatically).
2. Trigger **one parent at a time** — start with `parent_dag_variables`, let it finish, then run `parent_dag_xcom`.
3. Watch the final task (`check_tracker` / `check_children_xcom`) logs for the completion summary.

## Common Pitfalls

These are the exact issues that cause "parent runs but children don't":

1. **Child DAGs are paused.** A triggered DAG that is paused will never execute — its
   tasks sit queued forever and the parent waits indefinitely. These examples set
   `is_paused_upon_creation=False` so children auto-unpause on a fresh deploy.

2. **Worker-slot deadlock on cold start.** The parent uses
   `TriggerDagRunOperator(wait_for_completion=True)`, which holds a worker slot while
   waiting. If you trigger many blocking parent tasks at once against a small/cold-start
   worker pool (e.g. `min workers = 1`), every slot gets pinned by waiting parent tasks
   and there are none left for the children to run — a deadlock. Autoscaling eventually
   adds workers, but it lags. **Fix:** run one parent at a time, and/or raise `min workers`.
   For large fan-out, consider `deferrable=True` (Airflow 2.10+) which frees the slot
   while waiting.

3. **Cross-DAG XCom pulls need `include_prior_dates=True`.** Child DAG runs triggered by
   the parent may have a different logical date, so the parent uses `include_prior_dates=True`
   when pulling child XComs.

## Environment tested
- Amazon MWAA, Apache Airflow **2.10.3**
- Environment class: `mw1.medium`, workers min 1 / max 10
