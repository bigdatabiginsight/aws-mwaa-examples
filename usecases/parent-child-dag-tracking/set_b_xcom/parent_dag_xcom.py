"""
SET B - Parent DAG using XCOM approach.
Triggers 6 child DAGs and tracks their execution using XCom (run-scoped).

XComs are tied to the specific DAG run — no persistent state across runs.
The parent pulls XCom values from child DAGs after they complete.

Compatible with MWAA 2.10.3
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

CHILD_DAGS = [
    'child_xcom_1',
    'child_xcom_2',
    'child_xcom_3',
    'child_xcom_4',
    'child_xcom_5',
    'child_xcom_6',
]


def check_children_xcom(**context):
    """
    Pull XCom from each child DAG's 'push_status' task.
    XComs are scoped to the DAG run, so we use include_prior_dates=True
    to pull from the most recent run of each child.
    """
    ti = context['ti']

    print("=" * 50)
    print("XCOM TRACKER - FINAL STATUS")
    print("=" * 50)

    results = {}
    for child_dag_id in CHILD_DAGS:
        xcom_value = ti.xcom_pull(
            dag_id=child_dag_id,
            task_ids='push_status',
            key='child_execution_result',
            include_prior_dates=True,
        )
        if xcom_value:
            result = json.loads(xcom_value)
            results[child_dag_id] = result
            print(f"  {child_dag_id}: {result['status']} (completed at {result['completed_at']})")
        else:
            results[child_dag_id] = None
            print(f"  {child_dag_id}: NO XCOM FOUND")

    # Push aggregated summary
    summary = {
        'total': len(CHILD_DAGS),
        'found': sum(1 for v in results.values() if v is not None),
        'missing': sum(1 for v in results.values() if v is None),
        'checked_at': datetime.utcnow().isoformat(),
    }
    ti.xcom_push(key='parent_summary', value=json.dumps(summary))
    print(f"\nSummary: {json.dumps(summary, indent=2)}")

    if summary['missing'] > 0:
        missing = [d for d, v in results.items() if v is None]
        raise Exception(f"Missing XCom from: {missing}")


with DAG(
    dag_id='parent_dag_xcom',
    default_args=default_args,
    description='SET B: Parent DAG - tracks child execution via XCom',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['set-b', 'xcom', 'parent'],
) as dag:

    triggers = []
    for child_dag_id in CHILD_DAGS:
        t = TriggerDagRunOperator(
            task_id=f'trigger_{child_dag_id}',
            trigger_dag_id=child_dag_id,
            wait_for_completion=True,
            poke_interval=30,
            allowed_states=['success'],
            failed_states=['failed'],
            conf={'triggered_by': 'parent_dag_xcom', 'parent_ds': '{{ ds }}'},
        )
        triggers.append(t)

    check = PythonOperator(
        task_id='check_children_xcom',
        python_callable=check_children_xcom,
    )

    triggers >> check
