"""
SET A - Parent DAG using VARIABLES approach.
Triggers 6 child DAGs and tracks their execution using Airflow Variables (persistent key-value store).

Compatible with MWAA 2.10.3
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
import json


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

CHILD_DAGS = [
    'child_var_1',
    'child_var_2',
    'child_var_3',
    'child_var_4',
    'child_var_5',
    'child_var_6',
]


def initialize_tracker(**context):
    """
    Create/reset the Variable-based tracker at the start of each parent run.
    Stores a JSON object with status of all children.
    """
    tracker = {
        'parent_execution_date': context['ds'],
        'parent_run_id': context['run_id'],
        'started_at': datetime.utcnow().isoformat(),
        'children': {dag_id: 'pending' for dag_id in CHILD_DAGS},
    }
    Variable.set('child_dag_tracker', json.dumps(tracker))
    print(f"Tracker initialized:\n{json.dumps(tracker, indent=2)}")


def check_tracker(**context):
    """
    Read the Variable tracker after all children have completed.
    Summarize which children succeeded/failed/are still pending.
    """
    tracker = json.loads(Variable.get('child_dag_tracker'))

    completed = [d for d, s in tracker['children'].items() if s == 'success']
    failed = [d for d, s in tracker['children'].items() if s == 'failed']
    pending = [d for d, s in tracker['children'].items() if s == 'pending']

    print("=" * 50)
    print("VARIABLE TRACKER - FINAL STATUS")
    print("=" * 50)
    print(f"Completed ({len(completed)}): {completed}")
    print(f"Failed    ({len(failed)}):    {failed}")
    print(f"Pending   ({len(pending)}):   {pending}")
    print(f"\nFull tracker:\n{json.dumps(tracker, indent=2)}")

    if failed or pending:
        raise Exception(f"Not all children succeeded. Failed: {failed}, Pending: {pending}")


with DAG(
    dag_id='parent_dag_variables',
    default_args=default_args,
    description='SET A: Parent DAG - tracks child execution via Airflow Variables',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['set-a', 'variables', 'parent'],
) as dag:

    init = PythonOperator(
        task_id='initialize_tracker',
        python_callable=initialize_tracker,
    )

    triggers = []
    for child_dag_id in CHILD_DAGS:
        t = TriggerDagRunOperator(
            task_id=f'trigger_{child_dag_id}',
            trigger_dag_id=child_dag_id,
            wait_for_completion=True,
            poke_interval=30,
            allowed_states=['success'],
            failed_states=['failed'],
            conf={'triggered_by': 'parent_dag_variables', 'parent_ds': '{{ ds }}'},
        )
        triggers.append(t)

    check = PythonOperator(
        task_id='check_tracker',
        python_callable=check_tracker,
    )

    init >> triggers >> check
