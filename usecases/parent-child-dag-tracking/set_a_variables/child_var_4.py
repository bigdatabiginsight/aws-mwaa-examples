"""
SET A - Child DAG 4 (Variables approach)
Simulates: Data Load - Target Warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json
import time

default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=1)}
DAG_ID = 'child_var_4'


def do_work(**context):
    print(f"[{DAG_ID}] Starting: Data Load")
    print(f"[{DAG_ID}] Triggered by: {context['dag_run'].conf.get('triggered_by', 'manual')}")
    time.sleep(5)
    print(f"[{DAG_ID}] Work complete.")


def update_tracker(**context):
    try:
        tracker = json.loads(Variable.get('child_dag_tracker'))
        tracker['children'][DAG_ID] = 'success'
        tracker[f'{DAG_ID}_completed_at'] = datetime.utcnow().isoformat()
        Variable.set('child_dag_tracker', json.dumps(tracker))
        print(f"[{DAG_ID}] Tracker updated: success")
    except Exception as e:
        print(f"[{DAG_ID}] Could not update tracker: {e}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='SET A: Child 4 - Data Load (Variables)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['set-a', 'variables', 'child'],
) as dag:
    work = PythonOperator(task_id='do_work', python_callable=do_work)
    report = PythonOperator(task_id='update_tracker', python_callable=update_tracker)
    work >> report
