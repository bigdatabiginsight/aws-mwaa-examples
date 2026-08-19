"""
SET B - Child DAG 4 (XCom approach)
Simulates: Data Load - Target Warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import time

default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=1)}
DAG_ID = 'child_xcom_4'


def do_work(**context):
    print(f"[{DAG_ID}] Starting: Data Load")
    print(f"[{DAG_ID}] Triggered by: {context['dag_run'].conf.get('triggered_by', 'manual')}")
    time.sleep(5)
    print(f"[{DAG_ID}] Work complete.")


def push_status(**context):
    result = {
        'dag_id': DAG_ID,
        'status': 'success',
        'execution_date': context['ds'],
        'run_id': context['run_id'],
        'completed_at': datetime.utcnow().isoformat(),
        'triggered_by': context['dag_run'].conf.get('triggered_by', 'manual'),
    }
    context['ti'].xcom_push(key='child_execution_result', value=json.dumps(result))
    print(f"[{DAG_ID}] XCom pushed: {json.dumps(result, indent=2)}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='SET B: Child 4 - Data Load (XCom)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['set-b', 'xcom', 'child'],
) as dag:
    work = PythonOperator(task_id='do_work', python_callable=do_work)
    status = PythonOperator(task_id='push_status', python_callable=push_status)
    work >> status
