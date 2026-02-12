# dags/example_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    print("Hello Airflow!")
    return "작업 완료"

default_args = {
    'owner': 'LEE',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='첫 번째 DAG',
    schedule='@daily',  # 매일 실행
    start_date=datetime(2026, 1, 28),
    catchup=False,  # 과거 실행 건너뛰기
    tags=['example']
) as dag:
    
    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )