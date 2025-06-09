from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def dummy_task():
    print("Hello from DAG")

with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="print_hello",
        python_callable=dummy_task
    )
