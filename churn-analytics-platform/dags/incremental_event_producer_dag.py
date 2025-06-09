from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='incremental_event_producer_dag',
    default_args=default_args,
    description='Run the incremental event producer script',
    schedule_interval=None,
    start_date=datetime(2025, 6, 6),
    catchup=False,
    tags=['churn', 'kafka', 'event_producer'],
) as dag:

    run_event_producer = BashOperator(
        task_id='run_incremental_event_producer',
        bash_command='python /opt/airflow/data_producer/incremental_event_producer.py'
    )
