from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='stream_processor_dag',
    default_args=default_args,
    schedule_interval=None,  # manual or trigger on demand
    catchup=False,
    tags=['streaming', 'kafka', 'churn'],
) as dag:

    run_stream_processor = BashOperator(
        task_id='run_stream_processor',
        bash_command='python /opt/airflow/streaming_pipeline/stream_processor.py',
    )

    run_stream_processor
