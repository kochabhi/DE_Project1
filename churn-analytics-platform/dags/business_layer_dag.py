from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='business_layer_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=20),  # Trigger manually or integrate later
    catchup=False,
    tags=['transformation', 'churn', 'etl'],
) as dag:

    run_data_transformation = BashOperator(
        task_id='run_data_transformation',
        bash_command='python /opt/airflow/notebooks/business_layer.py',
    )

    run_data_transformation
