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
    dag_id='data_transformation_dag',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or integrate later
    catchup=False,
    tags=['transformation', 'churn', 'etl'],
) as dag:

    run_data_transformation = BashOperator(
        task_id='run_data_transformation',
        bash_command='python /opt/airflow/notebooks/data_transformation.py',
    )

    run_data_transformation
