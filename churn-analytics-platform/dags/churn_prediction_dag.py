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
    dag_id='churn_prediction_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['streaming', 'kafka', 'churn'],
) as dag:

    run_churn_prediction = BashOperator(
        task_id='run_churn_prediction',
        bash_command='python /opt/airflow/ml_model/churn_prediction.py',
    )

    run_churn_prediction
