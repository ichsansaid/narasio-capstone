from app.coindesk.commands.coindesk_fetch import case_narasio

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the default_args dictionary to specify default parameters for the DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 15),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

# Define the DAG with the default_args
dag = DAG(
    'coin_desk_data_fetch',  # Replace with your DAG name
    default_args=default_args,
    schedule_interval=timedelta(hours=1),  # Set to run hourly
    catchup=False,  # Prevent catching up on past runs
)

# Define the PythonOperator that calls the case_narasio function
fetch_coin_desk_data = PythonOperator(
    task_id='fetch_coin_desk_data',
    python_callable=case_narasio,
    dag=dag,
)

# Set the task dependencies
fetch_coin_desk_data