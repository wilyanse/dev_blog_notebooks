import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from helpers.file_sort import sort_files as sort_files

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'file_sorting_dag',
    default_args=default_args,
    description='DAG to sort files into subdirectories based on filename',
    schedule_interval=timedelta(hours=1),  # Run every minute
    start_date=datetime(2024, 8, 24),
    catchup=False,
)

# Define the task using PythonOperator
sort_files_task = PythonOperator(
    task_id='sort_files_task',
    python_callable=sort_files,
    dag=dag,
)

sort_files_task
