import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
import glob
import re
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        #logging.FileHandler(r'./logfile.log')
    ]
)

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

# Define the function to sort files
def sort_files(**kwargs):
    folder_to_watch = "/data"  # Replace with the correct path
    logging.info(f"Monitoring folder: {folder_to_watch}")

    # Check if folder exists
    if not os.path.exists(folder_to_watch):
        logging.error(f"Folder does not exist: {folder_to_watch}")
        return

    # Log the current files in the folder
    files_in_directory = os.listdir(folder_to_watch)
    logging.debug(f"Files in directory before processing: {files_in_directory}")

    # Use glob to find files in the folder
    for file_path in glob.glob(os.path.join(folder_to_watch, "*")):
        file_name = os.path.basename(file_path)
        logging.info(f"Processing file: {file_name}")

        # Check if the path is a file
        if os.path.isfile(file_path):
            # Use regex to capture the base name
            match = re.match(r'(\w+)', file_name)
            if match:
                base_name = match.group(1)
                dest_dir = os.path.join(folder_to_watch, base_name)

                # Ensure the destination directory exists
                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                    logging.info(f"Created directory: {dest_dir}")

                # Move the file to the destination directory
                try:
                    shutil.move(file_path, os.path.join(dest_dir, file_name))
                    logging.info(f"Moved file: {file_name} to {dest_dir}")
                except Exception as e:
                    logging.error(f"Failed to move file: {file_name}. Error: {e}")
            else:
                logging.warning(f"Filename does not match expected pattern: {file_name}")
        else:
            logging.info(f"Skipping directory: {file_path}")

    # Log the updated files in the folder
    files_in_directory_after = os.listdir(folder_to_watch)
    logging.debug(f"Files in directory after processing: {files_in_directory_after}")

# Define the task using PythonOperator
sort_files_task = PythonOperator(
    task_id='sort_files_task',
    python_callable=sort_files,
    dag=dag,
)

sort_files_task
