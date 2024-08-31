from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Append the directory where the script is located inside the container
sys.path.append('/opt/airflow')

# Import your script
from hubspot_extractor_BigQuery import extract_and_load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hubspot_to_bigquery',
    default_args=default_args,
    description='Pull data from HubSpot and insert into BigQuery',
    schedule_interval=timedelta(days=1),
)

run_task = PythonOperator(
    task_id='run_hubspot_script',
    python_callable=extract_and_load_data,
    dag=dag,
)
