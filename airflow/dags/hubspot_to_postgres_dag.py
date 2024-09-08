from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import os
import requests
from requests.exceptions import RequestException
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
from psycopg2 import sql, extras

# Load environment variables from .env
load_dotenv('ll_hubspot.env')

# Use the private app access token from the environment variables
HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_PRIVATE_TOKEN')

# PostgreSQL connection ID in Airflow, set to 'airflow_db'
POSTGRES_CONN_ID = 'airflow_db'

# Constants
MAX_COLUMN_NAME_LENGTH = 63
BATCH_SIZE = 100

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG
dag = DAG(
    'hubspot_to_postgres',
    default_args=default_args,
    description='Extract data from HubSpot and load it into Postgres',
    schedule_interval='@daily',
)

def truncate_column_name(column_name):
    """Truncate column names to avoid exceeding the limit of PostgreSQL."""
    return column_name[:MAX_COLUMN_NAME_LENGTH]

def create_table_if_not_exists(cursor, table_name, columns):
    """Create the table if it does not exist based on the sample data."""
    columns_def = [
        sql.Identifier(truncate_column_name(col)) + sql.SQL(' ') + sql.SQL(dtype)
        for col, dtype in columns.items()
    ]
    query = sql.SQL('CREATE TABLE IF NOT EXISTS {} ({})').format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(columns_def)
    )
    cursor.execute(query)

def get_hubspot_data(endpoint, retries=3, backoff_factor=1.0, timeout=30):
    """Fetch data from HubSpot's API."""
    url = f'https://api.hubapi.com{endpoint}'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            if attempt < retries - 1:
                time.sleep(backoff_factor * (2 ** attempt))
            else:
                raise

def insert_data_into_postgresql(data, table_name):
    """Insert data into PostgreSQL using PostgresHook."""
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    if data:
        # Define initial columns and their types
        columns = {
            'id': 'BIGINT PRIMARY KEY',
            'createdAt': 'TIMESTAMP',
            'updatedAt': 'TIMESTAMP'
        }

        # Extend columns based on the sample record's properties
        sample_record = data[0]['properties']
        for key, value in sample_record.items():
            truncated_key = truncate_column_name(key)
            if isinstance(value, str):
                columns[truncated_key] = 'TEXT'
            elif isinstance(value, int):
                columns[truncated_key] = 'INT'
            elif isinstance(value, float):
                columns[truncated_key] = 'FLOAT'
            else:
                columns[truncated_key] = 'TEXT'

        # Create table if it doesn't exist
        create_table_if_not_exists(cursor, table_name, columns)

        # Prepare the insert query
        placeholders = sql.SQL(', ').join(sql.Placeholder() * len(columns))
        columns_names = sql.SQL(', ').join(map(sql.Identifier, columns.keys()))
        query = sql.SQL(
            'INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO NOTHING'
        ).format(sql.Identifier(table_name), columns_names, placeholders)

        # Insert data in batches
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            records_to_insert = []
            for record in batch:
                properties = record['properties']
                records_to_insert.append([properties.get(key) for key in columns.keys()])
            extras.execute_batch(cursor, query, records_to_insert)

        conn.commit()
    cursor.close()
    conn.close()

def extract_and_load_data():
    """Extract data from HubSpot and load it into Postgres."""
    endpoint = '/crm/v3/objects/contacts'
    data = get_hubspot_data(endpoint)
    if data:
        insert_data_into_postgresql(data['results'], 'contacts')

# Define Airflow task
extract_and_load_task = PythonOperator(
    task_id='extract_and_load_data',
    python_callable=extract_and_load_data,
    dag=dag
)
