from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from google.cloud import bigquery
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from the correct .env file (update for Docker path)
from dotenv import load_dotenv
load_dotenv('/opt/airflow/config/ll_hubspot.env')

# Ensure the HubSpot private token is correctly loaded
HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_PRIVATE_TOKEN')
if not HUBSPOT_ACCESS_TOKEN:
    raise Exception("HubSpot Private Token is not found in environment variables.")

# BigQuery connection configuration
BIGQUERY_PROJECT_ID = 'rare-guide-433209-e6'
BIGQUERY_DATASET_ID = 'HUBSPOT'
TABLE_NAME = 'll_hs_contacts'

# Path to your service account JSON file (update for Docker path)
SERVICE_ACCOUNT_JSON = '/opt/airflow/config/rare-guide-433209-e6-7bc861ae2907.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_JSON

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'hubspot_to_bigquery_dag',
    default_args=default_args,
    description='Extract all HubSpot contact data and load into BigQuery',
    schedule_interval=timedelta(days=1),  # Runs daily
    catchup=False,
)

MAX_COLUMN_NAME_LENGTH = 300  # Adjusted to accommodate longer property names
BATCH_SIZE = 100  # Batch size for inserting data into BigQuery

# Helper function to truncate column names
def truncate_column_name(column_name):
    return column_name[:MAX_COLUMN_NAME_LENGTH]

# Fetch a list of contact properties from HubSpot
def get_contact_properties():
    url = 'https://api.hubapi.com/properties/v1/contacts/properties'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error(f"Failed to retrieve properties: {response.status_code} {response.reason}")
        return []

    properties = response.json()
    # Extract property names
    property_names = [prop['name'] for prop in properties]
    logger.info(f"Fetched {len(property_names)} properties.")
    return property_names

# Fetch contacts from HubSpot in batches and load into BigQuery
def fetch_and_load_hubspot_contacts():
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TABLE_NAME}"

    # Get contact properties
    properties = get_contact_properties()
    if not properties:
        logger.error("No contact properties found.")
        return

    # Define the schema based on the properties
    schema = [
        bigquery.SchemaField('id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('createdAt', 'TIMESTAMP'),
        bigquery.SchemaField('updatedAt', 'TIMESTAMP'),
    ]
    for field in properties:
        field_name = truncate_column_name(field)
        schema.append(bigquery.SchemaField(field_name, 'STRING'))

    # Create the table if it doesn't exist, or update the schema if it does
    try:
        table = client.get_table(table_id)
        logger.info(f"Table {table_id} already exists.")
        # Get existing schema fields
        existing_fields = {field.name for field in table.schema}
        new_fields = []
        for field in schema:
            if field.name not in existing_fields:
                new_fields.append(field)
        if new_fields:
            # Update schema to add new fields
            updated_schema = table.schema + new_fields
            table.schema = updated_schema
            table = client.update_table(table, ["schema"])
            logger.info(f"Updated table schema with {len(new_fields)} new fields.")
    except Exception as e:
        # Create the table
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        logger.info(f"Created table {table_id}.")

    # Prepare to fetch data
    url = 'https://api.hubapi.com/crm/v3/objects/contacts'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
    }

    params = {
        'limit': 100,  # Max per page
        'properties': ','.join(properties),  # Include all properties
    }

    after = None
    total_records = 0

    while True:
        if after:
            params['after'] = after
        else:
            params.pop('after', None)

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
        except Exception as e:
            logger.error(f"Error fetching data from HubSpot: {e}")
            break

        if response.status_code != 200:
            logger.error(f"Error fetching data from HubSpot: {response.status_code} {response.text}")
            break

        data = response.json()
        results = data.get('results', [])
        if not results:
            logger.info("No more contacts to fetch.")
            break

        # Transform data for BigQuery insertion
        rows_to_insert = []
        for contact in results:
            properties_data = contact.get('properties', {})
            row = {
                'id': contact['id'],
                'createdAt': contact.get('createdAt'),
                'updatedAt': contact.get('updatedAt'),
            }
            # Add properties
            for field in properties:
                field_name = truncate_column_name(field)
                value = properties_data.get(field)
                row[field_name] = value if value is not None else None
            rows_to_insert.append(row)

        # Insert data into BigQuery
        try:
            errors = client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                logger.error(f"Errors while inserting rows: {errors}")
            else:
                logger.info(f"Inserted {len(rows_to_insert)} rows into {table_id}.")
                total_records += len(rows_to_insert)
        except Exception as e:
            logger.error(f"Exception while inserting rows: {e}")

        # Check for more pages
        paging = data.get('paging', {})
        next_page = paging.get('next', {})
        after = next_page.get('after')
        if not after:
            break

    logger.info(f"Total records inserted: {total_records}")

# Define task for the DAG
extract_and_load_task = PythonOperator(
    task_id='fetch_and_load_hubspot_contacts',
    python_callable=fetch_and_load_hubspot_contacts,
    dag=dag,
)
