from dotenv import load_dotenv
import os
import requests
from requests.exceptions import RequestException
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
from dateutil import parser
import time
import json
from multiprocessing import Pool

# Load environment variables from .env file
load_dotenv('ll_hubspot.env')

# Use the private app access token
HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_PRIVATE_TOKEN')

# BigQuery configuration
SERVICE_ACCOUNT_FILE = 'rare-guide-433209-e6-f3984db59647.json'
PROJECT_ID = 'rare-guide-433209-e6'
DATASET_ID = 'HUBSPOT'

# Maximum length for column names in BigQuery (1024 characters by default)
MAX_COLUMN_NAME_LENGTH = 1024

# File to store the last successful data pull state
LAST_PULL_STATE_FILE = 'last_pull_state.json'

BATCH_SIZE = 1000  # Batch size for inserting data into BigQuery

# Authenticate with Google Cloud using the service account file
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def truncate_column_name(column_name):
    return column_name[:MAX_COLUMN_NAME_LENGTH]

def get_last_pull_state():
    try:
        with open(LAST_PULL_STATE_FILE, 'r') as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_last_pull_state(state):
    try:
        with open(LAST_PULL_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        print(f"Failed to save last pull state: {e}")

def get_hubspot_data(endpoint, retries=3, backoff_factor=1.0, timeout=30):
    url = f'https://api.hubapi.com{endpoint}'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 401:
                raise Exception("Unauthorized access - please check your access token.")
            elif response.status_code == 429:  # Too many requests
                retry_after = int(response.headers.get("Retry-After", 60))
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            if attempt < retries - 1:
                time.sleep(backoff_factor * (2 ** attempt))
            else:
                raise

def get_all_contact_properties():
    url = 'https://api.hubapi.com/properties/v1/contacts/properties'
    headers = {
        'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve properties: {response.status_code} {response.reason}")
    properties = response.json()
    return [prop['name'] for prop in properties]

def create_table_if_not_exists(table_id, columns):
    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField(truncate_column_name(col), dtype)
        for col, dtype in columns.items()
    ]

    # Create the table if it doesn't exist
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)

def convert_datetime(iso_datetime):
    try:
        return parser.isoparse(iso_datetime)
    except Exception as e:
        print(f"Error converting datetime: {iso_datetime}, Error: {e}")
        return None

def insert_data_into_bigquery(data, table_id):
    if data:
        # Define initial columns and their types
        columns = {
            'id': 'INTEGER',
            'createdAt': 'TIMESTAMP',
            'updatedAt': 'TIMESTAMP'
        }

        # Extend columns based on the sample record's properties
        sample_record = data[0]['properties']
        for key, value in sample_record.items():
            truncated_key = truncate_column_name(key)
            if isinstance(value, str):
                columns[truncated_key] = 'STRING'
            elif isinstance(value, int):
                columns[truncated_key] = 'INTEGER'
            elif isinstance(value, float):
                columns[truncated_key] = 'FLOAT'
            else:
                columns[truncated_key] = 'STRING'

        # Create the table if it doesn't exist
        create_table_if_not_exists(table_id, columns)

        # Prepare the data for insertion
        rows_to_insert = []
        for record in data:
            properties = record['properties']
            row = {
                'id': int(record['id']),
                'createdAt': convert_datetime(record['createdAt']),
                'updatedAt': convert_datetime(record['updatedAt'])
            }
            for key, value in properties.items():
                row[truncate_column_name(key)] = value
            rows_to_insert.append(row)

        # Insert data into BigQuery in batches
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")

def process_endpoint(args):
    table_name, endpoint = args
    last_pull_state = get_last_pull_state()
    last_pull_info = last_pull_state.get(table_name, {})
    after = last_pull_info.get('after')
    max_timestamp = last_pull_info.get('timestamp')

    if max_timestamp:
        endpoint += f"&since={max_timestamp}"

    has_more = True
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    while has_more:
        if after:
            endpoint_with_after = f"{endpoint}&after={after}"
        else:
            endpoint_with_after = endpoint
        print(f"Extracting data from {endpoint_with_after}")
        response = get_hubspot_data(endpoint_with_after)
        data = response.get('results', [])
        if data:
            insert_data_into_bigquery(data, table_id)
            print(f"Inserted {len(data)} records into {table_name} table")
            record_max_timestamp = max(record['updatedAt'] for record in data)
            if not max_timestamp or record_max_timestamp > max_timestamp:
                max_timestamp = record_max_timestamp
            last_pull_state[table_name] = {'after': after, 'timestamp': max_timestamp}
            save_last_pull_state(last_pull_state)
        else:
            print("No data found in response")
        has_more = response.get('paging', {}).get('next', {}).get('after') is not None
        after = response.get('paging', {}).get('next', {}).get('after')

def extract_and_load_data():
    endpoints = {
        'contacts': '/crm/v3/objects/contacts?properties=' + ','.join(get_all_contact_properties())
    }
    with Pool(processes=len(endpoints)) as pool:
        pool.map(process_endpoint, endpoints.items())

if __name__ == "__main__":
    extract_and_load_data()
