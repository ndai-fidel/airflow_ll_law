from dotenv import load_dotenv
import os
import requests
from requests.exceptions import RequestException
import psycopg2
from psycopg2 import sql, extras
from datetime import datetime
from dateutil import parser
import time
import json
from multiprocessing import Pool

# Load environment variables from .env file
load_dotenv('ll_hubspot.env')

# Use the private app access token
HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_PRIVATE_TOKEN')

# PostgreSQL connection configuration
POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

# Maximum length for column names in PostgreSQL (64 characters by default)
MAX_COLUMN_NAME_LENGTH = 63

# File to store the last successful data pull state
LAST_PULL_STATE_FILE = 'last_pull_state.json'

BATCH_SIZE = 100  # Batch size for inserting data into PostgreSQL

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

def create_table_if_not_exists(cursor, table_name, columns):
    # Create the table if it doesn't exist
    columns_def = [sql.Identifier(truncate_column_name(col)) + sql.SQL(' ') + sql.SQL(dtype) for col, dtype in columns.items()]
    query = sql.SQL('CREATE TABLE IF NOT EXISTS {} ({})').format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(columns_def)
    )
    cursor.execute(query)

def convert_datetime(iso_datetime):
    try:
        return parser.isoparse(iso_datetime)
    except Exception as e:
        print(f"Error converting datetime: {iso_datetime}, Error: {e}")
        return None

def insert_data_into_postgresql(data, table_name):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
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
            if len(truncated_key) > MAX_COLUMN_NAME_LENGTH:
                continue
            if isinstance(value, str):
                columns[truncated_key] = 'TEXT'
            elif isinstance(value, int):
                columns[truncated_key] = 'INT'
            elif isinstance(value, float):
                columns[truncated_key] = 'FLOAT'
            else:
                columns[truncated_key] = 'TEXT'

        # Create the table if it doesn't exist
        create_table_if_not_exists(cursor, table_name, columns)

        # Fetch existing columns in the table
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        existing_columns = [row[0] for row in cursor.fetchall()]

        # Add any missing columns to the table
        for column_name, column_type in columns.items():
            if column_name not in existing_columns:
                alter_table_query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    sql.Identifier(table_name),
                    sql.Identifier(column_name),
                    sql.SQL(column_type)
                )
                cursor.execute(alter_table_query)

        # Prepare for data insertion
        placeholders = sql.SQL(', ').join(sql.Placeholder() * len(columns))
        columns_names = sql.SQL(', ').join(map(sql.Identifier, columns.keys()))
        update_columns = sql.SQL(', ').join([
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
            for col in columns.keys() if col != 'id'
        ])
        query = sql.SQL('INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO UPDATE SET {}').format(
            sql.Identifier(table_name),
            columns_names,
            placeholders,
            update_columns
        )

        # Insert data in batches
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            records_to_insert = []
            for record in batch:
                properties = record['properties']
                converted_properties = []
                for key in columns.keys():
                    if key in properties:
                        if isinstance(properties[key], str) and properties[key].endswith('Z'):
                            converted_properties.append(convert_datetime(properties[key]))
                        else:
                            converted_properties.append(properties[key])
                    elif key == 'createdAt':
                        converted_properties.append(convert_datetime(record['createdAt']))
                    elif key == 'updatedAt':
                        converted_properties.append(convert_datetime(record['updatedAt']))
                    else:
                        converted_properties.append(record.get(key))
                records_to_insert.append(converted_properties)

            extras.execute_batch(cursor, query, records_to_insert)

        conn.commit()
    cursor.close()
    conn.close()

def process_endpoint(args):
    table_name, endpoint = args
    last_pull_state = get_last_pull_state()
    last_pull_info = last_pull_state.get(table_name, {})
    after = last_pull_info.get('after')
    max_timestamp = last_pull_info.get('timestamp')

    if max_timestamp:
        endpoint += f"&since={max_timestamp}"

    has_more = True

    while has_more:
        if after:
            endpoint_with_after = f"{endpoint}&after={after}"
        else:
            endpoint_with_after = endpoint
        print(f"Extracting data from {endpoint_with_after}")
        response = get_hubspot_data(endpoint_with_after)
        data = response.get('results', [])
        if data:
            insert_data_into_postgresql(data, table_name)
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
