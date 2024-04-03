import re
import json
import requests
import pandas as pd
import sys
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

"""
# Instructions
Run $ python3 main.py start_date end_date

Example of date
Start Date: 2022-01-01
End Date: 2022-02-01
Will fill all Jan. data, exluding 02-01

# Assumptions
No more than 5000 records per 6-hour interval
"""

""""
to-do
* partitions
"""

# Config
API_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/messari/aave-v3-ethereum'
DATASET = 'aave'
TABLE_LIST = ['aave-v3-deposits']
INTERVAL_HOURS = 6
NAME_MAP = {
    "timestamp": "block_timestamp"
}

# Util
def flatten_json(data, prefix=''):
    return {
        f"{prefix}_{k}" if prefix else k: v
        for k, v in data.items()
        for k, v in (flatten_json(v, f"{prefix}.{k}" if prefix else k).items() if isinstance(v, dict) else [(k, v)])
    }

def flatten_list_of_json(data_list):
    return [flatten_json(item) for item in data_list]

def get_operation_name(query):
    match = re.search(r'{\s*(\w+)', query)
    if match:
        return match.group(1)
    else:
        return None

def epoch_to_utc(epoch_time):
    utc_time = datetime.utcfromtimestamp(epoch_time)
    return utc_time

def utc_to_epoch(utc_time):
    epoch_time = int(utc_time.replace(tzinfo=timezone.utc).timestamp())
    return epoch_time

def date_to_epoch(date_string):
    dt = datetime.strptime(date_string, "%Y-%m-%d")
    epoch_time = int(dt.replace(tzinfo=timezone.utc).timestamp())
    return epoch_time


def json_map_names(name_map, json_list):
    # Parse the JSON list if it's in string format
    data = json.loads(json_list) if isinstance(json_list, str) else json_list
    
    # Check if the list is empty
    if not data:
        return data
    
    # Iterate through each item in the list
    for item in data:
        for key, value in list(item.items()):
            # Check if the key is in the name_map
            if key in name_map:
                # Map the name and update the dictionary
                new_key = name_map[key]
                item[new_key] = item.pop(key)
    
    # Convert back to JSON string if needed
    return data  # Or return data if you want the Python list of dicts


# Body
def get_data_from_api(table_name,start_date,end_date,url=API_ENDPOINT,interval_hours = INTERVAL_HOURS):
    """
    Call the API and return the data.

    Args:
        table_name (str): The name of the table.

    Returns:
        dict: The data returned from the API.
    """
    with open(f'queries/{table_name}_query.graphql', 'r') as file:
        raw_query = file.read()

    query_head_name = get_operation_name(raw_query)

    start_timestamp = date_to_epoch(start_date)
    end_timestamp = date_to_epoch(end_date)

    # Convert seconds to timedelta for 1 hour
    time_step = timedelta(hours=interval_hours)

    # Initialize variables for the loop
    current_start_timestamp = start_timestamp
    all_data = []

    while current_start_timestamp < end_timestamp:
        # Calculate the end timestamp for the current 1-hour interval
        current_end_timestamp = utc_to_epoch(epoch_to_utc(current_start_timestamp) + time_step) 

        # Make sure we don't go beyond the overall end timestamp
        if current_end_timestamp > end_timestamp:
            current_end_timestamp = end_timestamp

        for inner_epoch in range(5):  # Up to 5 inner-epochs to respect the skip limit
            print(f"Epoch {inner_epoch} for interval {datetime.utcfromtimestamp(current_start_timestamp).\
                    strftime('%Y-%m-%d %H:%M:%S')} UTC to {datetime.utcfromtimestamp(current_end_timestamp).strftime('%Y-%m-%d %H:%M:%S')} UTC")

            # Adjust the GraphQL query for pagination, current time interval, and inner epoch
            query = raw_query % (inner_epoch * 1000, current_start_timestamp, current_end_timestamp)

            headers = {"Content-Type": "application/json"}
            response = requests.post(url, json={"query": query}, headers=headers)

            if response.status_code == 200:
                faltten_data = flatten_list_of_json(
                                response.json().get('data', {}).get(query_head_name, [])
                                )
                data = json_map_names(NAME_MAP,faltten_data)

                if not data:
                    # No more data to fetch in this inner epoch, break and move to the next interval
                    break

                all_data.extend(data)

                # If less than 1000 items were returned, it means we've likely fetched all items for this interval
                if len(data) < 1000:
                    break
            else:
                print(f"Failed to fetch data: HTTP {response.status_code}")
                break

        # Move to the next 1-hour interval
        current_start_timestamp = current_end_timestamp
        
    return all_data

def load_data_into_bigquery(table_name, data):
    """Load data into BigQuery, creating the table if it doesn't exist."""
    with open(f'schemas/{table_name}_schema.json', 'r') as file:
        schema_def = json.load(file)

    schema = [bigquery.SchemaField(field['name'], 
                                    field['type'], 
                                    mode=field.get('mode', 'NULLABLE'), 
                                    description=field.get('description', None)) for field in schema_def]

    # Create time partitioning
    time_partitioning = bigquery.TimePartitioning(field='block_timestamp')

    client = bigquery.Client()
    dataset_ref = client.dataset(DATASET)
    table_ref = dataset_ref.table(table_name)

    try:
        client.get_table(table_ref)  # Check if table exists
    except NotFound:
        # If the table does not exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = time_partitioning  # Set the time partitioning property separately
        client.create_table(table)
        print(f"Table {table_name} created.")

    job_config = bigquery.LoadJobConfig(schema=schema, time_partitioning=time_partitioning)
    job = client.load_table_from_json(data, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Data loaded into table {table_name}.")


# Main 
def main(start_date, end_date):
    for table_name in TABLE_LIST:  # List all your tables here
        data = get_data_from_api(table_name, start_date, end_date)
        load_data_into_bigquery(table_name, data)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
