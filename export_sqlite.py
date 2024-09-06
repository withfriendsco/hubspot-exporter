import os
import requests
import time
import sqlite3
import csv
import json
from dotenv import load_dotenv
import logging
import pickle

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Get HubSpot Personal Access Token from environment variable
ACCESS_TOKEN = os.getenv('HUBSPOT_ACCESS_TOKEN')

# Base URL for HubSpot API
BASE_URL = 'https://api.hubapi.com'

# Headers for API requests, including the personal access token
HEADERS = {
    'Authorization': f'Bearer {ACCESS_TOKEN}',
    'Content-Type': 'application/json'
}

# Helper function for API requests with retry logic and rate limiting
def make_request(url, method='GET', headers=None, params=None, json=None):
    """Make a request with retry logic for handling rate limits and timeouts."""
    max_retries = 5
    retry_delay = 5  # seconds
    for attempt in range(max_retries):
        try:
            logging.info(f"Making {method} request to {url}")
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params, timeout=10)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=json, timeout=10)
            response.raise_for_status()
            logging.info(f"Request successful: {response.status_code}")
            return response
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logging.warning(f"Error: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error(f"Max retries reached. Error: {e}")
                raise

def get_all_properties(object_type):
    """Fetch all properties for the given HubSpot object type."""
    logging.info(f"Fetching properties for {object_type}")
    url = f"{BASE_URL}/crm/v3/properties/{object_type}"
    response = make_request(url, headers=HEADERS)
    properties = [prop['name'] for prop in response.json().get('results', [])]
    logging.info(f"Fetched {len(properties)} properties for {object_type}")
    return properties

def fetch_associations(object_type, object_id, to_object_type):
    """Fetch associations between objects."""
    logging.info(f"Fetching associations for {object_type} {object_id} to {to_object_type}")
    url = f"{BASE_URL}/crm/v4/objects/{object_type}/{object_id}/associations/{to_object_type}"
    response = make_request(url, headers=HEADERS)
    associations = response.json().get('results', [])
    logging.info(f"Fetched {len(associations)} associations")
    return associations

def create_database():
    """Create SQLite database and tables with individual property columns."""
    logging.info("Creating/connecting to SQLite database")
    conn = sqlite3.connect('hubspot_data.db')
    c = conn.cursor()
    
    for object_type in ['companies', 'contacts', 'notes', 'tasks', 'calls']:
        logging.info(f"Creating table for {object_type}")
        properties = get_all_properties(object_type)
        columns = [f"{prop} TEXT" for prop in properties]
        c.execute(f'''CREATE TABLE IF NOT EXISTS {object_type}
                     (id TEXT PRIMARY KEY, {', '.join(columns)})''')
    
    logging.info("Creating associations table")
    c.execute('''CREATE TABLE IF NOT EXISTS associations
                 (from_object_type TEXT, from_object_id TEXT, 
                  to_object_type TEXT, to_object_id TEXT)''')
    
    conn.commit()
    logging.info("Database creation completed")
    return conn

def fetch_and_store_data(object_type, fetch_function, conn, limit=None):
    logging.info(f"Starting to fetch and store data for {object_type}")
    c = conn.cursor()
    checkpoint_file = f"{object_type}_checkpoint.pkl"
    completion_file = f"{object_type}_completed.txt"
    
    if os.path.exists(completion_file):
        logging.info(f"{object_type} data has already been fully fetched. Skipping.")
        return
    
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'rb') as f:
            after = pickle.load(f)
        logging.info(f"Resuming {object_type} from checkpoint: {after}")
    else:
        after = None
        logging.info(f"Starting new fetch for {object_type}")
    
    properties = get_all_properties(object_type)
    total_processed = 0
    last_id = None
    consecutive_same_id = 0
    
    while True:
        logging.info(f"Fetching batch of {object_type} after {after}")
        batch = fetch_function(after)
        if not batch:
            logging.info(f"No more {object_type} to fetch")
            break
        
        for item in batch:
            columns = ['id'] + properties
            values = [item['id']] + [item['properties'].get(prop, '') for prop in properties]
            placeholders = ', '.join(['?' for _ in columns])
            c.execute(f"INSERT OR REPLACE INTO {object_type} ({', '.join(columns)}) VALUES ({placeholders})", values)
        
        conn.commit()
        after = batch[-1]['id']
        
        # Check if we're stuck on the same ID
        if after == last_id:
            consecutive_same_id += 1
            if consecutive_same_id >= 3:
                logging.warning(f"Stuck on the same ID ({after}) for {consecutive_same_id} iterations. Breaking loop.")
                break
        else:
            consecutive_same_id = 0
        
        last_id = after
        total_processed += len(batch)
        logging.info(f"Processed {len(batch)} {object_type}. Total processed: {total_processed}")
        
        if limit and total_processed >= limit:
            logging.info(f"Reached limit of {limit} records for {object_type}")
            break
        
        # Save checkpoint
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(after, f)
    
    # Remove checkpoint file when done
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        logging.info(f"Removed checkpoint file for {object_type}")
    
    logging.info(f"Completed fetching and storing data for {object_type}. Total processed: {total_processed}")
    
    # Mark as completed
    with open(completion_file, 'w') as f:
        f.write(f"Completed on {time.strftime('%Y-%m-%d %H:%M:%S')}")

def fetch_and_store_associations(object_type, conn, limit=None):
    logging.info(f"Starting to fetch and store associations for {object_type}")
    c = conn.cursor()
    checkpoint_file = f"{object_type}_associations_checkpoint.pkl"
    
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'rb') as f:
            start_index = pickle.load(f)
        logging.info(f"Resuming {object_type} associations from checkpoint: {start_index}")
    else:
        start_index = 0
        logging.info(f"Starting new association fetch for {object_type}")
    
    c.execute(f"SELECT id FROM {object_type}")
    object_ids = c.fetchall()
    total_objects = len(object_ids)
    
    for index, (object_id,) in enumerate(object_ids[start_index:], start=start_index):
        logging.info(f"Fetching associations for {object_type} {object_id} ({index + 1}/{total_objects})")
        
        # Define the association types based on the object_type
        if object_type == 'companies':
            association_types = ['contacts']
        elif object_type in ['notes', 'tasks', 'calls']:
            association_types = ['companies', 'contacts']
        else:  # contacts
            association_types = []
        
        for assoc_type in association_types:
            associations = fetch_associations(object_type, object_id, assoc_type)
            for assoc in associations:
                associated_id = assoc.get('id') or assoc.get('toObjectId') or 'unknown'
                c.execute("INSERT INTO associations VALUES (?, ?, ?, ?)",
                          (object_type, object_id, assoc_type, associated_id))
        
        conn.commit()
        logging.info(f"Processed associations for {object_type} {object_id}")
        
        if limit and index + 1 >= limit:
            logging.info(f"Reached limit of {limit} records for {object_type} associations")
            break
        
        # Save checkpoint
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(index + 1, f)
    
    logging.info(f"Completed fetching and storing associations for {object_type}")

def export_to_csv(conn, table_name):
    logging.info(f"Starting export of {table_name} to CSV")
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table_name}")
    
    with open(f"{table_name}-sqlite-version.csv", 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([description[0] for description in c.description])  # Write headers
        csv_writer.writerows(c)
    
    logging.info(f"Exported {table_name} to CSV")

def fetch_companies(after=None):
    """Fetch a batch of companies."""
    properties = get_all_properties('companies')
    url = f"{BASE_URL}/crm/v3/objects/companies"
    params = {
        'properties': ','.join(properties),
        'limit': 100,
        'after': after
    }
    response = make_request(url, method='GET', headers=HEADERS, params=params)
    result = response.json()
    return result.get('results', [])

def fetch_contacts(after=None):
    """Fetch a batch of contacts."""
    properties = get_all_properties('contacts')
    url = f"{BASE_URL}/crm/v3/objects/contacts"
    params = {
        'properties': ','.join(properties),
        'limit': 100,
        'after': after
    }
    response = make_request(url, method='GET', headers=HEADERS, params=params)
    result = response.json()
    return result.get('results', [])

def fetch_notes(after=None):
    """Fetch a batch of notes."""
    properties = get_all_properties('notes')
    url = f"{BASE_URL}/crm/v3/objects/notes"
    params = {
        'properties': ','.join(properties),
        'limit': 100,
        'after': after
    }
    response = make_request(url, method='GET', headers=HEADERS, params=params)
    result = response.json()
    return result.get('results', [])

def fetch_tasks(after=None):
    """Fetch a batch of tasks."""
    properties = get_all_properties('tasks')
    url = f"{BASE_URL}/crm/v3/objects/tasks"
    params = {
        'properties': ','.join(properties),
        'limit': 100,
        'after': after
    }
    response = make_request(url, method='GET', headers=HEADERS, params=params)
    result = response.json()
    return result.get('results', [])

def fetch_calls(after=None):
    """Fetch a batch of calls."""
    properties = get_all_properties('calls')
    url = f"{BASE_URL}/crm/v3/objects/calls"
    params = {
        'properties': ','.join(properties),
        'limit': 100,
        'after': after
    }
    response = make_request(url, method='GET', headers=HEADERS, params=params)
    result = response.json()
    return result.get('results', [])

def main():
    logging.info("Starting main process")
    conn = create_database()

    # Fetch and store data
    for object_type in ['companies', 'contacts', 'notes', 'tasks', 'calls']:
        logging.info(f"Processing {object_type}")
        fetch_function = globals()[f"fetch_{object_type}"]
        fetch_and_store_data(object_type, fetch_function, conn)

    # Fetch and store associations
    for object_type in ['companies', 'notes', 'tasks', 'calls']:
        logging.info(f"Processing associations for {object_type}")
        fetch_and_store_associations(object_type, conn)

    # Export to CSV
    for table_name in ['companies', 'contacts', 'notes', 'tasks', 'calls', 'associations']:
        logging.info(f"Exporting {table_name} to CSV")
        export_to_csv(conn, table_name)

    # Remove all checkpoint and completion files after successful completion
    for object_type in ['companies', 'contacts', 'notes', 'tasks', 'calls']:
        data_checkpoint = f"{object_type}_checkpoint.pkl"
        assoc_checkpoint = f"{object_type}_associations_checkpoint.pkl"
        completion_file = f"{object_type}_completed.txt"
        for file in [data_checkpoint, assoc_checkpoint, completion_file]:
            if os.path.exists(file):
                os.remove(file)
                logging.info(f"Removed {file}")

    conn.close()
    logging.info("Main process completed")

def test_run():
    logging.info("Starting test run")
    conn = create_database()
    test_limit = 50

    # Fetch and store data
    for object_type in ['companies', 'contacts', 'notes', 'tasks', 'calls']:
        logging.info(f"Processing {object_type} (test run)")
        fetch_function = globals()[f"fetch_{object_type}"]
        fetch_and_store_data(object_type, fetch_function, conn, limit=test_limit)

    # Fetch and store associations
    for object_type in ['companies', 'notes', 'tasks', 'calls']:
        logging.info(f"Processing associations for {object_type} (test run)")
        fetch_and_store_associations(object_type, conn, limit=test_limit)

    # Export to CSV
    for table_name in ['companies', 'contacts', 'notes', 'tasks', 'calls', 'associations']:
        logging.info(f"Exporting {table_name} to CSV (test run)")
        export_to_csv(conn, table_name)

    conn.close()
    logging.info("Test run completed")

if __name__ == "__main__":
    main()