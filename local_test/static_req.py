import requests
import json
import os
import logging
import time
import re
import pyodbc
from dotenv import load_dotenv

# Dictionary containing table mappings
from mappings import TABLE_MAPPINGS

# Load environment variables
load_dotenv()

# Configure logging (stores logs in a file and prints to console)
LOG_FILE = "api_process.log"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Configuration
AUTH_URL = os.getenv("AUTH_URL")
DATA_API_URL = os.getenv("DATA_API_URL")
SUBJECT = os.getenv("SUBJECT")
SECRET = os.getenv("SECRET")
START_DATE = "2025-01-01T00:00:00Z"
END_DATE = "2025-01-30T00:00:00Z"
OUTPUT_FILE = "booking_data_1M.json"

# SQL Server Connection Details
SERVER = 'Pri-Yoga'
DATABASE = 'MCIS_BOOKING_TEST'
# username = 'mcis\Priyanshu.Shah'
# password = '$u5ZK%Ecvi'
DRIVER = '{ODBC Driver 17 for SQL Server}'

# Track already fetched URLs
fetched_urls = set()

def authenticate(auth_url, subject, secret):
    """Authenticate and return access token."""
    logging.info("Authenticating...")
    auth_payload = {"subject": subject, "secret": secret}
    
    try:
        response = requests.post(auth_url, json=auth_payload)
        response.raise_for_status()
        auth_data = response.json()
        logging.info(f"Authentication successful. Token: {auth_data.get('accessToken')}")
        return auth_data.get("accessToken")
    except requests.RequestException as e:
        logging.error(f"Authentication failed: {e}")
        raise

def fetch_data(url, headers, payload=None):
    """Fetch data from API once, skipping duplicates."""
    global fetched_urls

    # Skip already fetched URLs
    if url in fetched_urls:
        logging.debug(f"Skipping already fetched URL: {url}")
        return None

    try:
        response = requests.get(url, headers=headers, json=payload) if payload else requests.get(url, headers=headers)
        if response.status_code == 200:
            fetched_urls.add(url)  # Mark as fetched
            return response.json()
        else:
            logging.error(f"Failed to fetch {url}: {response.status_code} - {response.text}")
            return None
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def fetch_booking_data(api_url, access_token, start_date, end_date):
    """Fetch all booking data using payload and pagination."""
    logging.info("Fetching booking data...")
    
    headers = {"Authorization": f"Bearer {access_token}"}
    all_bookings = []
    page = 1

    while api_url:
        logging.info(f"Fetching page {page} from {api_url}...")

        data_payload = {
            "expectedStartDateGTE": start_date,
            "expectedStartDateLT": end_date
        }
        data = fetch_data(api_url, headers, payload=data_payload)

        if data:
            all_bookings.extend(data.get("items", []))
            logging.debug(f"Fetched {len(data.get('items', []))} bookings from page {page}")
            api_url = data.get("next", None)  # Move to the next page
        else:
            logging.error(f"Failed to fetch data from {api_url}, stopping pagination.")
            break

        page += 1
        time.sleep(1)
    
    logging.info(f"Total bookings fetched: {len(all_bookings)}")
    return all_bookings

def extract_uris(item):
    """Extract all inner API URIs from a single booking item."""
    extracted_uris = {}

    for key, value in item.items():
        if isinstance(value, dict) and "uri" in value:
            extracted_uris[key] = value["uri"]
            logging.debug(f"Extracted URI for {key}: {value['uri']}")

    return extracted_uris

def fetch_additional_details(item, access_token):
    """Fetch all inner URLs and append details to their corresponding place."""
    booking_id = item["id"]
    logging.info(f"Fetching additional details for booking ID {booking_id}...")

    extracted_uris = extract_uris(item)
    additional_details = {}
    headers = {"Authorization": f"Bearer {access_token}"}

    for key, url in extracted_uris.items():
        # Skip already fetched inner APIs but continue with others
        if url in fetched_urls:
            logging.debug(f"Skipping already fetched inner API: {url}")
            continue  

        logging.debug(f"Fetching data from {url}")
        extra_data = fetch_data(url, headers)
        if extra_data:
            additional_details[key] = extra_data
            logging.debug(f"Fetched data for {key}")
        else:
            logging.warning(f"No data returned for {key}")

        time.sleep(0.5)  # Avoid overwhelming API

    return additional_details

def process_bookings(access_token):
    """Main function to fetch all bookings and append extra details."""
    bookings = fetch_booking_data(DATA_API_URL, access_token, START_DATE, END_DATE)
    detailed_bookings = []

    for item in bookings:
        logging.info(f"Processing booking ID: {item['id']}")
        item["extra_details"] = fetch_additional_details(item, access_token)
        detailed_bookings.append(item)

    return detailed_bookings

def save_json_to_file(data, filename):
    """Save JSON data to a file."""
    logging.info(f"Saving JSON data to {filename}...")
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    logging.info(f"Data saved successfully to {filename}")

# ---------------------------------------------
# Function to establish SQL Server connection
def get_db_connection():
    conn = pyodbc.connect(
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return conn, conn.cursor()

# Function to load JSON data
def load_json_data(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

# Dictionary containing table mappings (imported from previous step)
from mappings import TABLE_MAPPINGS

# Helper function for safe retrieval of values
def get_value(obj, key, default=None):
    keys = key.split('.')
    for k in keys:
        if isinstance(obj, dict) and k in obj:
            obj = obj[k]
        else:
            return default
    return obj

# Check if a record exists
def record_exists(cursor, table_name, primary_key, primary_key_value):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {primary_key} = ?", (primary_key_value,))
    return cursor.fetchone()[0] > 0

# Extract all table-related elements from JSON
def extract_table_elements(json_data):
    extracted_data = {table: set() for table in TABLE_MAPPINGS}
    
    def recursive_search(data):
        if isinstance(data, dict):
            for key, value in data.items():
                if key in TABLE_MAPPINGS and isinstance(value, dict) and "id" in value:
                    extracted_data[key].add(tuple(value.items()))
                recursive_search(value)
        elif isinstance(data, list):
            for item in data:
                recursive_search(item)
    
    recursive_search(json_data)
    return {table: [dict(item) for item in extracted_data[table]] for table in extracted_data}

# Insert data into any table dynamically, skipping existing records
def bulk_insert(cursor, conn, table_name, records, primary_key):
    if table_name not in TABLE_MAPPINGS:
        logging.warning(f"Skipping {table_name}: No mapping found.")
        return
    
    mapping = TABLE_MAPPINGS[table_name]
    sql_columns = list(mapping.keys())
    json_keys = list(mapping.values())
    
    sql_insert = f"INSERT INTO {table_name} ({', '.join(sql_columns)}) SELECT {', '.join(['?' for _ in sql_columns])} WHERE NOT EXISTS (SELECT 1 FROM {table_name} WHERE {primary_key} = ?);"
    
    values_to_insert = []
    
    for record in records:
        primary_key_value = get_value(record, mapping.get(primary_key))
        
        # Skip inserting if the primary key is NULL
        if primary_key_value is None:
            logging.warning(f"Skipping {table_name} record due to missing primary key: {record}")
            continue
        
        # Convert nested JSON objects to string format
        row_values = [json.dumps(get_value(record, json_key)) if isinstance(get_value(record, json_key), dict) else get_value(record, json_key) for json_key in json_keys]
        row_values.append(primary_key_value)  # Add primary key for WHERE NOT EXISTS check
        values_to_insert.append(tuple(row_values))
    
    try:
        if values_to_insert:
            cursor.executemany(sql_insert, values_to_insert)
            conn.commit()
            logging.info(f"Inserted {len(values_to_insert)} records into {table_name} successfully.")
    except pyodbc.IntegrityError as e:
        logging.error(f"Foreign Key or Duplicate Key Error in {table_name}: {e}")
    except Exception as e:
        logging.error(f"Error inserting into {table_name}: {e}")

# Process each table in correct order to handle foreign key dependencies
def process_data(json_data, cursor, conn):
    logging.info("Starting data import...")
    
    # Insert dependencies first
    ordered_tables = ["Locations", "ContractTypes", "Languages", "Customers", "Companies", "Clients", "BookingModes", "Status"]
    for table in ordered_tables:
        if table in TABLE_MAPPINGS:
            bulk_insert(cursor, conn, table, json_data, list(TABLE_MAPPINGS[table].keys())[0])
    
    # Insert main Bookings table and remaining tables
    for table, mapping in TABLE_MAPPINGS.items():
        if table not in ordered_tables:
            bulk_insert(cursor, conn, table, json_data, list(mapping.keys())[0])
    
    logging.info("Data import completed successfully.")



# ---------------------------------------------
# Execution
if __name__ == "__main__":
    try:
        logging.info("Starting process...")
        # Authenticate and fetch data
        # access_token = authenticate(AUTH_URL, SUBJECT, SECRET)
        # all_data = process_bookings(access_token)
        
        # Save data to a file
        # save_json_to_file(all_data, OUTPUT_FILE)
        
        # SQL connection & Load data from file
        conn, cursor = get_db_connection()
        json_data = load_json_data("sample_records.json")
        process_data(json_data, cursor, conn)
        cursor.close()
        conn.close()
        logging.info("Database connection closed.")
        logging.info("Process completed successfully.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")