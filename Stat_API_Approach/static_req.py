import requests
import json
import os
import logging
import time
import re
import pyodbc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging (stores logs in a file and prints to console)
LOG_FILE = "api_process_long.log"
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
OUTPUT_FILE = "booking_data_long.json"

# SQL Server Connection Details
server = 'YOUR_SERVER'
database = 'YOUR_DATABASE'
username = 'YOUR_USERNAME'
password = 'YOUR_PASSWORD'
driver = '{ODBC Driver 17 for SQL Server}'

#DB Mapping
MAPPING = {
    "Bookings": {
        "booking_id": "id",
        "uri": "uri",
        "version_value": "versionValue",
        "access_code": "accessCode",
        "accounting_reference": "accountingReference",
        "actual_duration_hrs": "actualDurationHrs",
        "actual_duration_mins": "actualDurationMins",
        "actual_start_date": "actualStartDate",
        "actual_end_date": "actualEndDate",
        "actual_location_display": "actualLocationDisplayLabel",
        "assigned_by": "assignedBy",
        "assigned_by_username": "assignedByUsername",
        "assignment_date": "assignmentDate",
        "booking_date": "bookingDate",
        "booking_time": "bookingTime",
        "cancellation_date": "cancellationDate",
        "cancellation_reason": "cancellationReason",
        "company_special_instructions": "companySpecialInstructions",
        "confirmation_date": "confirmationDate",
        "created_by": "createdBy",
        "created_date": "createdDate",
        "expected_start_date": "expectedStartDate",
        "expected_end_date": "expectedEndDate",
        "status_id": ("status", "id"),
        "billing_customer_id": ("billingCustomer", "id"),
        "billing_location_id": ("billingLocation", "id"),
        "booking_mode_id": ("bookingMode", "id"),
        "client_id": ("client", "id"),
        "company_id": ("company", "id"),
        "customer_id_fk": ("customer", "id"),
        "default_language_id": ("defaultLanguage", "id"),
        "employment_category_id": ("employmentCategory", "id"),
        "invoice_status_id": ("invoiceStatus", "id"),
        "interpreter_id": ("interpreter", "id"),
        "language_id": ("language", "id"),
        "local_booking_mode_id": ("localBookingMode", "id"),
        "location_id_fk": ("actualLocation", "id"),
        "overflow_type_id": ("overflowType", "id"),
        "payment_status_id": ("paymentStatus", "id"),
        "preferred_interpreter_id": ("preferredInterpreter", "id"),
        "primary_ref_id": ("primaryRef", "id"),
        "super_booking_id": ("superBooking", "id"),
        "requestor_id": ("requestor", "id"),
        "visit_id": ("visit", "id")
    },
    "Locations": {
        "location_id": "actualLocation.id",
        "uri": "actualLocation.uri",
        "active": "actualLocation.active",
        "addr_entered": "actualLocation.addrEntered",
        "cost_center": "actualLocation.costCenter",
        "cost_center_name": "actualLocation.costCenterName",
        "description": "actualLocation.description",
        "display_label": "actualLocation.displayLabel",
        "lat": "actualLocation.lat",
        "lng": "actualLocation.lng",
        "uuid": "actualLocation.uuid"
    },
    "BookingModes": {
        "booking_mode_id": "bookingMode.id",
        "uri": "bookingMode.uri",
        "description": "bookingMode.description",
        "name": "bookingMode.name",
        "name_key": "bookingMode.nameKey"
    },
    "Clients": {
        "client_id": "client.id",
        "uri": "client.uri",
        "display_name": "client.displayName",
        "name": "client.name",
        "uuid": "client.uuid"
    },
    "Companies": {
        "company_id": "company.id",
        "uri": "company.uri",
        "description": "company.description",
        "name": "company.name",
        "uuid": "company.uuid"
    },
    "ContractTypes": {
        "contract_type_id": "billingCustomer.contractType.id",
        "uri": "billingCustomer.contractType.uri",
        "description": "billingCustomer.contractType.description",
        "name": "billingCustomer.contractType.name"
    },
    "Customers": {
        "customer_id": "billingCustomer.id",
        "uri": "billingCustomer.uri",
        "display_name": "billingCustomer.displayName",
        "name": "billingCustomer.name",
        "uuid": "billingCustomer.uuid",
        "contract_type_id": ("billingCustomer.contractType", "id")
    },
    "Languages": {
        "language_id": "defaultLanguage.id",
        "uri": "defaultLanguage.uri",
        "description": "defaultLanguage.description",
        "display_name": "defaultLanguage.displayName",
        "iso639_3_tag": "defaultLanguage.iso639_3Tag"
    },
    "EmploymentCategories": {
        "employment_category_id": "employmentCategory.id",
        "uri": "employmentCategory.uri",
        "description": "employmentCategory.description",
        "name": "employmentCategory.name"
    },
    "InvoiceStatuses": {
        "invoice_status_id": "invoiceStatus.id",
        "uri": "invoiceStatus.uri",
        "description": "invoiceStatus.description",
        "name": "invoiceStatus.name"
    },
    "PaymentStatuses": {
        "payment_status_id": "paymentStatus.id",
        "uri": "paymentStatus.uri",
        "description": "paymentStatus.description",
        "name": "paymentStatus.name"
    },
    "OverflowTypes": {
        "overflow_type_id": "overflowType.id",
        "uri": "overflowType.uri",
        "description": "overflowType.description"
    },
    "SuperBookings": {
        "super_booking_id": "superBooking.id",
        "uri": "superBooking.uri",
        "uuid": "superBooking.uuid"
    },
    "Status": {
        "status_id": "status.id",
        "uri": "status.uri",
        "description": "status.description",
        "name": "status.name"
    },
    "Requestors": {
        "requestor_id": "requestor.id",
        "uri": "requestor.uri",
        "display_label": "requestor.displayLabel",
        "display_name": "requestor.displayName",
        "name": "requestor.name",
        "uuid": "requestor.uuid",
        "email": "requestor.email"
    },
    "Visits": {
        "visit_id": "visit.id",
        "uri": "visit.uri",
        "contact_rate_plan": "visit.contactRatePlan",
        "customer_rate_plan": "visit.customerRatePlan",
        "uuid": "visit.uuid",
        "visit_status_id": ("visit.status", "id")
    },
    "Interpreters": {
        "interpreter_id": "interpreter.id",
        "uri": "interpreter.uri",
        "display_name": "interpreter.displayName",
        "name": "interpreter.name",
        "email": "interpreter.primaryEmail.emailAddress",
        "phone": "interpreter.primaryNumber.parsedNumber",
        "time_zone": "interpreter.timeZone",
        "uuid": "interpreter.uuid"
    },
    "Refs": {
        "ref_id": "primaryRef.id",
        "uri": "primaryRef.uri",
        "description": "primaryRef.description",
        "name": "primaryRef.name",
        "ref_value": "primaryRef.ref"
    },
    "BookingRefs": {
        "booking_id": "id",
        "ref_id": ("primaryRef", "id")
    }
}


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

# Connect to SQL Server
def connect_to_db():
    try:
        conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
        logging.info("Connected to SQL Server successfully.")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

# Load JSON data
def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logging.info(f"Loaded JSON data from {file_path}")
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logging.error(f"Failed to load JSON: {e}")
        return None

# Extract mapped values from JSON
def extract_values(mapping, json_data):
    extracted_data = []
    for record in json_data:
        row = {}
        for sql_col, json_key in mapping.items():
            if isinstance(json_key, tuple):  # Handle nested JSON keys
                json_obj, nested_key = json_key
                row[sql_col] = record.get(json_obj, {}).get(nested_key, None)
            else:
                row[sql_col] = record.get(json_key, None)
        extracted_data.append(row)
    return extracted_data

# Bulk insert with duplicate handling
def bulk_insert(cursor, table_name, data):
    if not data:
        return

    columns = ", ".join(data[0].keys())
    placeholders = ", ".join(["?" for _ in data[0]])
    
    # Use MERGE to handle duplicates
    merge_query = f"""
    MERGE INTO {table_name} AS target
    USING (SELECT {', '.join(['?' for _ in data[0]])}) AS source ({columns})
    ON target.{list(data[0].keys())[0]} = source.{list(data[0].keys())[0]}
    WHEN MATCHED THEN
        UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in data[0].keys()])}
    WHEN NOT MATCHED THEN
        INSERT ({columns}) VALUES ({placeholders});
    """

    values = [tuple(row.values()) for row in data]

    try:
        cursor.executemany(merge_query, values)
        logging.info(f"Inserted/Updated {len(values)} records into {table_name}.")
    except Exception as e:
        logging.error(f"Error inserting into {table_name}: {e}")

# Process JSON and insert data dynamically
def process_and_insert_data(file_path):
    conn = connect_to_db()
    if not conn:
        return
    
    cursor = conn.cursor()
    bookings = load_json(file_path)
    if not bookings:
        return

    # Insert into each table dynamically
    for table, mapping in MAPPING.items():
        try:
            data = extract_values(mapping, bookings)
            bulk_insert(cursor, table, data)
        except Exception as e:
            logging.error(f"Error processing table {table}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Database operations completed.")

# Execution
if __name__ == "__main__":
    try:
        logging.info("Starting process...")
        access_token = authenticate(AUTH_URL, SUBJECT, SECRET)
        all_data = process_bookings(access_token)
        save_json_to_file(all_data, OUTPUT_FILE)
        
        process_and_insert_data(OUTPUT_FILE)
        logging.info("Process completed successfully.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
