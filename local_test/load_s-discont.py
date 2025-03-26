import pyodbc
import json
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Logging
logging.basicConfig(filename="import_log.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

server = 'Pri-Yoga'
database = 'MCIS_BOOKING_TEST'
driver = '{ODBC Driver 17 for SQL Server}'

# Function to establish SQL Server connection using environment variables
def get_db_connection():
    conn = pyodbc.connect(
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return conn, conn.cursor()

# Function to load JSON data
def load_json_data(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

# Import table mappings
from mappings import TABLE_MAPPINGS

# Helper function for safe retrieval of values
def get_value(obj, key, default=None, max_length=None):
    keys = key.split('.')
    for k in keys:
        if isinstance(obj, dict) and k in obj:
            obj = obj[k]
        else:
            return default
    
    if isinstance(obj, str) and max_length:
        return obj[:max_length]  # Truncate long strings to fit column size
    return obj

# Extract all table-related elements from JSON recursively
def extract_table_elements(json_data, table_name):
    """
    Extracts records for a given table from the JSON structure.
    Handles both direct and nested keys like 'billingCustomer.id'.
    """
    records = []

    def recursive_extract(data, table_name, parent_key=""):
        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{parent_key}.{key}" if parent_key else key
                
                # Match table name to both normal & dot notation keys
                if table_name.lower() in key.lower() or table_name.lower() in full_key.lower():
                    if isinstance(value, dict) and "id" in value:
                        records.append(value)

                recursive_extract(value, table_name, full_key)
        elif isinstance(data, list):
            for index, item in enumerate(data):
                recursive_extract(item, table_name, f"{parent_key}[{index}]")

    recursive_extract(json_data, table_name)
    
    logging.info(f"Extracted {len(records)} records for {table_name}")
    print(f"Extracted {len(records)} records for {table_name}")  # Debugging print

    return records

# Check if a record exists
def record_exists(cursor, table, primary_key, value):
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {primary_key} = ?", (value,))
    return cursor.fetchone()[0] > 0

# Insert foreign key records first
def insert_foreign_key_records(cursor, conn, json_data):
    fk_tables = ["ContractTypes", "Customers", "Companies", "Languages"]
    for table in fk_tables:
        records = extract_table_elements(json_data, table)
        bulk_insert_skip_existing(cursor, conn, table, records, "id")

# Insert data into any table dynamically, skipping existing records
def bulk_insert_skip_existing(cursor, conn, table_name, records, primary_key, column_size_limits={}):
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
        
        if primary_key_value is None or record_exists(cursor, table_name, primary_key, primary_key_value):
            logging.warning(f"Skipping {table_name} record due to missing or duplicate primary key: {record}")
            continue
        
        row_values = [
            json.dumps(get_value(record, json_key, max_length=column_size_limits.get(json_key))) 
            if isinstance(get_value(record, json_key), dict) else get_value(record, json_key, max_length=column_size_limits.get(json_key)) 
            for json_key in json_keys
        ]
        row_values.append(primary_key_value)
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

# Validate foreign key dependencies before inserting dependent tables
def validate_foreign_keys(table, data, cursor):
    if table == "Customers":
        return [row for row in data if record_exists(cursor, "ContractTypes", "contract_type_id", row.get("contract_type_id"))]
    elif table == "Bookings":
        return [row for row in data if record_exists(cursor, "Customers", "customer_id", row.get("billing_customer_id"))]
    return data

# Process each table in correct order to handle foreign key dependencies
def process_data(json_data, cursor, conn):
    logging.info("Starting data import...")
    
    # Step 1: Insert foreign key tables first
    insert_foreign_key_records(cursor, conn, json_data)

    # Step 2: Extract all table-related elements from JSON
    extracted_data = {table: extract_table_elements(json_data, table) for table in TABLE_MAPPINGS}
    
    # Log extracted data before insertion
    for table, records in extracted_data.items():
        logging.info(f"Extracted {len(records)} records for {table}.")

    # Step 3: Insert extracted elements first
    for table, records in extracted_data.items():
        bulk_insert_skip_existing(cursor, conn, table, records, "id")

    # Step 4: Define Column Size Limits
    column_size_limits = {
        "contact_rate_plan": 255,
        "payment_method": 255,
    }

    # Step 5: Insert remaining JSON data while validating foreign keys
    for table, mapping in TABLE_MAPPINGS.items():
        if table not in extracted_data:
            valid_data = validate_foreign_keys(table, json_data, cursor)
            bulk_insert_skip_existing(cursor, conn, table, valid_data, list(mapping.keys())[0], column_size_limits)
    
    logging.info("Data import completed successfully.")

# Main Execution
if __name__ == "__main__":
    conn, cursor = get_db_connection()
    json_data = load_json_data("booking_data_flat.json")
    process_data(json_data, cursor, conn)
    cursor.close()
    conn.close()
    logging.info("Database connection closed.")
