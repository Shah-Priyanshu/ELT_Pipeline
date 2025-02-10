import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv
import logging
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Current time
current_time = time.strftime("%Y-%m-%d %H:%M:%S")

# Configuration
AUTH_URL = os.getenv("AUTH_URL")
DATA_API_URL = os.getenv("DATA_API_URL")
SUBJECT = os.getenv("SUBJECT")
SECRET = os.getenv("SECRET")
START_DATE = "2024-07-02T15:00:00Z"
END_DATE = "2024-12-30T15:00:00Z"
OUTPUT_FILE = f"booking_data_20.json"
EXCEL_FILE = f"API_Report_20.xlsx"

# Define JSON-to-Excel mapping
json_to_excel_mapping = {
    "id": "job # (id)",
    "booking.id": "booking # (id)",
    "teamId": "team # (id)",
    "createdDate": "created date/time",
    "createdBy": "created by",
    "originOfRequest": "origin of request",
    "assignedBy": "assigned by",
    "assignedByUsername": "assigned by username",
    "expectedStartDate": "job date",
    "expectedStartTime": "job time",
    "expectedEndDate": "expected end date",
    "expectedEndTime": "expected end time",
    "actualStartDate": "actual start date",
    "actualStartTime": "actual start time",
    "actualEndDate": "actual end date",
    "actualEndTime": "actual end time",
    "genderRequirement": "interpreter gender required",
    "company.name": "organization",
    "customer.displayName": "customer",
    "customer.accountingReference": "customer accounting reference",
    "customer.contractType.name": "customer contract type",
    "client.displayName": "client",
    "consumerCount": "number consumers",
    "location.displayLabel": "place of appointment",
    "defaultLanguage.displayName": "language",
    "preferredInterpreter.displayName": "preferred interpreter",
    "preferredInterpreter.primaryNumber.parsedNumber": "interpreter phone",
    "preferredInterpreter.primaryEmail.emailAddress": "interpreter email",
    "preferredInterpreter.paymentMethod.name": "payment method",
    "status.name": "status",
    "invoiceStatus.name": "invoice status",
    "paymentStatus.name": "payment status",
    "expectedDurationHrs": "duration (hrs)",
    "expectedDurationMins": "estimated duration (hrs)",
    "billingCustomer.displayName": "billing customer",
    "contactRatePlan.description": "contact rate plan",
    "customerRatePlan.description": "customer rate plan",
    "incidentals": "incidentals",
    "visitDetails": "visit details",
    "visitNotes": "visit notes",
    "invoiceNumber": "invoice number",
    "mileage": "mileage",
    "vosRequired": "vos required",
    "requestor.displayLabel": "requested by",
    "notes": "appointment details",
    "billingNotes": "billing notes",
    "interpreterNotes": "interpreter notes",
    
    # Placeholder mappings for unmatched Excel columns
    "expected_start_date_": "expected start date",
    "expected_start_time_": "expected start time",
    "interpreter_arrival_date_": "interpreter arrival date",
    "interpreter_arrival_time_": "interpreter arrival time",
    # "customer_account_executive_": "customer account executive",
    # "customer_billing_account_": "customer billing account",
    # "customer_business_unit_": "customer business unit",
    "billable_to_": "billable to",
    "client_accounting_reference_": "client accounting reference",
    "consumer_": "consumer",
    "consumer_record_number_": "consumer record number",
    "interpreter_": "interpreter",
    "interpreter_gender_": "interpreter gender",
    "interpreter_accounting_reference_": "interpreter accounting reference",
    "interpreter_team_(if_applicable)_": "interpreter team (if applicable)",
    "closed_using_": "closed using",
    "unmet_": "unmet",
    "interpreter_job_notes_": "interpreter job notes",
    "estimated_duration_": "estimated duration",
    "offer_mode_": "offer mode",
    "interpreter_assignment_date_time_": "interpreter assignment date time",
    "requestor_confirmed_date_time_": "requestor confirmed date time",
    "interpreter_arrival_time_outbound_": "interpreter arrival time outbound",
    "interpreter_departure_time_outbound_": "interpreter departure time outbound",
    "interpreter_arrival_time_inbound_": "interpreter arrival time inbound",
    "interpreter_departure_time_inbound_": "interpreter departure time inbound",
    "payment_number_": "payment number",
    
    # Placeholder for financial & miscellaneous fields
    "interpretation.1_": "Interpretation.1",
    "translation.1_": "Translation.1",
    "rush_fee.1_": "Rush Fee.1",
    "cancel_fee.1_": "Cancel Fee.1",
    "mileage.1_": "Mileage.1",
    "travel_(misc).1_": "Travel (Misc).1",
    "taxi.1_": "Taxi.1",
    "hotel.1_": "Hotel.1",
    "parking.1_": "Parking.1",
    "tolls.1_": "Tolls.1",
    "food_&_drink.1_": "Food & Drink.1",
    "miscellaneous.1_": "Miscellaneous.1",
    "tax.1_": "Tax.1",
    "credit.1_": "Credit.1",
    "deduction.1_": "Deduction.1",
    "misc._fee.1_": "Misc. Fee.1",
    "deduction_(hours).1_": "Deduction (Hours).1",
    "misc._(hours).1_": "Misc. (Hours).1",
    "travel_time_(hours).1_": "Travel Time (Hours).1",
    "clock_(hours).1_": "Clock (Hours).1",
    "meal_break_(hours).1_": "Meal Break (Hours).1",
    "other_(not_classified).1_": "Other (not classified).1",
}

def authenticate(auth_url, subject, secret):
    """Authenticate and return access token."""
    logging.info("Authenticating...")
    auth_payload = {
        "subject": subject,
        "secret": secret
    }
    response = requests.post(auth_url, json=auth_payload)
    if response.status_code == 200:
        auth_data = response.json()
        logging.info("Authentication successful.")
        logging.info(auth_data.get("accessToken"))
        return auth_data.get("accessToken")
    else:
        logging.error(f"Authentication failed: {response.status_code} - {response.text}")
        raise ValueError(f"Authentication failed: {response.status_code} - {response.text}")

def fetch_booking_data(api_url, access_token, start_date, end_date):
    """Fetch booking data using the access token."""
    logging.info("Fetching booking data...")
    data_payload = {
        "expectedStartDateGTE": start_date,
        "expectedStartDateLT": end_date
    }
    data_headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(api_url, json=data_payload, headers=data_headers)
    if response.status_code == 200:
        logging.info("Booking data fetched successfully.")
        return response.json()
    else:
        logging.error(f"Data API request failed: {response.status_code} - {response.text}")
        raise ValueError(f"Data API request failed: {response.status_code} - {response.text}")

def save_json_to_file(data, filename):
    """Save JSON data to a file."""
    logging.info(f"Saving JSON data to {filename}...")
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    logging.info(f"Data saved successfully to {filename}")

def extract_and_map_data(json_data, mapping):
    """Extracts data from JSON and maps it to the defined Excel format."""
    logging.info("Extracting and mapping data...")
    data_rows = []
    for item in json_data.get("items", []):
        row = {}
        for json_key, excel_col in mapping.items():
            keys = json_key.split(".")  # Handle nested JSON fields
            value = item
            for key in keys:
                value = value.get(key, None) if isinstance(value, dict) else None
            # Check for genderRequirement field and apply the condition
            if json_key == "genderRequirement":
                value = value.get("name", "no") if value else "no"
            row[excel_col] = value
        data_rows.append(row)
    logging.info("Data extraction and mapping completed.")
    return pd.DataFrame(data_rows)

if __name__ == "__main__":
    
    try:
        # Process
        logging.info("Starting process...")
        access_token = authenticate(AUTH_URL, SUBJECT, SECRET)
        booking_data = fetch_booking_data(DATA_API_URL, access_token, START_DATE, END_DATE)
        save_json_to_file(booking_data, OUTPUT_FILE) # For debugging

        # Extract and map data
        booking_data = json.load(open('booking_data.json', "r", encoding="utf-8"))
        df = extract_and_map_data(booking_data, json_to_excel_mapping)
        df.to_excel(EXCEL_FILE, index=False)
        logging.info(f"Excel report saved to {EXCEL_FILE}")
        logging.info("Process completed successfully.")        
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        

        # modify the api to check for the lastmodified date filter
        # look into timezones and how to handle them