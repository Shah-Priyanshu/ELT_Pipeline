import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv
import logging
import time
import re

# Load environment variables
load_dotenv()

# Configure logging to store logs in a file
LOG_FILE = "api_process_dr.log"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),  # Logs to a file
        logging.StreamHandler()  # Logs to console as well
    ]
)

# Configuration
AUTH_URL = os.getenv("AUTH_URL")
DATA_API_URL = os.getenv("DATA_API_URL")
SUBJECT = os.getenv("SUBJECT")
SECRET = os.getenv("SECRET")
START_DATE = "2024-12-28T15:00:00Z"
END_DATE = "2024-12-30T15:00:00Z"
OUTPUT_FILE = "booking_data_dr.json"

# Dictionary of inner API endpoints and required parameters
get_api_endpoints = {
    "/ii/api/v2/invoiceCreditMemo": {"required_params": []},
    "/ii/api/v2/visit": {"required_params": []},
    "/ii/api/v2/paymentCreditMemo": {"required_params": []},
    "/ii/api/v2/dictionary": {"required_params": []},
    "/ii/api/v2/{parentEntityType}/{parentEntityId}/payableItem": {"required_params": ["parentEntityType", "parentEntityId"]},
    "/ii/api/v2/requestor": {"required_params": []},
    "/ii/api/v2/client": {"required_params": []},
    "/ii/api/v2/joboffer": {"required_params": []},
    "/ii/api/v2/customer": {"required_params": []},
    "/ii/api/v2/invoice": {"required_params": []},
    "/ii/api/v2/payment": {"required_params": []},
    "/ii/api/v2/language": {"required_params": []},
    "/ii/api/v2/contact": {"required_params": ["id"]},
    "/ii/api/v2/consumer": {"required_params": []},
    "/ii/api/v2/{entityType}/{entityId}/address": {"required_params": ["entityType", "entityId"]},
    "/ii/api/v2/companyLanguage": {"required_params": []},
    "/ii/api/v2/address": {"required_params": []},
    "/ii/api/v2/booking": {"required_params": []}
}

def authenticate(auth_url, subject, secret):
    """Authenticate and return access token."""
    logging.info("Authenticating...")
    auth_payload = {"subject": subject, "secret": secret}
    
    response = requests.post(auth_url, json=auth_payload)
    
    if response.status_code == 200:
        auth_data = response.json()
        logging.info(f"Authentication successful. Token: {auth_data.get('accessToken')}")
        return auth_data.get("accessToken")
    
    logging.error(f"Authentication failed: {response.status_code} - {response.text}")
    raise ValueError(f"Authentication failed: {response.status_code} - {response.text}")

def fetch_data(url, headers):
    """Fetch data from API and handle errors."""
    logging.debug(f"Fetching data from: {url}")
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logging.debug(f"Successfully fetched data from {url}")
            return response.json()
        else:
            logging.error(f"Error fetching {url}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Exception occurred for {url}: {e}")
        return None

def fetch_booking_data(api_url, access_token, start_date, end_date):
    """Fetch all booking data including pagination."""
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
        response = requests.get(api_url, json=data_payload, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            all_bookings.extend(data.get("items", []))
            logging.debug(f"Fetched {len(data.get('items', []))} bookings from page {page}")
            api_url = data.get("next", None)  # Move to the next page
        else:
            logging.error(f"Data API request failed: {response.status_code} - {response.text}")
            break

        page += 1
        time.sleep(1)  # Delay to prevent excessive API calls
    
    logging.info(f"Total bookings fetched: {len(all_bookings)}")
    return all_bookings

def extract_relevant_uris(item):
    """Extract all relevant API URIs from a single booking item."""
    extracted_uris = {}

    for field, details in get_api_endpoints.items():
        # Check if the API pattern is inside any field value
        for key, value in item.items():
            if isinstance(value, dict) and "uri" in value:
                url = value["uri"]
                if re.search(field.replace("{", "").replace("}", ""), url):
                    extracted_uris[field] = url
                    logging.debug(f"Extracted URI for {field}: {url}")

    return extracted_uris

def fetch_additional_details(item, access_token):
    """Fetch additional details from extracted URIs in a single booking item."""
    booking_id = item["id"]
    logging.info(f"Fetching additional details for booking ID {booking_id}...")

    extracted_uris = extract_relevant_uris(item)
    additional_details = {}
    headers = {"Authorization": f"Bearer {access_token}"}

    for api_pattern, url in extracted_uris.items():
        required_params = get_api_endpoints[api_pattern]["required_params"]
        formatted_url = url

        if required_params:
            param_values = {param: str(item.get(param, "")) for param in required_params}
            formatted_url = api_pattern.format(**param_values)
        
        logging.debug(f"Fetching {api_pattern} data from {formatted_url}")
        extra_data = fetch_data(formatted_url, headers)
        if extra_data:
            additional_details[api_pattern] = extra_data
            logging.debug(f"Fetched data for {api_pattern}: {extra_data}")
        else:
            logging.warning(f"No data returned for {api_pattern}")

        time.sleep(0.5)  # Small delay to avoid overwhelming API

    return additional_details

def process_bookings(access_token):
    """Main function to fetch all bookings and additional details."""
    bookings = fetch_booking_data(DATA_API_URL, access_token, START_DATE, END_DATE)
    detailed_bookings = []

    for item in bookings:
        logging.info(f"Processing booking ID: {item['id']}")
        booking_details = {
            "id": item["id"],
            "main_data": item,
            "extra_details": fetch_additional_details(item, access_token)
        }
        detailed_bookings.append(booking_details)

    return detailed_bookings

def save_json_to_file(data, filename):
    """Save JSON data to a file."""
    logging.info(f"Saving JSON data to {filename}...")
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    logging.info(f"Data saved successfully to {filename}")

# Execution
if __name__ == "__main__":
    try:
        logging.info("Starting process...")

        # Authenticate
        access_token = authenticate(AUTH_URL, SUBJECT, SECRET)

        # Fetch booking data
        all_data = process_bookings(access_token)
        save_json_to_file(all_data, OUTPUT_FILE)
        logging.info("Process completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
