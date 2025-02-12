import requests
import json
import os
import logging
import time
import re
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
START_DATE = "2024-06-30T00:00:00Z"
END_DATE = "2024-12-30T00:00:00Z"
OUTPUT_FILE = "booking_data_long.json"

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

# Execution
if __name__ == "__main__":
    try:
        logging.info("Starting process...")
        access_token = authenticate(AUTH_URL, SUBJECT, SECRET)
        all_data = process_bookings(access_token)
        save_json_to_file(all_data, OUTPUT_FILE)
        logging.info("Process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
