"""
MCIS ELT Pipeline - Booking Data Extraction Module

This module implements the Extract-Load-Transform (ELT) pipeline for booking data.
It handles the extraction of booking data from the API, enrichment with related data,
and loading into a SQL Server database.

Main components:
- API authentication and data fetching
- Data flattening and transformation
- Database operations (upsert)
- Asynchronous processing
"""

import os
import json
import logging
from datetime import datetime, timedelta
import asyncio
import aiohttp
import pyodbc
import requests
import time
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional

# Load environment variables from .env file
load_dotenv()

# Configure logging with both file and console output
LOG_FILE = "api_process.log"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def get_db_connection():
    """
    Establishes a connection to the SQL Server database using Windows authentication.
    
    Returns:
        pyodbc.Connection: Database connection object if successful, None otherwise
    
    Raises:
        Exception: If connection fails
    """
    try:
        conn = pyodbc.connect(
            f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;TrustServerCertificate=yes;'
        )
        logging.info("Connected to SQL Server successfully.")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

# ---------------------------------
# Helper functions for data extraction and flattening
# ---------------------------------

def get_nested_value(data: Dict[str, Any], key_path: str, sep: str = '.') -> Any:
    """
    Safely traverses a nested dictionary using a dot-separated key path.
    
    Args:
        data: The nested dictionary to traverse
        key_path: Dot-separated string indicating the path to the desired value
        sep: Separator character used in the key_path (default: '.')
    
    Returns:
        The value at the specified path, or None if the path doesn't exist
    """
    keys = key_path.split(sep)
    for k in keys:
        if isinstance(data, dict) and k in data:
            data = data[k]
        else:
            return None
    return data

def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """
    Recursively flattens a nested dictionary into a single-level dictionary.
    Nested keys are concatenated using the separator.
    
    Args:
        d: The nested dictionary to flatten
        parent_key: The parent key for nested dictionaries (used in recursion)
        sep: Separator character for concatenated keys
    
    Returns:
        Dict with flattened structure where all nested keys are concatenated
    """
    items = {}
    for key, value in d.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep=sep))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    items.update(flatten_dict(item, f"{new_key}{sep}{i}", sep=sep))
                else:
                    items[f"{new_key}{sep}{i}"] = item
        else:
            items[new_key] = value
    return items

def extract_values(mapping: Dict[str, str], records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extracts values from records based on a column mapping configuration.
    
    Args:
        mapping: Dictionary mapping column names to key paths in the record
        records: List of record dictionaries to extract values from
    
    Returns:
        List of dictionaries with extracted values according to the mapping
    """
    extracted = []
    for record in records:
        row = {}
        for col, key_path in mapping.items():
            value = get_nested_value(record, key_path)
            if value is None:
                logging.debug(f"Missing value for column '{col}' using key '{key_path}' in record id {record.get('id', 'unknown')}")
                value = ""
            row[col] = value
        extracted.append(row)
    logging.info(f"Extracted {len(extracted)} rows using mapping.")
    return extracted

def extract_locations(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extracts and normalizes location data from booking records.
    Handles multiple location types (actual, billing, default) and deduplicates.
    
    Args:
        records: List of booking records containing location data
    
    Returns:
        List of unique, normalized location dictionaries
    """
    loc_dict = {}
    for record in records:
        for prefix in ["actualLocation", "billingLocation", "location"]:
            loc_id = get_nested_value(record, f"{prefix}.id")
            if loc_id:
                if loc_id not in loc_dict:
                    loc_dict[loc_id] = {
                        "location_id": loc_id,
                        "uri": get_nested_value(record, f"{prefix}.uri") or "",
                        "active": get_nested_value(record, f"{prefix}.active") or "",
                        "addr_entered": get_nested_value(record, f"{prefix}.addrEntered") or get_nested_value(record, f"{prefix}.addressEntered") or "",
                        "cost_center": get_nested_value(record, f"{prefix}.costCenter") or "",
                        "cost_center_name": get_nested_value(record, f"{prefix}.costCenterName") or "",
                        "description": get_nested_value(record, f"{prefix}.description") or "",
                        "display_label": get_nested_value(record, f"{prefix}.displayLabel") or "",
                        "lat": get_nested_value(record, f"{prefix}.lat") or get_nested_value(record, f"{prefix}.latitude") or "",
                        "lng": get_nested_value(record, f"{prefix}.lng") or get_nested_value(record, f"{prefix}.longitude") or "",
                        "uuid": get_nested_value(record, f"{prefix}.uuid") or ""
                    }
    locs = list(loc_dict.values())
    logging.info(f"Extracted {len(locs)} unique location records.")
    return locs

def flatten_inner_api_data(inner_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flattens nested API response data without adding identifier prefixes.
    Merges all nested dictionaries into a single-level dictionary.
    
    Args:
        inner_data: Nested dictionary from API response
    
    Returns:
        Flattened dictionary with merged key-value pairs
    """
    flat_data = {}
    for key, value in inner_data.items():
        if isinstance(value, dict):
            # Flatten without passing a parent key so that the keys inside are not prefixed.
            flat_dict = flatten_dict(value)
            flat_data.update(flat_dict)
        elif isinstance(value, list):
            # If the result is a list, flatten each dict element
            for item in value:
                if isinstance(item, dict):
                    flat_data.update(flatten_dict(item))
                else:
                    # For non-dict items, store them under the original key (could be adjusted if needed)
                    flat_data[key] = item
        else:
            flat_data[key] = value
    return flat_data

# ---------------------------------
# API and Data Fetching Functions
# ---------------------------------

def authenticate(auth_url: str, subject: str, secret: str) -> str:
    """
    Authenticates with the API using provided credentials.
    
    Args:
        auth_url: Authentication endpoint URL
        subject: API subject identifier
        secret: API secret key
    
    Returns:
        str: Access token for API requests
    
    Raises:
        requests.RequestException: If authentication fails
    """
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

async def fetch_data_async(session: aiohttp.ClientSession, url: str, headers: Dict[str, str], payload: Optional[Dict] = None) -> Dict:
    """
    Asynchronously fetches data from an API endpoint.
    
    Args:
        session: aiohttp client session
        url: API endpoint URL
        headers: Request headers including authentication
        payload: Optional request payload for POST requests
    
    Returns:
        Dict containing the API response data
    """
    try:
        if payload:
            async with session.post(url, json=payload, headers=headers) as resp:
                data = await resp.json()
        else:
            async with session.get(url, headers=headers) as resp:
                data = await resp.json()
        logging.debug(f"Fetched async data from {url}")
        return data
    except Exception as e:
        logging.error(f"Async error fetching {url}: {e}")
        return None

def fetch_booking_data_paged(access_token: str) -> List[Dict[str, Any]]:
    """
    Fetches booking data from the API using pagination.
    
    Args:
        access_token: Valid API access token
    
    Returns:
        List of booking records from all pages
    
    Note:
        Uses global variables DATA_API_URL, START_DATE, END_DATE, and FILTER_DATE
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    all_bookings = []
    next_url = DATA_API_URL
    payload = {
        "expectedStartDateGTE": START_DATE,
        "expectedStartDateLT": END_DATE,
        "pageSize": "50",
        "Filter": {
            "groupOp": "AND",
            "rules": [
                {
                    "op": "ged",
                    "field": "lastModifiedDate",
                    "data": "{FILTER_DATE}",
                    "type": "date"
                }
            ]
        }
    }
    
    while next_url:
        logging.info(f"Fetching page: {next_url}")
        response = requests.get(next_url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        total = data.get("count")
        logging.info(f"Total bookings: {total}")
        all_bookings.extend(items)
        next_url = data.get("next")
    logging.info(f"Fetched total {len(all_bookings)} booking items with pagination.")
    return all_bookings

def find_all_uris(data: Dict[str, Any]) -> Dict[str, str]:
    """
    Recursively finds all URIs in the nested data structure.
    
    Args:
        data: Nested dictionary to search for URIs
    
    Returns:
        Dictionary mapping composite key paths to their corresponding URIs
    """
    uris = {}
    def recurse(current: Any, path: str = "") -> None:
        """
        Helper function for recursive URI search.
        
        Args:
            current: Current data structure being searched
            path: Current path in the data structure
        """
        if isinstance(current, dict):
            if "uri" in current and isinstance(current["uri"], str) and current["uri"].startswith("http"):
                composite_key = path.rstrip(".")
                uris[composite_key] = current["uri"]
            for k, v in current.items():
                new_path = f"{path}{k}."
                recurse(v, new_path)
        elif isinstance(current, list):
            for idx, item in enumerate(current):
                new_path = f"{path}[{idx}]."
                recurse(item, new_path)
    recurse(data)
    return uris

async def enrich_record(record: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Enriches a booking record by fetching additional data from related URIs.
    
    Args:
        record: The booking record to enrich
        headers: Request headers including authentication
    
    Returns:
        Dictionary containing flattened data from all related API endpoints
    """
    uri_dict = {}
    # Check top-level keys for direct URIs
    for key, value in record.items():
        if isinstance(value, str) and value.startswith("http"):
            uri_dict[key] = value
        elif isinstance(value, dict) and "uri" in value and isinstance(value["uri"], str) and value["uri"].startswith("http"):
            uri_dict[key] = value["uri"]
    
    # Also use recursive search to find any nested URIs
    recursive_uris = find_all_uris(record)
    uri_dict.update(recursive_uris)
    
    extra = {}
    async with aiohttp.ClientSession() as session:
        tasks = []
        keys = []
        for key, url in uri_dict.items():
            keys.append(key)
            tasks.append(fetch_data_async(session, url, headers))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for key, res in zip(keys, results):
            if isinstance(res, Exception):
                logging.error(f"Error fetching {key}: {res}")
            else:
                extra[key] = res
    
    # Flatten each inner API result individually and merge
    flat_extra = {}
    for key, data in extra.items():
        if isinstance(data, dict):
            flat_item = flatten_dict(data)  # flatten without a parent key
            flat_extra.update(flat_item)
        else:
            flat_extra[key] = data
    logging.debug(f"Flattened extra data keys: {list(flat_extra.keys())} for booking id {record.get('id', 'unknown')}")
    return flat_extra

async def process_bookings_async(access_token: str) -> List[Dict[str, Any]]:
    """
    Main processing function for booking data.
    Fetches, enriches, and processes all booking records.
    
    Args:
        access_token: Valid API access token
    
    Returns:
        List of processed and enriched booking records
    """
    logging.info("Fetching booking data with pagination...")
    bookings = fetch_booking_data_paged(access_token)
    logging.info(f"Fetched {len(bookings)} booking items.")
    headers = {"Authorization": f"Bearer {access_token}"}
    processed = []
    for record in bookings:
        extra_flat = await enrich_record(record, headers)
        # Merge the enriched data into the record
        record.update(extra_flat)
        # Now flatten the entire booking record
        flat_record = flatten_dict(record, parent_key="")
        processed.append(flat_record)
    return processed

# ---------------------------------
# Database Insertion Functions
# ---------------------------------

TABLE_ORDER = [
    "ContractType", "Customer", "Location", "BookingMode", "Client",
    "Company", "Language", "EmploymentCategory", "InvoiceStatus",
    "PaymentStatus", "OverflowType", "SuperBooking", "BookingStatus",
    "Requestor", "VisitStatus", "Visit", "Interpreter", "Ref",
    "BookingRef", "BookingOverriddenRequirement", "BookingInvalidField",
    "BookingRequirement", "Booking"
]

TABLE_MAPPING = {
    # ---------------------------------------------------------------------
    # A) ADDRESS MAPPINGS (FOUR VARIANTS)
    # ---------------------------------------------------------------------

    "LocationAddress": {
        "table_name": "Address",
        "columns": {
            "address_id":        "location.id",
            "uri":               "location.uri",
            "active":            "location.active",
            "addr_entered":      "location.addrEntered",
            "addr_formatted":    "location.addrFormatted",
            "cost_center":       "location.costCenter",
            "cost_center_name":  "location.costCenterName",
            "description":       "location.description",
            "display_label":     "location.displayLabel",
            "lat":               "location.lat",
            "lng":               "location.lng",
            "notes":             "location.notes",
            "public_notes":      "location.publicNotes",
            "city_town":         "location.cityTown",
            "postal_code":       "location.postalCode",
            "state_county":      "location.stateCounty",
            "country":           "location.country",
            "timezone":          "location.timezone",
            "validated":         "location.validated",
            "validation_status": "location.validationStatus",
            "uuid":              "location.uuid"
        }
    },

    "ActualLocationAddress": {
        "table_name": "Address",
        "columns": {
            "address_id":        "actualLocation.id",
            "uri":               "actualLocation.uri",
            "active":            "actualLocation.active",
            "addr_entered":      "actualLocation.addrEntered",
            "addr_formatted":    "actualLocation.addrFormatted",
            "cost_center":       "actualLocation.costCenter",
            "cost_center_name":  "actualLocation.costCenterName",
            "description":       "actualLocation.description",
            "display_label":     "actualLocation.displayLabel",
            "lat":               "actualLocation.lat",
            "lng":               "actualLocation.lng",
            "notes":             "actualLocation.notes",
            "public_notes":      "actualLocation.publicNotes",
            "city_town":         "actualLocation.cityTown",
            "postal_code":       "actualLocation.postalCode",
            "state_county":      "actualLocation.stateCounty",
            "country":           "actualLocation.country",
            "timezone":          "actualLocation.timezone",
            "validated":         "actualLocation.validated",
            "validation_status": "actualLocation.validationStatus",
            "uuid":              "actualLocation.uuid"
        }
    },

    "BillingLocationAddress": {
        "table_name": "Address",
        "columns": {
            "address_id":        "billingLocation.id",
            "uri":               "billingLocation.uri",
            "active":            "billingLocation.active",
            "addr_entered":      "billingLocation.addrEntered",
            "addr_formatted":    "billingLocation.addrFormatted",
            "cost_center":       "billingLocation.costCenter",
            "cost_center_name":  "billingLocation.costCenterName",
            "description":       "billingLocation.description",
            "display_label":     "billingLocation.displayLabel",
            "lat":               "billingLocation.lat",
            "lng":               "billingLocation.lng",
            "notes":             "billingLocation.notes",
            "public_notes":      "billingLocation.publicNotes",
            "city_town":         "billingLocation.cityTown",
            "postal_code":       "billingLocation.postalCode",
            "state_county":      "billingLocation.stateCounty",
            "country":           "billingLocation.country",
            "timezone":          "billingLocation.timezone",
            "validated":         "billingLocation.validated",
            "validation_status": "billingLocation.validationStatus",
            "uuid":              "billingLocation.uuid"
        }
    },

    "SubLocationAddress": {
        "table_name": "Address",
        "columns": {
            "address_id":        "subLocation.id",
            "uri":               "subLocation.uri",
            "active":            "subLocation.active",
            "addr_entered":      "subLocation.addrEntered",
            "addr_formatted":    "subLocation.addrFormatted",
            "cost_center":       "subLocation.costCenter",
            "cost_center_name":  "subLocation.costCenterName",
            "description":       "subLocation.description",
            "display_label":     "subLocation.displayLabel",
            "lat":               "subLocation.lat",
            "lng":               "subLocation.lng",
            "notes":             "subLocation.notes",
            "public_notes":      "subLocation.publicNotes",
            "city_town":         "subLocation.cityTown",
            "postal_code":       "subLocation.postalCode",
            "state_county":      "subLocation.stateCounty",
            "country":           "subLocation.country",
            "timezone":          "subLocation.timezone",
            "validated":         "subLocation.validated",
            "validation_status": "subLocation.validationStatus",
            "uuid":              "subLocation.uuid"
        }
    },

    # ---------------------------------------------------------------------
    # B) OTHER TABLES (Company, Config, Integration, Customer, etc.)
    #    (same approach: each sub-object in the flattened record has
    #    a dictionary entry mapping JSON -> table columns)
    # ---------------------------------------------------------------------

    "Company": {
        "table_name": "Company",
        "columns": {
            "company_id": "company.id",
            "uri": "company.uri",
            "description": "company.description",
            "name": "company.name",
            "uuid": "company.uuid"
        }
    },

    "Config": {
        "table_name": "Config",
        "columns": {
            "config_id": "config.id",
            "uri": "config.uri",
            "uuid": "config.uuid",
            "allow_free_text": "config.allowFreeText",
            "customer_specific": "config.customerSpecific",
            "enable_dropdown": "config.enableDropdown",
            "required": "config.required",
            "select_field": "config.selectField",
            "visible_for_interpreters": "config.visibleForInterpreters"
        }
    },
    
    "Ref_Config":{
        "table_name": "Config",
        "columns": {
            "config_id": "refs.{i}.config.id",
            "uri": "refs.{i}.config.uri",
            "uuid": "refs.{i}.config.uuid",
            "allow_free_text": "refs.{i}.config.allowFreeText",
            "customer_specific": "refs.{i}.config.customerSpecific",
            "enable_dropdown": "refs.{i}.config.enableDropdown",
            "required": "refs.{i}.config.required",
            "select_field": "refs.{i}.config.selectField",
            "visible_for_interpreters": "refs.{i}.config.visibleForInterpreters"
        }
    },

    "Integration": {
        "table_name": "Integration",
        "columns": {
            "integration_id": "integration.id",
            "uri": "integration.uri",
            "description": "integration.description",
            "name": "integration.name",
            "uuid": "integration.uuid"
        }
    },

    "Customer": {
        "table_name": "Customer",
        "columns": {
            "customer_id": "customer.id", 
            "uri": "customer.uri",
            "version_value": "customer.versionValue",
            "access_code": "customer.accessCode",
            "accounting_reference": "customer.accountingReference",
            "contract_type_id": "customer.contractType.id",
            "display_name": "customer.displayName",
            "name": "customer.name",
            "uuid": "customer.uuid",
            "company_id": "customer.company.id",
            "config_id": "customer.config.id",
            "parent_accounting_ref": "parentAccountingReference",
            "payment_days_due": "paymentDaysDue",
            "payment_terms": "paymentTerms",
            "enable_all_services": "enableAllServices",
            "umbrella_number": "umbrellaNumber",
            "website": "website",
            "status_name": "status.name",
            "status_name_key": "status.nameKey",
            "active": "active",
            "last_modified_by": "lastModifiedBy",
            "last_modified_date": "lastModifiedDate",
            "created_by": "createdBy",
            "created_date": "createdDate",
            "customer_special_instructions": "customerSpecialInstructions",
            "account_executive": "accountExecutive",
            "account_manager": "accountManager",
            "accounting_sick_leave_code": "accountingSickLeaveCode",
            "billing_account": "billingAccount",
            "billing_method": "billingMethod",
            "notes": "notes",
            "parent_name": "parentName",
            "purchase_order_number": "purchaseOrderNumber",
            "is_synchronized": "isSynchronized",
            "is_synchronized_manually": "isSynchronizedManually",
            "last_synchronized_date": "lastSynchronizedDate"
        }
    },

    "Interpreter": {
        "table_name": "Interpreter",
        "columns": {
            "interpreter_id": "interpreter.id",
            "uri": "interpreter.uri",
            "display_name": "interpreter.displayName",
            "name": "interpreter.name",
            "first_name": "firstName",  
            "last_name": "lastName",
            "middle_name": "interpreter.middleName",
            "nick_name": "interpreter.nickName",
            "gender_id": "interpreter.gender.id",
            "time_zone": "interpreter.timeZone",
            "time_zone_display_name": "interpreter.timeZoneDisplayName",
            "primary_email": "interpreter.primaryEmail.emailAddress",
            "primary_phone": "interpreter.primaryNumber.parsedNumber",
            "payment_method_id": "interpreter.paymentMethod.id",
            "employment_category_id": "employmentCategory.id",
            "assignment_tier_id": "assignmentTier.id",
            "out_of_office": "outOfOffice",
            "out_of_office_since": "outOfOfficeSince",
            "enable_all_services": "enableAllServices",
            "currency_code": "currencyCode", 
            "region": "region",
            "active_note": "activeNote",
            "uuid": "interpreter.uuid",
            "created_by": "interpreter.createdBy",
            "created_date": "interpreter.createdDate",
            "last_modified_by": "interpreter.lastModifiedBy",
            "last_modified_date": "interpreter.lastModifiedDate",
            "iol_nrcpd_number": "iolNrcpdNumber",
            "re_activation_date": "reActivationDate",
            "referral_source": "referralSource",
            "recruiter_name": "recruiterName",
            "has_children": "hasChildren",
            "has_transportation": "hasTransportation",
            "search_radius": "searchRadius",
            "induction_date": "inductionDate",
            "date_photo_sent_to_interpreter": "datePhotoSentToInterpreter",
            "date_photo_sent_to_printer": "datePhotoSentToPrinter",
            "document_id": "document.id",
            "eft_id": "eft.id",
            "sort_code": "sortCode",
            "swift": "swift",
            "iban": "iban",
            "bank_account": "bankAccount",
            "bank_account_description": "bankAccountDescription",
            "bank_account_reference": "bankAccountReference",
            "bank_branch": "bankBranch",
            "routing_pstn": "routingPstn",
            "is_synchronized": "isSynchronized",
            "is_synchronized_manually": "isSynchronizedManually",
            "last_synchronized_date": "lastSynchronizedDate",
            "original_start_date": "originalStartDate",
            "notes": "notes",
            "taleo_id": "taleoId"
        }
    },

    "Requestor": {
        "table_name": "Requestor",
        "columns": {
            "requestor_id": "requestor.id",
            "uri": "requestor.uri",
            "display_label": "requestor.displayLabel",
            "display_name": "requestor.displayName",
            "name": "requestor.name",
            "email": "email",
            "number": "number",
            "account_locked": "accountLocked",
            "account_expired": "accountExpired",
            "enabled": "enabled",
            "password_expired": "passwordExpired",
            "password_last_change": "passwordLastChange",
            "locale_or_default": "localeOrDefault",
            "uuid": "requestor.uuid",
            "created_date": "requestor.createdDate",
            "last_modified_date": "requestor.lastModifiedDate",
            "can_switch_location": "canSwitchLocation",
            "fax_number": "faxNumber",
            "other": "other",
            "mfa_enabled": "mfaEnabled",
            "mfa_method": "mfaMethod",
            "username": "username",
            "primary_company_id": "primaryCompany.id",
            "tz": "tz",
            "associations_uri": "associations",
            "business_units_uri": "businessUnits",
            "client_association_id": "clientAssociation.id",
            "customer_association_id": "customerAssociation.id",
            "password_last_change_str": "passwordLastChange"
        }
    },

    "Visit": {
        "table_name": "Visit",
        "columns": {
            "visit_id": "visit.id",
            "uri": "visit.uri",
            "contact_rate_plan": "visit.contactRatePlan",
            "customer_rate_plan": "visit.customerRatePlan",
            "created_by": "visit.createdBy",
            "created_date": "visit.createdDate",
            "last_modified_by": "visit.lastModifiedBy",
            "last_modified_date": "visit.lastModifiedDate",
            "uuid": "visit.uuid",
            "status_id": "visit.status.id",
            "status_uri": "visit.status.uri",
            "status_version_value": "visit.status.versionValue",
            "status_default_option": "visit.status.defaultOption",
            "status_description": "visit.status.description",
            "status_in_order": "visit.status.inOrder",
            "status_l10n_key": "visit.status.l10nKey",
            "status_message": "visit.status.message",
            "status_name": "visit.status.name",
            "status_name_key": "visit.status.nameKey"
        }
    },

    # ---------------------------------------------------------------------
    # BOOKING
    # ---------------------------------------------------------------------
    "Booking": {
        "table_name": "Booking",
        "columns": {
            "booking_id": "id",
            "uri": "uri",
            "version_value": "versionValue",
            "access_code": "accessCode",
            "accounting_reference": "accountingReference",
            "actual_duration_hrs": "actualDurationHrs",
            "actual_duration_mins": "actualDurationMins",
            "actual_end_date": "actualEndDate",
            "actual_location_id": "actualLocation.id",
            "actual_location_display": "actualLocationDisplayLabel",
            "actual_start_date": "actualStartDate",
            "assigned_by": "assignedBy",
            "assigned_by_username": "assignedByUsername",
            "assignment_date": "assignmentDate",
            "auto_offer_batch_frequency": "autoOfferBatchSizeFrequency",
            "auto_verify_duration": "autoVerifyDuration",
            "auto_verify_incidentals": "autoVerifyIncidentals",
            "average_rating": "averageRating",
            "billing_customer_id": "billingCustomer.id",
            "billing_location_id": "billingLocation.id",
            "billing_notes": "billingNotes",
            "booking_date": "bookingDate",
            "booking_details": "bookingDetails",
            "booking_mode_id": "bookingMode.id",
            "booking_requirements_uri": "bookingRequirements",
            "booking_time": "bookingTime",
            "cancellation_date": "cancellationDate",
            "cancellation_reason": "cancellationReason",
            "client_id": "client.id",
            "client_name": "client.name",
            "client_uuid": "client.uuid",
            "company_id": "company.id",
            "company_special_instructions": "companySpecialInstructions",
            "confirmation_date": "confirmationDate",
            "consumer_id": "consumer.id",
            "consumer_count": "consumerCount",
            "consumer_count_enabled": "consumerCountEnabled",
            "consumers_uri": "consumers",
            "contact_arrival_date": "contactArrivalDate",
            "contact_late_mins": "contactLateMins",
            "contact_rate_plan": "contactRatePlan",
            "contact_special_instructions": "contactSpecialInstructions",
            "created_by": "createdBy",
            "created_date": "createdDate",
            "currency_code": "currencyCode",
            "currency_symbol": "currencySymbol",
            "custom_consumer": "customConsumer",
            "custom_requestor": "customRequestor",
            "customer_id": "customer.id",
            "customer_business_unit": "customerBusinessUnit",
            "customer_duration_override_hrs": "customerDurationOverrideHrs",
            "customer_notes": "customerNotes",
            "customer_rate_plan": "customerRatePlan",
            "customer_rate_plan_association": "customerRatePlanAssociation",
            "customer_rate_plan_override": "customerRatePlanOverride",
            "customer_rate_zone_override": "customerRateZoneOverride",
            "customer_special_instructions": "customerSpecialInstructions",
            "default_language_id": "defaultLanguage.id",
            "disclaimer_accepted": "disclaimerAccepted",
            "disclaimer_accepted_date": "disclaimerAcceptedDate",
            "disclaimer_accepted_initials": "disclaimerAcceptedInitials",
            "duration_override_hrs": "durationOverrideHrs",
            "employment_category_id": "employmentCategory.id",
            "esignature_grace_period": "esignatureGracePeriod",
            "esignature_required": "esignatureRequired",
            "exclude_from_auto_offer": "excludeFromAutoOffer",
            "exclude_from_job_offer_pool": "excludeFromJobOfferPool",
            "expected_duration_hrs": "expectedDurationHrs",
            "expected_duration_mins": "expectedDurationMins",
            "expected_end_date": "expectedEndDate",
            "expected_end_time": "expectedEndTime",
            "expected_start_date": "expectedStartDate",
            "expected_start_time": "expectedStartTime",
            "final_notes": "finalNotes",
            "first_assignment_date": "firstAssignmentDate",
            "first_confirmation_date": "firstConfirmationDate",
            "first_offer_date": "firstOfferDate",
            "first_open_date": "firstOpenDate",
            "flag_for_finance": "flagForFinance",
            "gender_requirement": "genderRequirement",
            "general_customer_account_manager": "generalCustomerAccountManager",
            "general_customer_business_unit": "generalCustomerBusinessUnit",
            "incidentals_uri": "incidentals",
            "integration_id": "integration.id",
            "interpreter_id": "interpreter.id",
            "interpreter_business_unit": "interpreterBusinessUnit",
            "interpreter_notes": "interpreterNotes",
            "interpreter_submitted": "interpreterSubmitted",
            "invoice_status_id": "invoiceStatus.id",
            "is_auto_verify_ready": "isAutoVerifyReady",
            "is_cancelled": "isCancelled",
            "is_fields_locked": "isFieldsLocked",
            "is_synchronized": "isSynchronized",
            "is_synchronized_manually": "isSynchronizedManually",
            "is_verified": "isVerified",
            "job_complete_email_sent": "jobCompleteEmailSent",
            "job_offers_uri": "jobOffers",
            "language_id": "language.id",
            "language_code": "languageCode",
            "last_modified_by": "lastModifiedBy",
            "last_modified_date": "lastModifiedDate",
            "last_synchronized_date": "lastSynchronizedDate",
            "local_booking_mode": "localBookingMode",
            "location_id": "location.id",
            "location_note": "locationNote",
            "locked": "locked",
            "mileage": "mileage",
            "notes": "notes",
            "notification_email": "notificationEmail",
            "notify": "notify",
            "num_jobs": "numJobs",
            "number_accepted_offers": "numberAcceptedOffers",
            "number_for_telephone_translation": "numberForTelephoneTranslation",
            "offer_date": "offerDate",
            "offer_mode": "offerMode",
            "open_date": "openDate",
            "origin_of_request": "originOfRequest",
            "overflow_job_location_uuid": "overflowJobLocationUuid",
            "overflow_type_id": "overflowType.id",
            "override_booking_mode": "overrideBookingMode",
            "override_requirements": "overrideRequirements",
            "owner": "owner",
            "payment_status_id": "paymentStatus.id",
            "place_of_appointment": "placeOfAppointment",
            "preferred_interpreter_id": "preferredInterpreter.id",
            "preferred_interpreter_declined": "preferredInterpreterDeclined",
            "prevent_edit": "preventEdit",
            "primary_ref_id": "primaryRef.id",
            "rate_plan_override": "ratePlanOverride",
            "rate_zone_override": "rateZoneOverride",
            "refs_uri": "refs",
            "reminder_email_sent_scheduled_phone": "reminderEmailSentScheduledPhone",
            "requested_by": "requestedBy",
            "requestor_id": "requestor.id",
            "requirements_uri": "requirements",
            "sessions_uri": "sessions",
            "shift_enabled": "shiftEnabled",
            "signature_hash": "signatureHash",
            "signature_height": "signatureHeight",
            "signature_width": "signatureWidth",
            "signature_location": "signatureLocation",
            "signature_raw": "signatureRaw",
            "signer": "signer",
            "site_contact": "siteContact",
            "sla_reporting_enabled": "slaReportingEnabled",
            "start_editing": "startEditing",
            "status_name": "status.name",
            "status_uri": "status.uri",
            "sub_location_id": "subLocation.id",
            "super_booking_id": "superBooking.id",
            "sync_uuid": "syncUuid",
            "team_id": "teamId",
            "team_size": "teamSize",
            "time_interpreter_arrived_inbound": "timeInterpreterArrivedInbound",
            "time_interpreter_arrived_outbound": "timeInterpreterArrivedOutbound",
            "time_interpreter_departed_inbound": "timeInterpreterDepartedInbound",
            "time_interpreter_departed_outbound": "timeInterpreterDepartedOutbound",
            "time_reconfirmed_customer": "timeReconfirmedCustomer",
            "time_tracking_enabled": "timeTrackingEnabled",
            "time_zone": "timeZone",
            "time_zone_display_name": "timeZoneDisplayName",
            "unarchived_updates": "unarchivedUpdates",
            "unfulfilled_date": "unfulfilledDate",
            "unfulfilled_reason": "unfulfilledReason",
            "user_editing": "userEditing",
            "uuid": "uuid",
            "valid": "valid",
            "verified_date": "verifiedDate",
            "visit_id": "visit.id",
            "vos_required": "vosRequired",

            "actual_location_uri": "actualLocation.uri",
            "billing_location_uri": "billingLocation.uri",
            "location_uri": "location.uri",
            "company_uri": "company.uri"
        }
    },

    "BookingRefs": {
        "table_name": "BookingRefs",
        "columns": {
            "ref_id": "refs.{i}.id",
            "booking_id": "id",
            "uri": "refs.{i}.uri",
            "version_value": "refs.{i}.versionValue",
            "name": "refs.{i}.name",
            "ref": "refs.{i}.ref",
            "reference_value": "refs.{i}.referenceValue",
            "description": "refs.{i}.description",
            "super_booking_id": "refs.{i}.superBooking.id",
            "approved": "refs.{i}.approved",
            "consumer_id": "refs.{i}.consumer.id",
            "dependent": "refs.{i}.dependent",
            "dependent_id": "refs.{i}.dependentId",
            "company_id": "refs.{i}.company.id",
            "config_id": "refs.{i}.config.id",
            "customer_id": "refs.{i}.customer.id"
        }
    },

    "BookingRequirements": {
        "table_name": "BookingRequirements",
        "columns": {
            "requirement_id": "requirements.{i}.id",
            "booking_id": "id",
            "version_value": "requirements.{i}.versionValue",
            "company_id": "requirements.{i}.company.id",
            "config_id": "requirements.{i}.config.id",
            "created_by": "requirements.{i}.createdBy",
            "created_date": "requirements.{i}.createdDate",
            "criteria_id": "requirements.{i}.criteria.id",
            "criteria_name": "requirements.{i}.criteria.name",
            "criteria_type_id": "requirements.{i}.criteria.type.id",
            "dependent": "requirements.{i}.dependent",
            "dependent_id": "requirements.{i}.dependentId",
            "last_modified_by": "requirements.{i}.lastModifiedBy",
            "last_modified_date": "requirements.{i}.lastModifiedDate",
            "optional": "requirements.{i}.optional",
            "required_flag": "requirements.{i}.required",
            "super_booking_id": "requirements.{i}.superBooking.id",
            "sync_uuid": "requirements.{i}.syncUuid",
            "uuid": "requirements.{i}.uuid"
        }
    }
}

def get_json_value(record: Dict[str, Any], json_path: str) -> Any:
    """Safely fetch a key from the flattened JSON record."""
    return record.get(json_path, None)

def upsert_entity(record: Dict[str, Any], mapping: Dict[str, Any], cursor) -> None:
    """
    Performs an upsert operation for a single entity in the database.
    
    Args:
        record: The record to upsert
        mapping: Dictionary defining the table and column mappings
        cursor: Database cursor for executing SQL
    """
    table_name = mapping["table_name"]
    columns_map = mapping["columns"]

    db_columns = []
    values = []

    # Gather DB columns and corresponding values from JSON
    for db_col, json_key in columns_map.items():
        val = get_json_value(record, json_key)
        db_columns.append(db_col)
        values.append(val)

    # Find PK col (heuristic)
    pk_col = None
    for c in db_columns:
        if c.endswith("_id") or c in ("booking_id", "company_id", "ref_id", "requirement_id"):
            pk_col = c
            break

    if not pk_col:
        logging.debug(f"No primary key column found for table {table_name}, skipping upsert.")
        return

    pk_index = db_columns.index(pk_col)
    pk_value = values[pk_index]

    # If there's no PK value, skip
    if pk_value is None:
        logging.debug(f"No PK value for table {table_name} in record. Possibly missing data.")
        return

    # Check if row exists
    try:
        select_sql = f"SELECT 1 FROM {table_name} WHERE {pk_col} = ?"
        cursor.execute(select_sql, (pk_value,))
        row = cursor.fetchone()
    except Exception as ex:
        logging.error(f"Error checking existence in {table_name}: {ex}")
        raise

    # Build update/insert
    try:
        if row:
            # Update
            update_cols = [c for c in db_columns if c != pk_col]
            set_clause = ", ".join(f"{uc} = ?" for uc in update_cols)
            update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {pk_col} = ?"
            update_values = [values[db_columns.index(uc)] for uc in update_cols]
            update_values.append(pk_value)
            cursor.execute(update_sql, update_values)
            logging.debug(f"Updated {table_name} PK={pk_value}")
        else:
            # Insert
            col_list = ", ".join(db_columns)
            placeholders = ", ".join("?" for _ in db_columns)
            insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
            cursor.execute(insert_sql, values)
            logging.debug(f"Inserted into {table_name} PK={pk_value}")
    except Exception as ex:
        logging.error(f"Error upserting {table_name}: {ex}")
        raise

def upsert_array_entities(record: Dict[str, Any], mapping: Dict[str, Any], cursor, max_items: int = 50) -> None:
    """
    Handles upserting array-type entities into the database.
    
    Args:
        record: The record containing the array data
        mapping: Dictionary defining the table and array mappings
        cursor: Database cursor for executing SQL
        max_items: Maximum number of array items to process
    """
    table_name = mapping["table_name"]
    columns_map = mapping["columns"]

    # Identify PK
    pk_col = None
    for c in columns_map:
        if c.endswith("_id") or c in ("ref_id", "requirement_id"):
            pk_col = c
            break

    if not pk_col:
        logging.debug(f"No PK column found for array table {table_name}, skipping.")
        return

    pk_json_path_template = columns_map[pk_col]  # e.g. "refs.{i}.id"

    for i in range(max_items):
        # Check pk for this array index
        pk_json_key = pk_json_path_template.replace("{i}", str(i))
        pk_value = record.get(pk_json_key)
        if pk_value is None:
            break  # No more items

        # Build lists
        db_columns = []
        values = []
        for db_col, json_path_template in columns_map.items():
            actual_json_path = json_path_template.replace("{i}", str(i))
            val = record.get(actual_json_path)
            db_columns.append(db_col)
            values.append(val)

        try:
            # check existence
            select_sql = f"SELECT 1 FROM {table_name} WHERE {pk_col} = ?"
            cursor.execute(select_sql, (pk_value,))
            row = cursor.fetchone()

            if row:
                # update
                update_cols = [c for c in db_columns if c != pk_col]
                set_clause = ", ".join(f"{uc} = ?" for uc in update_cols)
                update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {pk_col} = ?"
                update_values = [values[db_columns.index(uc)] for uc in update_cols]
                update_values.append(pk_value)
                cursor.execute(update_sql, update_values)
                logging.debug(f"Updated array table {table_name} index={i}, PK={pk_value}")
            else:
                # insert
                col_list = ", ".join(db_columns)
                placeholders = ", ".join("?" for _ in db_columns)
                insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
                cursor.execute(insert_sql, values)
                logging.debug(f"Inserted into array table {table_name} index={i}, PK={pk_value}")
        except Exception as ex:
            logging.error(f"Error upserting array entity in {table_name}, index={i}, PK={pk_value}: {ex}")
            raise

def process_flattened_record(record: Dict[str, Any], cursor) -> None:
    """
    Insert/Upsert sub-entities in a specific order, then the main booking,
    and finally any arrays like BookingRefs, BookingRequirements.
    Adjust the order to match your actual foreign key dependencies.
    """

    # 1) Upsert "Company" if relevant
    if "company.id" in record:
        upsert_entity(record, TABLE_MAPPING["Company"], cursor)
    # 2) Upsert "Config", "Integration", etc. similarly if they exist
    if "config.id" in record:
        upsert_entity(record, TABLE_MAPPING["Config"], cursor)
        upsert_entity(record, TABLE_MAPPING["Ref_Config"], cursor)
    if "integration.id" in record:
        upsert_entity(record, TABLE_MAPPING["Integration"], cursor)

    # 3) Upsert addresses (LocationAddress, ActualLocationAddress, BillingLocationAddress, SubLocationAddress)
    upsert_entity(record, TABLE_MAPPING["LocationAddress"], cursor)
    upsert_entity(record, TABLE_MAPPING["ActualLocationAddress"], cursor)
    upsert_entity(record, TABLE_MAPPING["BillingLocationAddress"], cursor)
    upsert_entity(record, TABLE_MAPPING["SubLocationAddress"], cursor)

    # 4) Upsert "Customer"
    upsert_entity(record, TABLE_MAPPING["Customer"], cursor)

    # 5) Upsert "Interpreter"
    upsert_entity(record, TABLE_MAPPING["Interpreter"], cursor)

    # 6) Upsert "Requestor"
    upsert_entity(record, TABLE_MAPPING["Requestor"], cursor)

    # 7) Upsert "Visit"
    upsert_entity(record, TABLE_MAPPING["Visit"], cursor)

    # 8) Finally, upsert the main "Booking"
    upsert_entity(record, TABLE_MAPPING["Booking"], cursor)

    # 9) Upsert array-based references
    upsert_array_entities(record, TABLE_MAPPING["BookingRefs"], cursor)

    # 10) Upsert array-based requirements
    if "BookingRequirements" in TABLE_MAPPING:
        upsert_array_entities(record, TABLE_MAPPING["BookingRequirements"], cursor)

    logging.info(f"Finished DB insertion for booking id={record.get('id')}")

def insert_bookings_into_db(bookings: List[Dict[str, Any]]) -> None:
    """
    Main database insertion function for booking records.
    
    Args:
        bookings: List of processed booking records to insert
    """
    conn = get_db_connection()
    if not conn:
        logging.error("No DB connection. Cannot insert records.")
        return

    try:
        conn.autocommit = False
        cursor = conn.cursor()

        for record in bookings:
            try:
                process_flattened_record(record, cursor)
                conn.commit()
            except Exception as ex:
                logging.exception(f"Error inserting record id={record.get('id')}, rolling back.")
                conn.rollback()

        cursor.close()
        conn.close()
        logging.info("All booking records processed and committed.")
    except Exception as ex:
        logging.critical(f"Fatal error during DB inserts: {ex}")


async def main():
    logging.info("Starting enhanced process...")
    access_token = authenticate(AUTH_URL, SUBJECT, SECRET)
    bookings = await process_bookings_async(access_token)
    logging.info(f"Fetched, enriched, and flattened {len(bookings)} booking records.")
    if bookings:
        sample_keys = list(bookings[0].keys())
        logging.debug(f"Sample flattened record keys: {sample_keys}")
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(bookings, f, indent=4)
        logging.info(f"Flat JSON data saved to {OUTPUT_FILE}")
    except Exception as e:
        logging.error(f"Error saving flat JSON: {e}")
    insert_bookings_into_db(bookings)
    logging.info("Enhanced process completed successfully.")
    
async def run_main_loop():
    """
    Runs the main pipeline in a loop for different date ranges.
    Processes data in 6-month chunks from 2019 to present.
    """
    date_1 = datetime.strptime("2019-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
    current_time = datetime.now()
    while date_1 < current_time:
        next_date = date_1 + timedelta(days=6*30)  # Approximate 6 months as 6*30 days
        if next_date > current_time:
            next_date = current_time
        global START_DATE, END_DATE
        START_DATE = date_1.strftime("%Y-%m-%dT%H:%M:%SZ")
        END_DATE = next_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        logging.info(f"Fetching bookings from {START_DATE} to {END_DATE}")
        await main()
        date_1 = next_date


# Updated connection info for local testing
SERVER = 'Pri-Yoga'
DATABASE = 'MCIS_BOOKING_TEST'
DRIVER = '{ODBC Driver 17 for SQL Server}'
AUTH_URL = os.getenv("AUTH_URL")
DATA_API_URL = os.getenv("DATA_API_URL")
SUBJECT = os.getenv("SUBJECT")
SECRET = os.getenv("SECRET")
OUTPUT_FILE = "booking_data_{FILTER_DATE}.json"
FILTER_DATE = datetime.now().strftime("%Y-%m-%d")

if __name__ == "__main__":
    asyncio.run(run_main_loop())