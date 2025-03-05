import requests
import json
import os
import logging
import time
import pyodbc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging (stores logs in a file and prints to console)
LOG_FILE = "api_process_1M_9.log"
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
# server = 'PROMETHEUS'
# database = 'MCIS_BOOKING_TEST'
# username = 'mcis\\Priyanshu.Shah'
# password = '$u5ZK%Ecvi'
# driver = '{ODBC Driver 17 for SQL Server}'

#local
server = 'Pri-Yoga'
database = 'MCIS_BOOKING_TEST9'
# username = 'mcis\Priyanshu.Shah'
# password = '$u5ZK%Ecvi'
driver = '{ODBC Driver 17 for SQL Server}'


# Mapping dictionary for tables (for tables with straightforward mappings)
TABLE_MAPPINGS = {
    "ContractTypes": {
        "contract_type_id": "customer.contractType.id",
        "uri": "customer.contractType.uri",
        "version_value": "customer.contractType.versionValue",
        "default_option": "customer.contractType.defaultOption",
        "description": "customer.contractType.description",
        "l10nKey": "customer.contractType.l10nKey",
        "name": "customer.contractType.name",
        "name_key": "customer.contractType.nameKey"
    },
    "Customers": {
        "customer_id": "customer.id",
        "uri": "customer.uri",
        "display_name": "customer.displayName",
        "name": "customer.name",
        "uuid": "customer.uuid",
        "contract_type_id": "customer.contractType.id"
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
    "Languages": {
        "language_id": "language.id",
        "uri": "language.uri",
        "bpin_opi_enabled": "language.bpinOpiEnabled",
        "bpin_vri_enabled": "language.bpinVriEnabled",
        "description": "language.description",
        "display_name": "language.displayName",
        "is_sign": "language.isSign",
        "iso639_3_tag": "language.iso6393Tag",
        "mmis_code": "language.mmisCode",
        "opi_enabled": "language.opiEnabled",
        "subtag": "language.subtag",
        "vri_enabled": "language.vriEnabled"
    },
    "EmploymentCategories": {
        "employment_category_id": "employmentCategory.id",
        "uri": "employmentCategory.uri",
        "description": "employmentCategory.description",
        "name": "employmentCategory.name",
        "name_key": "employmentCategory.nameKey"
    },
    "InvoiceStatuses": {
        "invoice_status_id": "invoiceStatus.id",
        "uri": "invoiceStatus.uri",
        "description": "invoiceStatus.description",
        "name": "invoiceStatus.name",
        "name_key": "invoiceStatus.nameKey"
    },
    "PaymentStatuses": {
        "payment_status_id": "paymentStatus.id",
        "uri": "paymentStatus.uri",
        "description": "paymentStatus.description",
        "name": "paymentStatus.name",
        "name_key": "paymentStatus.nameKey"
    },
    "OverflowTypes": {
        "overflow_type_id": "overflowType.id",
        "uri": "overflowType.uri",
        "description": "overflowType.description",
        "name_key": "overflowType.nameKey"
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
        "name": "status.name",
        "name_key": "status.nameKey"
    },
    "Requestors": {
        "requestor_id": "requestor.id",
        "uri": "requestor.uri",
        "display_label": "requestor.displayLabel",
        "display_name": "requestor.displayName",
        "name": "requestor.name",
        "uuid": "requestor.uuid",
        "email": "requestor.email",
        "enabled": "requestor.enabled",
        "fax_number": "requestor.faxNumber",
        "first_name": "requestor.firstName",
        "last_name": "requestor.lastName",
        "password_last_change": "requestor.passwordLastChange",
        "username": "requestor.username"
    },
    "VisitStatuses": {
        "visit_status_id": "visit.Status.id",
        "uri": "visit.Status.uri",
        "version_value": "visit.Status.versionValue",
        "default_option": "visit.Status.defaultOption",
        "description": "visit.Status.description",
        "in_order": "visit.Status.inOrder",
        "l10nKey": "visit.Status.l10nKey",
        "message": "visit.Status.message",
        "name": "visit.Status.name",
        "name_key": "visit.Status.nameKey"
    },
    "Visits": {
        "visit_id": "visit.id",
        "uri": "visit.uri",
        "contact_rate_plan": "visit.contactRatePlan",  # Will be truncated if needed
        "customer_rate_plan": "visit.customerRatePlan",
        "uuid": "visit.uuid",
        "visit_status_id": "visit.visitStatus.id"
    },
    "Interpreters": {
        "interpreter_id": "interpreter.id",
        "uri": "interpreter.uri",
        "display_name": "interpreter.displayName",
        "name": "interpreter.name",
        "email": "interpreter.email",
        "phone": "interpreter.phone",
        "payment_method": "interpreter.paymentMethod",
        "time_zone": "interpreter.timeZone",
        "uuid": "interpreter.uuid"
    },
    "BookingRefs": {
        "booking_id": "bookingRef.booking.id",
        "ref_id": "bookingRef.ref.id"
    },
    "BookingOverriddenRequirements": {
        "booking_id": "bookingOverriddenRequirement.booking.id",
        "requirement": "bookingOverriddenRequirement.requirement"
    },
    "BookingInvalidFields": {
        "booking_id": "bookingInvalidField.booking.id",
        "field_name": "bookingInvalidField.fieldName"
    },
    "BookingRequirements": {
        "booking_id": "bookingRequirement.booking.id",
        "requirement": "bookingRequirement.requirement"
    },
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
        "actual_location_display": "actualLocation.displayLabel",
        "assigned_by": "assignedBy",
        "assigned_by_username": "assignedByUsername",
        "assignment_date": "assignmentDate",
        "auto_offer_batch_freq": "autoOfferBatchSizeFrequency",
        "auto_verify_duration": "autoVerifyDuration",
        "auto_verify_incidentals": "autoVerifyIncidentals",
        "average_rating": "averageRating",
        "billing_notes": "billingNotes",
        "booking_date": "bookingDate",
        "booking_details": "bookingDetails",
        "booking_requirements": "bookingRequirements",
        "booking_time": "bookingTime",
        "cancellation_date": "cancellationDate",
        "cancellation_reason": "cancellationReason",
        "company_special_instructions": "company.specialInstructions",
        "confirmation_date": "confirmationDate",
        "consumer": "consumer",
        "consumer_count": "consumerCount",
        "consumer_count_enabled": "consumerCountEnabled",
        "contact_arrival_date": "contactArrivalDate",
        "contact_late_mins": "contactLateMins",
        "contact_rate_plan": "contact_rate_plan",  # Processed specially in get_nested_value
        "contact_special_instructions": "contactSpecialInstructions",
        "created_by": "createdBy",
        "created_date": "createdDate",
        "currency_code": "currencyCode",
        "currency_symbol": "currencySymbol",
        "custom_consumer": "customConsumer",
        "custom_requestor": "customRequestor",
        "customer_business_unit": "customer.businessUnit",
        "customer_duration_override_hrs": "customer.durationOverrideHrs",
        "customer_notes": "customerNotes",
        "customer_rate_plan": "customer.ratePlan",
        "customer_rate_plan_assoc": "customer.ratePlanAssoc",
        "customer_rate_plan_override": "customer.ratePlanOverride",
        "customer_rate_zone_override": "customer.rateZoneOverride",
        "customer_special_instructions": "customer.specialInstructions",
        "disclaimer_accepted": "disclaimerAccepted",
        "disclaimer_accepted_date": "disclaimerAcceptedDate",
        "disclaimer_accepted_initials": "disclaimerAcceptedInitials",
        "duration_override_hrs": "durationOverrideHrs",
        "esignature_grace_period": "esignatureGracePeriod",
        "esignature_required": "esignatureRequired",
        "exclude_from_auto_offer": "excludeFromAutoOffer",
        "exclude_from_job_offer_pool": "excludeFromJobOfferPool",
        "expected_duration_hrs": "expectedDurationHrs",
        "expected_duration_mins": "expectedDurationMins",
        "expected_start_date": "expectedStartDate",
        "expected_start_time": "expectedStartTime",
        "expected_end_date": "expectedEndDate",
        "expected_end_time": "expectedEndTime",
        "final_notes": "finalNotes",
        "first_assignment_date": "firstAssignmentDate",
        "first_confirmation_date": "firstConfirmationDate",
        "first_offer_date": "firstOfferDate",
        "first_open_date": "firstOpenDate",
        "flag_for_finance": "flagForFinance",
        "gender_requirement": "genderRequirement",
        "general_customer_account_manager": "customer.accountManager",
        "general_customer_business_unit": "customer.businessUnit",
        "incidentals": "incidentals",
        "integration": "integration",
        "interpreter_notes": "interpreter.notes",
        "interpreter_submitted": "interpreter.submitted",
        "is_auto_verify_ready": "isAutoVerifyReady",
        "is_cancelled": "isCancelled",
        "is_fields_locked": "isFieldsLocked",
        "is_synchronized": "isSynchronized",
        "is_synchronized_manually": "isSynchronizedManually",
        "is_verified": "isVerified",
        "job_complete_email_sent": "jobCompleteEmailSent",
        "job_offers": "jobOffers",
        "language_code": "language.code",
        "last_modified_by": "lastModifiedBy",
        "last_modified_date": "lastModifiedDate",
        "last_synchronized_date": "lastSynchronizedDate",
        "location_note": "location.note",
        "locked": "locked",
        "mileage": "mileage",
        "notes": "notes",
        "notification_email": "notificationEmail",
        "notify": "notify",
        "num_jobs": "numJobs",
        "number_accepted_offers": "numberAcceptedOffers",
        "number_for_telephone_trans": "numberForTelephoneTrans",
        "offer_date": "offerDate",
        "offer_mode": "offerMode",
        "open_date": "openDate",
        "origin_of_request": "originOfRequest",
        "overflow_job_location_uuid": "overflowJobLocationUUID",
        "override_booking_mode": "overrideBookingMode",
        "override_requirements": "overrideRequirements",
        "owner": "owner",
        "place_of_appointment": "placeOfAppointment",
        "preferred_interpreter_declined": "preferredInterpreterDeclined",
        "prevent_edit": "preventEdit",
        "rate_plan_override": "ratePlanOverride",
        "rate_zone_override": "rateZoneOverride",
        "reminder_email_sent": "reminderEmailSent",
        "requested_by": "requestedBy",
        "sessions": "sessions",
        "shift_enabled": "shiftEnabled",
        "signature_hash": "signatureHash",
        "signature_height": "signatureHeight",
        "signature_location": "signatureLocation",
        "signature_raw": "signatureRaw",
        "signature_width": "signatureWidth",
        "signer": "signer",
        "site_contact": "siteContact",
        "sla_reporting_enabled": "slaReportingEnabled",
        "start_editing": "startEditing",
        "sub_location": "sub_location",  # Slight rename if needed
        "sync_uuid": "syncUuid",
        "team_id": "team.id",
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
        "vos": "vos",
        "vos_required": "vosRequired",
        "extra_details": "extraDetails",
        "actual_location_id": "actualLocation.id",
        "billing_customer_id": "billingCustomer.id",
        "billing_location_id": "billingLocation.id",
        "booking_mode_id": "bookingMode.id",
        "client_id": "client.id",
        "company_id": "company.id",
        "customer_id_fk": "customer.id",
        "default_language_id": "defaultLanguage.id",
        "employment_category_id": "employmentCategory.id",
        "invoice_status_id": "invoiceStatus.id",
        "interpreter_id": "interpreter.id",
        "language_id": "language.id",
        "local_booking_mode_id": "localBookingMode.id",
        "location_id_fk": "location.id",
        "overflow_type_id": "overflowType.id",
        "payment_status_id": "paymentStatus.id",
        "preferred_interpreter_id": "preferredInterpreter.id",
        "primary_ref_id": "primaryRef.id",
        "status_id": "status.id",
        "super_booking_id": "superBooking.id",
        "requestor_id": "requestor.id",
        "visit_id": "visit.id"
    }
}

# Define explicit insertion order to satisfy FK constraints.
TABLE_ORDER = [
    "ContractTypes", "Customers", "Locations", "BookingModes", "Clients",
    "Companies", "Languages", "EmploymentCategories", "InvoiceStatuses",
    "PaymentStatuses", "OverflowTypes", "SuperBookings", "Status", "Requestors",
    "VisitStatuses", "Visits", "Interpreters", "Refs", "BookingRefs",
    "BookingOverriddenRequirements", "BookingInvalidFields", "BookingRequirements",
    "Bookings"
]

# Track already fetched URLs
fetched_urls = set()

def authenticate(auth_url, subject, secret):
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
    global fetched_urls
    if url in fetched_urls:
        logging.debug(f"Skipping already fetched URL: {url}")
        return None
    try:
        response = requests.get(url, headers=headers, json=payload) if payload else requests.get(url, headers=headers)
        if response.status_code == 200:
            fetched_urls.add(url)
            logging.debug(f"Fetched data from {url}")
            return response.json()
        else:
            logging.error(f"Failed to fetch {url}: {response.status_code} - {response.text}")
            return None
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def fetch_booking_data(api_url, access_token, start_date, end_date):
    logging.info("Fetching booking data...")
    headers = {"Authorization": f"Bearer {access_token}"}
    all_bookings = []
    page = 1
    while api_url:
        logging.info(f"Fetching page {page} from {api_url}...")
        data_payload = {"expectedStartDateGTE": start_date, "expectedStartDateLT": end_date}
        data = fetch_data(api_url, headers, payload=data_payload)
        if data:
            items = data.get("items", [])
            logging.debug(f"Fetched {len(items)} bookings from page {page}")
            all_bookings.extend(items)
            api_url = data.get("next", None)
        else:
            logging.error(f"Failed to fetch data from {api_url}, stopping pagination.")
            break
        page += 1
        time.sleep(1)
    logging.info(f"Total bookings fetched: {len(all_bookings)}")
    return all_bookings

def extract_uris(item):
    extracted_uris = {}
    for key, value in item.items():
        if isinstance(value, dict) and "uri" in value:
            extracted_uris[key] = value["uri"]
            logging.debug(f"Extracted URI for {key}: {value['uri']}")
    return extracted_uris

def fetch_additional_details(item, access_token):
    booking_id = item["id"]
    logging.info(f"Fetching additional details for booking ID {booking_id}...")
    extracted_uris = extract_uris(item)
    additional_details = {}
    headers = {"Authorization": f"Bearer {access_token}"}
    for key, url in extracted_uris.items():
        if url in fetched_urls:
            logging.debug(f"Skipping already fetched inner API: {url}")
            continue
        logging.debug(f"Fetching additional detail from {url}")
        extra_data = fetch_data(url, headers)
        if extra_data:
            additional_details[key] = extra_data
            logging.debug(f"Fetched additional detail for {key}")
        else:
            logging.warning(f"No data returned for {key}")
        time.sleep(0.5)
    return additional_details

def process_bookings(access_token):
    bookings = fetch_booking_data(DATA_API_URL, access_token, START_DATE, END_DATE)
    detailed_bookings = []
    for item in bookings:
        logging.info(f"Processing booking ID: {item['id']}")
        item["extra_details"] = fetch_additional_details(item, access_token)
        detailed_bookings.append(item)
    return detailed_bookings

def save_json_to_file(data, filename):
    logging.info(f"Saving JSON data to {filename}...")
    try:
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        logging.info(f"Data saved successfully to {filename}")
    except Exception as e:
        logging.error(f"Error saving JSON to {filename}: {e}")

def connect_to_db():
    try:
        conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;')
        logging.info("Connected to SQL Server successfully.")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logging.info(f"Loaded JSON data from {file_path}")
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logging.error(f"Failed to load JSON: {e}")
        return None

def get_nested_value(data, key, default=""):
    """Retrieve nested value using dotted key notation.
       Returns the value if found, otherwise returns the default."""
    for part in key.split('.'):
        if isinstance(data, dict):
            data = data.get(part, default)
        else:
            return default
    if data is None:
        return default
    if isinstance(data, dict):
        return json.dumps(data)
    return data


def extract_values(mapping, json_data):
    extracted_data = []
    for record in json_data:
        row = {}
        for sql_col, json_key in mapping.items():
            value = get_nested_value(record, json_key, default="")
            if value == "":
                logging.debug(f"Missing value for column '{sql_col}' using key '{json_key}' in record id {record.get('id')}")
            row[sql_col] = value
        extracted_data.append(row)
    logging.info(f"Extracted {len(extracted_data)} rows for mapping.")
    return extracted_data


def extract_locations(json_data):
    """Extract both actual and billing locations and deduplicate by location_id."""
    loc_dict = {}
    for record in json_data:
        actual = record.get("actualLocation") or record.get("location")
        if actual and actual.get("id") is not None:
            lid = actual.get("id")
            loc_dict[lid] = {
                "location_id": lid,
                "uri": actual.get("uri"),
                "active": actual.get("active"),
                "addr_entered": actual.get("addrEntered") or actual.get("addressEntered"),
                "cost_center": actual.get("costCenter"),
                "cost_center_name": actual.get("costCenterName"),
                "description": actual.get("description"),
                "display_label": actual.get("displayLabel"),
                "lat": actual.get("lat") or actual.get("latitude"),
                "lng": actual.get("lng") or actual.get("longitude"),
                "uuid": actual.get("uuid")
            }
        billing = record.get("billingLocation")
        if billing and billing.get("id") is not None:
            bid = billing.get("id")
            loc_dict[bid] = {
                "location_id": bid,
                "uri": billing.get("uri"),
                "active": billing.get("active"),
                "addr_entered": billing.get("addrEntered") or billing.get("addressEntered"),
                "cost_center": billing.get("costCenter"),
                "cost_center_name": billing.get("costCenterName"),
                "description": billing.get("description"),
                "display_label": billing.get("displayLabel"),
                "lat": billing.get("lat") or billing.get("latitude"),
                "lng": billing.get("lng") or billing.get("longitude"),
                "uuid": billing.get("uuid")
            }
    locs = list(loc_dict.values())
    logging.info(f"Extracted {len(locs)} unique location records.")
    return locs

def extract_refs(json_data):
    """Extract Refs from both the 'primaryRef' field and the 'refs' array.
       Also check extra_details for primaryRef if missing at the top level."""
    ref_dict = {}
    for record in json_data:
        # Check for primaryRef in the record or in extra_details
        primary = record.get("primaryRef") or record.get("extra_details", {}).get("primaryRef")
        if primary and primary.get("id") is not None:
            rid = primary.get("id")
            if rid not in ref_dict:
                ref_dict[rid] = {
                    "ref_id": rid,
                    "uri": primary.get("uri"),
                    "version_value": primary.get("versionValue"),
                    "approved": primary.get("approved"),
                    "auto_complete": json.dumps(primary.get("autoComplete")) if primary.get("autoComplete") else None,
                    "company_id": primary.get("company", {}).get("id") if primary.get("company") else None,
                    "config_desc": primary.get("configDescription"),
                    "consumer": primary.get("consumer"),
                    "customer_id": primary.get("customer", {}).get("id") if primary.get("customer") else None,
                    "dependent": primary.get("dependent"),
                    "dependent_id": primary.get("dependentId"),
                    "description": primary.get("description"),
                    "name": primary.get("name"),
                    "ref_value": primary.get("ref"),
                    "reference_value": primary.get("referenceValue"),
                    "reference_value_url": primary.get("referenceValueUrl"),
                    "super_booking_id": primary.get("superBooking", {}).get("id") if primary.get("superBooking") else None
                }
        # Process the refs array if present
        refs = record.get("refs")
        if isinstance(refs, list):
            for item in refs:
                rid = item.get("id")
                if rid is not None and rid not in ref_dict:
                    ref_dict[rid] = {
                        "ref_id": rid,
                        "uri": item.get("uri"),
                        "version_value": item.get("versionValue"),
                        "approved": item.get("approved"),
                        "auto_complete": json.dumps(item.get("autoComplete")) if item.get("autoComplete") else None,
                        "company_id": item.get("company", {}).get("id") if item.get("company") else None,
                        "config_desc": item.get("configDescription"),
                        "consumer": item.get("consumer"),
                        "customer_id": item.get("customer", {}).get("id") if item.get("customer") else None,
                        "dependent": item.get("dependent"),
                        "dependent_id": item.get("dependentId"),
                        "description": item.get("description"),
                        "name": item.get("name"),
                        "ref_value": item.get("ref"),
                        "reference_value": item.get("referenceValue"),
                        "reference_value_url": item.get("referenceValueUrl"),
                        "super_booking_id": item.get("superBooking", {}).get("id") if item.get("superBooking") else None
                    }
    refs_list = list(ref_dict.values())
    logging.info(f"Extracted {len(refs_list)} ref records.")
    return refs_list

def verify_refs(bookings, refs_data):
    """For each booking, verify that its primaryRef.id is present in refs_data.
       Log a warning and optionally add a fallback record if missing."""
    extracted_ref_ids = {ref["ref_id"] for ref in refs_data if ref.get("ref_id") is not None}
    missing = []
    for record in bookings:
        primary_id = get_nested_value(record, "primaryRef.id") or get_nested_value(record, "extra_details.primaryRef.id")
        if primary_id and primary_id not in extracted_ref_ids:
            missing.append(primary_id)
    if missing:
        logging.warning(f"Bookings reference {len(missing)} primaryRef IDs missing in Refs: {missing}")
    else:
        logging.info("All primaryRef IDs in bookings are present in Refs.")

def bulk_insert(cursor, table_name, data):
    if not data:
        logging.info(f"No data to insert for table {table_name}")
        return
    columns = ", ".join(data[0].keys())
    placeholders = ", ".join(["?" for _ in data[0]])
    source_columns = ", ".join(["source." + col for col in data[0].keys()])
    merge_query = f"""
    MERGE INTO {table_name} AS target
    USING (SELECT {placeholders}) AS source ({columns})
    ON target.{list(data[0].keys())[0]} = source.{list(data[0].keys())[0]}
    WHEN MATCHED THEN
        UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in data[0].keys()])}
    WHEN NOT MATCHED THEN
        INSERT ({columns}) VALUES ({source_columns});
    """
    values = [tuple(row.values()) for row in data]
    try:
        cursor.executemany(merge_query, values)
        logging.info(f"Inserted/Updated {len(values)} records into {table_name} using batch MERGE.")
    except Exception as batch_error:
        logging.error(f"Batch insert error into {table_name}: {batch_error}. Attempting individual inserts...")
        success_count = 0
        for idx, row in enumerate(values):
            try:
                cursor.execute(merge_query, row)
                success_count += 1
            except Exception as individual_error:
                logging.error(f"Error inserting row {idx} into {table_name}: {row} - {individual_error}")
        logging.info(f"Individually inserted/updated {success_count} records into {table_name}.")

def process_and_insert_data(file_path):
    conn = connect_to_db()
    if not conn:
        return
    cursor = conn.cursor()
    bookings = load_json(file_path)
    if not bookings:
        return

    # Process tables in the specified order.
    for table in TABLE_ORDER:
        try:
            if table == "Locations":
                data = extract_locations(bookings)
            elif table == "Refs":
                data = extract_refs(bookings)
                # Verify that all bookings' primary references are captured.
                verify_refs(bookings, data)
            else:
                data = extract_values(TABLE_MAPPINGS[table], bookings)
            # Filter out rows with missing primary key.
            if table == "Locations":
                primary_key = "location_id"
            elif table == "Refs":
                primary_key = "ref_id"
            else:
                primary_key = list(TABLE_MAPPINGS[table].keys())[0]
            filtered_data = [row for row in data if row.get(primary_key) is not None]
            logging.info(f"Table {table}: {len(filtered_data)} rows ready for insertion (after filtering).")
            bulk_insert(cursor, table, filtered_data)
        except Exception as e:
            logging.error(f"Error processing table {table}: {e}")
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Database operations completed.")

if __name__ == "__main__":
    try:
        logging.info("Starting process...")
        # access_token = authenticate(AUTH_URL, SUBJECT, SECRET)
        # all_data = process_bookings(access_token)
        # save_json_to_file(all_data, OUTPUT_FILE)
        process_and_insert_data(OUTPUT_FILE)
        logging.info("Process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
