import os
import json
import logging
import time
import asyncio
import aiohttp
import pyodbc
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
LOG_FILE = "api_process_1M_enhanced.log"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Updated connection info for local testing
server = 'Pri-Yoga'
database = 'MCIS_BOOKING_TEST1'
driver = '{ODBC Driver 17 for SQL Server}'

def get_db_connection():
    try:
        conn = pyodbc.connect(
            f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;'
        )
        logging.info("Connected to SQL Server successfully.")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

AUTH_URL = os.getenv("AUTH_URL")
DATA_API_URL = os.getenv("DATA_API_URL")
SUBJECT = os.getenv("SUBJECT")
SECRET = os.getenv("SECRET")
START_DATE = "2025-01-01T00:00:00Z"
END_DATE = "2025-01-30T00:00:00Z"

OUTPUT_FILE = "booking_data_flat.json"

# Global cache for async API responses
api_cache = {}

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

async def fetch_data_async(session, url, headers, payload=None):
    if url in api_cache:
        logging.debug(f"Cache hit for URL: {url}")
        return api_cache[url]
    try:
        if payload:
            async with session.post(url, json=payload, headers=headers) as resp:
                data = await resp.json()
        else:
            async with session.get(url, headers=headers) as resp:
                data = await resp.json()
        api_cache[url] = data
        logging.debug(f"Fetched async data from {url}")
        return data
    except Exception as e:
        logging.error(f"Async error fetching {url}: {e}")
        return None

def flatten_json(y, parent_key='', sep='.'):
    """Recursively flattens nested JSON into a flat dictionary."""
    items = {}
    if isinstance(y, dict):
        for k, v in y.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.update(flatten_json(v, new_key, sep=sep))
    elif isinstance(y, list):
        for i, v in enumerate(y):
            new_key = f"{parent_key}{sep}{i}"
            items.update(flatten_json(v, new_key, sep=sep))
    else:
        items[parent_key] = y
    return items

async def enrich_record(record, headers):
    """Fetch additional details asynchronously for a record based on keys ending with 'uri'."""
    uris = {key: record[key] for key in record if key.endswith("uri") and isinstance(record[key], str) and record[key].startswith("http")}
    enriched = {}
    async with aiohttp.ClientSession() as session:
        tasks = []
        for key, url in uris.items():
            tasks.append(fetch_data_async(session, url, headers))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (key, url), res in zip(uris.items(), results):
            if isinstance(res, Exception):
                logging.error(f"Error fetching {url}: {res}")
            else:
                enriched[key] = res
    return enriched

async def fetch_booking_data_paged(access_token):
    """Fetch booking data using pagination by following the 'next' URL."""
    headers = {"Authorization": f"Bearer {access_token}"}
    all_bookings = []
    next_url = DATA_API_URL
    payload = {"expectedStartDateGTE": START_DATE, "expectedStartDateLT": END_DATE}
    while next_url:
        logging.info(f"Fetching page: {next_url}")
        response = requests.get(next_url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        all_bookings.extend(items)
        next_url = data.get("next")  # Expect full URL or None
        time.sleep(1)  # avoid hammering API
    logging.info(f"Fetched total {len(all_bookings)} booking items with pagination.")
    return all_bookings

async def process_bookings_async(access_token):
    logging.info("Fetching booking data with pagination...")
    bookings = await fetch_booking_data_paged(access_token)
    logging.info(f"Fetched {len(bookings)} booking items.")
    flat_bookings = []
    for record in bookings:
        flat_record = flatten_json(record)
        enriched = await enrich_record(record, {"Authorization": f"Bearer {access_token}"})
        # Remove any nested extra details and store the enrichment separately
        flat_record.pop("extra_details", None)
        flat_record["_enriched"] = enriched
        flat_bookings.append(flat_record)
    return flat_bookings

# Update TABLE_ORDER and TABLE_MAPPINGS to use singular names that match the DB design.
TABLE_ORDER = [
    "ContractTypes", "Customers", "Locations", "BookingModes", "Clients",
    "Companies", "Languages", "EmploymentCategories", "InvoiceStatuses",
    "PaymentStatuses", "OverflowTypes", "SuperBookings", "Status", "Requestors",
    "VisitStatuses", "Visits", "Interpreters", "Refs", "BookingRefs",
    "BookingOverriddenRequirements", "BookingInvalidFields", "BookingRequirements",
    "Bookings"
]

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
    # For locations, we extract from several potential prefixes
    "Locations": {},  # handled separately by extract_locations()
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
        "iso6393Tag": "language.iso6393Tag",
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
    "BookingStatuses": {
        "booking_status_id": "status.id",
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
        "firstName": "requestor.firstName",
        "lastName": "requestor.lastName",
        "passwordLastChange": "requestor.passwordLastChange",
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
        "contact_rate_plan": "visit.contactRatePlan",
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
    "Refs": {
        "ref_id": "refs.id",
        "uri": "refs.uri",
        "version_value": "refs.versionValue",
        "approved": "refs.approved",
        "auto_complete": "refs.autoComplete",
        "company_id": "refs.company.id",
        "config_desc": "refs.configDescription",
        "consumer": "refs.consumer",
        "customer_id": "refs.customer.id",
        "dependent": "refs.dependent",
        "dependent_id": "refs.dependentId",
        "description": "refs.description",
        "name": "refs.name",
        "ref_value": "refs.refValue",
        "reference_value": "refs.referenceValue",
        "reference_value_url": "refs.referenceValueUrl",
        "super_booking_id": "refs.superBooking.id"
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
        "contact_rate_plan": "contact_rate_plan",
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
        "sub_location": "subLocation",
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
        "vos_required": "vosRequired"
    },
    # BookingExtraDetails is handled separately.
}

def extract_values(mapping, flat_data):
    extracted = []
    for record in flat_data:
        row = {}
        for col, key in mapping.items():
            value = record.get(key, "")
            if value == "":
                logging.debug(f"Missing value for column '{col}' using key '{key}' in record id {record.get('id', 'unknown')}")
            row[col] = value
        extracted.append(row)
    logging.info(f"Extracted {len(extracted)} rows using mapping.")
    return extracted

def extract_locations(flat_data):
    loc_dict = {}
    for record in flat_data:
        for prefix in ["actualLocation", "billingLocation", "location"]:
            loc_id = record.get(f"{prefix}.id")
            if loc_id:
                loc_dict[loc_id] = {
                    "location_id": loc_id,
                    "uri": record.get(f"{prefix}.uri", ""),
                    "active": record.get(f"{prefix}.active", ""),
                    "addr_entered": record.get(f"{prefix}.addrEntered", "") or record.get(f"{prefix}.addressEntered", ""),
                    "cost_center": record.get(f"{prefix}.costCenter", ""),
                    "cost_center_name": record.get(f"{prefix}.costCenterName", ""),
                    "description": record.get(f"{prefix}.description", ""),
                    "display_label": record.get(f"{prefix}.displayLabel", ""),
                    "lat": record.get(f"{prefix}.lat", "") or record.get(f"{prefix}.latitude", ""),
                    "lng": record.get(f"{prefix}.lng", "") or record.get(f"{prefix}.longitude", ""),
                    "uuid": record.get(f"{prefix}.uuid", "")
                }
    locs = list(loc_dict.values())
    logging.info(f"Extracted {len(locs)} unique location records.")
    return locs

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
    except Exception as e:
        logging.error(f"Batch insert error into {table_name}: {e}. Attempting individual inserts...")
        success_count = 0
        for idx, row in enumerate(values):
            try:
                cursor.execute(merge_query, row)
                success_count += 1
            except Exception as ie:
                logging.error(f"Error inserting row {idx} into {table_name}: {row} - {ie}")
        logging.info(f"Individually inserted/updated {success_count} records into {table_name}.")

def insert_extra_details(cursor, flat_bookings):
    """Insert extra details into BookingExtraDetails table.
       Expected columns: booking_id (FK) and extra_details (NVARCHAR(MAX))."""
    details = []
    for record in flat_bookings:
        booking_id = record.get("id")
        enriched = record.get("_enriched", {})
        extra_json = json.dumps(enriched)
        if booking_id:
            details.append({"booking_id": booking_id, "extra_details": extra_json})
    if not details:
        logging.info("No extra details to insert.")
        return
    columns = "booking_id, extra_details"
    placeholders = "?, ?"
    insert_query = f"INSERT INTO BookingExtraDetails ({columns}) VALUES ({placeholders});"
    values = [(d["booking_id"], d["extra_details"]) for d in details]
    try:
        cursor.executemany(insert_query, values)
        logging.info(f"Inserted/Updated {len(values)} records into BookingExtraDetails.")
    except Exception as e:
        logging.error(f"Error inserting into BookingExtraDetails: {e}")

def process_and_insert_data(flat_bookings):
    conn = get_db_connection()
    if not conn:
        return
    cursor = conn.cursor()
    for table in TABLE_ORDER:
        try:
            if table == "Location":
                data = extract_locations(flat_bookings)
            elif table == "BookingExtraDetails":
                # Skip here; will be handled separately
                continue
            else:
                mapping = TABLE_MAPPINGS.get(table, {})
                data = extract_values(mapping, flat_bookings)
            # Use the first column in the mapping as primary key (for filtering)
            if table == "Location":
                primary_key = "location_id"
            elif table in TABLE_MAPPINGS and TABLE_MAPPINGS[table]:
                primary_key = list(TABLE_MAPPINGS[table].keys())[0]
            else:
                primary_key = "id"
            filtered_data = [row for row in data if row.get(primary_key) not in (None, "")]
            logging.info(f"Table {table}: {len(filtered_data)} rows ready for insertion (after filtering).")
            bulk_insert(cursor, table, filtered_data)
        except Exception as e:
            logging.error(f"Error processing table {table}: {e}")
    # Insert extra details separately
    insert_extra_details(cursor, flat_bookings)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Database operations completed.")

async def main():
    logging.info("Starting enhanced process...")
    access_token = authenticate(AUTH_URL, SUBJECT, SECRET)
    flat_bookings = await process_bookings_async(access_token)
    logging.info(f"Flattened and enriched {len(flat_bookings)} booking records.")
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(flat_bookings, f, indent=4)
        logging.info(f"Flat JSON data saved to {OUTPUT_FILE}")
    except Exception as e:
        logging.error(f"Error saving flat JSON: {e}")
    process_and_insert_data(flat_bookings)
    logging.info("Enhanced process completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
