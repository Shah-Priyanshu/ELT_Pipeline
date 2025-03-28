CREATE TABLE Bookings (
    booking_id                  INT PRIMARY KEY,       -- JSON "id"
    uri                         VARCHAR(255) NOT NULL,
    version_value               INT,                   -- JSON "versionValue"
    access_code                 VARCHAR(50),
    accounting_reference        VARCHAR(50),
    actual_duration_hrs         FLOAT,
    actual_duration_mins        FLOAT,
    actual_start_date           TIMESTAMP,
    actual_end_date             TIMESTAMP,
    actual_location_display     VARCHAR(255),          -- "actualLocationDisplayLabel"
    
    assigned_by                 VARCHAR(100),
    assigned_by_username        VARCHAR(100),
    assignment_date             TIMESTAMP,
    
    auto_verify_duration        BOOLEAN,
    auto_verify_incidentals     BOOLEAN,
    
    average_rating              FLOAT,
    billing_notes               TEXT,
    
    booking_date                TIMESTAMP,
    booking_time                TIMESTAMP,
    cancellation_date           TIMESTAMP,
    cancellation_reason         TEXT,
    
    company_special_instructions TEXT,
    confirmation_date           TIMESTAMP,
    
    -- If consumer data is rarely searched, store it as JSON:
    consumer                    JSONB,
    consumer_count              INT,
    consumer_count_enabled      BOOLEAN,
    
    contact_arrival_date        TIMESTAMP,
    contact_late_mins           INT,
    contact_rate_plan           VARCHAR(50),
    contact_special_instructions TEXT,
    
    created_by                  VARCHAR(100),
    created_date                TIMESTAMP,
    
    currency_code               VARCHAR(10),
    currency_symbol             VARCHAR(10),
    
    custom_consumer             TEXT,
    custom_requestor            TEXT,
    
    customer_business_unit      VARCHAR(100),
    customer_duration_override  FLOAT,
    customer_notes              TEXT,
    customer_rate_plan          VARCHAR(50),
    customer_rate_plan_assoc    VARCHAR(50),
    customer_rate_plan_override VARCHAR(50),
    customer_rate_zone_override VARCHAR(50),
    customer_special_instructions TEXT,
    
    disclaimer_accepted         BOOLEAN,
    disclaimer_accepted_date    TIMESTAMP,
    disclaimer_accepted_initials VARCHAR(10),
    
    duration_override_hrs       FLOAT,
    esignature_grace_period     INT,
    esignature_required         BOOLEAN,
    exclude_from_auto_offer     BOOLEAN,
    exclude_from_job_offer_pool BOOLEAN,
    
    expected_duration_hrs       FLOAT,
    expected_duration_mins      FLOAT,
    expected_start_date         TIMESTAMP,
    expected_end_date           TIMESTAMP,
    expected_start_time         TIMESTAMP,
    expected_end_time           TIMESTAMP,
    
    final_notes                 TEXT,
    first_assignment_date       TIMESTAMP,
    first_confirmation_date     TIMESTAMP,
    first_offer_date            TIMESTAMP,
    first_open_date             TIMESTAMP,
    flag_for_finance            BOOLEAN,
    gender_requirement          VARCHAR(50),
    general_customer_account_manager VARCHAR(100),
    general_customer_business_unit   VARCHAR(100),
    
    incidentals                 VARCHAR(255),   -- URL string
    integration                 JSONB,
    interpreter_notes           TEXT,
    interpreter_submitted       BOOLEAN,
    invalid_fields              JSONB,          -- array data
    is_auto_verify_ready        BOOLEAN,
    is_cancelled                BOOLEAN,
    is_fields_locked            BOOLEAN,
    is_synchronized             BOOLEAN,
    is_synchronized_manually    BOOLEAN,
    is_verified                 BOOLEAN,
    job_complete_email_sent     BOOLEAN,
    job_offers                  VARCHAR(255),   -- URL
    language_code               VARCHAR(10),
    last_modified_by            VARCHAR(100),
    last_modified_date          TIMESTAMP,
    last_synchronized_date      TIMESTAMP,
    location_note               VARCHAR(255),
    locked                      BOOLEAN,
    mileage                     FLOAT,
    notes                       TEXT,
    notification_email          VARCHAR(100),
    notify                      BOOLEAN,
    num_jobs                    INT,
    number_accepted_offers      INT,
    number_for_telephone_trans  VARCHAR(50),
    offer_date                  TIMESTAMP,
    offer_mode                  VARCHAR(50),
    open_date                   TIMESTAMP,
    origin_of_request           VARCHAR(50),
    overflow_job_location_uuid  VARCHAR(50),
    override_booking_mode       VARCHAR(50),
    override_requirements       BOOLEAN,
    owner                       VARCHAR(100),
    place_of_appointment        VARCHAR(255),
    preferred_interpreter_declined BOOLEAN,
    prevent_edit                BOOLEAN,
    rate_plan_override          VARCHAR(50),
    rate_zone_override          VARCHAR(50),
    reminder_email_sent         BOOLEAN,
    requested_by                VARCHAR(100),
    requirements                JSONB,          -- stored as JSON array/object
    sessions                    VARCHAR(255),   -- URL for sessions
    shift_enabled               BOOLEAN,
    signature_hash              VARCHAR(255),
    signature_height            FLOAT,
    signature_location          VARCHAR(255),
    signature_raw               TEXT,
    signature_width             FLOAT,
    signer                      VARCHAR(100),
    site_contact                VARCHAR(100),
    sla_reporting_enabled       BOOLEAN,
    start_editing               TIMESTAMP,
    sub_location                VARCHAR(100),
    sync_uuid                   VARCHAR(50),
    team_id                     INT,
    team_size                   INT,
    time_interpreter_arrived_inbound  TIMESTAMP,
    time_interpreter_arrived_outbound TIMESTAMP,
    time_interpreter_departed_inbound  TIMESTAMP,
    time_interpreter_departed_outbound TIMESTAMP,
    time_reconfirmed_customer   TIMESTAMP,
    time_tracking_enabled       BOOLEAN,
    time_zone                   VARCHAR(50),
    time_zone_display_name      VARCHAR(50),
    unarchived_updates          BOOLEAN,
    unfulfilled_date            TIMESTAMP,
    unfulfilled_reason          TEXT,
    user_editing                VARCHAR(100),
    uuid                        VARCHAR(50) NOT NULL UNIQUE,
    valid                       BOOLEAN,
    verified_date               TIMESTAMP,
    vos                         JSONB,          -- if complex, stored as JSON array/object
    vos_required                BOOLEAN,
    
    -- Store the nested extra_details block as JSON since it is complex and redundant with core fields:
    extra_details               JSONB,
    
    -- Foreign keys linking to normalized entities:
    actual_location_id          INT,  -- from actualLocation
    billing_customer_id         INT,  -- from billingCustomer (if different from customer)
    billing_location_id         INT,  -- from billingLocation
    booking_mode_id             INT,  -- from bookingMode
    client_id                   INT,
    company_id                  INT,
    customer_id_fk              INT,  -- customer (could be same as billing_customer_id if shared)
    default_language_id         INT,
    employment_category_id      INT,
    invoice_status_id           INT,
    interpreter_id_fk           INT,
    language_id_fk              INT,  -- can be same as default_language_id if desired
    local_booking_mode_id       INT,  -- from localBookingMode
    location_id_fk              INT,  -- from top-level location
    overflow_type_id            INT,
    payment_status_id           INT,
    preferred_interpreter_id    INT,
    primary_ref_id              INT,  -- from primaryRef
    status_id                   INT,
    super_booking_id            INT,
    requestor_id                INT,
    visit_id                    INT,
    
    CONSTRAINT fk_actual_location FOREIGN KEY (actual_location_id) REFERENCES Locations(location_id),
    CONSTRAINT fk_billing_location FOREIGN KEY (billing_location_id) REFERENCES Locations(location_id),
    CONSTRAINT fk_booking_mode FOREIGN KEY (booking_mode_id) REFERENCES BookingModes(booking_mode_id),
    CONSTRAINT fk_client FOREIGN KEY (client_id) REFERENCES Clients(client_id),
    CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES Companies(company_id),
    CONSTRAINT fk_customer FOREIGN KEY (customer_id_fk) REFERENCES Customers(customer_id),
    CONSTRAINT fk_default_language FOREIGN KEY (default_language_id) REFERENCES Languages(language_id),
    CONSTRAINT fk_employment_category FOREIGN KEY (employment_category_id) REFERENCES EmploymentCategories(employment_category_id),
    CONSTRAINT fk_invoice_status FOREIGN KEY (invoice_status_id) REFERENCES InvoiceStatuses(invoice_status_id),
    CONSTRAINT fk_interpreter FOREIGN KEY (interpreter_id_fk) REFERENCES Interpreters(interpreter_id),
    CONSTRAINT fk_language FOREIGN KEY (language_id_fk) REFERENCES Languages(language_id),
    CONSTRAINT fk_local_booking_mode FOREIGN KEY (local_booking_mode_id) REFERENCES BookingModes(booking_mode_id),
    CONSTRAINT fk_location FOREIGN KEY (location_id_fk) REFERENCES Locations(location_id),
    CONSTRAINT fk_overflow_type FOREIGN KEY (overflow_type_id) REFERENCES OverflowTypes(overflow_type_id),
    CONSTRAINT fk_payment_status FOREIGN KEY (payment_status_id) REFERENCES PaymentStatuses(payment_status_id),
    CONSTRAINT fk_preferred_interpreter FOREIGN KEY (preferred_interpreter_id) REFERENCES Interpreters(interpreter_id),
    CONSTRAINT fk_primary_ref FOREIGN KEY (primary_ref_id) REFERENCES Refs(ref_id),
    CONSTRAINT fk_status FOREIGN KEY (status_id) REFERENCES Status(status_id),
    CONSTRAINT fk_super_booking FOREIGN KEY (super_booking_id) REFERENCES SuperBookings(super_booking_id),
    CONSTRAINT fk_requestor FOREIGN KEY (requestor_id) REFERENCES Requestors(requestor_id),
    CONSTRAINT fk_visit FOREIGN KEY (visit_id) REFERENCES Visits(visit_id)
);

CREATE TABLE Locations (
    location_id    INT PRIMARY KEY,
    uri            VARCHAR(255),
    active         BOOLEAN,
    addr_entered   VARCHAR(255),  -- "addrEntered"
    cost_center    VARCHAR(100),
    cost_center_name VARCHAR(100),
    description    VARCHAR(255),
    display_label  VARCHAR(255),
    lat            FLOAT,
    lng            FLOAT,
    uuid           VARCHAR(50) UNIQUE
);

CREATE TABLE BookingModes (
    booking_mode_id INT PRIMARY KEY,
    uri             VARCHAR(255),
    description     VARCHAR(255),
    name            VARCHAR(100),
    name_key        VARCHAR(50)
);


CREATE TABLE Clients (
    client_id     INT PRIMARY KEY,
    uri           VARCHAR(255),
    display_name  VARCHAR(255),
    name          VARCHAR(255),
    uuid          VARCHAR(50) UNIQUE
);

CREATE TABLE Companies (
    company_id    INT PRIMARY KEY,
    uri           VARCHAR(255),
    description   VARCHAR(255),
    name          VARCHAR(255),
    uuid          VARCHAR(50) UNIQUE
);

CREATE TABLE ContractTypes (
    contract_type_id INT PRIMARY KEY,
    uri              VARCHAR(255),
    version_value    INT,
    default_option   BOOLEAN,
    description      VARCHAR(255),
    l10n_key         VARCHAR(50),
    name             VARCHAR(100),
    name_key         VARCHAR(50)
);

CREATE TABLE Customers (
    customer_id   INT PRIMARY KEY,
    uri           VARCHAR(255),
    display_name  VARCHAR(255),
    name          VARCHAR(255),
    uuid          VARCHAR(50) UNIQUE,
    contract_type_id INT,
    CONSTRAINT fk_contract_type
      FOREIGN KEY (contract_type_id) REFERENCES ContractTypes(contract_type_id)
);

CREATE TABLE Languages (
    language_id     INT PRIMARY KEY,
    uri             VARCHAR(255),
    bpin_opi_enabled BOOLEAN,
    bpin_vri_enabled BOOLEAN,
    description     VARCHAR(100),
    display_name    VARCHAR(100),
    is_sign         BOOLEAN,
    iso639_3_tag    VARCHAR(10),
    mmis_code       VARCHAR(50),
    opi_enabled     BOOLEAN,
    subtag          VARCHAR(10),
    vri_enabled     BOOLEAN
);

CREATE TABLE EmploymentCategories (
    employment_category_id INT PRIMARY KEY,
    uri                    VARCHAR(255),
    description            VARCHAR(255),
    name                   VARCHAR(100),
    name_key               VARCHAR(50)
);

CREATE TABLE InvoiceStatuses (
    invoice_status_id INT PRIMARY KEY,
    uri               VARCHAR(255),
    description       VARCHAR(255),
    name              VARCHAR(100),
    name_key          VARCHAR(50)
);

CREATE TABLE PaymentStatuses (
    payment_status_id INT PRIMARY KEY,
    uri               VARCHAR(255),
    description       VARCHAR(255),
    name              VARCHAR(100),
    name_key          VARCHAR(50)
);

CREATE TABLE OverflowTypes (
    overflow_type_id INT PRIMARY KEY,
    uri              VARCHAR(255),
    description      VARCHAR(255),
    name_key         VARCHAR(50)
);

CREATE TABLE SuperBookings (
    super_booking_id INT PRIMARY KEY,
    uri              VARCHAR(255),
    uuid             VARCHAR(50)
);

CREATE TABLE Status (
    status_id INT PRIMARY KEY,
    uri       VARCHAR(255),
    description VARCHAR(255),
    name      VARCHAR(100),
    name_key  VARCHAR(50)
);

CREATE TABLE Requestors (
    requestor_id INT PRIMARY KEY,
    uri          VARCHAR(255),
    display_label VARCHAR(255),
    display_name VARCHAR(255),
    name         VARCHAR(100),
    uuid         VARCHAR(50) UNIQUE,
    email        VARCHAR(100),
    enabled      BOOLEAN,
    fax_number   VARCHAR(50),
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    password_last_change TIMESTAMP,
    username     VARCHAR(100)
);

CREATE TABLE VisitStatuses (
    visit_status_id INT PRIMARY KEY,
    uri             VARCHAR(255),
    version_value   INT,
    default_option  BOOLEAN,
    description     VARCHAR(255),
    in_order        INT,
    l10n_key        VARCHAR(50),
    message         TEXT,
    name            VARCHAR(100),
    name_key        VARCHAR(50)
);

CREATE TABLE Visits (
    visit_id        INT PRIMARY KEY,
    uri             VARCHAR(255),
    contact_rate_plan VARCHAR(100),
    customer_rate_plan VARCHAR(100),
    uuid            VARCHAR(50),
    visit_status_id INT,
    CONSTRAINT fk_visit_status
      FOREIGN KEY (visit_status_id) REFERENCES VisitStatuses(visit_status_id)
);

CREATE TABLE Interpreters (
    interpreter_id INT PRIMARY KEY,
    uri            VARCHAR(255),
    display_name   VARCHAR(255),
    name           VARCHAR(100),
    email          VARCHAR(100),
    phone          VARCHAR(20),
    payment_method VARCHAR(50),
    time_zone      VARCHAR(50),
    uuid           VARCHAR(50) UNIQUE
);

CREATE TABLE Refs (
    ref_id         INT PRIMARY KEY,
    uri            VARCHAR(255),
    version_value  INT,
    approved       BOOLEAN,
    auto_complete  VARCHAR(255), -- if needed (or you can normalize this further)
    company_id     INT,
    config          JSONB,       -- store the config object as JSON (or normalize if queried frequently)
    consumer       TEXT,
    customer_id    INT,
    dependent      TEXT,
    dependent_id   INT,
    description    TEXT,
    name           VARCHAR(100),
    ref_value      VARCHAR(50),
    reference_value VARCHAR(100),
    reference_value_url VARCHAR(255),
    super_booking_id INT,
    CONSTRAINT fk_ref_company FOREIGN KEY (company_id) REFERENCES Companies(company_id),
    CONSTRAINT fk_ref_super_booking FOREIGN KEY (super_booking_id) REFERENCES SuperBookings(super_booking_id)
);

-- Linking table for many-to-many relationship between Bookings and Refs
CREATE TABLE BookingReferences (
    booking_id INT,
    ref_id     INT,
    PRIMARY KEY (booking_id, ref_id),
    CONSTRAINT fk_br_booking FOREIGN KEY (booking_id) REFERENCES Bookings(booking_id),
    CONSTRAINT fk_br_ref FOREIGN KEY (ref_id) REFERENCES Refs(ref_id)
);

CREATE TABLE BookingOverriddenRequirements (
    booking_id  INT,
    requirement VARCHAR(255),
    PRIMARY KEY (booking_id, requirement),
    FOREIGN KEY (booking_id) REFERENCES Bookings(booking_id)
);

