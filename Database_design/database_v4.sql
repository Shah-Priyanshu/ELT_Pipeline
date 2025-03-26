-------------------------------------------------------------------
-- 1) DROP TABLES IF EXISTS (to allow rerun)
-------------------------------------------------------------------
IF OBJECT_ID('[dbo].[BookingRefs]', 'U') IS NOT NULL DROP TABLE [dbo].[BookingRefs];
IF OBJECT_ID('[dbo].[BookingRequirements]', 'U') IS NOT NULL DROP TABLE [dbo].[BookingRequirements];
IF OBJECT_ID('[dbo].[Booking]', 'U') IS NOT NULL DROP TABLE [dbo].[Booking];
IF OBJECT_ID('[dbo].[Visit]', 'U') IS NOT NULL DROP TABLE [dbo].[Visit];
IF OBJECT_ID('[dbo].[Requestor]', 'U') IS NOT NULL DROP TABLE [dbo].[Requestor];
IF OBJECT_ID('[dbo].[Interpreter]', 'U') IS NOT NULL DROP TABLE [dbo].[Interpreter];
IF OBJECT_ID('[dbo].[Customer]', 'U') IS NOT NULL DROP TABLE [dbo].[Customer];
IF OBJECT_ID('[dbo].[Address]', 'U') IS NOT NULL DROP TABLE [dbo].[Address];
IF OBJECT_ID('[dbo].[Integration]', 'U') IS NOT NULL DROP TABLE [dbo].[Integration];
IF OBJECT_ID('[dbo].[Config]', 'U') IS NOT NULL DROP TABLE [dbo].[Config];
IF OBJECT_ID('[dbo].[Company]', 'U') IS NOT NULL DROP TABLE [dbo].[Company];
GO

-------------------------------------------------------------------
-- 2) COMPANY
-------------------------------------------------------------------
CREATE TABLE [dbo].[Company] (
    company_id      INT             NOT NULL,
    uri             VARCHAR(500)    NULL,
    description     VARCHAR(500)    NULL,
    name            VARCHAR(255)    NULL,
    uuid            UNIQUEIDENTIFIER NULL,
    CONSTRAINT PK_Company PRIMARY KEY (company_id)
);
GO

-------------------------------------------------------------------
-- 3) CONFIG
-------------------------------------------------------------------
CREATE TABLE [dbo].[Config] (
    config_id               INT             NOT NULL,
    uri                     VARCHAR(500)    NULL,
    uuid                    UNIQUEIDENTIFIER NULL,
    allow_free_text         BIT             NULL,
    customer_specific       BIT             NULL,
    enable_dropdown         BIT             NULL,
    required                BIT             NULL,
    select_field            BIT             NULL,
    visible_for_interpreters BIT            NULL,
    CONSTRAINT PK_Config PRIMARY KEY (config_id)
);
GO

-------------------------------------------------------------------
-- 4) INTEGRATION
-------------------------------------------------------------------
CREATE TABLE [dbo].[Integration] (
    integration_id  INT             NOT NULL,
    uri             VARCHAR(500)    NULL,
    description     VARCHAR(500)    NULL,
    name            VARCHAR(255)    NULL,
    uuid            UNIQUEIDENTIFIER NULL,
    CONSTRAINT PK_Integration PRIMARY KEY (integration_id)
);
GO

-------------------------------------------------------------------
-- 5) ADDRESS
-- used by location, actualLocation, billingLocation, subLocation, etc.
-------------------------------------------------------------------
CREATE TABLE [dbo].[Address] (
    address_id               INT             NOT NULL,
    uri                      VARCHAR(500)    NULL,
    active                   BIT             NULL,
    addr_entered             VARCHAR(500)    NULL,
    addr_formatted           VARCHAR(500)    NULL,
    cost_center              VARCHAR(100)    NULL,
    cost_center_name         VARCHAR(255)    NULL,
    description              VARCHAR(500)    NULL,
    display_label            VARCHAR(255)    NULL,
    lat                      DECIMAL(9,6)    NULL,
    lng                      DECIMAL(9,6)    NULL,
    notes                    VARCHAR(MAX)    NULL,
    public_notes             VARCHAR(MAX)    NULL,
    city_town                VARCHAR(100)    NULL,
    postal_code              VARCHAR(50)     NULL,
    state_county             VARCHAR(100)    NULL,
    country                  VARCHAR(100)    NULL,
    timezone                 VARCHAR(100)    NULL,
    validated                BIT             NULL,
    validation_status        VARCHAR(100)    NULL,
    uuid                     UNIQUEIDENTIFIER NULL,
    CONSTRAINT PK_Address PRIMARY KEY (address_id)
);
GO

-------------------------------------------------------------------
-- 6) CUSTOMER
-- merges "customer.*" and "billingCustomer.*" fields
-------------------------------------------------------------------
CREATE TABLE [dbo].[Customer] (
    customer_id              INT             NOT NULL,
    uri                      VARCHAR(500)    NULL,
    version_value            INT             NULL,
    access_code              VARCHAR(100)    NULL,
    accounting_reference     VARCHAR(100)    NULL,
    contract_type_id         INT             NULL,  -- if you want a separate ContractType table
    display_name             VARCHAR(255)    NULL,
    name                     VARCHAR(255)    NULL,
    uuid                     UNIQUEIDENTIFIER NULL,
    company_id               INT             NULL,   -- references [Company]
    config_id                INT             NULL,   -- references [Config]
    parent_accounting_ref    VARCHAR(100)    NULL,
    payment_days_due         INT             NULL,
    payment_terms            VARCHAR(100)    NULL,
    enable_all_services      BIT             NULL,
    umbrella_number          VARCHAR(100)    NULL,
    website                  VARCHAR(255)    NULL,
    status_name              VARCHAR(100)    NULL,   -- from "status.name"
    status_name_key          VARCHAR(100)    NULL,   -- from "status.nameKey"
    active                   BIT             NULL,    -- interpret from "status.nameKey" if you like
    last_modified_by         VARCHAR(100)    NULL,
    last_modified_date       DATETIME        NULL,
    created_by               VARCHAR(100)    NULL,
    created_date             DATETIME        NULL,
    customer_special_instructions VARCHAR(MAX) NULL, -- from "customerSpecialInstructions"
    account_executive        VARCHAR(255)    NULL,   -- from "accountExecutive"
    account_manager          VARCHAR(255)    NULL,   -- from "accountManager"
    accounting_sick_leave_code VARCHAR(100)  NULL,   -- from "accountingSickLeaveCode"
    billing_account          VARCHAR(100)    NULL,
    billing_method           VARCHAR(100)    NULL,   -- if used
    notes                    VARCHAR(MAX)    NULL,   -- from "notes"
    parent_name              VARCHAR(255)    NULL,   -- from "parentName"
    purchase_order_number    VARCHAR(100)    NULL,
    is_synchronized          BIT             NULL,
    is_synchronized_manually BIT             NULL,
    last_synchronized_date   DATETIME        NULL,
    CONSTRAINT PK_Customer PRIMARY KEY (customer_id),
    CONSTRAINT FK_Customer_Company FOREIGN KEY (company_id) REFERENCES [dbo].[Company](company_id),
    CONSTRAINT FK_Customer_Config  FOREIGN KEY (config_id)  REFERENCES [dbo].[Config](config_id)
);
GO

-------------------------------------------------------------------
-- 7) INTERPRETER (a.k.a. "contact" for the assigned interpreter)
-------------------------------------------------------------------
CREATE TABLE [dbo].[Interpreter] (
    interpreter_id               INT             NOT NULL,
    uri                          VARCHAR(500)    NULL,
    display_name                 VARCHAR(255)    NULL,
    name                         VARCHAR(255)    NULL,
    first_name                   VARCHAR(255)    NULL,  -- caution: your JSON's top-level firstName is actually requestor's
    last_name                    VARCHAR(255)    NULL,
    middle_name                  VARCHAR(255)    NULL,
    nick_name                    VARCHAR(255)    NULL,
    gender_id                    INT             NULL,  -- or store as string
    time_zone                    VARCHAR(100)    NULL,
    time_zone_display_name       VARCHAR(50)     NULL,
    primary_email                VARCHAR(255)    NULL,
    primary_phone                VARCHAR(50)     NULL,
    payment_method_id            INT             NULL,
    employment_category_id       INT             NULL,
    assignment_tier_id           INT             NULL,
    out_of_office                BIT             NULL,
    out_of_office_since          DATETIME        NULL,
    enable_all_services          BIT             NULL,
    currency_code                VARCHAR(20)     NULL,
    region                       VARCHAR(100)    NULL,
    active_note                  VARCHAR(MAX)    NULL,
    uuid                         UNIQUEIDENTIFIER NULL,
    created_by                   VARCHAR(100)    NULL,
    created_date                 DATETIME        NULL,
    last_modified_by             VARCHAR(100)    NULL,
    last_modified_date           DATETIME        NULL,
    iol_nrcpd_number             VARCHAR(100)    NULL,
    re_activation_date           DATETIME        NULL,
    referral_source              VARCHAR(255)    NULL,
    recruiter_name               VARCHAR(255)    NULL,
    has_children                 BIT             NULL,
    has_transportation           BIT             NULL,
    search_radius                INT             NULL,
    induction_date               DATETIME        NULL,
    date_photo_sent_to_interpreter DATETIME      NULL,
    date_photo_sent_to_printer   DATETIME        NULL,
    document_id                  INT             NULL,   -- if you store doc references
    eft_id                       INT             NULL,   -- from "eft.id" yes/no
    sort_code                    VARCHAR(100)    NULL,
    swift                        VARCHAR(100)    NULL,
    iban                         VARCHAR(100)    NULL,
    bank_account                 VARCHAR(100)    NULL,
    bank_account_description     VARCHAR(255)    NULL,
    bank_account_reference       VARCHAR(255)    NULL,
    bank_branch                  VARCHAR(100)    NULL,
    routing_pstn                 BIT             NULL,
    is_synchronized              BIT             NULL,
    is_synchronized_manually     BIT             NULL,
    last_synchronized_date       DATETIME        NULL,
    original_start_date          DATETIME        NULL,
    notes                        VARCHAR(MAX)    NULL,
    taleo_id                     VARCHAR(50)     NULL,
    CONSTRAINT PK_Interpreter PRIMARY KEY (interpreter_id)
);
GO

-------------------------------------------------------------------
-- 8) REQUESTOR
-- for "requestor.*" fields (the person requesting the booking)
-------------------------------------------------------------------
CREATE TABLE [dbo].[Requestor] (
    requestor_id             INT             NOT NULL,
    uri                      VARCHAR(500)    NULL,
    display_label            VARCHAR(255)    NULL,
    display_name             VARCHAR(255)    NULL,
    name                     VARCHAR(255)    NULL,
    email                    VARCHAR(255)    NULL,
    number                   VARCHAR(50)     NULL,
    account_locked           BIT             NULL,
    account_expired          BIT             NULL,
    enabled                  BIT             NULL,
    password_expired         BIT             NULL,
    password_last_change     DATETIME        NULL,
    locale_or_default        VARCHAR(50)     NULL,
    uuid                     UNIQUEIDENTIFIER NULL,
    created_date             DATETIME        NULL,
    last_modified_date       DATETIME        NULL,
    can_switch_location      BIT             NULL,
    fax_number               VARCHAR(50)     NULL,
    other                    VARCHAR(255)    NULL,
    mfa_enabled              BIT             NULL,
    mfa_method               VARCHAR(50)     NULL,
    username                 VARCHAR(255)    NULL,
    primary_company_id       INT             NULL,
    tz                       VARCHAR(100)    NULL,
    associations_uri         VARCHAR(500)    NULL,  -- "associations" link
    business_units_uri       VARCHAR(500)    NULL,  -- "businessUnits" link
    client_association_id    INT             NULL,
    customer_association_id  INT             NULL,
    password_last_change_str VARCHAR(255)    NULL,  -- if storing the raw string
    CONSTRAINT PK_Requestor PRIMARY KEY (requestor_id)
);
GO

-------------------------------------------------------------------
-- 9) VISIT
-- many "visit.*" fields can go here
-------------------------------------------------------------------
CREATE TABLE [dbo].[Visit] (
    visit_id             INT             NOT NULL,
    uri                  VARCHAR(500)    NULL,
    contact_rate_plan    VARCHAR(100)    NULL,
    customer_rate_plan   VARCHAR(100)    NULL,
    created_by           VARCHAR(100)    NULL,
    created_date         DATETIME        NULL,
    last_modified_by     VARCHAR(100)    NULL,
    last_modified_date   DATETIME        NULL,
    uuid                 UNIQUEIDENTIFIER NULL,
    status_id            INT             NULL,  -- "visit.status.id"
    status_uri           VARCHAR(500)    NULL,
    status_version_value INT             NULL,
    status_default_option BIT            NULL,
    status_description   VARCHAR(500)    NULL,
    status_in_order      INT             NULL,
    status_l10n_key      VARCHAR(100)    NULL,
    status_message       VARCHAR(500)    NULL,
    status_name          VARCHAR(100)    NULL,
    status_name_key      VARCHAR(100)    NULL,
    CONSTRAINT PK_Visit PRIMARY KEY (visit_id)
);
GO

-------------------------------------------------------------------
-- 10) BOOKING
-- The "monster" table with nearly all top-level booking fields
-------------------------------------------------------------------
CREATE TABLE [dbo].[Booking] (
    booking_id                INT             NOT NULL,  -- from "id"
    uri                       VARCHAR(500)    NULL,
    version_value             INT             NULL,
    access_code               VARCHAR(100)    NULL,
    accounting_reference      VARCHAR(100)    NULL,
    actual_duration_hrs       DECIMAL(5,2)    NULL,
    actual_duration_mins      DECIMAL(9,2)    NULL,
    actual_end_date           DATETIME        NULL,
    actual_location_id        INT             NULL,
    actual_location_display   VARCHAR(255)    NULL,
    actual_start_date         DATETIME        NULL,
    assigned_by               VARCHAR(100)    NULL,
    assigned_by_username      VARCHAR(100)    NULL,
    assignment_date           DATETIME        NULL,
    auto_offer_batch_frequency INT            NULL,
    auto_verify_duration      BIT             NULL,
    auto_verify_incidentals   BIT             NULL,
    average_rating            DECIMAL(4,2)    NULL,
    billing_customer_id       INT             NULL,
    billing_location_id       INT             NULL,
    billing_notes             VARCHAR(MAX)    NULL,
    booking_date              DATETIME        NULL,
    booking_details           VARCHAR(MAX)    NULL,
    booking_mode_id           INT             NULL,  -- if you store in a separate BookingMode table
    booking_requirements_uri  VARCHAR(500)    NULL,  -- from "bookingRequirements"
    booking_time              DATETIME        NULL,
    cancellation_date         DATETIME        NULL,
    cancellation_reason       VARCHAR(255)    NULL,
    client_id                 INT             NULL,   -- "client.id"
    client_name               VARCHAR(255)    NULL,   -- "client.name"
    client_uuid               UNIQUEIDENTIFIER NULL,
    company_id                INT             NULL,   -- references Company
    company_special_instructions VARCHAR(MAX) NULL,
    confirmation_date         DATETIME        NULL,
    consumer_id               INT             NULL,   -- if you store a separate "Consumer" table
    consumer_count            INT             NULL,
    consumer_count_enabled    BIT             NULL,
    consumers_uri             VARCHAR(500)    NULL,   -- "consumers"
    contact_arrival_date      DATETIME        NULL,
    contact_late_mins         INT             NULL,
    contact_rate_plan         VARCHAR(100)    NULL,
    contact_special_instructions VARCHAR(MAX) NULL,
    created_by                VARCHAR(100)    NULL,
    created_date              DATETIME        NULL,
    currency_code             VARCHAR(20)     NULL,
    currency_symbol           VARCHAR(10)     NULL,
    custom_consumer           VARCHAR(255)    NULL,
    custom_requestor          VARCHAR(255)    NULL,
    customer_id               INT             NULL,   -- references main Customer
    customer_business_unit    VARCHAR(255)    NULL,
    customer_duration_override_hrs DECIMAL(5,2) NULL,
    customer_notes            VARCHAR(MAX)    NULL,
    customer_rate_plan        VARCHAR(100)    NULL,
    customer_rate_plan_association VARCHAR(100) NULL,
    customer_rate_plan_override    VARCHAR(100) NULL,
    customer_rate_zone_override    VARCHAR(100) NULL,
    customer_special_instructions  VARCHAR(MAX) NULL,
    default_language_id       INT             NULL,
    disclaimer_accepted       BIT             NULL,
    disclaimer_accepted_date  DATETIME        NULL,
    disclaimer_accepted_initials VARCHAR(50)  NULL,
    duration_override_hrs     DECIMAL(5,2)    NULL,
    employment_category_id    INT             NULL,
    esignature_grace_period   INT             NULL,
    esignature_required       BIT             NULL,
    exclude_from_auto_offer   BIT             NULL,
    exclude_from_job_offer_pool BIT             NULL,
    expected_duration_hrs     DECIMAL(5,2)    NULL,
    expected_duration_mins    DECIMAL(9,2)    NULL,
    expected_end_date         DATETIME        NULL,
    expected_end_time         DATETIME        NULL,
    expected_start_date       DATETIME        NULL,
    expected_start_time       DATETIME        NULL,
    final_notes               VARCHAR(MAX)    NULL,
    first_assignment_date     DATETIME        NULL,
    first_confirmation_date   DATETIME        NULL,
    first_offer_date          DATETIME        NULL,
    first_open_date           DATETIME        NULL,
    flag_for_finance          BIT             NULL,
    gender_requirement        VARCHAR(100)    NULL,
    general_customer_account_manager VARCHAR(255) NULL,
    general_customer_business_unit   VARCHAR(255) NULL,
    incidentals_uri           VARCHAR(500)    NULL,
    integration_id            INT             NULL,
    interpreter_id            INT             NULL,
    interpreter_business_unit VARCHAR(255)    NULL,
    interpreter_notes         VARCHAR(MAX)    NULL,
    interpreter_submitted     BIT             NULL,
    invoice_status_id         INT             NULL,
    is_auto_verify_ready      BIT             NULL,
    is_cancelled              BIT             NULL,
    is_fields_locked          BIT             NULL,
    is_synchronized           BIT             NULL,
    is_synchronized_manually  BIT             NULL,
    is_verified               BIT             NULL,
    job_complete_email_sent   BIT             NULL,
    job_offers_uri            VARCHAR(500)    NULL,
    language_id               INT             NULL,
    language_code             VARCHAR(20)     NULL,
    last_modified_by          VARCHAR(100)    NULL,
    last_modified_date        DATETIME        NULL,
    last_synchronized_date    DATETIME        NULL,
    local_booking_mode        VARCHAR(255)    NULL,
    location_id               INT             NULL,
    location_note             VARCHAR(MAX)    NULL,
    locked                    BIT             NULL,
    mileage                   DECIMAL(8,2)    NULL,
    notes                     VARCHAR(MAX)    NULL,
    notification_email        VARCHAR(255)    NULL,
    notify                    BIT             NULL,
    num_jobs                  INT             NULL,
    number_accepted_offers    INT             NULL,
    number_for_telephone_translation VARCHAR(50) NULL,
    offer_date                DATETIME        NULL,
    offer_mode                VARCHAR(50)     NULL,
    open_date                 DATETIME        NULL,
    origin_of_request         VARCHAR(50)     NULL,
    overflow_job_location_uuid UNIQUEIDENTIFIER NULL,
    overflow_type_id          INT             NULL,
    override_booking_mode     BIT             NULL,
    override_requirements     BIT             NULL,
    owner                     VARCHAR(100)    NULL,
    payment_status_id         INT             NULL,
    place_of_appointment      VARCHAR(255)    NULL,
    preferred_interpreter_id  INT             NULL,
    preferred_interpreter_declined BIT         NULL,
    prevent_edit              BIT             NULL,
    primary_ref_id            INT             NULL,
    rate_plan_override        VARCHAR(100)    NULL,
    rate_zone_override        VARCHAR(100)    NULL,
    refs_uri                  VARCHAR(500)    NULL,   -- "refs.0" array link is not official, but we store if you want
    reminder_email_sent_scheduled_phone BIT     NULL,
    requested_by              VARCHAR(255)    NULL,
    requestor_id              INT             NULL,
    requirements_uri          VARCHAR(500)    NULL,
    sessions_uri              VARCHAR(500)    NULL,
    shift_enabled             BIT             NULL,
    signature_hash            VARCHAR(255)    NULL,
    signature_height          INT             NULL,
    signature_width           INT             NULL,
    signature_location        VARCHAR(255)    NULL,
    signature_raw             VARCHAR(MAX)    NULL,
    signer                    VARCHAR(100)    NULL,
    site_contact              VARCHAR(255)    NULL,
    sla_reporting_enabled     BIT             NULL,
    start_editing             DATETIME        NULL,
    status_name               VARCHAR(100)    NULL,
    status_uri                VARCHAR(500)    NULL,
    sub_location_id           INT             NULL,
    super_booking_id          INT             NULL,
    sync_uuid                 UNIQUEIDENTIFIER NULL,
    team_id                   INT             NULL,
    team_size                 INT             NULL,
    time_interpreter_arrived_inbound  DATETIME NULL,
    time_interpreter_arrived_outbound DATETIME NULL,
    time_interpreter_departed_inbound DATETIME NULL,
    time_interpreter_departed_outbound DATETIME NULL,
    time_reconfirmed_customer DATETIME        NULL,
    time_tracking_enabled     BIT             NULL,
    time_zone                 VARCHAR(100)    NULL,
    time_zone_display_name    VARCHAR(50)     NULL,
    unarchived_updates        BIT             NULL,
    unfulfilled_date          DATETIME        NULL,
    unfulfilled_reason        VARCHAR(255)    NULL,
    user_editing              VARCHAR(100)    NULL,
    uuid                      UNIQUEIDENTIFIER NULL,
    valid                     BIT             NULL,
    verified_date             DATETIME        NULL,
    visit_id                  INT             NULL,
    vos_required              BIT             NULL,

    -- Additional link fields to top-level references (these might be optional):
    actual_location_uri       VARCHAR(500)    NULL,
    billing_location_uri      VARCHAR(500)    NULL,
    location_uri              VARCHAR(500)    NULL,
    company_uri               VARCHAR(500)    NULL,

    CONSTRAINT PK_Booking PRIMARY KEY (booking_id),

    -- foreign keys
    CONSTRAINT FK_Booking_AddrActual FOREIGN KEY (actual_location_id) REFERENCES [dbo].[Address](address_id),
    CONSTRAINT FK_Booking_AddrBilling FOREIGN KEY (billing_location_id) REFERENCES [dbo].[Address](address_id),
    CONSTRAINT FK_Booking_AddrLocation FOREIGN KEY (location_id) REFERENCES [dbo].[Address](address_id),
    CONSTRAINT FK_Booking_AddrSubLoc FOREIGN KEY (sub_location_id) REFERENCES [dbo].[Address](address_id),

    CONSTRAINT FK_Booking_BillingCust FOREIGN KEY (billing_customer_id) REFERENCES [dbo].[Customer](customer_id),
    CONSTRAINT FK_Booking_Customer    FOREIGN KEY (customer_id)        REFERENCES [dbo].[Customer](customer_id),
    CONSTRAINT FK_Booking_Company     FOREIGN KEY (company_id)         REFERENCES [dbo].[Company](company_id),
    CONSTRAINT FK_Booking_Interpreter FOREIGN KEY (interpreter_id)     REFERENCES [dbo].[Interpreter](interpreter_id),
    CONSTRAINT FK_Booking_PrefInterp  FOREIGN KEY (preferred_interpreter_id) REFERENCES [dbo].[Interpreter](interpreter_id),
    CONSTRAINT FK_Booking_Requestor   FOREIGN KEY (requestor_id)       REFERENCES [dbo].[Requestor](requestor_id),
    CONSTRAINT FK_Booking_Visit       FOREIGN KEY (visit_id)           REFERENCES [dbo].[Visit](visit_id),
    CONSTRAINT FK_Booking_Integration FOREIGN KEY (integration_id)     REFERENCES [dbo].[Integration](integration_id)
);
GO

-------------------------------------------------------------------
-- 11) BOOKINGREFS (for refs.0, refs.1, ...)
-------------------------------------------------------------------
CREATE TABLE [dbo].[BookingRefs] (
    ref_id          INT             NOT NULL,
    booking_id      INT             NOT NULL,
    uri             VARCHAR(500)    NULL,
    version_value   INT             NULL,
    name            VARCHAR(255)    NULL,
    ref             VARCHAR(255)    NULL,
    reference_value VARCHAR(255)    NULL,
    description     VARCHAR(MAX)    NULL,
    super_booking_id INT            NULL,
    approved        BIT             NULL,
    consumer_id     INT             NULL,
    dependent       VARCHAR(255)    NULL,
    dependent_id    INT             NULL,
    company_id      INT             NULL,
    config_id       INT             NULL,
    customer_id     INT             NULL,
    CONSTRAINT PK_BookingRefs PRIMARY KEY (ref_id),
    CONSTRAINT FK_BookingRefs_Booking FOREIGN KEY (booking_id)
        REFERENCES [dbo].[Booking](booking_id),
    CONSTRAINT FK_BookingRefs_Company FOREIGN KEY (company_id)
        REFERENCES [dbo].[Company](company_id),
    CONSTRAINT FK_BookingRefs_Config FOREIGN KEY (config_id)
        REFERENCES [dbo].[Config](config_id),
    CONSTRAINT FK_BookingRefs_Cust FOREIGN KEY (customer_id)
        REFERENCES [dbo].[Customer](customer_id)
);
GO

-------------------------------------------------------------------
-- 12) BOOKINGREQUIREMENTS (for requirements.0, requirements.1, ...)
-------------------------------------------------------------------
CREATE TABLE [dbo].[BookingRequirements] (
    requirement_id       INT             NOT NULL,
    booking_id           INT             NOT NULL,
    version_value        INT             NULL,
    company_id           INT             NULL,
    config_id            INT             NULL,
    created_by           VARCHAR(100)    NULL,
    created_date         DATETIME        NULL,
    criteria_id          INT             NULL,
    criteria_name        VARCHAR(255)    NULL,
    criteria_type_id     INT             NULL,
    dependent            VARCHAR(255)    NULL,
    dependent_id         INT             NULL,
    last_modified_by     VARCHAR(100)    NULL,
    last_modified_date   DATETIME        NULL,
    optional             BIT             NULL,
    required_flag        BIT             NULL,
    super_booking_id     INT             NULL,
    sync_uuid            UNIQUEIDENTIFIER NULL,
    uuid                 UNIQUEIDENTIFIER NULL,
    CONSTRAINT PK_BookingRequirements PRIMARY KEY (requirement_id),
    CONSTRAINT FK_BookingReq_Booking FOREIGN KEY (booking_id)
        REFERENCES [dbo].[Booking](booking_id),
    CONSTRAINT FK_BookingReq_Company FOREIGN KEY (company_id)
        REFERENCES [dbo].[Company](company_id),
    CONSTRAINT FK_BookingReq_Config FOREIGN KEY (config_id)
        REFERENCES [dbo].[Config](config_id)
);
GO
