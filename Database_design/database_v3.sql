CREATE TABLE [Bookings] (
  [booking_id] int PRIMARY KEY,
  [uri] varchar(255) NOT NULL,
  [version_value] int,
  [access_code] varchar(50),
  [accounting_reference] varchar(50),
  [actual_duration_hrs] float,
  [actual_duration_mins] float,
  [actual_start_date] timestamp,
  [actual_end_date] timestamp,
  [actual_location_display] varchar(255),
  [assigned_by] varchar(100),
  [assigned_by_username] varchar(100),
  [assignment_date] timestamp,
  [auto_offer_batch_freq] varchar(50),
  [auto_verify_duration] boolean,
  [auto_verify_incidentals] boolean,
  [average_rating] float,
  [billing_notes] text,
  [booking_date] timestamp,
  [booking_details] text,
  [booking_requirements] varchar(255),
  [booking_time] timestamp,
  [cancellation_date] timestamp,
  [cancellation_reason] text,
  [company_special_instructions] text,
  [confirmation_date] timestamp,
  [consumer] text,
  [consumer_count] int,
  [consumer_count_enabled] boolean,
  [contact_arrival_date] timestamp,
  [contact_late_mins] int,
  [contact_rate_plan] varchar(50),
  [contact_special_instructions] text,
  [created_by] varchar(100),
  [created_date] timestamp,
  [currency_code] varchar(10),
  [currency_symbol] varchar(10),
  [custom_consumer] text,
  [custom_requestor] text,
  [customer_business_unit] varchar(100),
  [customer_duration_override_hrs] float,
  [customer_notes] text,
  [customer_rate_plan] varchar(50),
  [customer_rate_plan_assoc] varchar(50),
  [customer_rate_plan_override] varchar(50),
  [customer_rate_zone_override] varchar(50),
  [customer_special_instructions] text,
  [disclaimer_accepted] boolean,
  [disclaimer_accepted_date] timestamp,
  [disclaimer_accepted_initials] varchar(10),
  [duration_override_hrs] float,
  [esignature_grace_period] int,
  [esignature_required] boolean,
  [exclude_from_auto_offer] boolean,
  [exclude_from_job_offer_pool] boolean,
  [expected_duration_hrs] float,
  [expected_duration_mins] float,
  [expected_start_date] timestamp,
  [expected_start_time] timestamp,
  [expected_end_date] timestamp,
  [expected_end_time] timestamp,
  [final_notes] text,
  [first_assignment_date] timestamp,
  [first_confirmation_date] timestamp,
  [first_offer_date] timestamp,
  [first_open_date] timestamp,
  [flag_for_finance] boolean,
  [gender_requirement] varchar(50),
  [general_customer_account_manager] varchar(100),
  [general_customer_business_unit] varchar(100),
  [incidentals] varchar(255),
  [integration] text,
  [interpreter_notes] text,
  [interpreter_submitted] boolean,
  [is_auto_verify_ready] boolean,
  [is_cancelled] boolean,
  [is_fields_locked] boolean,
  [is_synchronized] boolean,
  [is_synchronized_manually] boolean,
  [is_verified] boolean,
  [job_complete_email_sent] boolean,
  [job_offers] varchar(255),
  [language_code] varchar(10),
  [last_modified_by] varchar(100),
  [last_modified_date] timestamp,
  [last_synchronized_date] timestamp,
  [location_note] varchar(255),
  [locked] boolean,
  [mileage] float,
  [notes] text,
  [notification_email] varchar(100),
  [notify] boolean,
  [num_jobs] int,
  [number_accepted_offers] int,
  [number_for_telephone_trans] varchar(50),
  [offer_date] timestamp,
  [offer_mode] varchar(50),
  [open_date] timestamp,
  [origin_of_request] varchar(50),
  [overflow_job_location_uuid] varchar(50),
  [override_booking_mode] varchar(50),
  [override_requirements] boolean,
  [owner] varchar(100),
  [place_of_appointment] varchar(255),
  [preferred_interpreter_declined] boolean,
  [prevent_edit] boolean,
  [rate_plan_override] varchar(50),
  [rate_zone_override] varchar(50),
  [reminder_email_sent] boolean,
  [requested_by] varchar(100),
  [sessions] varchar(255),
  [shift_enabled] boolean,
  [signature_hash] varchar(255),
  [signature_height] float,
  [signature_location] varchar(255),
  [signature_raw] text,
  [signature_width] float,
  [signer] varchar(100),
  [site_contact] varchar(100),
  [sla_reporting_enabled] boolean,
  [start_editing] timestamp,
  [sub_location] varchar(100),
  [sync_uuid] varchar(50),
  [team_id] int,
  [team_size] int,
  [time_interpreter_arrived_inbound] timestamp,
  [time_interpreter_arrived_outbound] timestamp,
  [time_interpreter_departed_inbound] timestamp,
  [time_interpreter_departed_outbound] timestamp,
  [time_reconfirmed_customer] timestamp,
  [time_tracking_enabled] boolean,
  [time_zone] varchar(50),
  [time_zone_display_name] varchar(50),
  [unarchived_updates] boolean,
  [unfulfilled_date] timestamp,
  [unfulfilled_reason] text,
  [user_editing] varchar(100),
  [uuid] varchar(50) UNIQUE NOT NULL,
  [valid] boolean,
  [verified_date] timestamp,
  [vos] varchar(255),
  [vos_required] boolean,
  [extra_details] json,
  [actual_location_id] int,
  [billing_customer_id] int,
  [billing_location_id] int,
  [booking_mode_id] int,
  [client_id] int,
  [company_id] int,
  [customer_id_fk] int,
  [default_language_id] int,
  [employment_category_id] int,
  [invoice_status_id] int,
  [interpreter_id] int,
  [language_id] int,
  [local_booking_mode_id] int,
  [location_id_fk] int,
  [overflow_type_id] int,
  [payment_status_id] int,
  [preferred_interpreter_id] int,
  [primary_ref_id] int,
  [status_id] int,
  [super_booking_id] int,
  [requestor_id] int,
  [visit_id] int
)
GO

CREATE TABLE [Locations] (
  [location_id] int PRIMARY KEY,
  [uri] varchar(255),
  [active] boolean,
  [addr_entered] varchar(255),
  [cost_center] varchar(100),
  [cost_center_name] varchar(100),
  [description] varchar(255),
  [display_label] varchar(255),
  [lat] float,
  [lng] float,
  [uuid] varchar(50) UNIQUE
)
GO

CREATE TABLE [BookingModes] (
  [booking_mode_id] int PRIMARY KEY,
  [uri] varchar(255),
  [description] varchar(255),
  [name] varchar(100),
  [name_key] varchar(50)
)
GO

CREATE TABLE [Clients] (
  [client_id] int PRIMARY KEY,
  [uri] varchar(255),
  [display_name] varchar(255),
  [name] varchar(255),
  [uuid] varchar(50) UNIQUE
)
GO

CREATE TABLE [Companies] (
  [company_id] int PRIMARY KEY,
  [uri] varchar(255),
  [description] varchar(255),
  [name] varchar(255),
  [uuid] varchar(50) UNIQUE
)
GO

CREATE TABLE [ContractTypes] (
  [contract_type_id] int PRIMARY KEY,
  [uri] varchar(255),
  [version_value] int,
  [default_option] boolean,
  [description] varchar(255),
  [l10n_key] varchar(50),
  [name] varchar(100),
  [name_key] varchar(50)
)
GO

CREATE TABLE [Customers] (
  [customer_id] int PRIMARY KEY,
  [uri] varchar(255),
  [display_name] varchar(255),
  [name] varchar(255),
  [uuid] varchar(50) UNIQUE,
  [contract_type_id] int
)
GO

CREATE TABLE [Languages] (
  [language_id] int PRIMARY KEY,
  [uri] varchar(255),
  [bpin_opi_enabled] boolean,
  [bpin_vri_enabled] boolean,
  [description] varchar(100),
  [display_name] varchar(100),
  [is_sign] boolean,
  [iso639_3_tag] varchar(10),
  [mmis_code] varchar(50),
  [opi_enabled] boolean,
  [subtag] varchar(10),
  [vri_enabled] boolean
)
GO

CREATE TABLE [EmploymentCategories] (
  [employment_category_id] int PRIMARY KEY,
  [uri] varchar(255),
  [description] varchar(255),
  [name] varchar(100),
  [name_key] varchar(50)
)
GO

CREATE TABLE [InvoiceStatuses] (
  [invoice_status_id] int PRIMARY KEY,
  [uri] varchar(255),
  [description] varchar(255),
  [name] varchar(100),
  [name_key] varchar(50)
)
GO

CREATE TABLE [PaymentStatuses] (
  [payment_status_id] int PRIMARY KEY,
  [uri] varchar(255),
  [description] varchar(255),
  [name] varchar(100),
  [name_key] varchar(50)
)
GO

CREATE TABLE [OverflowTypes] (
  [overflow_type_id] int PRIMARY KEY,
  [uri] varchar(255),
  [description] varchar(255),
  [name_key] varchar(50)
)
GO

CREATE TABLE [SuperBookings] (
  [super_booking_id] int PRIMARY KEY,
  [uri] varchar(255),
  [uuid] varchar(50)
)
GO

CREATE TABLE [Status] (
  [status_id] int PRIMARY KEY,
  [uri] varchar(255),
  [description] varchar(255),
  [name] varchar(100),
  [name_key] varchar(50)
)
GO

CREATE TABLE [Requestors] (
  [requestor_id] int PRIMARY KEY,
  [uri] varchar(255),
  [display_label] varchar(255),
  [display_name] varchar(255),
  [name] varchar(100),
  [uuid] varchar(50) UNIQUE,
  [email] varchar(100),
  [enabled] boolean,
  [fax_number] varchar(50),
  [first_name] varchar(100),
  [last_name] varchar(100),
  [password_last_change] timestamp,
  [username] varchar(100)
)
GO

CREATE TABLE [VisitStatuses] (
  [visit_status_id] int PRIMARY KEY,
  [uri] varchar(255),
  [version_value] int,
  [default_option] boolean,
  [description] varchar(255),
  [in_order] int,
  [l10n_key] varchar(50),
  [message] text,
  [name] varchar(100),
  [name_key] varchar(50)
)
GO

CREATE TABLE [Visits] (
  [visit_id] int PRIMARY KEY,
  [uri] varchar(255),
  [contact_rate_plan] varchar(100),
  [customer_rate_plan] varchar(100),
  [uuid] varchar(50),
  [visit_status_id] int
)
GO

CREATE TABLE [Interpreters] (
  [interpreter_id] int PRIMARY KEY,
  [uri] varchar(255),
  [display_name] varchar(255),
  [name] varchar(100),
  [email] varchar(100),
  [phone] varchar(20),
  [payment_method] varchar(50),
  [time_zone] varchar(50),
  [uuid] varchar(50) UNIQUE
)
GO

CREATE TABLE [Refs] (
  [ref_id] int PRIMARY KEY,
  [uri] varchar(255),
  [version_value] int,
  [approved] boolean,
  [auto_complete] varchar(255),
  [company_id] int,
  [config_desc] text,
  [consumer] text,
  [customer_id] int,
  [dependent] text,
  [dependent_id] int,
  [description] text,
  [name] varchar(100),
  [ref_value] varchar(50),
  [reference_value] varchar(100),
  [reference_value_url] varchar(255),
  [super_booking_id] int
)
GO

CREATE TABLE [BookingRefs] (
  [booking_id] int,
  [ref_id] int,
  PRIMARY KEY ([booking_id], [ref_id])
)
GO

CREATE TABLE [BookingOverriddenRequirements] (
  [booking_id] int,
  [requirement] varchar(255),
  PRIMARY KEY ([booking_id], [requirement])
)
GO

CREATE TABLE [BookingInvalidFields] (
  [booking_id] int,
  [field_name] varchar(100),
  PRIMARY KEY ([booking_id], [field_name])
)
GO

CREATE TABLE [BookingRequirements] (
  [booking_id] int,
  [requirement] varchar(255),
  PRIMARY KEY ([booking_id], [requirement])
)
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([actual_location_id]) REFERENCES [Locations] ([location_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([billing_customer_id]) REFERENCES [Customers] ([customer_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([billing_location_id]) REFERENCES [Locations] ([location_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([booking_mode_id]) REFERENCES [BookingModes] ([booking_mode_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([client_id]) REFERENCES [Clients] ([client_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([company_id]) REFERENCES [Companies] ([company_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([customer_id_fk]) REFERENCES [Customers] ([customer_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([default_language_id]) REFERENCES [Languages] ([language_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([employment_category_id]) REFERENCES [EmploymentCategories] ([employment_category_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([invoice_status_id]) REFERENCES [InvoiceStatuses] ([invoice_status_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([interpreter_id]) REFERENCES [Interpreters] ([interpreter_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([language_id]) REFERENCES [Languages] ([language_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([local_booking_mode_id]) REFERENCES [BookingModes] ([booking_mode_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([location_id_fk]) REFERENCES [Locations] ([location_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([overflow_type_id]) REFERENCES [OverflowTypes] ([overflow_type_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([payment_status_id]) REFERENCES [PaymentStatuses] ([payment_status_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([preferred_interpreter_id]) REFERENCES [Interpreters] ([interpreter_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([primary_ref_id]) REFERENCES [Refs] ([ref_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([status_id]) REFERENCES [Status] ([status_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([super_booking_id]) REFERENCES [SuperBookings] ([super_booking_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([requestor_id]) REFERENCES [Requestors] ([requestor_id])
GO

ALTER TABLE [Bookings] ADD FOREIGN KEY ([visit_id]) REFERENCES [Visits] ([visit_id])
GO

ALTER TABLE [Customers] ADD FOREIGN KEY ([contract_type_id]) REFERENCES [ContractTypes] ([contract_type_id])
GO

ALTER TABLE [Visits] ADD FOREIGN KEY ([visit_status_id]) REFERENCES [VisitStatuses] ([visit_status_id])
GO

ALTER TABLE [Refs] ADD FOREIGN KEY ([company_id]) REFERENCES [Companies] ([company_id])
GO

ALTER TABLE [Refs] ADD FOREIGN KEY ([super_booking_id]) REFERENCES [SuperBookings] ([super_booking_id])
GO

ALTER TABLE [BookingRefs] ADD FOREIGN KEY ([booking_id]) REFERENCES [Bookings] ([booking_id])
GO

ALTER TABLE [BookingRefs] ADD FOREIGN KEY ([ref_id]) REFERENCES [Refs] ([ref_id])
GO

ALTER TABLE [BookingOverriddenRequirements] ADD FOREIGN KEY ([booking_id]) REFERENCES [Bookings] ([booking_id])
GO

ALTER TABLE [BookingInvalidFields] ADD FOREIGN KEY ([booking_id]) REFERENCES [Bookings] ([booking_id])
GO

ALTER TABLE [BookingRequirements] ADD FOREIGN KEY ([booking_id]) REFERENCES [Bookings] ([booking_id])
GO
