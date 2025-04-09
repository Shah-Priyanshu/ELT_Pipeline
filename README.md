# MCIS ELT Pipeline

## Overview
This project implements an ELT (Extract, Load, Transform) pipeline for handling booking data from an API endpoint. The pipeline fetches booking data, enriches it with related information, flattens the data structure, and loads it into a SQL Server database.

## Features
- Asynchronous data fetching with pagination support
- Data enrichment through related API endpoints
- Automatic flattening of nested JSON structures
- Robust database upsert operations
- Error handling and logging
- Configurable date ranges for historical data loading
- Support for multiple entity types (Bookings, Customers, Interpreters, etc.)

## Prerequisites
- Python 3.7+
- SQL Server with ODBC Driver 17
- Required Python packages (see requirements.txt)
- Valid API credentials

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd MCIS_ELT_Pipeline
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file with the following variables:
```env
AUTH_URL=your_auth_url
DATA_API_URL=your_data_api_url
SUBJECT=your_subject
SECRET=your_secret
DB_DRIVER = '{ODBC Driver 17 for SQL Server}'
SERVER =your_DB_server
DATABASE =your_DB
```

## Configuration
The main configuration parameters are defined at the bottom of the script:
- `OUTPUT_FILE`: JSON output file pattern
- Date ranges for data fetching are handled in the `run_main_loop` function

## Database Schema
The pipeline supports the following main entities:
- Booking
- Company
- Customer
- Interpreter
- Location (multiple variants)
- Visit
- BookingRefs
- BookingRequirements

Each entity has its own mapping defined in the `TABLE_MAPPING` dictionary.
DB design visual display: https://dbdiagram.io/d/Booking-DB-v4-67f6a5094f7afba184fb9262

## Key Components

### Data Extraction
- `authenticate()`: Handles API authentication
- `fetch_data_async()`: Asynchronously fetches data from API endpoints
- `fetch_booking_data_paged()`: Implements pagination for booking data retrieval

### Data Processing
- `flatten_dict()`: Converts nested JSON structures into flat key-value pairs
- `extract_values()`: Maps API data to database columns
- `process_bookings_async()`: Main processing pipeline for booking data

### Database Operations
- `get_db_connection()`: Establishes database connection
- `upsert_entity()`: Handles both insert and update operations
- `process_flattened_record()`: Orchestrates the database loading process
- `insert_bookings_into_db()`: Main database insertion handler

## Usage

### Basic Usage
Run the script with:
```bash
python data_extraction_booking.py
```

The script will:
1. Authenticate with the API
2. Fetch booking data in 6-month chunks from 2019 to present
3. Process and enrich the data
4. Save to JSON files (one per chunk)
5. Load data into the SQL Server database

### Logging
- Logs are written to `api_process.log`
- Includes DEBUG, INFO, and ERROR level messages
- Tracks API calls, data processing, and database operations

## Error Handling
The pipeline includes comprehensive error handling for:
- API authentication failures
- Network issues
- Data processing errors
- Database connection problems
- Invalid data formats

## Extending the Pipeline

### Adding New Entities
1. Define the table mapping in `TABLE_MAPPING`
2. Add appropriate processing in `process_flattened_record()`
3. Update database schema accordingly

### Modifying Data Processing
- Adjust `flatten_dict()` for different JSON structures
- Modify `extract_values()` for new mapping requirements
- Update `enrich_record()` for additional API calls

### Changing Time Ranges
Modify the `run_main_loop()` function to adjust:
- Start date (currently set to 2019-01-01)
- Chunk size (currently 6 months)
- End date handling

## STEP-BY-STEP TO SCHEDULE PYTHON SCRIPT
### 1. Open Task Scheduler (Start > Task Scheduler)
### 2. Create a new task (not a basic task):
   - General tab:
      - Name: MCIS ELT Pipeline
      - Run whether user is logged on or not
      - Run with highest privileges

### 3. Triggers tab:
   - New > Set to daily or on your preferred schedule (e.g., 1 AM)

### 4. Actions tab:
   - New > Action: Start a program
      - Program/script: Full path to python.exe
      Example: C:\Users\YourUsername\AppData\Local\Programs\Python\Python310\python.exe
      - Add arguments: Full path to your script
      Example: C:\path\to\MCIS_ELT_Pipeline\data_extraction_booking.py
      - Start in: Directory of your script
      Example: C:\path\to\MCIS_ELT_Pipeline

### 5. Settings tab:
   - Allow task to be run on demand
   - If the task fails, restart every 1 minute, up to 3 times (optional)

### 6. Click OK, enter admin credentials if prompted.

### 7. To test:
   - Right-click the task → Run
   - Check output/logs or Task Scheduler history for success/failure

    ## Troubleshooting
### Common Issues
1. Database Connection Failures
   - Verify SQL Server credentials
   - Check ODBC driver installation
   - Ensure network connectivity

2. API Authentication Issues
   - Verify environment variables
   - Check API credentials
   - Confirm API endpoint availability

3. Memory Usage
   - Adjust batch sizes in pagination
   - Monitor JSON file sizes
   - Check database transaction sizes

### Regular Tasks
1. Monitor log files
2. Archive JSON output files
3. Verify database indexes
4. Check API rate limits
