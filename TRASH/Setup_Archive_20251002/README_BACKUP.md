# Data Backup - TELCO_NETWORK_OPTIMIZATION_PROD

## Overview

This directory contains CSV exports of the production data from the Snowflake demo environment.

**Export Date:** October 1, 2025  
**Database:** TELCO_NETWORK_OPTIMIZATION_PROD  
**Schema:** RAW

## Exported Files

### 1. support_tickets.csv
- **Size:** 73 MB
- **Rows:** 178,169
- **Columns:** 9
  - TICKET_ID (TEXT)
  - CUSTOMER_NAME (TEXT)
  - CUSTOMER_EMAIL (TEXT)
  - SERVICE_TYPE (TEXT)
  - REQUEST (TEXT - up to 16MB)
  - CONTACT_PREFERENCE (TEXT)
  - CELL_ID (NUMBER)
  - SENTIMENT_SCORE (FLOAT)
  - BACKUP_TIMESTAMP (TIMESTAMP_LTZ)

### 2. cell_tower.csv
- **Size:** 1.8 GB
- **Rows:** 2,626,336
- **Columns:** 70 (detailed LTE cell tower metrics and performance data)
- **Key Columns:**
  - CELL_ID, CALL_RELEASE_CODE, LOOKUP_ID
  - Network identifiers (IMSI, IMEI, TAP codes)
  - Geographic coordinates (CELL_LATITUDE, CELL_LONGITUDE)
  - Performance metrics (PM_* columns)
  - Vendor information (VENDOR_NAME, SENDER_NAME)
  - Event timestamps and metadata

## How to Restore Data

### Option 1: Using the Provided SQL Script

The `load_from_csv_backup.sql` script provides a complete restore procedure:

```bash
# 1. Upload CSV files to Snowflake stage using SnowSQL
snowsql -d TELCO_NETWORK_OPTIMIZATION_PROD -s RAW

# 2. Upload files to stage
PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/support_tickets.csv @DATA_BACKUP_STAGE AUTO_COMPRESS=TRUE;
PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/cell_tower.csv @DATA_BACKUP_STAGE AUTO_COMPRESS=TRUE;

# 3. Run the load script
snowsql -d TELCO_NETWORK_OPTIMIZATION_PROD -s RAW -f load_from_csv_backup.sql
```

### Option 2: Manual Load Steps

1. **Create the file format:**
```sql
CREATE OR REPLACE FILE FORMAT CSV_IMPORT_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO';
```

2. **Create a stage:**
```sql
CREATE STAGE IF NOT EXISTS DATA_BACKUP_STAGE
  FILE_FORMAT = CSV_IMPORT_FORMAT;
```

3. **Upload files using SnowSQL:**
```bash
PUT file:///path/to/support_tickets.csv @DATA_BACKUP_STAGE AUTO_COMPRESS=TRUE;
PUT file:///path/to/cell_tower.csv @DATA_BACKUP_STAGE AUTO_COMPRESS=TRUE;
```

4. **Load the data:**
```sql
COPY INTO SUPPORT_TICKETS
FROM @DATA_BACKUP_STAGE/support_tickets.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'CSV_IMPORT_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO CELL_TOWER
FROM @DATA_BACKUP_STAGE/cell_tower.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'CSV_IMPORT_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';
```

### Option 3: Using User Stage (Simpler)

Instead of creating a named stage, you can use your user stage:

```bash
# Upload to user stage (referenced as @~)
snowsql -d TELCO_NETWORK_OPTIMIZATION_PROD -s RAW
PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/support_tickets.csv @~ AUTO_COMPRESS=TRUE;
PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/cell_tower.csv @~ AUTO_COMPRESS=TRUE;

# Then load from user stage
COPY INTO SUPPORT_TICKETS
FROM @~/support_tickets.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'CSV_IMPORT_FORMAT');
```

## Verification

After loading, verify the data:

```sql
-- Check row counts
SELECT 'SUPPORT_TICKETS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT 
FROM SUPPORT_TICKETS
UNION ALL
SELECT 'CELL_TOWER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT 
FROM CELL_TOWER;

-- Expected results:
-- SUPPORT_TICKETS: 178,169 rows
-- CELL_TOWER: 2,626,336 rows

-- Check for load errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SUPPORT_TICKETS',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));
```

## Prerequisites

Before loading data, ensure:

1. The target database and schema exist:
   ```sql
   CREATE DATABASE IF NOT EXISTS TELCO_NETWORK_OPTIMIZATION_PROD;
   CREATE SCHEMA IF NOT EXISTS RAW;
   ```

2. The tables are created with the correct schema (run `create_tables.sql` first if needed)

3. You have the necessary privileges:
   - CREATE STAGE
   - CREATE FILE FORMAT
   - INSERT on target tables
   - USAGE on database and schema

## Files in This Directory

- `support_tickets.csv` - Customer support ticket data
- `cell_tower.csv` - LTE cell tower performance metrics
- `load_from_csv_backup.sql` - Complete restore script with detailed instructions
- `create_tables.sql` - Table creation script (if tables don't exist)
- `README_BACKUP.md` - This file

## Export Details

The data was exported using SnowSQL with the following commands:

```bash
# Export SUPPORT_TICKETS
snowsql -d TELCO_NETWORK_OPTIMIZATION_PROD -s RAW \
  -o output_format=csv \
  -o header=true \
  -o timing=false \
  -o friendly=false \
  -q "SELECT * FROM SUPPORT_TICKETS;" \
  -o output_file=support_tickets.csv

# Export CELL_TOWER
snowsql -d TELCO_NETWORK_OPTIMIZATION_PROD -s RAW \
  -o output_format=csv \
  -o header=true \
  -o timing=false \
  -o friendly=false \
  -q "SELECT * FROM CELL_TOWER;" \
  -o output_file=cell_tower.csv
```

## Important Notes

1. **Timestamps:** The CSV export preserves timestamp data in a format that Snowflake can auto-parse. The `TIMESTAMP_FORMAT = 'AUTO'` setting handles this.

2. **Compression:** When using `PUT` with `AUTO_COMPRESS=TRUE`, files are automatically gzipped during upload, saving storage and improving transfer speeds.

3. **Character Encoding:** All text data uses UTF-8 encoding.

4. **NULL Values:** NULL values are represented as empty strings in the CSV and will be correctly loaded as NULL.

5. **Large File:** The `cell_tower.csv` file is 1.8GB and may take several minutes to upload depending on your network connection.

6. **Data Integrity:** Both exports completed successfully with no errors. All rows from the original tables are included.

## Troubleshooting

### Issue: "File format not found"
**Solution:** Run the CREATE FILE FORMAT statement from `load_from_csv_backup.sql`

### Issue: "Stage not found"
**Solution:** Either create the stage or use the user stage (@~) alternative

### Issue: "Table does not exist"
**Solution:** Run `create_tables.sql` to create the table structure first

### Issue: "Error parsing timestamp"
**Solution:** Ensure TIMESTAMP_FORMAT is set to 'AUTO' in the file format

### Issue: Upload takes too long
**Solution:** 
- Compress files locally before upload
- Use a faster network connection
- Upload during off-peak hours
- Consider using an external stage (S3, Azure, GCS) for large files

## Contact

For questions or issues with this backup, contact the repository owner.

