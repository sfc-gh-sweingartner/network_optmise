-- ========================================================================
-- Script to Load Backup Data from CSV files
-- ========================================================================
-- This script loads data from CSV exports back into Snowflake tables
-- Created: 2025-10-01
-- 
-- Prerequisites:
-- 1. CSV files must be uploaded to a Snowflake stage
-- 2. Target tables must exist with proper schema
-- 3. Database and schema must be created/selected
-- 
-- File locations (local):
--   - support_tickets.csv (73MB, 178,169 rows)
--   - cell_tower.csv (1.8GB, 2,626,336 rows)
-- ========================================================================

USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA RAW;

-- ========================================================================
-- STEP 1: Create a named file format for CSV imports
-- ========================================================================
CREATE OR REPLACE FILE FORMAT CSV_IMPORT_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  ESCAPE = 'NONE'
  ESCAPE_UNENCLOSED_FIELD = '\134'
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO'
  NULL_IF = ('NULL', 'null', '');

-- ========================================================================
-- STEP 2: Create a stage to hold the CSV files
-- ========================================================================
-- Note: You can use either an internal stage or an external stage (S3, Azure, GCS)

-- Option A: Create internal stage
CREATE STAGE IF NOT EXISTS DATA_BACKUP_STAGE
  FILE_FORMAT = CSV_IMPORT_FORMAT
  COMMENT = 'Stage for backup data CSV files';

-- Option B: Use user stage (simpler, no need to create)
-- User stage is referenced as @~ in COPY commands

-- ========================================================================
-- STEP 3: Upload files to stage
-- ========================================================================
-- From SnowSQL command line or Snowflake UI:
-- 
-- Using SnowSQL:
--   PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/support_tickets.csv @DATA_BACKUP_STAGE AUTO_COMPRESS=TRUE;
--   PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/cell_tower.csv @DATA_BACKUP_STAGE AUTO_COMPRESS=TRUE;
--
-- Or using user stage:
--   PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/support_tickets.csv @~ AUTO_COMPRESS=TRUE;
--   PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/cell_tower.csv @~ AUTO_COMPRESS=TRUE;

-- ========================================================================
-- STEP 4: Load SUPPORT_TICKETS table
-- ========================================================================
-- Truncate existing data (optional - remove if you want to append)
-- TRUNCATE TABLE SUPPORT_TICKETS;

COPY INTO SUPPORT_TICKETS (
    TICKET_ID,
    CUSTOMER_NAME,
    CUSTOMER_EMAIL,
    SERVICE_TYPE,
    REQUEST,
    CONTACT_PREFERENCE,
    CELL_ID,
    SENTIMENT_SCORE,
    BACKUP_TIMESTAMP
)
FROM @DATA_BACKUP_STAGE/support_tickets.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'CSV_IMPORT_FORMAT')
ON_ERROR = 'ABORT_STATEMENT'
PURGE = FALSE;

-- Alternative using user stage:
-- COPY INTO SUPPORT_TICKETS
-- FROM @~/support_tickets.csv.gz
-- FILE_FORMAT = (FORMAT_NAME = 'CSV_IMPORT_FORMAT')
-- ON_ERROR = 'ABORT_STATEMENT'
-- PURGE = FALSE;

-- ========================================================================
-- STEP 5: Load CELL_TOWER table
-- ========================================================================
-- Truncate existing data (optional - remove if you want to append)
-- TRUNCATE TABLE CELL_TOWER;

COPY INTO CELL_TOWER (
    CELL_ID,
    CALL_RELEASE_CODE,
    LOOKUP_ID,
    HOME_NETWORK_TAP_CODE,
    SERVING_NETWORK_TAP_CODE,
    IMSI_PREFIX,
    IMEI_PREFIX,
    HOME_NETWORK_NAME,
    HOME_NETWORK_COUNTRY,
    BID_SERVING_NETWORK,
    BID_DESCRIPTION,
    SERVICE_CATEGORY,
    CALL_EVENT_DESCRIPTION,
    ORIG_ID,
    EVENT_DATE,
    IMSI_SUFFIX,
    IMEI_SUFFIX,
    LOCATION_AREA_CODE,
    CHARGED_UNITS,
    MSISDN,
    EVENT_DTTM,
    CALL_ID,
    CAUSE_CODE_SHORT_DESCRIPTION,
    CAUSE_CODE_LONG_DESCRIPTION,
    CELL_LATITUDE,
    CELL_LONGITUDE,
    SENDER_NAME,
    VENDOR_NAME,
    HOSTNAME,
    TIMESTAMP,
    DURATION,
    MANAGED_ELEMENT,
    ENODEB_FUNCTION,
    WINDOW_START_AT,
    WINDOW_END_AT,
    INDEX,
    UE_MEAS_CONTROL,
    PM_UE_MEAS_CONTROL,
    PM_ACTIVE_UE_DL_MAX,
    PM_ACTIVE_UE_DL_SUM,
    PM_ACTIVE_UE_UL_MAX,
    PM_ACTIVE_UE_UL_SUM,
    PM_RRC_CONN_MAX,
    PM_PDCP_LAT_TIME_DL,
    PM_PDCP_LAT_PKT_TRANS_DL,
    PM_PDCP_LAT_TIME_UL,
    PM_PDCP_LAT_PKT_TRANS_UL,
    PM_UE_THP_TIME_DL,
    PM_PDCP_VOL_DL_DRB,
    PM_PDCP_VOL_DL_DRB_LAST_TTI,
    PM_UE_MEAS_RSRP_DELTA_INTRA_FREQ1,
    PM_UE_MEAS_RSRP_SERV_INTRA_FREQ1,
    PM_UE_MEAS_RSRQ_DELTA_INTRA_FREQ1,
    PM_UE_MEAS_RSRQ_SERV_INTRA_FREQ1,
    PM_ERAB_REL_ABNORMAL_ENB_ACT,
    PM_ERAB_REL_ABNORMAL_ENB,
    PM_ERAB_REL_NORMAL_ENB,
    PM_ERAB_REL_MME,
    PM_RRC_CONN_ESTAB_SUCC,
    PM_RRC_CONN_ESTAB_ATT,
    PM_RRC_CONN_ESTAB_ATT_REATT,
    PM_S1_SIG_CONN_ESTAB_SUCC,
    PM_S1_SIG_CONN_ESTAB_ATT,
    PM_ERAB_ESTAB_SUCC_INIT,
    PM_ERAB_ESTAB_ATT_INIT,
    PM_PRB_UTIL_DL,
    PM_PRB_UTIL_UL,
    UNIQUE_ID,
    BACKUP_TIMESTAMP,
    PERFORMANCE_TIER
)
FROM @DATA_BACKUP_STAGE/cell_tower.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'CSV_IMPORT_FORMAT')
ON_ERROR = 'ABORT_STATEMENT'
PURGE = FALSE;

-- Alternative using user stage:
-- COPY INTO CELL_TOWER
-- FROM @~/cell_tower.csv.gz
-- FILE_FORMAT = (FORMAT_NAME = 'CSV_IMPORT_FORMAT')
-- ON_ERROR = 'ABORT_STATEMENT'
-- PURGE = FALSE;

-- ========================================================================
-- STEP 6: Verify the data load
-- ========================================================================
SELECT 'SUPPORT_TICKETS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM SUPPORT_TICKETS
UNION ALL
SELECT 'CELL_TOWER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CELL_TOWER;

-- Check for any load errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SUPPORT_TICKETS',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));

SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CELL_TOWER',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));

-- ========================================================================
-- NOTES:
-- ========================================================================
-- 1. The CSV files are automatically compressed when uploaded with AUTO_COMPRESS=TRUE
-- 2. PURGE = FALSE means the staged files will NOT be deleted after loading
-- 3. Change ON_ERROR to 'CONTINUE' if you want to skip bad rows instead of aborting
-- 4. File format handles timestamp parsing automatically with TIMESTAMP_FORMAT = 'AUTO'
-- 5. The script assumes tables already exist. Run create_tables.sql first if needed.
-- 
-- To upload files from local filesystem using SnowSQL:
--   snowsql -d TELCO_NETWORK_OPTIMIZATION_PROD -s RAW
--   PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/support_tickets.csv @DATA_BACKUP_STAGE AUTO_COMPRESS=TRUE;
--   PUT file:///Users/sweingartner/Cursor/NetworkOptimisationSiS/Setup/cell_tower.csv @DATA_BACKUP_STAGE AUTO_COMPRESS=TRUE;
-- ========================================================================

