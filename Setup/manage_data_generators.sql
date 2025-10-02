-- ===============================================================================
-- DATA GENERATOR MANAGEMENT SCRIPT - DEMO STREAMING MODE
-- ===============================================================================
-- This script provides commands to control and monitor the data generators
--
-- DEMO MODE: Tasks run every 1 MINUTE, generating data with timestamps that
--            increment by 1 HOUR. This creates a "fast-forward" streaming demo.
--
-- SCHEDULE: Tasks run every 1 MINUTE (serverless compute)
-- OUTPUT: ~14,000 cell tower records per minute (one per CELL_ID, timestamp +1 HOUR)
--         1 support ticket per minute
--
-- EFFECT: 1 minute of real time = 1 hour of data time (great for demos!)
--
-- USAGE:
--   - Uncomment and run individual sections as needed
--   - Do not run the entire script at once
-- ===============================================================================

USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA GENERATE;

-- ===============================================================================
-- START DATA GENERATORS
-- ===============================================================================
-- Uncomment these lines to start generating data

 ALTER TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA RESUME;
 ALTER TASK GENERATE.TASK_GENERATE_SUPPORT_TICKET RESUME;
 SELECT 'Data generators started' AS STATUS;

-- ===============================================================================
-- STOP DATA GENERATORS
-- ===============================================================================
-- Uncomment these lines to stop generating data

 ALTER TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA SUSPEND;
 ALTER TASK GENERATE.TASK_GENERATE_SUPPORT_TICKET SUSPEND;
 SELECT 'Data generators stopped' AS STATUS;

-- ===============================================================================
-- CHECK TASK STATUS
-- ===============================================================================
-- View current status and configuration of tasks

SHOW TASKS LIKE 'TASK_GENERATE%' IN SCHEMA GENERATE;

-- ===============================================================================
-- MONITOR DATA GENERATION PROGRESS
-- ===============================================================================

-- Check how much data has been generated
SELECT 
    'CELL_TOWER_TEST' AS TABLE_NAME,
    COUNT(*) AS TOTAL_RECORDS,
    MIN(TIMESTAMP) AS EARLIEST_HOUR,
    MAX(TIMESTAMP) AS LATEST_HOUR,
    TIMESTAMPDIFF(HOUR, MIN(TIMESTAMP), MAX(TIMESTAMP)) + 1 AS HOURS_OF_DATA,
    COUNT(DISTINCT CELL_ID) AS UNIQUE_CELL_IDS
FROM GENERATE.CELL_TOWER_TEST
UNION ALL
SELECT 
    'SUPPORT_TICKETS_TEST' AS TABLE_NAME,
    COUNT(*) AS TOTAL_RECORDS,
    NULL AS EARLIEST_HOUR,
    NULL AS LATEST_HOUR,
    NULL AS HOURS_OF_DATA,
    COUNT(DISTINCT CELL_ID) AS UNIQUE_CELL_IDS
FROM GENERATE.SUPPORT_TICKETS_TEST;

-- View breakdown by timestamp hour (each timestamp represents 1 hour of data)
SELECT 
    TIMESTAMP AS HOUR,
    COUNT(*) AS RECORDS,
    COUNT(DISTINCT CELL_ID) AS UNIQUE_CELLS,
    MIN(EVENT_DTTM) AS EARLIEST_EVENT,
    MAX(EVENT_DTTM) AS LATEST_EVENT
FROM GENERATE.CELL_TOWER_TEST
GROUP BY TIMESTAMP
ORDER BY TIMESTAMP DESC
LIMIT 10;

-- ===============================================================================
-- VIEW RECENT GENERATED DATA
-- ===============================================================================

-- View latest cell tower records
SELECT 
    CELL_ID,
    EVENT_DTTM,
    VENDOR_NAME,
    BID_DESCRIPTION,
    SERVICE_CATEGORY,
    PM_PDCP_LAT_TIME_DL AS LATENCY_MS,
    ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) AS RRC_FAILURE_RATE,
    PM_PRB_UTIL_DL
FROM GENERATE.CELL_TOWER_TEST
ORDER BY EVENT_DTTM DESC
LIMIT 10;

-- View latest support tickets
SELECT 
    TICKET_ID,
    CUSTOMER_NAME,
    SERVICE_TYPE,
    CELL_ID,
    SENTIMENT_SCORE,
    LEFT(REQUEST, 100) || '...' AS REQUEST_PREVIEW
FROM GENERATE.SUPPORT_TICKETS_TEST
ORDER BY TICKET_ID DESC
LIMIT 10;

-- ===============================================================================
-- DATA QUALITY CHECKS
-- ===============================================================================

-- Verify cell tower data distributions match reference data
SELECT 
    'Vendor Distribution Check' AS CHECK_TYPE,
    VENDOR_NAME,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS PERCENTAGE
FROM GENERATE.CELL_TOWER_TEST
GROUP BY VENDOR_NAME
ORDER BY COUNT DESC;

SELECT 
    'Performance Tier Distribution Check' AS CHECK_TYPE,
    ref.PERFORMANCE_TIER,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS PERCENTAGE
FROM GENERATE.CELL_TOWER_TEST ct
JOIN GENERATE.REF_CELL_TOWER_ATTRIBUTES ref ON ct.CELL_ID = ref.CELL_ID
GROUP BY ref.PERFORMANCE_TIER
ORDER BY COUNT DESC;

-- Verify support ticket distributions
SELECT 
    'Service Type Distribution' AS CHECK_TYPE,
    SERVICE_TYPE,
    COUNT(*) AS COUNT,
    ROUND(AVG(SENTIMENT_SCORE), 2) AS AVG_SENTIMENT
FROM GENERATE.SUPPORT_TICKETS_TEST
GROUP BY SERVICE_TYPE
ORDER BY COUNT DESC;

SELECT 
    'Ticket-Tower Correlation Check' AS CHECK_TYPE,
    ref.PERFORMANCE_TIER,
    COUNT(*) AS TICKET_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS PERCENTAGE
FROM GENERATE.SUPPORT_TICKETS_TEST st
JOIN GENERATE.REF_CELL_TOWER_ATTRIBUTES ref ON st.CELL_ID = ref.CELL_ID
GROUP BY ref.PERFORMANCE_TIER
ORDER BY TICKET_COUNT DESC;

-- ===============================================================================
-- TEST PROCEDURES MANUALLY
-- ===============================================================================
-- Uncomment to manually test the procedures

-- CALL GENERATE.SP_GENERATE_CELL_TOWER_DATA();
-- CALL GENERATE.SP_GENERATE_SUPPORT_TICKET();

-- ===============================================================================
-- TRUNCATE TEST TABLES (START FRESH)
-- ===============================================================================
-- WARNING: This will delete all generated test data!
-- Uncomment only if you want to start over

-- TRUNCATE TABLE GENERATE.CELL_TOWER_TEST;
-- TRUNCATE TABLE GENERATE.SUPPORT_TICKETS_TEST;
-- SELECT 'Test tables truncated - ready for fresh start' AS STATUS;

-- ===============================================================================
-- VIEW TASK EXECUTION HISTORY (LAST 24 HOURS)
-- ===============================================================================

SELECT 
    name AS TASK_NAME,
    state AS EXECUTION_STATE,
    scheduled_time,
    completed_time,
    TIMESTAMPDIFF(SECOND, scheduled_time, completed_time) AS DURATION_SECONDS,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE name IN ('TASK_GENERATE_CELL_TOWER_DATA', 'TASK_GENERATE_SUPPORT_TICKET')
ORDER BY scheduled_time DESC
LIMIT 20;

-- ===============================================================================
-- VERIFY TIMESTAMP PATTERN (SHOULD MATCH PRODUCTION)
-- ===============================================================================

-- Verify timestamp pattern: TIMESTAMP should increment by 1 hour with pattern HH:00:00.001
SELECT 
    'Timestamp Pattern Check' AS CHECK_NAME,
    COUNT(*) AS TOTAL_RECORDS,
    SUM(CASE WHEN EXTRACT(MINUTE FROM TIMESTAMP) = 0 
             AND EXTRACT(SECOND FROM TIMESTAMP) = 0 
             AND EXTRACT(MILLISECOND FROM TIMESTAMP) = 1 THEN 1 ELSE 0 END) AS CORRECT_PATTERN,
    SUM(CASE WHEN EXTRACT(MINUTE FROM WINDOW_START_AT) = 30 
             AND EXTRACT(SECOND FROM WINDOW_START_AT) = 0 THEN 1 ELSE 0 END) AS WINDOW_START_CORRECT,
    SUM(CASE WHEN EXTRACT(MINUTE FROM WINDOW_END_AT) = 30 
             AND EXTRACT(SECOND FROM WINDOW_END_AT) = 0 THEN 1 ELSE 0 END) AS WINDOW_END_CORRECT
FROM GENERATE.CELL_TOWER_TEST;

-- Show sample with all timestamp columns
SELECT 
    CELL_ID,
    TIMESTAMP,
    EVENT_DTTM,
    WINDOW_START_AT,
    WINDOW_END_AT,
    TIMESTAMPDIFF(MINUTE, WINDOW_START_AT, TIMESTAMP) AS START_TO_TS_MINS,
    TIMESTAMPDIFF(MINUTE, TIMESTAMP, WINDOW_END_AT) AS TS_TO_END_MINS
FROM GENERATE.CELL_TOWER_TEST
ORDER BY TIMESTAMP DESC
LIMIT 5;

-- ===============================================================================
-- ADVANCED: COMPARE TEST DATA TO PRODUCTION DATA
-- ===============================================================================

-- Compare latency distributions
SELECT 
    'PRODUCTION' AS DATA_SOURCE,
    ROUND(AVG(PM_PDCP_LAT_TIME_DL), 2) AS AVG_LATENCY_MS,
    ROUND(MIN(PM_PDCP_LAT_TIME_DL), 2) AS MIN_LATENCY_MS,
    ROUND(MAX(PM_PDCP_LAT_TIME_DL), 2) AS MAX_LATENCY_MS
FROM RAW.CELL_TOWER
UNION ALL
SELECT 
    'TEST_GENERATED' AS DATA_SOURCE,
    ROUND(AVG(PM_PDCP_LAT_TIME_DL), 2) AS AVG_LATENCY_MS,
    ROUND(MIN(PM_PDCP_LAT_TIME_DL), 2) AS MIN_LATENCY_MS,
    ROUND(MAX(PM_PDCP_LAT_TIME_DL), 2) AS MAX_LATENCY_MS
FROM GENERATE.CELL_TOWER_TEST;

-- Compare RRC failure rates
SELECT 
    'PRODUCTION' AS DATA_SOURCE,
    ROUND(AVG(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / 
        NULLIF(PM_RRC_CONN_ESTAB_ATT, 0) * 100)), 2) AS AVG_RRC_FAILURE_RATE,
    ROUND(MIN(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / 
        NULLIF(PM_RRC_CONN_ESTAB_ATT, 0) * 100)), 2) AS MIN_RRC_FAILURE_RATE,
    ROUND(MAX(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / 
        NULLIF(PM_RRC_CONN_ESTAB_ATT, 0) * 100)), 2) AS MAX_RRC_FAILURE_RATE
FROM RAW.CELL_TOWER
WHERE PM_RRC_CONN_ESTAB_ATT > 0
UNION ALL
SELECT 
    'TEST_GENERATED' AS DATA_SOURCE,
    ROUND(AVG(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / 
        NULLIF(PM_RRC_CONN_ESTAB_ATT, 0) * 100)), 2) AS AVG_RRC_FAILURE_RATE,
    ROUND(MIN(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / 
        NULLIF(PM_RRC_CONN_ESTAB_ATT, 0) * 100)), 2) AS MIN_RRC_FAILURE_RATE,
    ROUND(MAX(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / 
        NULLIF(PM_RRC_CONN_ESTAB_ATT, 0) * 100)), 2) AS MAX_RRC_FAILURE_RATE
FROM GENERATE.CELL_TOWER_TEST
WHERE PM_RRC_CONN_ESTAB_ATT > 0;

-- ===============================================================================
-- PROMOTION TO PRODUCTION (USE WITH CAUTION!)
-- ===============================================================================
-- Once you're satisfied with the test data quality, you can modify the procedures
-- to write directly to RAW.CELL_TOWER and RAW.SUPPORT_TICKETS
--
-- Steps:
-- 1. Suspend both tasks
-- 2. Modify SP_GENERATE_CELL_TOWER_DATA to INSERT INTO RAW.CELL_TOWER
-- 3. Modify SP_GENERATE_SUPPORT_TICKET to INSERT INTO RAW.SUPPORT_TICKETS
-- 4. Resume tasks
--
-- WARNING: This will add data to production tables!
-- ===============================================================================

