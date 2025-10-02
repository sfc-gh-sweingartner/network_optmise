-- ===============================================================================
-- FIX NULL TIMESTAMPS IN CELL_TOWER TABLE
-- ===============================================================================
-- This script fixes the NULL values in TIMESTAMP, WINDOW_START_AT, and WINDOW_END_AT
-- columns by deriving them from EVENT_DTTM
--
-- Pattern observed in existing data:
-- - TIMESTAMP: Top of the hour with .001 milliseconds (e.g., 18:00:00.001)
-- - WINDOW_START_AT: 30 minutes before the hour (e.g., 17:30:00.001)
-- - WINDOW_END_AT: 30 minutes after the hour (e.g., 18:30:00.001)
-- - EVENT_DTTM: Random time within the hour (actual event time)
-- ===============================================================================

USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA RAW;

-- Check current NULL counts before fix
SELECT 
    'BEFORE FIX' AS STATUS,
    COUNT(*) AS TOTAL_ROWS,
    COUNT(*) - COUNT(TIMESTAMP) AS TIMESTAMP_NULLS,
    COUNT(*) - COUNT(WINDOW_START_AT) AS WINDOW_START_NULLS,
    COUNT(*) - COUNT(WINDOW_END_AT) AS WINDOW_END_NULLS
FROM CELL_TOWER;

-- Fix NULL timestamps by deriving from EVENT_DTTM
UPDATE CELL_TOWER
SET 
    -- Set TIMESTAMP to top of the hour with .001 milliseconds
    TIMESTAMP = DATEADD(MILLISECOND, 1, DATE_TRUNC('HOUR', EVENT_DTTM)),
    
    -- Set WINDOW_START_AT to 30 minutes before the hour with .001 milliseconds
    WINDOW_START_AT = DATEADD(MILLISECOND, 1, DATEADD(MINUTE, -30, DATE_TRUNC('HOUR', EVENT_DTTM))),
    
    -- Set WINDOW_END_AT to 30 minutes after the hour with .001 milliseconds
    WINDOW_END_AT = DATEADD(MILLISECOND, 1, DATEADD(MINUTE, 30, DATE_TRUNC('HOUR', EVENT_DTTM)))
WHERE TIMESTAMP IS NULL;

-- Verify the fix
SELECT 
    'AFTER FIX' AS STATUS,
    COUNT(*) AS TOTAL_ROWS,
    COUNT(*) - COUNT(TIMESTAMP) AS TIMESTAMP_NULLS,
    COUNT(*) - COUNT(WINDOW_START_AT) AS WINDOW_START_NULLS,
    COUNT(*) - COUNT(WINDOW_END_AT) AS WINDOW_END_NULLS
FROM CELL_TOWER;

-- Show some examples of the fixed data
SELECT 
    'SAMPLE OF FIXED DATA' AS INFO,
    CELL_ID,
    EVENT_DATE,
    EVENT_DTTM,
    TIMESTAMP,
    WINDOW_START_AT,
    WINDOW_END_AT
FROM CELL_TOWER
WHERE TIMESTAMP IS NOT NULL
ORDER BY TIMESTAMP DESC
LIMIT 10;

-- Verify the pattern matches (should show all records have proper hour alignment)
SELECT 
    'VALIDATION: All timestamps on the hour?' AS CHECK,
    COUNT(*) AS TOTAL,
    SUM(CASE WHEN EXTRACT(MINUTE FROM TIMESTAMP) = 0 AND EXTRACT(SECOND FROM TIMESTAMP) = 0 THEN 1 ELSE 0 END) AS ON_HOUR,
    SUM(CASE WHEN EXTRACT(MINUTE FROM WINDOW_START_AT) = 30 AND EXTRACT(SECOND FROM WINDOW_START_AT) = 0 THEN 1 ELSE 0 END) AS WINDOW_START_CORRECT,
    SUM(CASE WHEN EXTRACT(MINUTE FROM WINDOW_END_AT) = 30 AND EXTRACT(SECOND FROM WINDOW_END_AT) = 0 THEN 1 ELSE 0 END) AS WINDOW_END_CORRECT
FROM CELL_TOWER;

SELECT '✅ NULL TIMESTAMP FIX COMPLETE!' AS STATUS;

