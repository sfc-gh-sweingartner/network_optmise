-- ===============================================================================
-- STOP DEMO STREAMING - Quick Command
-- ===============================================================================
-- Copy and run these commands to stop your demo
-- ===============================================================================

USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA GENERATE;

-- Stop both tasks
ALTER TASK TASK_GENERATE_CELL_TOWER_DATA SUSPEND;
ALTER TASK TASK_GENERATE_SUPPORT_TICKET SUSPEND;

SELECT '✅ Demo streaming STOPPED!' AS STATUS;
SELECT '' AS BLANK;
SELECT 'Tasks suspended successfully' AS INFO;
SELECT '' AS BLANK;

-- Show final statistics (last 10 hours of generated data)
SELECT 'Recent Generated Data:' AS SECTION;

SELECT 
    TIMESTAMP AS HOUR,
    COUNT(*) AS CELL_TOWER_RECORDS
FROM RAW.CELL_TOWER
GROUP BY TIMESTAMP
ORDER BY TIMESTAMP DESC
LIMIT 10;

SELECT '' AS BLANK;
SELECT 'Recent Support Tickets:' AS SECTION;

SELECT 
    COUNT(*) AS TOTAL_TICKETS,
    MIN(TICKET_ID) AS FIRST_TICKET,
    MAX(TICKET_ID) AS LAST_TICKET
FROM RAW.SUPPORT_TICKETS
WHERE TICKET_ID >= 'TR10000';  -- Only show generated tickets

