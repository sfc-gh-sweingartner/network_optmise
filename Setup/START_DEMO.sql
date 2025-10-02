-- ===============================================================================
-- START DEMO STREAMING - Quick Command
-- ===============================================================================
-- Copy and run these commands to start your demo
-- Effect: Data will appear every MINUTE with timestamps incrementing by 1 HOUR
-- ===============================================================================

USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA GENERATE;

-- Start both serverless tasks
ALTER TASK TASK_GENERATE_CELL_TOWER_DATA RESUME;
ALTER TASK TASK_GENERATE_SUPPORT_TICKET RESUME;

SELECT '✅ Demo streaming STARTED!' AS STATUS;
SELECT '' AS BLANK;
SELECT 'What happens now:' AS INFO
UNION ALL SELECT '- Every MINUTE: ~14,000 cell tower records generated' AS INFO
UNION ALL SELECT '- Every MINUTE: 1 support ticket generated' AS INFO  
UNION ALL SELECT '- Timestamps increment by 1 HOUR per execution' AS INFO
UNION ALL SELECT '- Effect: 1 minute = 1 hour of data (60x speed!)' AS INFO;
SELECT '' AS BLANK;
SELECT 'Monitor with:' AS INFO
UNION ALL SELECT '  SELECT COUNT(*) FROM RAW.CELL_TOWER;' AS INFO
UNION ALL SELECT '  SELECT TIMESTAMP, COUNT(*) FROM RAW.CELL_TOWER GROUP BY TIMESTAMP ORDER BY TIMESTAMP DESC LIMIT 10;' AS INFO;

-- ===============================================================================
-- To stop the demo, run:
--   ALTER TASK TASK_GENERATE_CELL_TOWER_DATA SUSPEND;
--   ALTER TASK TASK_GENERATE_SUPPORT_TICKET SUSPEND;
-- ===============================================================================

