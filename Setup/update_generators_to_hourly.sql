-- ===============================================================================
-- UPDATE DATA GENERATORS TO HOURLY GENERATION
-- ===============================================================================
-- This script updates the stored procedures and tasks to generate data hourly
-- instead of every minute, following the pattern observed in production data:
--
-- TIMESTAMP pattern: Top of hour with .001 milliseconds (e.g., 18:00:00.001)
-- One row per CELL_ID per hour
-- ===============================================================================

USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA GENERATE;

-- ===============================================================================
-- STEP 1: SUSPEND EXISTING TASKS
-- ===============================================================================

ALTER TASK IF EXISTS TASK_GENERATE_CELL_TOWER_DATA SUSPEND;
ALTER TASK IF EXISTS TASK_GENERATE_SUPPORT_TICKET SUSPEND;

SELECT 'Tasks suspended' AS STATUS;

-- ===============================================================================
-- STEP 2: UPDATE CELL TOWER GENERATION PROCEDURE (HOURLY)
-- ===============================================================================

CREATE OR REPLACE PROCEDURE GENERATE.SP_GENERATE_CELL_TOWER_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    latest_timestamp TIMESTAMP_NTZ;
    new_timestamp TIMESTAMP_NTZ;
    rows_inserted INT;
BEGIN
    -- Get the latest timestamp from test table, or use current hour if empty
    SELECT COALESCE(MAX(TIMESTAMP), DATEADD(MILLISECOND, 1, DATE_TRUNC('HOUR', CURRENT_TIMESTAMP()))) 
    INTO :latest_timestamp
    FROM GENERATE.CELL_TOWER_TEST;
    
    -- New timestamp is 1 HOUR after the latest, with .001 milliseconds
    new_timestamp := DATEADD(HOUR, 1, :latest_timestamp);
    
    -- Generate one row for each cell ID
    INSERT INTO GENERATE.CELL_TOWER_TEST (
        CELL_ID, CALL_RELEASE_CODE, LOOKUP_ID, HOME_NETWORK_TAP_CODE, SERVING_NETWORK_TAP_CODE,
        IMSI_PREFIX, IMEI_PREFIX, HOME_NETWORK_NAME, HOME_NETWORK_COUNTRY, BID_SERVING_NETWORK,
        BID_DESCRIPTION, SERVICE_CATEGORY, CALL_EVENT_DESCRIPTION, ORIG_ID, EVENT_DATE,
        IMSI_SUFFIX, IMEI_SUFFIX, LOCATION_AREA_CODE, CHARGED_UNITS, MSISDN, EVENT_DTTM,
        CALL_ID, CAUSE_CODE_SHORT_DESCRIPTION, CAUSE_CODE_LONG_DESCRIPTION, CELL_LATITUDE, CELL_LONGITUDE,
        SENDER_NAME, VENDOR_NAME, HOSTNAME, TIMESTAMP, DURATION, MANAGED_ELEMENT, ENODEB_FUNCTION,
        WINDOW_START_AT, WINDOW_END_AT, INDEX, UE_MEAS_CONTROL, PM_UE_MEAS_CONTROL,
        PM_ACTIVE_UE_DL_MAX, PM_ACTIVE_UE_DL_SUM, PM_ACTIVE_UE_UL_MAX, PM_ACTIVE_UE_UL_SUM,
        PM_RRC_CONN_MAX, PM_PDCP_LAT_TIME_DL, PM_PDCP_LAT_PKT_TRANS_DL, PM_PDCP_LAT_TIME_UL,
        PM_PDCP_LAT_PKT_TRANS_UL, PM_UE_THP_TIME_DL, PM_PDCP_VOL_DL_DRB, PM_PDCP_VOL_DL_DRB_LAST_TTI,
        PM_UE_MEAS_RSRP_DELTA_INTRA_FREQ1, PM_UE_MEAS_RSRP_SERV_INTRA_FREQ1,
        PM_UE_MEAS_RSRQ_DELTA_INTRA_FREQ1, PM_UE_MEAS_RSRQ_SERV_INTRA_FREQ1,
        PM_ERAB_REL_ABNORMAL_ENB_ACT, PM_ERAB_REL_ABNORMAL_ENB, PM_ERAB_REL_NORMAL_ENB, PM_ERAB_REL_MME,
        PM_RRC_CONN_ESTAB_SUCC, PM_RRC_CONN_ESTAB_ATT, PM_RRC_CONN_ESTAB_ATT_REATT,
        PM_S1_SIG_CONN_ESTAB_SUCC, PM_S1_SIG_CONN_ESTAB_ATT, PM_ERAB_ESTAB_SUCC_INIT, PM_ERAB_ESTAB_ATT_INIT,
        PM_PRB_UTIL_DL, PM_PRB_UTIL_UL, UNIQUE_ID
    )
    WITH base_data AS (
        SELECT 
            ref.*,
            -- Determine service category once
            CASE WHEN UNIFORM(1, 100, RANDOM()) <= 60 THEN 'VOICE'
                 WHEN UNIFORM(1, 100, RANDOM()) <= 85 THEN 'GPRS'
                 ELSE 'SMS' END AS svc_cat,
            -- Call release code
            CASE WHEN UNIFORM(1, 100, RANDOM()) <= 85 THEN 0
                 WHEN UNIFORM(1, 100, RANDOM()) <= 95 THEN 9
                 ELSE 70 END AS rel_code,
            -- TIMESTAMP = Top of hour with .001 milliseconds
            :new_timestamp AS ts_hour,
            -- EVENT_DTTM = Random time within the hour
            DATEADD(SECOND, UNIFORM(0, 3599, RANDOM()), DATEADD(MILLISECOND, -1, :new_timestamp)) AS event_ts,
            -- WINDOW_START_AT = 30 minutes before the hour with .001 milliseconds
            DATEADD(MINUTE, -30, :new_timestamp) AS window_start,
            -- WINDOW_END_AT = 30 minutes after the hour with .001 milliseconds
            DATEADD(MINUTE, 30, :new_timestamp) AS window_end
        FROM GENERATE.REF_CELL_TOWER_ATTRIBUTES ref
    )
    SELECT 
        CELL_ID,
        rel_code,
        CELL_ID,
        HOME_NETWORK_TAP_CODE,
        'CANTS',
        CASE 
            WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 302
            WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 310
            WHEN HOME_NETWORK_TAP_CODE = 'GBRCL' THEN 234
            ELSE 540 END,
        40000000 + UNIFORM(1, 99999999, RANDOM()),
        HOME_NETWORK_NAME,
        HOME_NETWORK_COUNTRY,
        44000 + UNIFORM(1, 999, RANDOM()),
        BID_DESCRIPTION,
        svc_cat,
        CASE WHEN svc_cat = 'VOICE' THEN 'MOBILE ORIGINATED CALL'
             WHEN svc_cat = 'GPRS' THEN 'GPRS DATA SESSION'
             ELSE 'SMS DELIVERY' END,
        CELL_ID,
        DATE(ts_hour),
        200000000000 + UNIFORM(1, 999999999999, RANDOM()),
        300000 + UNIFORM(1, 999999, RANDOM()),
        LOCATION_AREA_CODE,
        CASE WHEN svc_cat = 'VOICE' THEN UNIFORM(60, 1800, RANDOM())
             WHEN svc_cat = 'GPRS' THEN UNIFORM(1000, 100000, RANDOM())
             ELSE 1 END,
        9000000000 + UNIFORM(1, 999999999, RANDOM()),
        event_ts,
        MD5(TO_VARCHAR(CELL_ID) || TO_VARCHAR(ts_hour)),
        -- Cause codes
        CASE WHEN rel_code = 0 THEN 'NORMAL_CALL_CLEARING'
             WHEN rel_code = 9 THEN 
                CASE WHEN UNIFORM(1, 100, RANDOM()) <= 30 THEN 'NETWORK_CONGESTION'
                     WHEN UNIFORM(1, 100, RANDOM()) <= 60 THEN 'CHANNEL_TYPE_NOT_IMPLEMENTED'
                     WHEN UNIFORM(1, 100, RANDOM()) <= 80 THEN 'CONNECTION_OUT_OF_SERVICE'
                     ELSE 'FACILITY_NOT_IMPLEMENTED' END
             ELSE 'HANDOVER_SUCCESSFUL' END,
        CASE WHEN rel_code = 0 THEN 'Call completed successfully without any issues'
             WHEN UNIFORM(1, 100, RANDOM()) <= 30 THEN 'This cause indicates that the network is experiencing high traffic volume and cannot process additional calls at this time'
             WHEN UNIFORM(1, 100, RANDOM()) <= 60 THEN 'The requested channel type is not implemented or supported by the network element'
             WHEN UNIFORM(1, 100, RANDOM()) <= 80 THEN 'The connection has been lost due to equipment failure or maintenance activities'
             ELSE 'Call was successfully handed over to another cell tower for continued service' END,
        CELL_LATITUDE,
        CELL_LONGITUDE,
        'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || CELL_ID || 'L2100',
        VENDOR_NAME,
        'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || CELL_ID || 'L2100',
        ts_hour,  -- TIMESTAMP column
        CASE WHEN svc_cat = 'VOICE' THEN UNIFORM(30, 1800, RANDOM())
             WHEN svc_cat = 'GPRS' THEN UNIFORM(60, 7200, RANDOM())
             ELSE UNIFORM(1, 10, RANDOM()) END,
        1,
        ENODEB_FUNCTION,
        window_start,  -- WINDOW_START_AT
        window_end,    -- WINDOW_END_AT
        'ManagedElement=1,ENodeBFunction=' || UNIFORM(1, 20, RANDOM()) || 
        ',EUtranCellFDD=' || CELL_ID || ',UeMeasControl=1,PmUeMeasControl=1',
        1,
        1,
        -- Performance metrics based on performance tier
        CASE WHEN ENODEB_FUNCTION = 10 THEN
            CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN UNIFORM(50, 120, RANDOM())
                 ELSE UNIFORM(30, 80, RANDOM()) END
        ELSE UNIFORM(10, 40, RANDOM()) END::DECIMAL(38,2),
        CASE WHEN ENODEB_FUNCTION = 10 THEN UNIFORM(500000, 1500000, RANDOM())
             ELSE UNIFORM(100000, 600000, RANDOM()) END::DECIMAL(38,2),
        CASE WHEN ENODEB_FUNCTION = 10 THEN UNIFORM(40, 100, RANDOM())
             ELSE UNIFORM(15, 50, RANDOM()) END::DECIMAL(38,2),
        CASE WHEN ENODEB_FUNCTION = 10 THEN UNIFORM(400000, 1200000, RANDOM())
             ELSE UNIFORM(80000, 500000, RANDOM()) END::DECIMAL(38,2),
        CASE WHEN ENODEB_FUNCTION = 10 THEN UNIFORM(800, 1500, RANDOM())
             ELSE UNIFORM(200, 600, RANDOM()) END::DECIMAL(38,2),
        -- Latency based on performance tier
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN ROUND(40 + (UNIFORM(0, 15, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            WHEN 'VERY_BAD' THEN ROUND(30 + (UNIFORM(0, 12, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            WHEN 'BAD' THEN ROUND(22 + (UNIFORM(0, 10, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            WHEN 'QUITE_BAD' THEN ROUND(15 + (UNIFORM(0, 8, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            WHEN 'PROBLEMATIC' THEN ROUND(10 + (UNIFORM(0, 5, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            ELSE 
                CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN ROUND(8 + (UNIFORM(0, 4, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
                     ELSE ROUND(10 + (UNIFORM(0, 5, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2) END
        END AS lat_dl,
        ROUND(lat_dl * (0.1 + (UNIFORM(0, 20, RANDOM()) * 0.01)), 2),
        -- Uplink latency as string
        TO_VARCHAR(
            ROUND(
                CASE PERFORMANCE_TIER
                    WHEN 'CATASTROPHIC' THEN 80 + (UNIFORM(0, 30, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    WHEN 'VERY_BAD' THEN 60 + (UNIFORM(0, 25, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    WHEN 'BAD' THEN 45 + (UNIFORM(0, 20, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    WHEN 'QUITE_BAD' THEN 30 + (UNIFORM(0, 15, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    WHEN 'PROBLEMATIC' THEN 22 + (UNIFORM(0, 10, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    ELSE 20 + (UNIFORM(0, 10, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                END
            , 2)
        ) AS lat_ul,
        TO_VARCHAR(ROUND(CAST(lat_ul AS NUMBER) * (0.1 + (UNIFORM(0, 20, RANDOM()) * 0.01)), 2)),
        UNIFORM(400000, 1000000, RANDOM())::DECIMAL(38,2),
        -- Data volume
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 8000000 + (UNIFORM(0, 7000000, RANDOM()))
            WHEN 'VERY_BAD' THEN 12000000 + (UNIFORM(0, 10000000, RANDOM()))
            WHEN 'BAD' THEN 18000000 + (UNIFORM(0, 15000000, RANDOM()))
            WHEN 'QUITE_BAD' THEN 22000000 + (UNIFORM(0, 18000000, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 28000000 + (UNIFORM(0, 20000000, RANDOM()))
            ELSE 35000000 + (UNIFORM(0, 25000000, RANDOM()))
        END::DECIMAL(38,2) AS vol_dl,
        ROUND(vol_dl * (0.03 + (UNIFORM(0, 30, RANDOM()) * 0.001)), 0),
        -- Signal quality
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 8 + (UNIFORM(0, 8, RANDOM()))
            WHEN 'VERY_BAD' THEN 6 + (UNIFORM(0, 7, RANDOM()))
            WHEN 'BAD' THEN 4 + (UNIFORM(0, 6, RANDOM()))
            WHEN 'QUITE_BAD' THEN 3 + (UNIFORM(0, 5, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 2 + (UNIFORM(0, 4, RANDOM()))
            ELSE 0 + (UNIFORM(0, 3, RANDOM()))
        END,
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN -105 - (UNIFORM(0, 10, RANDOM()))
            WHEN 'VERY_BAD' THEN -95 - (UNIFORM(0, 10, RANDOM()))
            WHEN 'BAD' THEN -85 - (UNIFORM(0, 10, RANDOM()))
            WHEN 'QUITE_BAD' THEN -75 - (UNIFORM(0, 10, RANDOM()))
            WHEN 'PROBLEMATIC' THEN -68 - (UNIFORM(0, 10, RANDOM()))
            ELSE -55 - (UNIFORM(0, 15, RANDOM()))
        END,
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 5 + (UNIFORM(0, 6, RANDOM()))
            WHEN 'VERY_BAD' THEN 4 + (UNIFORM(0, 5, RANDOM()))
            WHEN 'BAD' THEN 3 + (UNIFORM(0, 4, RANDOM()))
            WHEN 'QUITE_BAD' THEN 2 + (UNIFORM(0, 3, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 1 + (UNIFORM(0, 3, RANDOM()))
            ELSE 0 + (UNIFORM(0, 2, RANDOM()))
        END,
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN -18 - (UNIFORM(0, 5, RANDOM()))
            WHEN 'VERY_BAD' THEN -15 - (UNIFORM(0, 4, RANDOM()))
            WHEN 'BAD' THEN -12 - (UNIFORM(0, 4, RANDOM()))
            WHEN 'QUITE_BAD' THEN -9 - (UNIFORM(0, 3, RANDOM()))
            WHEN 'PROBLEMATIC' THEN -7 - (UNIFORM(0, 3, RANDOM()))
            ELSE -4 - (UNIFORM(0, 4, RANDOM()))
        END,
        -- E-RAB metrics
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN ROUND(20.0 + (UNIFORM(0, 500, RANDOM()) * 0.01), 2)
            WHEN 'VERY_BAD' THEN ROUND(15.0 + (UNIFORM(0, 500, RANDOM()) * 0.01), 2)
            WHEN 'BAD' THEN ROUND(10.0 + (UNIFORM(0, 500, RANDOM()) * 0.01), 2)
            WHEN 'QUITE_BAD' THEN ROUND(5.0 + (UNIFORM(0, 500, RANDOM()) * 0.01), 2)
            WHEN 'PROBLEMATIC' THEN ROUND(2.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            ELSE ROUND(0.1 + (UNIFORM(0, 190, RANDOM()) * 0.01), 2)
        END,
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN ROUND(22.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            WHEN 'VERY_BAD' THEN ROUND(17.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            WHEN 'BAD' THEN ROUND(12.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            WHEN 'QUITE_BAD' THEN ROUND(7.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            WHEN 'PROBLEMATIC' THEN ROUND(3.5 + (UNIFORM(0, 250, RANDOM()) * 0.01), 2)
            ELSE ROUND(0.5 + (UNIFORM(0, 200, RANDOM()) * 0.01), 2)
        END,
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 200 + (UNIFORM(0, 300, RANDOM()))
            WHEN 'VERY_BAD' THEN 400 + (UNIFORM(0, 600, RANDOM()))
            WHEN 'BAD' THEN 600 + (UNIFORM(0, 800, RANDOM()))
            WHEN 'QUITE_BAD' THEN 800 + (UNIFORM(0, 700, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 1000 + (UNIFORM(0, 800, RANDOM()))
            ELSE 1200 + (UNIFORM(0, 800, RANDOM()))
        END,
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 80 + (UNIFORM(0, 120, RANDOM()))
            WHEN 'VERY_BAD' THEN 60 + (UNIFORM(0, 80, RANDOM()))
            WHEN 'BAD' THEN 40 + (UNIFORM(0, 60, RANDOM()))
            ELSE 10 + (UNIFORM(0, 50, RANDOM()))
        END,
        -- RRC connection metrics
        ROUND(UNIFORM(15000, 35000, RANDOM()) * 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN (1 - UNIFORM(0.4, 0.8, RANDOM()))
                WHEN 'VERY_BAD' THEN (1 - UNIFORM(0.2, 0.5, RANDOM()))
                WHEN 'BAD' THEN (1 - UNIFORM(0.1, 0.3, RANDOM()))
                WHEN 'QUITE_BAD' THEN (1 - UNIFORM(0.05, 0.15, RANDOM()))
                WHEN 'PROBLEMATIC' THEN (1 - UNIFORM(0.02, 0.08, RANDOM()))
                ELSE (1 - UNIFORM(0.001, 0.03, RANDOM()))
            END, 0)::DECIMAL(38,2),
        UNIFORM(15000, 35000, RANDOM())::DECIMAL(38,2),
        UNIFORM(5000, 15000, RANDOM())::DECIMAL(38,2),
        -- S1 signaling
        ROUND(UNIFORM(12000, 25000, RANDOM()) * (0.85 + UNIFORM(0, 13, RANDOM()) * 0.01), 0)::DECIMAL(38,2),
        UNIFORM(12000, 25000, RANDOM())::DECIMAL(38,2),
        -- E-RAB establishment
        ROUND(UNIFORM(18000, 32000, RANDOM()) * 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN (0.55 + UNIFORM(0, 10, RANDOM()) * 0.01)
                WHEN 'VERY_BAD' THEN (0.65 + UNIFORM(0, 10, RANDOM()) * 0.01)
                WHEN 'BAD' THEN (0.75 + UNIFORM(0, 10, RANDOM()) * 0.01)
                WHEN 'QUITE_BAD' THEN (0.80 + UNIFORM(0, 10, RANDOM()) * 0.01)
                WHEN 'PROBLEMATIC' THEN (0.88 + UNIFORM(0, 8, RANDOM()) * 0.01)
                ELSE (0.93 + UNIFORM(0, 6, RANDOM()) * 0.01)
            END, 0)::DECIMAL(38,2),
        UNIFORM(18000, 32000, RANDOM())::DECIMAL(38,2),
        -- PRB utilization
        CASE PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 90 + (UNIFORM(0, 10, RANDOM()))
            WHEN 'VERY_BAD' THEN 75 + (UNIFORM(0, 20, RANDOM()))
            WHEN 'BAD' THEN 60 + (UNIFORM(0, 25, RANDOM()))
            WHEN 'QUITE_BAD' THEN 45 + (UNIFORM(0, 25, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 30 + (UNIFORM(0, 25, RANDOM()))
            ELSE 
                CASE 
                    WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 25 + (UNIFORM(0, 30, RANDOM()))
                    WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%SCOTLAND%' THEN 5 + (UNIFORM(0, 20, RANDOM()))
                    ELSE 15 + (UNIFORM(0, 25, RANDOM()))
                END
        END::DECIMAL(38,2) AS prb_dl,
        ROUND(prb_dl * (0.6 + (UNIFORM(0, 21, RANDOM()) * 0.01)), 0),
        MD5(TO_VARCHAR(CELL_ID) || TO_VARCHAR(ts_hour) || 'unique')
    FROM base_data;
    
    rows_inserted := SQLROWCOUNT;
    
    RETURN 'Generated ' || rows_inserted || ' cell tower records for hour: ' || TO_VARCHAR(:new_timestamp);
END;
$$;

SELECT 'Cell Tower procedure updated to hourly generation' AS STATUS;

-- ===============================================================================
-- STEP 3: SUPPORT TICKET PROCEDURE (NO CHANGES NEEDED - ALREADY APPROPRIATE)
-- ===============================================================================
-- The support ticket procedure generates 1 ticket at a time, which is fine.
-- Tasks will call it hourly instead of every minute.

SELECT 'Support Ticket procedure unchanged (already appropriate for hourly)' AS STATUS;

-- ===============================================================================
-- STEP 4: UPDATE TASKS TO RUN HOURLY
-- ===============================================================================

-- Drop and recreate tasks with hourly schedule
DROP TASK IF EXISTS TASK_GENERATE_CELL_TOWER_DATA;
DROP TASK IF EXISTS TASK_GENERATE_SUPPORT_TICKET;

CREATE TASK TASK_GENERATE_CELL_TOWER_DATA
    WAREHOUSE = MYWH
    SCHEDULE = '60 MINUTE'
AS
    CALL GENERATE.SP_GENERATE_CELL_TOWER_DATA();

CREATE TASK TASK_GENERATE_SUPPORT_TICKET
    WAREHOUSE = MYWH
    SCHEDULE = '60 MINUTE'
AS
    CALL GENERATE.SP_GENERATE_SUPPORT_TICKET();

SELECT 'Tasks recreated with 60 MINUTE schedule' AS STATUS;

-- ===============================================================================
-- STEP 5: TEST THE UPDATED PROCEDURE
-- ===============================================================================

-- Test the cell tower generation
CALL GENERATE.SP_GENERATE_CELL_TOWER_DATA();

-- Check the result
SELECT 
    COUNT(*) AS records_generated,
    MAX(TIMESTAMP) AS latest_timestamp,
    MAX(EVENT_DTTM) AS latest_event_dttm,
    MAX(WINDOW_START_AT) AS latest_window_start,
    MAX(WINDOW_END_AT) AS latest_window_end
FROM GENERATE.CELL_TOWER_TEST;

-- Verify timestamp pattern (should be on the hour with .001)
SELECT 
    TIMESTAMP,
    COUNT(*) AS record_count,
    MIN(EVENT_DTTM) AS min_event_time,
    MAX(EVENT_DTTM) AS max_event_time
FROM GENERATE.CELL_TOWER_TEST
GROUP BY TIMESTAMP
ORDER BY TIMESTAMP DESC
LIMIT 5;

SELECT '✅ HOURLY GENERATOR UPDATE COMPLETE!' AS STATUS;
SELECT 'Tasks are currently SUSPENDED' AS NOTE;
SELECT 'Use ALTER TASK ... RESUME to start hourly generation' AS INSTRUCTION;

