-- ===============================================================================
-- DATA GENERATOR SETUP SCRIPT - DEMO STREAMING MODE
-- ===============================================================================
-- This script creates:
-- 1. GENERATE schema with test tables
-- 2. Reference tables for data generation
-- 3. Stored procedures for generating cell tower and support ticket data
-- 4. SERVERLESS Snowflake tasks that run every MINUTE
--
-- DEMO MODE: Tasks run every MINUTE, generating data with timestamps that
--            increment by 1 HOUR. This creates a "fast-forward" streaming demo
--            where 1 minute of real time = 1 hour of data time.
--
-- PATTERN: Generates one row per CELL_ID per MINUTE (timestamp increments by 1 HOUR)
-- TIMESTAMP: Top of hour with .001 milliseconds (e.g., 22:00:00.001, then 23:00:00.001)
--
-- USAGE:
--   - Run this script once to set up everything
--   - Use manage_data_generators.sql to start/stop/monitor the generators
-- ===============================================================================

USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;

-- ===============================================================================
-- STEP 1: CREATE GENERATE SCHEMA AND TEST TABLES
-- ===============================================================================

CREATE SCHEMA IF NOT EXISTS GENERATE;

-- Note: Generator procedures now write directly to production tables:
--   - RAW.CELL_TOWER
--   - RAW.SUPPORT_TICKETS

SELECT 'Step 1 Complete: GENERATE schema created' AS STATUS;

-- ===============================================================================
-- STEP 2: CREATE REFERENCE TABLES
-- ===============================================================================

-- Reference table: Cell IDs with their static attributes
CREATE OR REPLACE TABLE GENERATE.REF_CELL_TOWER_ATTRIBUTES AS
SELECT 
    CELL_ID,
    HOME_NETWORK_TAP_CODE,
    HOME_NETWORK_NAME,
    HOME_NETWORK_COUNTRY,
    BID_DESCRIPTION,
    VENDOR_NAME,
    CELL_LATITUDE,
    CELL_LONGITUDE,
    ENODEB_FUNCTION,
    LOCATION_AREA_CODE,
    PERFORMANCE_TIER
FROM RAW.CELL_TOWER
GROUP BY 
    CELL_ID,
    HOME_NETWORK_TAP_CODE,
    HOME_NETWORK_NAME,
    HOME_NETWORK_COUNTRY,
    BID_DESCRIPTION,
    VENDOR_NAME,
    CELL_LATITUDE,
    CELL_LONGITUDE,
    ENODEB_FUNCTION,
    LOCATION_AREA_CODE,
    PERFORMANCE_TIER;

-- Reference table: Customer complaint texts
CREATE OR REPLACE TABLE GENERATE.REF_COMPLAINT_TEXTS (
    SERVICE_TYPE VARCHAR(60),
    COMPLAINT_TEXT VARCHAR(16777216),
    SENTIMENT_MIN FLOAT,
    SENTIMENT_MAX FLOAT
);

INSERT INTO GENERATE.REF_COMPLAINT_TEXTS VALUES
-- Cellular - Negative sentiment
('Cellular', 'I am experiencing frequent call drops in my area. This has been happening for the past week and is affecting my work calls. Please investigate and resolve this issue.', -0.95, -0.60),
('Cellular', 'My data connection is extremely slow, sometimes taking minutes to load basic websites. I have restarted my phone multiple times but the issue persists.', -0.90, -0.60),
('Cellular', 'I cannot make outgoing calls from my location. The calls fail immediately with a busy signal. This started 3 days ago and is still ongoing.', -0.95, -0.70),
('Cellular', 'Text messages are being delayed by several hours. I am missing important communications from family and work. Please fix this urgent issue.', -0.90, -0.65),
('Cellular', 'The network coverage in my neighborhood is poor. I only get 1-2 bars and calls often fail. When will you improve coverage in this area?', -0.85, -0.55),
('Cellular', 'My monthly bill shows data overage charges but I have been connected to WiFi most of the month. Please review my usage and adjust the charges.', -0.80, -0.30),
('Cellular', 'I traveled internationally and received a $200 roaming charge that was not explained to me. Please review these charges and provide a refund if appropriate.', -0.80, -0.30),
-- Cellular - Neutral/Positive sentiment
('Cellular', 'I want to add a new line to my existing plan for my teenager. Please let me know the process and any additional costs involved.', 0.10, 0.50),
('Cellular', 'I am interested in upgrading to a 5G plan. Can you explain the benefits and costs compared to my current LTE plan?', 0.30, 0.80),
('Cellular', 'I need to cancel my service as I am moving overseas permanently. What is the cancellation process and are there any early termination fees?', -0.20, 0.40),
-- Home Internet - Negative sentiment
('Home Internet', 'My internet connection keeps dropping every few hours. I work from home and this is causing significant disruptions to my productivity.', -0.95, -0.65),
('Home Internet', 'The internet speed I am receiving is much slower than what I am paying for. Speed tests show only 20% of the promised bandwidth.', -0.90, -0.60),
('Home Internet', 'There has been no internet service in my area for the past 8 hours. When will service be restored and will I receive a credit?', -0.95, -0.70),
('Home Internet', 'I received a bill that is $40 higher than usual with no explanation. Please review the charges and provide a detailed breakdown.', -0.80, -0.30),
-- Home Internet - Neutral/Positive sentiment
('Home Internet', 'I need technical support to set up my new router. The installation instructions are unclear and I cannot get online.', -0.30, 0.10),
('Home Internet', 'I want to upgrade to a faster internet plan. What options are available and what would be the monthly cost difference?', 0.20, 0.70),
('Home Internet', 'My WiFi signal is weak in parts of my house. Can you recommend solutions to improve coverage throughout my home?', -0.10, 0.30),
('Home Internet', 'I am moving to a new address next month. What is the process for transferring my internet service to the new location?', -0.20, 0.40),
-- Business Internet - Negative sentiment
('Business Internet', 'Our business internet has been unreliable for the past month with frequent outages affecting our operations. We need an immediate solution.', -0.95, -0.70),
('Business Internet', 'Our current internet speed cannot handle our video conferencing needs. We need an upgrade consultation to determine the right solution.', -0.50, -0.10),
-- Business Internet - Neutral/Positive sentiment
('Business Internet', 'We need to increase our bandwidth to support additional employees working remotely. What enterprise packages do you offer?', 0.20, 0.70),
('Business Internet', 'We require a dedicated support line for our business account. What premium support options are available?', 0.10, 0.50),
('Business Internet', 'We need a backup internet connection for redundancy. Can you provide information about failover solutions?', 0.20, 0.60),
('Business Internet', 'Our contract is up for renewal next quarter. We would like to discuss our options and negotiate better terms.', 0.00, 0.50);

-- Reference table: Customer names
CREATE OR REPLACE TABLE GENERATE.REF_CUSTOMER_NAMES (
    FIRST_NAME VARCHAR(60)
);

INSERT INTO GENERATE.REF_CUSTOMER_NAMES VALUES
('Jennifer'), ('Michael'), ('Sarah'), ('David'), ('Katherine'),
('James'), ('Lisa'), ('Robert'), ('Maria'), ('Christopher'),
('Amanda'), ('Matthew'), ('Jessica'), ('Andrew'), ('Ashley'),
('Daniel'), ('Emily'), ('Joshua'), ('Stephanie'), ('Brian'),
('Nicole'), ('Ryan'), ('Elizabeth'), ('Kevin'), ('Michelle'),
('Thomas'), ('Laura'), ('Jason'), ('Rebecca'), ('Justin');

-- Reference table: Last names
CREATE OR REPLACE TABLE GENERATE.REF_CUSTOMER_SURNAMES (
    LAST_NAME VARCHAR(60)
);

INSERT INTO GENERATE.REF_CUSTOMER_SURNAMES VALUES
('Smith'), ('Johnson'), ('Williams'), ('Brown'), ('Jones'),
('Garcia'), ('Miller'), ('Davis'), ('Rodriguez'), ('Martinez'),
('Hernandez'), ('Lopez'), ('Wilson'), ('Anderson'), ('Thomas'),
('Taylor'), ('Moore'), ('Jackson'), ('Martin'), ('Lee');

-- Reference table: Email domains
CREATE OR REPLACE TABLE GENERATE.REF_EMAIL_DOMAINS (
    DOMAIN VARCHAR(60)
);

INSERT INTO GENERATE.REF_EMAIL_DOMAINS VALUES
('gmail.com'), ('yahoo.com'), ('hotmail.com'), ('outlook.com'),
('icloud.com'), ('aol.com'), ('protonmail.com');

SELECT 'Step 2 Complete: Reference tables created and populated' AS STATUS;
SELECT 'Cell Towers:', COUNT(*) AS COUNT FROM GENERATE.REF_CELL_TOWER_ATTRIBUTES
UNION ALL
SELECT 'Complaint Texts:', COUNT(*) FROM GENERATE.REF_COMPLAINT_TEXTS
UNION ALL
SELECT 'Customer Names:', COUNT(*) FROM GENERATE.REF_CUSTOMER_NAMES
UNION ALL
SELECT 'Surnames:', COUNT(*) FROM GENERATE.REF_CUSTOMER_SURNAMES
UNION ALL
SELECT 'Email Domains:', COUNT(*) FROM GENERATE.REF_EMAIL_DOMAINS;

-- ===============================================================================
-- STEP 3: CREATE STORED PROCEDURE FOR CELL TOWER DATA GENERATION
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
            -- Get the latest TIMESTAMP from production table, or use current hour if empty
            SELECT COALESCE(MAX(TIMESTAMP), DATEADD(MILLISECOND, 1, DATE_TRUNC('HOUR', CURRENT_TIMESTAMP())))
            INTO :latest_timestamp
            FROM TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER;
    
    -- New timestamp is 1 HOUR after the latest, with .001 milliseconds
    new_timestamp := DATEADD(HOUR, 1, :latest_timestamp);
    
    -- Generate one row for each cell ID
    INSERT INTO TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER (
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
        -- PM_PDCP_LAT_PKT_TRANS_DL: Large independent value (not derived from lat_dl)
        CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN UNIFORM(300000, 800000, RANDOM())
             ELSE UNIFORM(400000, 1200000, RANDOM()) END::DECIMAL(38,2),
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
        -- PM_PDCP_LAT_PKT_TRANS_UL: NULL to match production data
        NULL,
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
        -- PM_PRB_UTIL_UL: Heavily skewed toward zero (median 0, average ~8)
        CASE 
            WHEN UNIFORM(1, 100, RANDOM()) <= 50 THEN 0  -- 50% are zero
            WHEN UNIFORM(1, 100, RANDOM()) <= 80 THEN UNIFORM(1, 15, RANDOM())  -- 30% low values
            WHEN UNIFORM(1, 100, RANDOM()) <= 95 THEN UNIFORM(15, 40, RANDOM()) -- 15% medium
            ELSE UNIFORM(40, 72, RANDOM())  -- 5% high
        END::DECIMAL(38,2),
        MD5(TO_VARCHAR(CELL_ID) || TO_VARCHAR(ts_hour) || 'unique')
    FROM base_data;
    
    rows_inserted := SQLROWCOUNT;
    
    RETURN 'Generated ' || rows_inserted || ' cell tower records for hour: ' || TO_VARCHAR(:new_timestamp);
END;
$$;

SELECT 'Step 3 Complete: Cell Tower data generation procedure created (HOURLY)' AS STATUS;

-- ===============================================================================
-- STEP 4: CREATE STORED PROCEDURE FOR SUPPORT TICKET GENERATION
-- ===============================================================================

CREATE OR REPLACE PROCEDURE GENERATE.SP_GENERATE_SUPPORT_TICKET()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    ticket_count INT;
    next_ticket_id VARCHAR;
BEGIN
    -- Get the next ticket ID
    SELECT 'TR' || LPAD(TO_VARCHAR(COALESCE(MAX(CAST(SUBSTR(TICKET_ID, 3) AS INT)), 10000) + 1), 5, '0')
    INTO :next_ticket_id
    FROM TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS;
    
    -- If no tickets exist yet, start from TR10001
    IF (next_ticket_id IS NULL) THEN
        next_ticket_id := 'TR10001';
    END IF;
    
    -- Generate one support ticket
    INSERT INTO TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS (
        TICKET_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, SERVICE_TYPE, REQUEST,
        CONTACT_PREFERENCE, CELL_ID, SENTIMENT_SCORE
    )
    WITH random_names AS (
        SELECT 
            (SELECT FIRST_NAME FROM GENERATE.REF_CUSTOMER_NAMES ORDER BY RANDOM() LIMIT 1) AS FIRST_NAME,
            (SELECT LAST_NAME FROM GENERATE.REF_CUSTOMER_SURNAMES ORDER BY RANDOM() LIMIT 1) AS LAST_NAME
    ),
    service_type_sel AS (
        SELECT 
            CASE WHEN UNIFORM(1, 100, RANDOM()) <= 70 THEN 'Cellular'
                 WHEN UNIFORM(1, 100, RANDOM()) <= 85 THEN 'Home Internet'
                 ELSE 'Business Internet' END AS SERVICE_TYPE
    ),
    complaint_sel AS (
        SELECT 
            COMPLAINT_TEXT,
            SENTIMENT_MIN,
            SENTIMENT_MAX
        FROM GENERATE.REF_COMPLAINT_TEXTS
        WHERE SERVICE_TYPE = (SELECT SERVICE_TYPE FROM service_type_sel)
        ORDER BY RANDOM()
        LIMIT 1
    ),
    cell_sel AS (
        -- 70% of tickets should be for problematic towers
        SELECT CELL_ID
        FROM GENERATE.REF_CELL_TOWER_ATTRIBUTES
        WHERE CASE 
            WHEN UNIFORM(1, 100, RANDOM()) <= 70 THEN PERFORMANCE_TIER IN ('BAD', 'VERY_BAD', 'CATASTROPHIC')
            ELSE PERFORMANCE_TIER IN ('GOOD', 'PROBLEMATIC')
        END
        ORDER BY RANDOM()
        LIMIT 1
    )
    SELECT 
        :next_ticket_id,
        n.FIRST_NAME || ' ' || n.LAST_NAME,
        LOWER(n.FIRST_NAME || '.' || n.LAST_NAME || '@' || 
            (SELECT DOMAIN FROM GENERATE.REF_EMAIL_DOMAINS ORDER BY RANDOM() LIMIT 1)),
        st.SERVICE_TYPE,
        c.COMPLAINT_TEXT,
        CASE WHEN UNIFORM(1, 100, RANDOM()) <= 60 THEN 'Email'
             WHEN UNIFORM(1, 100, RANDOM()) <= 80 THEN 'Phone'
             ELSE 'Text Message' END,
        (SELECT CELL_ID FROM cell_sel),
        ROUND(c.SENTIMENT_MIN + (UNIFORM(0, 100, RANDOM()) * 0.01) * (c.SENTIMENT_MAX - c.SENTIMENT_MIN), 2)
    FROM random_names n
    CROSS JOIN service_type_sel st
    CROSS JOIN complaint_sel c;
    
    ticket_count := SQLROWCOUNT;
    
    RETURN 'Generated ' || ticket_count || ' support ticket: ' || :next_ticket_id;
END;
$$;

SELECT 'Step 4 Complete: Support Ticket generation procedure created' AS STATUS;

-- ===============================================================================
-- STEP 5: CREATE SNOWFLAKE TASKS
-- ===============================================================================

-- Task for Cell Tower data generation (SERVERLESS, runs every MINUTE, data increments by 1 HOUR)
CREATE OR REPLACE TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA
    SCHEDULE = '1 MINUTE'
AS
    CALL GENERATE.SP_GENERATE_CELL_TOWER_DATA();

-- Task for Support Ticket generation (SERVERLESS, runs every MINUTE)
CREATE OR REPLACE TASK GENERATE.TASK_GENERATE_SUPPORT_TICKET
    SCHEDULE = '1 MINUTE'
AS
    CALL GENERATE.SP_GENERATE_SUPPORT_TICKET();

SELECT 'Step 5 Complete: Serverless tasks created (run every MINUTE for demo streaming)' AS STATUS;

-- ===============================================================================
-- SETUP COMPLETE
-- ===============================================================================

SELECT '✅ DATA GENERATOR SETUP COMPLETE - DEMO STREAMING MODE!' AS STATUS;
SELECT '' AS BLANK_LINE;
SELECT 'Summary:' AS SECTION;
SELECT '- GENERATE schema created' AS ITEM
UNION ALL SELECT '- Reference tables populated with existing data patterns' AS ITEM
UNION ALL SELECT '- Cell tower generation procedure created (writes to RAW.CELL_TOWER)' AS ITEM
UNION ALL SELECT '- Support ticket generation procedure created (writes to RAW.SUPPORT_TICKETS)' AS ITEM
UNION ALL SELECT '- Two SERVERLESS tasks created (run every MINUTE, suspended by default)' AS ITEM;
SELECT '' AS BLANK_LINE;
SELECT 'Demo Mode Details:' AS SECTION;
SELECT '- Tasks run every 1 MINUTE' AS ITEM
UNION ALL SELECT '- Each execution generates data with timestamps incrementing by 1 HOUR' AS ITEM
UNION ALL SELECT '- Effect: 1 minute of real time = 1 hour of data time (fast-forward demo)' AS ITEM
UNION ALL SELECT '- ~14,000 cell tower records generated per minute' AS ITEM
UNION ALL SELECT '- 1 support ticket generated per minute' AS ITEM;
SELECT '' AS BLANK_LINE;
SELECT 'Next Steps:' AS SECTION;
SELECT '1. Review the setup' AS ITEM
UNION ALL SELECT '2. Use manage_data_generators.sql or START_DEMO.sql to start tasks' AS ITEM
UNION ALL SELECT '3. Monitor RAW.CELL_TOWER and RAW.SUPPORT_TICKETS tables' AS ITEM
UNION ALL SELECT '4. Use STOP_DEMO.sql to suspend tasks when done' AS ITEM;

