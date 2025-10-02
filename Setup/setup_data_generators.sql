-- ===============================================================================
-- DATA GENERATOR SETUP SCRIPT
-- ===============================================================================
-- This script creates:
-- 1. GENERATE schema with test tables
-- 2. Reference tables for data generation
-- 3. Stored procedures for generating cell tower and support ticket data
-- 4. Snowflake tasks that run every minute
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

-- Create test tables with same structure as production tables
CREATE OR REPLACE TABLE GENERATE.CELL_TOWER_TEST LIKE RAW.CELL_TOWER;
CREATE OR REPLACE TABLE GENERATE.SUPPORT_TICKETS_TEST LIKE RAW.SUPPORT_TICKETS;

SELECT 'Step 1 Complete: GENERATE schema and test tables created' AS STATUS;

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
    -- Get the latest timestamp from test table, or use current time if empty
    SELECT COALESCE(MAX(EVENT_DTTM), CURRENT_TIMESTAMP()) INTO :latest_timestamp
    FROM GENERATE.CELL_TOWER_TEST;
    
    -- New timestamp is 1 minute after the latest
    new_timestamp := DATEADD(MINUTE, 1, :latest_timestamp);
    
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
    SELECT 
        ref.CELL_ID,
        -- Call release code based on performance tier
        CASE WHEN UNIFORM(1, 100, RANDOM()) <= 85 THEN 0
             WHEN UNIFORM(1, 100, RANDOM()) <= 95 THEN 9
             ELSE 70 END,
        ref.CELL_ID,
        ref.HOME_NETWORK_TAP_CODE,
        'CANTS',
        CASE 
            WHEN ref.HOME_NETWORK_TAP_CODE = 'CANTS' THEN 302
            WHEN ref.HOME_NETWORK_TAP_CODE = 'USNYC' THEN 310
            WHEN ref.HOME_NETWORK_TAP_CODE = 'GBRCL' THEN 234
            ELSE 540 END,
        40000000 + UNIFORM(1, 99999999, RANDOM()),
        ref.HOME_NETWORK_NAME,
        ref.HOME_NETWORK_COUNTRY,
        44000 + UNIFORM(1, 999, RANDOM()),
        ref.BID_DESCRIPTION,
        -- Service mix
        CASE WHEN UNIFORM(1, 100, RANDOM()) <= 60 THEN 'VOICE'
             WHEN UNIFORM(1, 100, RANDOM()) <= 85 THEN 'GPRS'
             ELSE 'SMS' END,
        CASE WHEN SERVICE_CATEGORY = 'VOICE' THEN 'MOBILE ORIGINATED CALL'
             WHEN SERVICE_CATEGORY = 'GPRS' THEN 'GPRS DATA SESSION'
             ELSE 'SMS DELIVERY' END,
        ref.CELL_ID,
        DATE(:new_timestamp),
        200000000000 + UNIFORM(1, 999999999999, RANDOM()),
        300000 + UNIFORM(1, 999999, RANDOM()),
        ref.LOCATION_AREA_CODE,
        CASE WHEN SERVICE_CATEGORY = 'VOICE' THEN UNIFORM(60, 1800, RANDOM())
             WHEN SERVICE_CATEGORY = 'GPRS' THEN UNIFORM(1000, 100000, RANDOM())
             ELSE 1 END,
        9000000000 + UNIFORM(1, 999999999, RANDOM()),
        :new_timestamp,
        MD5(TO_VARCHAR(ref.CELL_ID) || TO_VARCHAR(:new_timestamp)),
        -- Cause codes
        CASE WHEN CALL_RELEASE_CODE = 0 THEN 'NORMAL_CALL_CLEARING'
             WHEN CALL_RELEASE_CODE = 9 THEN 
                CASE WHEN UNIFORM(1, 100, RANDOM()) <= 30 THEN 'NETWORK_CONGESTION'
                     WHEN UNIFORM(1, 100, RANDOM()) <= 60 THEN 'CHANNEL_TYPE_NOT_IMPLEMENTED'
                     WHEN UNIFORM(1, 100, RANDOM()) <= 80 THEN 'CONNECTION_OUT_OF_SERVICE'
                     ELSE 'FACILITY_NOT_IMPLEMENTED' END
             ELSE 'HANDOVER_SUCCESSFUL' END,
        CASE WHEN CALL_RELEASE_CODE = 0 THEN 'Call completed successfully without any issues'
             WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'NETWORK_CONGESTION' THEN 'This cause indicates that the network is experiencing high traffic volume and cannot process additional calls at this time'
             WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'CHANNEL_TYPE_NOT_IMPLEMENTED' THEN 'The requested channel type is not implemented or supported by the network element'
             WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'CONNECTION_OUT_OF_SERVICE' THEN 'The connection has been lost due to equipment failure or maintenance activities'
             ELSE 'Call was successfully handed over to another cell tower for continued service' END,
        ref.CELL_LATITUDE,
        ref.CELL_LONGITUDE,
        'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || ref.CELL_ID || 'L2100',
        ref.VENDOR_NAME,
        'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || ref.CELL_ID || 'L2100',
        :new_timestamp,
        CASE WHEN SERVICE_CATEGORY = 'VOICE' THEN UNIFORM(30, 1800, RANDOM())
             WHEN SERVICE_CATEGORY = 'GPRS' THEN UNIFORM(60, 7200, RANDOM())
             ELSE UNIFORM(1, 10, RANDOM()) END,
        1,
        ref.ENODEB_FUNCTION,
        DATEADD(MINUTE, -30, :new_timestamp),
        DATEADD(MINUTE, 30, :new_timestamp),
        'ManagedElement=1,ENodeBFunction=' || UNIFORM(1, 20, RANDOM()) || 
        ',EUtranCellFDD=' || ref.CELL_ID || ',UeMeasControl=1,PmUeMeasControl=1',
        1,
        1,
        -- Performance metrics based on performance tier
        CASE WHEN ref.ENODEB_FUNCTION = 10 THEN
            CASE WHEN ref.BID_DESCRIPTION LIKE '%(5G)%' THEN UNIFORM(50, 120, RANDOM())
                 ELSE UNIFORM(30, 80, RANDOM()) END
        ELSE UNIFORM(10, 40, RANDOM()) END::DECIMAL(38,2),
        CASE WHEN ref.ENODEB_FUNCTION = 10 THEN UNIFORM(500000, 1500000, RANDOM())
             ELSE UNIFORM(100000, 600000, RANDOM()) END::DECIMAL(38,2),
        CASE WHEN ref.ENODEB_FUNCTION = 10 THEN UNIFORM(40, 100, RANDOM())
             ELSE UNIFORM(15, 50, RANDOM()) END::DECIMAL(38,2),
        CASE WHEN ref.ENODEB_FUNCTION = 10 THEN UNIFORM(400000, 1200000, RANDOM())
             ELSE UNIFORM(80000, 500000, RANDOM()) END::DECIMAL(38,2),
        CASE WHEN ref.ENODEB_FUNCTION = 10 THEN UNIFORM(800, 1500, RANDOM())
             ELSE UNIFORM(200, 600, RANDOM()) END::DECIMAL(38,2),
        -- Latency based on performance tier
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN ROUND(40 + (UNIFORM(0, 15, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            WHEN 'VERY_BAD' THEN ROUND(30 + (UNIFORM(0, 12, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            WHEN 'BAD' THEN ROUND(22 + (UNIFORM(0, 10, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            WHEN 'QUITE_BAD' THEN ROUND(15 + (UNIFORM(0, 8, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            WHEN 'PROBLEMATIC' THEN ROUND(10 + (UNIFORM(0, 5, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
            ELSE 
                CASE WHEN ref.BID_DESCRIPTION LIKE '%(5G)%' THEN ROUND(8 + (UNIFORM(0, 4, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2)
                     ELSE ROUND(10 + (UNIFORM(0, 5, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01), 2) END
        END,
        ROUND(PM_PDCP_LAT_TIME_DL * (0.1 + (UNIFORM(0, 20, RANDOM()) * 0.01)), 2),
        -- Uplink latency as string
        TO_VARCHAR(
            ROUND(
                CASE ref.PERFORMANCE_TIER
                    WHEN 'CATASTROPHIC' THEN 80 + (UNIFORM(0, 30, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    WHEN 'VERY_BAD' THEN 60 + (UNIFORM(0, 25, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    WHEN 'BAD' THEN 45 + (UNIFORM(0, 20, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    WHEN 'QUITE_BAD' THEN 30 + (UNIFORM(0, 15, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    WHEN 'PROBLEMATIC' THEN 22 + (UNIFORM(0, 10, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                    ELSE 20 + (UNIFORM(0, 10, RANDOM())) + (UNIFORM(0, 99, RANDOM()) * 0.01)
                END
            , 2)
        ),
        TO_VARCHAR(ROUND(CAST(PM_PDCP_LAT_TIME_UL AS NUMBER) * (0.1 + (UNIFORM(0, 20, RANDOM()) * 0.01)), 2)),
        UNIFORM(400000, 1000000, RANDOM())::DECIMAL(38,2),
        -- Data volume
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 8000000 + (UNIFORM(0, 7000000, RANDOM()))
            WHEN 'VERY_BAD' THEN 12000000 + (UNIFORM(0, 10000000, RANDOM()))
            WHEN 'BAD' THEN 18000000 + (UNIFORM(0, 15000000, RANDOM()))
            WHEN 'QUITE_BAD' THEN 22000000 + (UNIFORM(0, 18000000, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 28000000 + (UNIFORM(0, 20000000, RANDOM()))
            ELSE 35000000 + (UNIFORM(0, 25000000, RANDOM()))
        END::DECIMAL(38,2),
        ROUND(PM_PDCP_VOL_DL_DRB * (0.03 + (UNIFORM(0, 30, RANDOM()) * 0.001)), 0),
        -- Signal quality
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 8 + (UNIFORM(0, 8, RANDOM()))
            WHEN 'VERY_BAD' THEN 6 + (UNIFORM(0, 7, RANDOM()))
            WHEN 'BAD' THEN 4 + (UNIFORM(0, 6, RANDOM()))
            WHEN 'QUITE_BAD' THEN 3 + (UNIFORM(0, 5, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 2 + (UNIFORM(0, 4, RANDOM()))
            ELSE 0 + (UNIFORM(0, 3, RANDOM()))
        END,
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN -105 - (UNIFORM(0, 10, RANDOM()))
            WHEN 'VERY_BAD' THEN -95 - (UNIFORM(0, 10, RANDOM()))
            WHEN 'BAD' THEN -85 - (UNIFORM(0, 10, RANDOM()))
            WHEN 'QUITE_BAD' THEN -75 - (UNIFORM(0, 10, RANDOM()))
            WHEN 'PROBLEMATIC' THEN -68 - (UNIFORM(0, 10, RANDOM()))
            ELSE -55 - (UNIFORM(0, 15, RANDOM()))
        END,
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 5 + (UNIFORM(0, 6, RANDOM()))
            WHEN 'VERY_BAD' THEN 4 + (UNIFORM(0, 5, RANDOM()))
            WHEN 'BAD' THEN 3 + (UNIFORM(0, 4, RANDOM()))
            WHEN 'QUITE_BAD' THEN 2 + (UNIFORM(0, 3, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 1 + (UNIFORM(0, 3, RANDOM()))
            ELSE 0 + (UNIFORM(0, 2, RANDOM()))
        END,
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN -18 - (UNIFORM(0, 5, RANDOM()))
            WHEN 'VERY_BAD' THEN -15 - (UNIFORM(0, 4, RANDOM()))
            WHEN 'BAD' THEN -12 - (UNIFORM(0, 4, RANDOM()))
            WHEN 'QUITE_BAD' THEN -9 - (UNIFORM(0, 3, RANDOM()))
            WHEN 'PROBLEMATIC' THEN -7 - (UNIFORM(0, 3, RANDOM()))
            ELSE -4 - (UNIFORM(0, 4, RANDOM()))
        END,
        -- E-RAB metrics
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN ROUND(20.0 + (UNIFORM(0, 500, RANDOM()) * 0.01), 2)
            WHEN 'VERY_BAD' THEN ROUND(15.0 + (UNIFORM(0, 500, RANDOM()) * 0.01), 2)
            WHEN 'BAD' THEN ROUND(10.0 + (UNIFORM(0, 500, RANDOM()) * 0.01), 2)
            WHEN 'QUITE_BAD' THEN ROUND(5.0 + (UNIFORM(0, 500, RANDOM()) * 0.01), 2)
            WHEN 'PROBLEMATIC' THEN ROUND(2.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            ELSE ROUND(0.1 + (UNIFORM(0, 190, RANDOM()) * 0.01), 2)
        END,
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN ROUND(22.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            WHEN 'VERY_BAD' THEN ROUND(17.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            WHEN 'BAD' THEN ROUND(12.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            WHEN 'QUITE_BAD' THEN ROUND(7.0 + (UNIFORM(0, 300, RANDOM()) * 0.01), 2)
            WHEN 'PROBLEMATIC' THEN ROUND(3.5 + (UNIFORM(0, 250, RANDOM()) * 0.01), 2)
            ELSE ROUND(0.5 + (UNIFORM(0, 200, RANDOM()) * 0.01), 2)
        END,
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 200 + (UNIFORM(0, 300, RANDOM()))
            WHEN 'VERY_BAD' THEN 400 + (UNIFORM(0, 600, RANDOM()))
            WHEN 'BAD' THEN 600 + (UNIFORM(0, 800, RANDOM()))
            WHEN 'QUITE_BAD' THEN 800 + (UNIFORM(0, 700, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 1000 + (UNIFORM(0, 800, RANDOM()))
            ELSE 1200 + (UNIFORM(0, 800, RANDOM()))
        END,
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 80 + (UNIFORM(0, 120, RANDOM()))
            WHEN 'VERY_BAD' THEN 60 + (UNIFORM(0, 80, RANDOM()))
            WHEN 'BAD' THEN 40 + (UNIFORM(0, 60, RANDOM()))
            ELSE 10 + (UNIFORM(0, 50, RANDOM()))
        END,
        -- RRC connection metrics
        ROUND(UNIFORM(15000, 35000, RANDOM()) * 
            CASE ref.PERFORMANCE_TIER
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
            CASE ref.PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN (0.55 + UNIFORM(0, 10, RANDOM()) * 0.01)
                WHEN 'VERY_BAD' THEN (0.65 + UNIFORM(0, 10, RANDOM()) * 0.01)
                WHEN 'BAD' THEN (0.75 + UNIFORM(0, 10, RANDOM()) * 0.01)
                WHEN 'QUITE_BAD' THEN (0.80 + UNIFORM(0, 10, RANDOM()) * 0.01)
                WHEN 'PROBLEMATIC' THEN (0.88 + UNIFORM(0, 8, RANDOM()) * 0.01)
                ELSE (0.93 + UNIFORM(0, 6, RANDOM()) * 0.01)
            END, 0)::DECIMAL(38,2),
        UNIFORM(18000, 32000, RANDOM())::DECIMAL(38,2),
        -- PRB utilization
        CASE ref.PERFORMANCE_TIER
            WHEN 'CATASTROPHIC' THEN 90 + (UNIFORM(0, 10, RANDOM()))
            WHEN 'VERY_BAD' THEN 75 + (UNIFORM(0, 20, RANDOM()))
            WHEN 'BAD' THEN 60 + (UNIFORM(0, 25, RANDOM()))
            WHEN 'QUITE_BAD' THEN 45 + (UNIFORM(0, 25, RANDOM()))
            WHEN 'PROBLEMATIC' THEN 30 + (UNIFORM(0, 25, RANDOM()))
            ELSE 
                CASE 
                    WHEN ref.BID_DESCRIPTION LIKE '%LONDON%' OR ref.BID_DESCRIPTION LIKE '%NEW YORK%' THEN 25 + (UNIFORM(0, 30, RANDOM()))
                    WHEN ref.BID_DESCRIPTION LIKE '%ALBERTA%' OR ref.BID_DESCRIPTION LIKE '%SCOTLAND%' THEN 5 + (UNIFORM(0, 20, RANDOM()))
                    ELSE 15 + (UNIFORM(0, 25, RANDOM()))
                END
        END::DECIMAL(38,2),
        ROUND(PM_PRB_UTIL_DL * (0.6 + (UNIFORM(0, 21, RANDOM()) * 0.01)), 0),
        MD5(TO_VARCHAR(ref.CELL_ID) || TO_VARCHAR(:new_timestamp) || 'unique')
    FROM GENERATE.REF_CELL_TOWER_ATTRIBUTES ref;
    
    rows_inserted := SQLROWCOUNT;
    
    RETURN 'Generated ' || rows_inserted || ' cell tower records for timestamp: ' || TO_VARCHAR(:new_timestamp);
END;
$$;

SELECT 'Step 3 Complete: Cell Tower data generation procedure created' AS STATUS;

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
    FROM GENERATE.SUPPORT_TICKETS_TEST;
    
    -- If no tickets exist yet, start from TR10001
    IF (next_ticket_id IS NULL) THEN
        next_ticket_id := 'TR10001';
    END IF;
    
    -- Generate one support ticket
    INSERT INTO GENERATE.SUPPORT_TICKETS_TEST (
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

-- Task for Cell Tower data generation (runs every minute)
CREATE OR REPLACE TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA
    WAREHOUSE = MYWH
    SCHEDULE = '1 MINUTE'
AS
    CALL GENERATE.SP_GENERATE_CELL_TOWER_DATA();

-- Task for Support Ticket generation (runs every minute)
CREATE OR REPLACE TASK GENERATE.TASK_GENERATE_SUPPORT_TICKET
    WAREHOUSE = MYWH
    SCHEDULE = '1 MINUTE'
AS
    CALL GENERATE.SP_GENERATE_SUPPORT_TICKET();

SELECT 'Step 5 Complete: Snowflake tasks created (currently suspended)' AS STATUS;

-- ===============================================================================
-- SETUP COMPLETE
-- ===============================================================================

SELECT '✅ DATA GENERATOR SETUP COMPLETE!' AS STATUS;
SELECT '' AS BLANK_LINE;
SELECT 'Summary:' AS SECTION;
SELECT '- GENERATE schema created' AS ITEM
UNION ALL SELECT '- Test tables created (CELL_TOWER_TEST, SUPPORT_TICKETS_TEST)' AS ITEM
UNION ALL SELECT '- Reference tables populated with existing data patterns' AS ITEM
UNION ALL SELECT '- Cell tower generation procedure created' AS ITEM
UNION ALL SELECT '- Support ticket generation procedure created' AS ITEM
UNION ALL SELECT '- Two Snowflake tasks created (suspended by default)' AS ITEM;
SELECT '' AS BLANK_LINE;
SELECT 'Next Steps:' AS SECTION;
SELECT '1. Review the setup' AS ITEM
UNION ALL SELECT '2. Use manage_data_generators.sql to start/stop/monitor tasks' AS ITEM
UNION ALL SELECT '3. Monitor CELL_TOWER_TEST and SUPPORT_TICKETS_TEST tables' AS ITEM
UNION ALL SELECT '4. When satisfied, you can modify procedures to write to RAW schema' AS ITEM;

