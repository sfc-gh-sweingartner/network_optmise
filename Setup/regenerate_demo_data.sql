-- ===============================================================================
-- TELCO NETWORK OPTIMIZATION - DEMO DATA REGENERATION SCRIPT
-- ===============================================================================
-- This script backs up current data and generates rich, varied demo data
-- that will make AI demonstrations compelling and meaningful
-- ===============================================================================

-- Set context
USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA RAW;

-- ===============================================================================
-- STEP 1: BACKUP CURRENT DATA
-- ===============================================================================

-- Create backup tables with timestamp
CREATE OR REPLACE TABLE CELL_TOWER_BACKUP AS 
SELECT *, CURRENT_TIMESTAMP() AS BACKUP_TIMESTAMP 
FROM CELL_TOWER;

CREATE OR REPLACE TABLE SUPPORT_TICKETS_BACKUP AS 
SELECT *, CURRENT_TIMESTAMP() AS BACKUP_TIMESTAMP 
FROM SUPPORT_TICKETS;

-- Verify backup counts
SELECT 'CELL_TOWER_BACKUP' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM CELL_TOWER_BACKUP
UNION ALL
SELECT 'SUPPORT_TICKETS_BACKUP' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM SUPPORT_TICKETS_BACKUP;

-- ===============================================================================
-- STEP 2: TRUNCATE EXISTING TABLES
-- ===============================================================================

TRUNCATE TABLE CELL_TOWER;
TRUNCATE TABLE SUPPORT_TICKETS;

-- ===============================================================================
-- STEP 3: GENERATE RICH CELL TOWER DATA
-- ===============================================================================

-- Insert varied cell tower data with realistic performance distributions
INSERT INTO CELL_TOWER (
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
WITH RECURSIVE 
-- Generate row numbers for our data
row_generator AS (
    SELECT 1 as rn
    UNION ALL
    SELECT rn + 1 FROM row_generator WHERE rn < 50000  -- Generate 50K diverse records
),

-- Create base data with geographic and vendor diversity
base_data AS (
    SELECT 
        rn,
        30000000 + rn as CELL_ID,
        CASE WHEN UNIFORM(1, 100, RANDOM(rn)) <= 85 THEN 0  -- 85% normal calls
             WHEN UNIFORM(1, 100, RANDOM(rn)) <= 95 THEN 9   -- 10% abnormal 
             ELSE 70 END as CALL_RELEASE_CODE,   -- 5% handover
        rn as LOOKUP_ID,
        
        -- Geographic diversity with realistic Canadian/US/UK coverage
        CASE 
            WHEN UNIFORM(1, 100, RANDOM(rn*2)) <= 40 THEN 'CANTS'    -- 40% Canadian
            WHEN UNIFORM(1, 100, RANDOM(rn*2)) <= 75 THEN 'USNYC'    -- 35% US
            WHEN UNIFORM(1, 100, RANDOM(rn*2)) <= 90 THEN 'GBRCL'    -- 15% UK  
            ELSE 'LOTRW' END as HOME_NETWORK_TAP_CODE,   -- 10% Other
            
        'CANTS' as SERVING_NETWORK_TAP_CODE,
        
        CASE 
            WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 302
            WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 310
            WHEN HOME_NETWORK_TAP_CODE = 'GBRCL' THEN 234
            ELSE 540 END as IMSI_PREFIX,
            
        40000000 + UNIFORM(1, 99999999, RANDOM(rn*3)) as IMEI_PREFIX,
        
        CASE 
            WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 'TELUS'
            WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 'VERIZON'
            WHEN HOME_NETWORK_TAP_CODE = 'GBRCL' THEN 'EE'
            ELSE 'ORANGE' END as HOME_NETWORK_NAME,
            
        CASE 
            WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 'CANADA'
            WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 'UNITED STATES'
            WHEN HOME_NETWORK_TAP_CODE = 'GBRCL' THEN 'UNITED KINGDOM'
            ELSE 'FRANCE' END as HOME_NETWORK_COUNTRY,
            
        44000 + UNIFORM(1, 999, RANDOM(rn*4)) as BID_SERVING_NETWORK,
        
        -- Regional descriptions with technology mix
        CASE 
            WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 
                CASE WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 30 THEN 'ALBERTA (LTE)'
                     WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 50 THEN 'ONTARIO (5G)'
                     WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 70 THEN 'BRITISH COLUMBIA'
                     WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 85 THEN 'QUEBEC (LTE)'
                     ELSE 'MARITIME PROVINCES' END
            WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN
                CASE WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 25 THEN 'NEW YORK (5G)'
                     WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 50 THEN 'CALIFORNIA (LTE)'
                     WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 75 THEN 'TEXAS'
                     ELSE 'FLORIDA (5G)' END
            WHEN HOME_NETWORK_TAP_CODE = 'GBRCL' THEN
                CASE WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 40 THEN 'LONDON (5G)'
                     WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 70 THEN 'MANCHESTER'
                     ELSE 'SCOTLAND' END
            ELSE 'PARIS (LTE)' END as BID_DESCRIPTION,
            
        -- Service mix
        CASE WHEN UNIFORM(1, 100, RANDOM(rn*6)) <= 60 THEN 'VOICE'
             WHEN UNIFORM(1, 100, RANDOM(rn*6)) <= 85 THEN 'GPRS'
             ELSE 'SMS' END as SERVICE_CATEGORY,
             
        CASE WHEN SERVICE_CATEGORY = 'VOICE' THEN 'MOBILE ORIGINATED CALL'
             WHEN SERVICE_CATEGORY = 'GPRS' THEN 'GPRS DATA SESSION'
             ELSE 'SMS DELIVERY' END as CALL_EVENT_DESCRIPTION,
             
        rn as ORIG_ID,
        DATEADD(DAY, -UNIFORM(1, 90, RANDOM(rn*7)), CURRENT_DATE()) as EVENT_DATE,
        
        200000000000 + UNIFORM(1, 999999999999, RANDOM(rn*8)) as IMSI_SUFFIX,
        300000 + UNIFORM(1, 999999, RANDOM(rn*9)) as IMEI_SUFFIX,
        
        -- Location area codes by region
        CASE 
            WHEN BID_DESCRIPTION LIKE '%ALBERTA%' THEN 11000 + UNIFORM(1, 200, RANDOM(rn*10))
            WHEN BID_DESCRIPTION LIKE '%ONTARIO%' THEN 12000 + UNIFORM(1, 300, RANDOM(rn*10))
            WHEN BID_DESCRIPTION LIKE '%BRITISH%' THEN 13000 + UNIFORM(1, 250, RANDOM(rn*10))
            WHEN BID_DESCRIPTION LIKE '%NEW YORK%' THEN 21000 + UNIFORM(1, 400, RANDOM(rn*10))
            WHEN BID_DESCRIPTION LIKE '%CALIFORNIA%' THEN 22000 + UNIFORM(1, 500, RANDOM(rn*10))
            WHEN BID_DESCRIPTION LIKE '%LONDON%' THEN 31000 + UNIFORM(1, 200, RANDOM(rn*10))
            ELSE 40000 + UNIFORM(1, 100, RANDOM(rn*10)) END as LOCATION_AREA_CODE,
            
        -- Charged units vary by service type
        CASE WHEN SERVICE_CATEGORY = 'VOICE' THEN UNIFORM(60, 1800, RANDOM(rn*11))  -- 1-30 minutes
             WHEN SERVICE_CATEGORY = 'GPRS' THEN UNIFORM(1000, 100000, RANDOM(rn*11))  -- 1KB-100MB
             ELSE 1 END as CHARGED_UNITS,  -- SMS = 1 unit
             
        9000000000 + UNIFORM(1, 999999999, RANDOM(rn*12)) as MSISDN,
        
        DATEADD(HOUR, -UNIFORM(1, 2160, RANDOM(rn*13)), CURRENT_TIMESTAMP()) as EVENT_DTTM,
        
        MD5(TO_VARCHAR(rn) || TO_VARCHAR(CURRENT_TIMESTAMP())) as CALL_ID,
        
        -- Varied cause codes for failures
        CASE WHEN CALL_RELEASE_CODE = 0 THEN 'NORMAL_CALL_CLEARING'
             WHEN CALL_RELEASE_CODE = 9 THEN 
                CASE WHEN UNIFORM(1, 100, RANDOM(rn*14)) <= 30 THEN 'NETWORK_CONGESTION'
                     WHEN UNIFORM(1, 100, RANDOM(rn*14)) <= 60 THEN 'CHANNEL_TYPE_NOT_IMPLEMENTED'  
                     WHEN UNIFORM(1, 100, RANDOM(rn*14)) <= 80 THEN 'CONNECTION_OUT_OF_SERVICE'
                     ELSE 'FACILITY_NOT_IMPLEMENTED' END
             ELSE 'HANDOVER_SUCCESSFUL' END as CAUSE_CODE_SHORT_DESCRIPTION,
             
        CASE WHEN CALL_RELEASE_CODE = 0 THEN 'Call completed successfully without any issues'
             WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'NETWORK_CONGESTION' THEN 'This cause indicates that the network is experiencing high traffic volume and cannot process additional calls at this time'
             WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'CHANNEL_TYPE_NOT_IMPLEMENTED' THEN 'The requested channel type is not implemented or supported by the network element'
             WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'CONNECTION_OUT_OF_SERVICE' THEN 'The connection has been lost due to equipment failure or maintenance activities'
             ELSE 'Call was successfully handed over to another cell tower for continued service' END as CAUSE_CODE_LONG_DESCRIPTION,
             
        -- Realistic coordinates by region  
        CASE 
            WHEN BID_DESCRIPTION LIKE '%ALBERTA%' THEN 53.0 + (UNIFORM(1, 400, RANDOM(rn*15)) / 100.0)  -- Alberta lat range
            WHEN BID_DESCRIPTION LIKE '%ONTARIO%' THEN 42.0 + (UNIFORM(1, 1000, RANDOM(rn*15)) / 100.0)  -- Ontario lat range
            WHEN BID_DESCRIPTION LIKE '%BRITISH%' THEN 49.0 + (UNIFORM(1, 600, RANDOM(rn*15)) / 100.0)  -- BC lat range
            WHEN BID_DESCRIPTION LIKE '%NEW YORK%' THEN 40.5 + (UNIFORM(1, 200, RANDOM(rn*15)) / 100.0)  -- NY lat range  
            WHEN BID_DESCRIPTION LIKE '%CALIFORNIA%' THEN 32.0 + (UNIFORM(1, 1000, RANDOM(rn*15)) / 100.0)  -- CA lat range
            WHEN BID_DESCRIPTION LIKE '%LONDON%' THEN 51.3 + (UNIFORM(1, 200, RANDOM(rn*15)) / 100.0)  -- London lat range
            ELSE 48.8 + (UNIFORM(1, 200, RANDOM(rn*15)) / 100.0) END as CELL_LATITUDE,  -- Paris lat range
            
        CASE 
            WHEN BID_DESCRIPTION LIKE '%ALBERTA%' THEN -110.0 - (UNIFORM(1, 400, RANDOM(rn*16)) / 100.0)  -- Alberta lng range
            WHEN BID_DESCRIPTION LIKE '%ONTARIO%' THEN -75.0 - (UNIFORM(1, 800, RANDOM(rn*16)) / 100.0)  -- Ontario lng range  
            WHEN BID_DESCRIPTION LIKE '%BRITISH%' THEN -120.0 - (UNIFORM(1, 800, RANDOM(rn*16)) / 100.0)  -- BC lng range
            WHEN BID_DESCRIPTION LIKE '%NEW YORK%' THEN -74.0 - (UNIFORM(1, 200, RANDOM(rn*16)) / 100.0)  -- NY lng range
            WHEN BID_DESCRIPTION LIKE '%CALIFORNIA%' THEN -114.0 - (UNIFORM(1, 1000, RANDOM(rn*16)) / 100.0)  -- CA lng range
            WHEN BID_DESCRIPTION LIKE '%LONDON%' THEN -0.5 + (UNIFORM(1, 100, RANDOM(rn*16)) / 100.0)  -- London lng range  
            ELSE 2.0 + (UNIFORM(1, 200, RANDOM(rn*16)) / 100.0) END as CELL_LONGITUDE,  -- Paris lng range
            
        'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || (150000 + rn) || 'L2100' as SENDER_NAME,
        
        -- Vendor diversity with realistic market share
        CASE WHEN UNIFORM(1, 100, RANDOM(rn*17)) <= 45 THEN 'ERICSSON'   -- 45% market share
             WHEN UNIFORM(1, 100, RANDOM(rn*17)) <= 75 THEN 'NOKIA'      -- 30% market share
             WHEN UNIFORM(1, 100, RANDOM(rn*17)) <= 90 THEN 'HUAWEI'     -- 15% market share
             ELSE 'SAMSUNG' END as VENDOR_NAME,     -- 10% market share
             
        'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || (150000 + rn) || 'L2100' as HOSTNAME,
        
        EVENT_DTTM as TIMESTAMP,
        
        -- Duration varies by service type
        CASE WHEN SERVICE_CATEGORY = 'VOICE' THEN UNIFORM(30, 1800, RANDOM(rn*18))  -- 30 sec to 30 min
             WHEN SERVICE_CATEGORY = 'GPRS' THEN UNIFORM(60, 7200, RANDOM(rn*18))   -- 1 min to 2 hours
             ELSE UNIFORM(1, 10, RANDOM(rn*18)) END as DURATION,  -- SMS very short
             
        1 as MANAGED_ELEMENT,
        
        -- Cell type distribution
        CASE WHEN UNIFORM(1, 100, RANDOM(rn*19)) <= 70 THEN 10   -- 70% Macro cells  
             ELSE 14 END as ENODEB_FUNCTION,  -- 30% Small cells
             
        DATEADD(MINUTE, -30, EVENT_DTTM) as WINDOW_START_AT,
        DATEADD(MINUTE, 30, EVENT_DTTM) as WINDOW_END_AT,
        
        'ManagedElement=1,ENodeBFunction=' || UNIFORM(1, 20, RANDOM(rn*20)) || 
        ',EUtranCellFDD=' || CELL_ID || ',UeMeasControl=1,PmUeMeasControl=1' as INDEX,
        
        1 as UE_MEAS_CONTROL,
        1 as PM_UE_MEAS_CONTROL
        
    FROM row_generator
    WHERE rn <= 50000
)

-- Now add the performance metrics with realistic distributions
SELECT 
    base_data.*,
    
    -- Active UE metrics - higher for busy cells, varies by cell type and region
    CASE WHEN ENODEB_FUNCTION = 10 THEN   -- Macro cells
        CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN UNIFORM(50, 120, RANDOM(rn*21))  -- 5G has higher capacity
             ELSE UNIFORM(30, 80, RANDOM(rn*21)) END  -- LTE 
    ELSE UNIFORM(10, 40, RANDOM(rn*21)) END::DECIMAL(38,2) as PM_ACTIVE_UE_DL_MAX,  -- Small cells
    
    CASE WHEN ENODEB_FUNCTION = 10 THEN UNIFORM(500000, 1500000, RANDOM(rn*22))
         ELSE UNIFORM(100000, 600000, RANDOM(rn*22)) END::DECIMAL(38,2) as PM_ACTIVE_UE_DL_SUM,
         
    CASE WHEN ENODEB_FUNCTION = 10 THEN UNIFORM(40, 100, RANDOM(rn*23))
         ELSE UNIFORM(15, 50, RANDOM(rn*23)) END::DECIMAL(38,2) as PM_ACTIVE_UE_UL_MAX,
         
    CASE WHEN ENODEB_FUNCTION = 10 THEN UNIFORM(400000, 1200000, RANDOM(rn*24))
         ELSE UNIFORM(80000, 500000, RANDOM(rn*24)) END::DECIMAL(38,2) as PM_ACTIVE_UE_UL_SUM,
         
    -- RRC Connection metrics - KEY FOR FAILURE RATE DEMOS
    CASE WHEN ENODEB_FUNCTION = 10 THEN UNIFORM(800, 1500, RANDOM(rn*25))
         ELSE UNIFORM(200, 600, RANDOM(rn*25)) END::DECIMAL(38,2) as PM_RRC_CONN_MAX,
    
    -- Latency - varies by technology and congestion (CRITICAL FOR DEMOS)
    CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN UNIFORM(2000000, 8000000, RANDOM(rn*26))   -- 5G lower latency  
         WHEN VENDOR_NAME = 'ERICSSON' THEN UNIFORM(3000000, 12000000, RANDOM(rn*26))       -- Ericsson performance
         WHEN VENDOR_NAME = 'NOKIA' THEN UNIFORM(3500000, 13000000, RANDOM(rn*26))          -- Nokia performance
         WHEN VENDOR_NAME = 'HUAWEI' THEN UNIFORM(2800000, 11000000, RANDOM(rn*26))         -- Huawei performance
         ELSE UNIFORM(4000000, 15000000, RANDOM(rn*26)) END::DECIMAL(38,2) as PM_PDCP_LAT_TIME_DL,   -- Samsung
         
    CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN UNIFORM(300000, 800000, RANDOM(rn*27))
         ELSE UNIFORM(400000, 1200000, RANDOM(rn*27)) END::DECIMAL(38,2) as PM_PDCP_LAT_PKT_TRANS_DL,
         
    -- Uplink latency strings (as per original schema)
    CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN TO_VARCHAR(UNIFORM(1500000, 6000000, RANDOM(rn*28)))
         ELSE TO_VARCHAR(UNIFORM(2000000, 9000000, RANDOM(rn*28))) END as PM_PDCP_LAT_TIME_UL,
         
    CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN TO_VARCHAR(UNIFORM(200000, 600000, RANDOM(rn*29)))
         ELSE TO_VARCHAR(UNIFORM(300000, 900000, RANDOM(rn*29))) END as PM_PDCP_LAT_PKT_TRANS_UL,
         
    -- Throughput time
    UNIFORM(400000, 1000000, RANDOM(rn*30))::DECIMAL(38,2) as PM_UE_THP_TIME_DL,
    
    -- Data volume - varies significantly by location and usage
    CASE WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
             UNIFORM(30000000, 80000000, RANDOM(rn*31))   -- Urban areas = higher data volume
         WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%SCOTLAND%' THEN
             UNIFORM(10000000, 40000000, RANDOM(rn*31))   -- Rural areas = lower data volume  
         ELSE UNIFORM(15000000, 60000000, RANDOM(rn*31)) END::DECIMAL(38,2) as PM_PDCP_VOL_DL_DRB,
         
    UNIFORM(800000, 2000000, RANDOM(rn*32))::DECIMAL(38,2) as PM_PDCP_VOL_DL_DRB_LAST_TTI,
    
    -- Signal quality metrics
    UNIFORM(-3, 3, RANDOM(rn*33)) as PM_UE_MEAS_RSRP_DELTA_INTRA_FREQ1,
    UNIFORM(-10, 10, RANDOM(rn*34)) as PM_UE_MEAS_RSRP_SERV_INTRA_FREQ1, 
    UNIFORM(-2, 2, RANDOM(rn*35)) as PM_UE_MEAS_RSRQ_DELTA_INTRA_FREQ1,
    UNIFORM(-8, 8, RANDOM(rn*36)) as PM_UE_MEAS_RSRQ_SERV_INTRA_FREQ1,
    
    -- E-RAB Release metrics - CRITICAL FOR ABNORMAL RELEASE RATE DEMOS
    -- Create realistic failure patterns - some cells have issues, others don't
    CASE WHEN UNIFORM(1, 100, RANDOM(rn*37)) <= 80 THEN UNIFORM(5, 40, RANDOM(rn*37))      -- 80% healthy cells
         WHEN UNIFORM(1, 100, RANDOM(rn*37)) <= 95 THEN UNIFORM(40, 90, RANDOM(rn*37))     -- 15% problematic cells
         ELSE UNIFORM(90, 150, RANDOM(rn*37)) END::DECIMAL(38,2) as PM_ERAB_REL_ABNORMAL_ENB_ACT,  -- 5% severely impacted
         
    CASE WHEN PM_ERAB_REL_ABNORMAL_ENB_ACT < 40 THEN UNIFORM(20, 60, RANDOM(rn*38))        -- Correlate with ACT metric
         WHEN PM_ERAB_REL_ABNORMAL_ENB_ACT < 90 THEN UNIFORM(60, 120, RANDOM(rn*38))
         ELSE UNIFORM(120, 200, RANDOM(rn*38)) END::DECIMAL(38,2) as PM_ERAB_REL_ABNORMAL_ENB,
         
    UNIFORM(500, 2000, RANDOM(rn*39))::DECIMAL(38,2) as PM_ERAB_REL_NORMAL_ENB,
    UNIFORM(50, 300, RANDOM(rn*40))::DECIMAL(38,2) as PM_ERAB_REL_MME,
    
    -- RRC Connection Establishment - CRITICAL FOR CONNECTION SUCCESS RATE DEMOS
    -- Create varied success rates from 60% to 99%
    UNIFORM(15000, 35000, RANDOM(rn*41))::DECIMAL(38,2) as PM_RRC_CONN_ESTAB_ATT,
    
    -- Success rate varies by region, vendor, and cell health
    CASE WHEN VENDOR_NAME = 'ERICSSON' AND BID_DESCRIPTION LIKE '%(5G)%' THEN 
             (PM_RRC_CONN_ESTAB_ATT * UNIFORM(92, 99, RANDOM(rn*42)) / 100)  -- Premium vendor + 5G = 92-99%
         WHEN VENDOR_NAME = 'ERICSSON' THEN 
             (PM_RRC_CONN_ESTAB_ATT * UNIFORM(88, 96, RANDOM(rn*42)) / 100)  -- Premium vendor = 88-96%
         WHEN VENDOR_NAME = 'NOKIA' THEN
             (PM_RRC_CONN_ESTAB_ATT * UNIFORM(85, 94, RANDOM(rn*42)) / 100)  -- Good vendor = 85-94%
         WHEN VENDOR_NAME = 'HUAWEI' THEN
             (PM_RRC_CONN_ESTAB_ATT * UNIFORM(82, 92, RANDOM(rn*42)) / 100)  -- Decent vendor = 82-92%
         ELSE 
             (PM_RRC_CONN_ESTAB_ATT * UNIFORM(75, 90, RANDOM(rn*42)) / 100)  -- Others = 75-90%
    END::DECIMAL(38,2) as PM_RRC_CONN_ESTAB_SUCC,
    
    UNIFORM(5000, 15000, RANDOM(rn*43))::DECIMAL(38,2) as PM_RRC_CONN_ESTAB_ATT_REATT,
    
    -- S1 Signaling - should correlate with RRC success
    UNIFORM(12000, 25000, RANDOM(rn*44))::DECIMAL(38,2) as PM_S1_SIG_CONN_ESTAB_ATT,
    (PM_S1_SIG_CONN_ESTAB_ATT * UNIFORM(85, 98, RANDOM(rn*45)) / 100)::DECIMAL(38,2) as PM_S1_SIG_CONN_ESTAB_SUCC,
    
    -- E-RAB Establishment - varies with network quality
    UNIFORM(18000, 32000, RANDOM(rn*46))::DECIMAL(38,2) as PM_ERAB_ESTAB_ATT_INIT,
    (PM_ERAB_ESTAB_ATT_INIT * UNIFORM(80, 96, RANDOM(rn*47)) / 100)::DECIMAL(38,2) as PM_ERAB_ESTAB_SUCC_INIT,
    
    -- PRB Utilization - CRITICAL FOR CAPACITY DEMOS
    -- Create realistic utilization patterns - some cells are congested, others aren't
    CASE WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
             UNIFORM(70, 100, RANDOM(rn*48))  -- Urban areas = high utilization
         WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%SCOTLAND%' THEN  
             UNIFORM(15, 60, RANDOM(rn*48))   -- Rural areas = low utilization
         ELSE UNIFORM(30, 85, RANDOM(rn*48)) END::DECIMAL(38,2) as PM_PRB_UTIL_DL,
         
    CASE WHEN PM_PRB_UTIL_DL > 80 THEN UNIFORM(60, 95, RANDOM(rn*49))     -- Correlate UL with DL
         WHEN PM_PRB_UTIL_DL > 50 THEN UNIFORM(25, 70, RANDOM(rn*49))
         ELSE UNIFORM(10, 45, RANDOM(rn*49)) END::DECIMAL(38,2) as PM_PRB_UTIL_UL,
         
    MD5(TO_VARCHAR(rn) || TO_VARCHAR(CELL_ID) || 'unique') as UNIQUE_ID
    
FROM base_data;

-- ===============================================================================
-- STEP 4: GENERATE RICH SUPPORT TICKETS DATA
-- ===============================================================================

-- Generate varied support tickets that correlate with cell tower issues
INSERT INTO SUPPORT_TICKETS (
    TICKET_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, SERVICE_TYPE, REQUEST, 
    CONTACT_PREFERENCE, CELL_ID, SENTIMENT_SCORE
)
WITH RECURSIVE
ticket_generator AS (
    SELECT 1 as tn
    UNION ALL  
    SELECT tn + 1 FROM ticket_generator WHERE tn < 10000  -- Generate 10K diverse tickets
),

-- Get cells with various performance levels for correlation
cell_performance AS (
    SELECT 
        CELL_ID,
        BID_DESCRIPTION,
        VENDOR_NAME,
        -- Calculate failure rates for correlation
        CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
            ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100)
            ELSE 0 END as RRC_FAILURE_RATE,
        PM_PRB_UTIL_DL,
        PM_ERAB_REL_ABNORMAL_ENB,
        CASE WHEN PM_PRB_UTIL_DL > 80 OR RRC_FAILURE_RATE > 15 OR PM_ERAB_REL_ABNORMAL_ENB > 80 
             THEN 'PROBLEMATIC' ELSE 'HEALTHY' END as CELL_HEALTH
    FROM CELL_TOWER
),

ticket_data AS (
    SELECT 
        tn,
        'TR' || LPAD(TO_VARCHAR(10000 + tn), 5, '0') as TICKET_ID,
        
        -- Diverse customer names
        CASE WHEN UNIFORM(1, 20, RANDOM(tn)) = 1 THEN 'Jennifer'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 2 THEN 'Michael'  
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 3 THEN 'Sarah'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 4 THEN 'David'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 5 THEN 'Katherine' 
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 6 THEN 'James'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 7 THEN 'Lisa'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 8 THEN 'Robert'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 9 THEN 'Maria'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 10 THEN 'Christopher'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 11 THEN 'Amanda'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 12 THEN 'Matthew'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 13 THEN 'Jessica'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 14 THEN 'Andrew'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 15 THEN 'Ashley'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 16 THEN 'Daniel'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 17 THEN 'Emily'
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 18 THEN 'Joshua'  
             WHEN UNIFORM(1, 20, RANDOM(tn)) = 19 THEN 'Stephanie'
             ELSE 'Brian' END as CUSTOMER_NAME,
             
        CUSTOMER_NAME || CASE WHEN UNIFORM(1, 10, RANDOM(tn*2)) <= 3 THEN '.smith'
                              WHEN UNIFORM(1, 10, RANDOM(tn*2)) <= 6 THEN '.johnson' 
                              WHEN UNIFORM(1, 10, RANDOM(tn*2)) <= 8 THEN '.williams'
                              ELSE '.brown' END || '@' ||
        CASE WHEN UNIFORM(1, 10, RANDOM(tn*3)) <= 3 THEN 'gmail.com'
             WHEN UNIFORM(1, 10, RANDOM(tn*3)) <= 6 THEN 'yahoo.com'
             WHEN UNIFORM(1, 10, RANDOM(tn*3)) <= 8 THEN 'hotmail.com'
             ELSE 'outlook.com' END as CUSTOMER_EMAIL,
             
        -- Service type distribution
        CASE WHEN UNIFORM(1, 100, RANDOM(tn*4)) <= 70 THEN 'Cellular'
             WHEN UNIFORM(1, 100, RANDOM(tn*4)) <= 85 THEN 'Home Internet'
             ELSE 'Business Internet' END as SERVICE_TYPE,
             
        -- Contact preference
        CASE WHEN UNIFORM(1, 100, RANDOM(tn*5)) <= 60 THEN 'Email'
             WHEN UNIFORM(1, 100, RANDOM(tn*5)) <= 80 THEN 'Phone'
             ELSE 'Text Message' END as CONTACT_PREFERENCE,
             
        -- Assign to random cell, but bias towards problematic cells for realistic correlation
        (SELECT CELL_ID FROM cell_performance 
         WHERE CASE WHEN UNIFORM(1, 100, RANDOM(tn*6)) <= 70 THEN CELL_HEALTH = 'PROBLEMATIC'
                    ELSE CELL_HEALTH = 'HEALTHY' END
         ORDER BY RANDOM() LIMIT 1) as CELL_ID
         
    FROM ticket_generator
    WHERE tn <= 10000
)

SELECT 
    TICKET_ID,
    CUSTOMER_NAME, 
    CUSTOMER_EMAIL,
    SERVICE_TYPE,
    
    -- Generate realistic requests based on service type and cell performance
    CASE WHEN SERVICE_TYPE = 'Cellular' THEN
        CASE WHEN UNIFORM(1, 10, RANDOM(tn*7)) = 1 THEN 
            'I am experiencing frequent call drops in my area. This has been happening for the past week and is affecting my work calls. Please investigate and resolve this issue.'
        WHEN UNIFORM(1, 10, RANDOM(tn*7)) = 2 THEN
            'My data connection is extremely slow, sometimes taking minutes to load basic websites. I have restarted my phone multiple times but the issue persists.'  
        WHEN UNIFORM(1, 10, RANDOM(tn*7)) = 3 THEN
            'I traveled internationally and received a $200 roaming charge that was not explained to me. Please review these charges and provide a refund if appropriate.'
        WHEN UNIFORM(1, 10, RANDOM(tn*7)) = 4 THEN
            'I cannot make outgoing calls from my location. The calls fail immediately with a busy signal. This started 3 days ago and is still ongoing.'
        WHEN UNIFORM(1, 10, RANDOM(tn*7)) = 5 THEN
            'Text messages are being delayed by several hours. I am missing important communications from family and work. Please fix this urgent issue.'
        WHEN UNIFORM(1, 10, RANDOM(tn*7)) = 6 THEN  
            'The network coverage in my neighborhood is poor. I only get 1-2 bars and calls often fail. When will you improve coverage in this area?'
        WHEN UNIFORM(1, 10, RANDOM(tn*7)) = 7 THEN
            'I want to add a new line to my existing plan for my teenager. Please let me know the process and any additional costs involved.'
        WHEN UNIFORM(1, 10, RANDOM(tn*7)) = 8 THEN
            'My monthly bill shows data overage charges but I have been connected to WiFi most of the month. Please review my usage and adjust the charges.'
        WHEN UNIFORM(1, 10, RANDOM(tn*7)) = 9 THEN
            'I need to cancel my service as I am moving overseas permanently. What is the cancellation process and are there any early termination fees?'
        ELSE
            'I am interested in upgrading to a 5G plan. Can you explain the benefits and costs compared to my current LTE plan?'
        END
    WHEN SERVICE_TYPE = 'Home Internet' THEN  
        CASE WHEN UNIFORM(1, 8, RANDOM(tn*8)) = 1 THEN
            'My internet connection keeps dropping every few hours. I work from home and this is causing significant disruptions to my productivity.'
        WHEN UNIFORM(1, 8, RANDOM(tn*8)) = 2 THEN
            'The internet speed I am receiving is much slower than what I am paying for. Speed tests show only 20% of the promised bandwidth.'
        WHEN UNIFORM(1, 8, RANDOM(tn*8)) = 3 THEN  
            'I need technical support to set up my new router. The installation instructions are unclear and I cannot get online.'
        WHEN UNIFORM(1, 8, RANDOM(tn*8)) = 4 THEN
            'There has been no internet service in my area for the past 8 hours. When will service be restored and will I receive a credit?'
        WHEN UNIFORM(1, 8, RANDOM(tn*8)) = 5 THEN
            'I want to upgrade to a faster internet plan. What options are available and what would be the monthly cost difference?'  
        WHEN UNIFORM(1, 8, RANDOM(tn*8)) = 6 THEN
            'My WiFi signal is weak in parts of my house. Can you recommend solutions to improve coverage throughout my home?'
        WHEN UNIFORM(1, 8, RANDOM(tn*8)) = 7 THEN
            'I received a bill that is $40 higher than usual with no explanation. Please review the charges and provide a detailed breakdown.'
        ELSE
            'I am moving to a new address next month. What is the process for transferring my internet service to the new location?'
        END
    ELSE -- Business Internet
        CASE WHEN UNIFORM(1, 6, RANDOM(tn*9)) = 1 THEN
            'Our business internet has been unreliable for the past month with frequent outages affecting our operations. We need an immediate solution.'
        WHEN UNIFORM(1, 6, RANDOM(tn*9)) = 2 THEN  
            'We need to increase our bandwidth to support additional employees working remotely. What enterprise packages do you offer?'
        WHEN UNIFORM(1, 6, RANDOM(tn*9)) = 3 THEN
            'Our current internet speed cannot handle our video conferencing needs. We need an upgrade consultation to determine the right solution.'
        WHEN UNIFORM(1, 6, RANDOM(tn*9)) = 4 THEN
            'We require a dedicated support line for our business account. What premium support options are available?'
        WHEN UNIFORM(1, 6, RANDOM(tn*9)) = 5 THEN
            'We need a backup internet connection for redundancy. Can you provide information about failover solutions?'
        ELSE  
            'Our contract is up for renewal next quarter. We would like to discuss our options and negotiate better terms.'
        END
    END as REQUEST,
    
    CONTACT_PREFERENCE,
    CELL_ID,
    
    -- Sentiment score correlates with issue type and cell performance
    CASE WHEN REQUEST LIKE '%frequent call drops%' OR REQUEST LIKE '%extremely slow%' OR REQUEST LIKE '%cannot make%' THEN 
             UNIFORM(-95, -60, RANDOM(tn*10)) / 100.0  -- Very negative for service issues
         WHEN REQUEST LIKE '%roaming charge%' OR REQUEST LIKE '%bill%' OR REQUEST LIKE '%charges%' THEN
             UNIFORM(-80, -30, RANDOM(tn*10)) / 100.0  -- Negative for billing issues  
         WHEN REQUEST LIKE '%upgrade%' OR REQUEST LIKE '%add%' OR REQUEST LIKE '%interested%' THEN
             UNIFORM(30, 80, RANDOM(tn*10)) / 100.0   -- Positive for sales inquiries
         WHEN REQUEST LIKE '%moving%' OR REQUEST LIKE '%cancel%' OR REQUEST LIKE '%transfer%' THEN  
             UNIFORM(-20, 40, RANDOM(tn*10)) / 100.0  -- Neutral for service changes
         ELSE UNIFORM(-40, 20, RANDOM(tn*10)) / 100.0 END as SENTIMENT_SCORE  -- Slightly negative for support requests
         
FROM ticket_data;

-- ===============================================================================
-- STEP 5: VERIFICATION QUERIES  
-- ===============================================================================

-- Verify data diversity and ranges
SELECT 
    'CELL_TOWER Data Verification' as CHECK_TYPE,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT VENDOR_NAME) as DISTINCT_VENDORS,
    COUNT(DISTINCT BID_DESCRIPTION) as DISTINCT_REGIONS, 
    COUNT(DISTINCT SERVICE_CATEGORY) as DISTINCT_SERVICES
FROM CELL_TOWER
UNION ALL
SELECT 
    'SUPPORT_TICKETS Data Verification' as CHECK_TYPE,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT SERVICE_TYPE) as DISTINCT_SERVICE_TYPES,
    COUNT(DISTINCT CONTACT_PREFERENCE) as DISTINCT_CONTACT_PREFS,
    NULL as FOURTH_METRIC
FROM SUPPORT_TICKETS;

-- Verify performance metric ranges for demo effectiveness  
SELECT
    'RRC Failure Rates' as METRIC,
    MIN(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
        ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
        ELSE NULL END) as MIN_VALUE,
    MAX(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
        ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
        ELSE NULL END) as MAX_VALUE,
    AVG(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
        ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
        ELSE NULL END) as AVG_VALUE
FROM CELL_TOWER
UNION ALL
SELECT
    'PRB Utilization DL' as METRIC, 
    MIN(PM_PRB_UTIL_DL) as MIN_VALUE,
    MAX(PM_PRB_UTIL_DL) as MAX_VALUE, 
    AVG(PM_PRB_UTIL_DL) as AVG_VALUE
FROM CELL_TOWER
UNION ALL  
SELECT
    'Abnormal E-RAB Release' as METRIC,
    MIN(PM_ERAB_REL_ABNORMAL_ENB) as MIN_VALUE,
    MAX(PM_ERAB_REL_ABNORMAL_ENB) as MAX_VALUE,
    AVG(PM_ERAB_REL_ABNORMAL_ENB) as AVG_VALUE  
FROM CELL_TOWER
UNION ALL
SELECT
    'Sentiment Scores' as METRIC,
    MIN(SENTIMENT_SCORE) as MIN_VALUE,
    MAX(SENTIMENT_SCORE) as MAX_VALUE,
    AVG(SENTIMENT_SCORE) as AVG_VALUE
FROM SUPPORT_TICKETS;

-- Sample the top 10 worst performing cells for demo validation
SELECT 
    'Top 10 Worst RRC Connection Failure Rates' as DEMO_QUERY,
    CELL_ID,
    BID_DESCRIPTION,
    VENDOR_NAME,
    PM_RRC_CONN_ESTAB_ATT as ATTEMPTS,
    PM_RRC_CONN_ESTAB_SUCC as SUCCESSES,
    ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE_PERCENT
FROM CELL_TOWER 
WHERE PM_RRC_CONN_ESTAB_ATT > 0
ORDER BY FAILURE_RATE_PERCENT DESC
LIMIT 10;

-- ===============================================================================
-- SCRIPT COMPLETE
-- ===============================================================================

SELECT 'Data regeneration completed successfully!' as STATUS,
       'Original data backed up to *_BACKUP tables' as BACKUP_STATUS,
       'Demo data now has rich variety for compelling AI demonstrations' as DEMO_READINESS;
