#!/usr/bin/env python3
"""
Telco Network Optimization - Demo Data Regeneration Script (Fixed Version)
Generates rich, varied demo data for compelling AI demonstrations
"""

import snowflake.connector
import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Create Snowflake connection using private key authentication"""
    try:
        # Read private key
        private_key_path = '/Users/sweingartner/.ssh/rsa_key.p8'
        private_key_passphrase = 'cLbz!g3hmZGa!Jan'
        
        with open(private_key_path, 'rb') as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=private_key_passphrase.encode(),
                backend=default_backend()
            )
        
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        conn = snowflake.connector.connect(
            user='STEPHEN_PYTHON',
            account='rxb32947',
            private_key=pkb,
            warehouse='MYWH',
            database='TELCO_NETWORK_OPTIMIZATION_PROD',
            schema='RAW',
            role='ACCOUNTADMIN'
        )
        
        logger.info("Successfully connected to Snowflake")
        return conn
        
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        raise

def execute_sql(conn, sql, description=""):
    """Execute SQL and return results"""
    try:
        logger.info(f"Executing: {description}")
        cursor = conn.cursor()
        cursor.execute(sql)
        
        # Try to fetch results if it's a SELECT statement
        if sql.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            return results
        else:
            logger.info(f"SQL executed successfully: {description}")
            return None
            
    except Exception as e:
        logger.error(f"Error executing SQL {description}: {str(e)}")
        raise
    finally:
        cursor.close()

def main():
    """Main execution function"""
    logger.info("Starting Telco Network Optimization Demo Data Regeneration (Fixed Version)")
    
    conn = None
    try:
        # Connect to Snowflake
        conn = get_snowflake_connection()
        
        # Check if backup tables exist
        check_backup_sql = "SELECT COUNT(*) FROM CELL_TOWER_BACKUP"
        backup_count = execute_sql(conn, check_backup_sql, "Checking backup tables")
        logger.info(f"✅ Backup tables confirmed - {backup_count[0][0]:,} records in CELL_TOWER_BACKUP")
        
        # Generate varied cell tower data using a simpler approach
        logger.info("=== GENERATING RICH CELL TOWER DATA (10,000 diverse records) ===")
        
        # Part 1: Generate base diverse data
        base_data_sql = """
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
        row_generator AS (
            SELECT 1 as rn
            UNION ALL
            SELECT rn + 1 FROM row_generator WHERE rn < 10000
        ),
        random_seeds AS (
            SELECT 
                rn,
                ABS(HASH(rn)) % 100 as rand1,
                ABS(HASH(rn*2)) % 100 as rand2,
                ABS(HASH(rn*3)) % 100 as rand3,
                ABS(HASH(rn*4)) % 100 as rand4,
                ABS(HASH(rn*5)) % 100 as rand5,
                ABS(HASH(rn*6)) % 100 as rand6,
                ABS(HASH(rn*7)) % 100 as rand7,
                ABS(HASH(rn*8)) % 100 as rand8,
                ABS(HASH(rn*9)) % 100 as rand9,
                ABS(HASH(rn*10)) % 100 as rand10
            FROM row_generator
        ),
        base_data AS (
            SELECT 
                rs.rn,
                30000000 + rs.rn as CELL_ID,
                CASE WHEN rs.rand1 <= 85 THEN 0 ELSE 9 END as CALL_RELEASE_CODE,
                rs.rn as LOOKUP_ID,
                CASE WHEN rs.rand2 <= 40 THEN 'CANTS'
                     WHEN rs.rand2 <= 75 THEN 'USNYC'
                     ELSE 'GBRCL' END as HOME_NETWORK_TAP_CODE,
                'CANTS' as SERVING_NETWORK_TAP_CODE,
                CASE WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 302
                     WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 310
                     ELSE 234 END as IMSI_PREFIX,
                40000000 + (ABS(HASH(rs.rn*11)) % 99999999) as IMEI_PREFIX,
                CASE WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 'TELUS'
                     WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 'VERIZON'
                     ELSE 'EE' END as HOME_NETWORK_NAME,
                CASE WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 'CANADA'
                     WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 'UNITED STATES'
                     ELSE 'UNITED KINGDOM' END as HOME_NETWORK_COUNTRY,
                44000 + (ABS(HASH(rs.rn*12)) % 999) as BID_SERVING_NETWORK,
                CASE WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 
                    CASE WHEN rs.rand3 <= 30 THEN 'ALBERTA (LTE)'
                         WHEN rs.rand3 <= 60 THEN 'ONTARIO (5G)'
                         WHEN rs.rand3 <= 85 THEN 'BRITISH COLUMBIA'
                         ELSE 'QUEBEC (LTE)' END
                     WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN
                    CASE WHEN rs.rand3 <= 50 THEN 'NEW YORK (5G)'
                         WHEN rs.rand3 <= 75 THEN 'CALIFORNIA (LTE)'
                         ELSE 'TEXAS' END
                     ELSE 
                    CASE WHEN rs.rand3 <= 50 THEN 'LONDON (5G)'
                         ELSE 'MANCHESTER' END END as BID_DESCRIPTION,
                CASE WHEN rs.rand4 <= 60 THEN 'VOICE'
                     WHEN rs.rand4 <= 85 THEN 'GPRS'
                     ELSE 'SMS' END as SERVICE_CATEGORY,
                CASE WHEN SERVICE_CATEGORY = 'VOICE' THEN 'MOBILE ORIGINATED CALL'
                     WHEN SERVICE_CATEGORY = 'GPRS' THEN 'GPRS DATA SESSION'
                     ELSE 'SMS DELIVERY' END as CALL_EVENT_DESCRIPTION,
                rs.rn as ORIG_ID,
                DATEADD(DAY, -((ABS(HASH(rs.rn*13)) % 90) + 1), CURRENT_DATE()) as EVENT_DATE,
                200000000000 + (ABS(HASH(rs.rn*14)) % 999999999999) as IMSI_SUFFIX,
                300000 + (ABS(HASH(rs.rn*15)) % 999999) as IMEI_SUFFIX,
                CASE WHEN BID_DESCRIPTION LIKE '%ALBERTA%' THEN 11000 + (ABS(HASH(rs.rn*16)) % 200)
                     WHEN BID_DESCRIPTION LIKE '%ONTARIO%' THEN 12000 + (ABS(HASH(rs.rn*16)) % 300)
                     WHEN BID_DESCRIPTION LIKE '%BRITISH%' THEN 13000 + (ABS(HASH(rs.rn*16)) % 250)
                     WHEN BID_DESCRIPTION LIKE '%NEW YORK%' THEN 21000 + (ABS(HASH(rs.rn*16)) % 400)
                     WHEN BID_DESCRIPTION LIKE '%CALIFORNIA%' THEN 22000 + (ABS(HASH(rs.rn*16)) % 500)
                     ELSE 31000 + (ABS(HASH(rs.rn*16)) % 200) END as LOCATION_AREA_CODE,
                CASE WHEN SERVICE_CATEGORY = 'VOICE' THEN 60 + (ABS(HASH(rs.rn*17)) % 1740)
                     WHEN SERVICE_CATEGORY = 'GPRS' THEN 1000 + (ABS(HASH(rs.rn*17)) % 99000)
                     ELSE 1 END as CHARGED_UNITS,
                9000000000 + (ABS(HASH(rs.rn*18)) % 999999999) as MSISDN,
                DATEADD(HOUR, -((ABS(HASH(rs.rn*19)) % 2160) + 1), CURRENT_TIMESTAMP()) as EVENT_DTTM,
                MD5(TO_VARCHAR(rs.rn) || TO_VARCHAR(CURRENT_TIMESTAMP())) as CALL_ID,
                CASE WHEN CALL_RELEASE_CODE = 0 THEN 'NORMAL_CALL_CLEARING'
                     ELSE CASE WHEN rs.rand5 <= 30 THEN 'NETWORK_CONGESTION'
                               WHEN rs.rand5 <= 60 THEN 'CHANNEL_TYPE_NOT_IMPLEMENTED'
                               WHEN rs.rand5 <= 80 THEN 'CONNECTION_OUT_OF_SERVICE'
                               ELSE 'FACILITY_NOT_IMPLEMENTED' END END as CAUSE_CODE_SHORT_DESCRIPTION,
                CASE WHEN CALL_RELEASE_CODE = 0 THEN 'Call completed successfully without any issues'
                     WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'NETWORK_CONGESTION' THEN 'Network is experiencing high traffic volume and cannot process additional calls'
                     WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'CHANNEL_TYPE_NOT_IMPLEMENTED' THEN 'The requested channel type is not implemented by the network element'
                     WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'CONNECTION_OUT_OF_SERVICE' THEN 'Connection lost due to equipment failure or maintenance activities'
                     ELSE 'Facility is not implemented or available in the current network configuration' END as CAUSE_CODE_LONG_DESCRIPTION,
                -- Geographic coordinates by region
                CASE WHEN BID_DESCRIPTION LIKE '%ALBERTA%' THEN 53.0 + ((ABS(HASH(rs.rn*20)) % 400) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%ONTARIO%' THEN 45.0 + ((ABS(HASH(rs.rn*20)) % 600) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%BRITISH%' THEN 49.0 + ((ABS(HASH(rs.rn*20)) % 600) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%NEW YORK%' THEN 40.5 + ((ABS(HASH(rs.rn*20)) % 200) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%CALIFORNIA%' THEN 34.0 + ((ABS(HASH(rs.rn*20)) % 800) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%TEXAS%' THEN 31.0 + ((ABS(HASH(rs.rn*20)) % 600) / 100.0)
                     ELSE 51.3 + ((ABS(HASH(rs.rn*20)) % 200) / 100.0) END as CELL_LATITUDE,
                CASE WHEN BID_DESCRIPTION LIKE '%ALBERTA%' THEN -110.0 - ((ABS(HASH(rs.rn*21)) % 400) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%ONTARIO%' THEN -79.0 - ((ABS(HASH(rs.rn*21)) % 400) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%BRITISH%' THEN -125.0 + ((ABS(HASH(rs.rn*21)) % 800) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%NEW YORK%' THEN -74.0 - ((ABS(HASH(rs.rn*21)) % 200) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%CALIFORNIA%' THEN -124.0 + ((ABS(HASH(rs.rn*21)) % 1000) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%TEXAS%' THEN -106.0 + ((ABS(HASH(rs.rn*21)) % 800) / 100.0)
                     ELSE -0.5 + ((ABS(HASH(rs.rn*21)) % 100) / 100.0) END as CELL_LONGITUDE,
                'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || (150000 + rs.rn) || 'L2100' as SENDER_NAME,
                CASE WHEN rs.rand6 <= 45 THEN 'ERICSSON'
                     WHEN rs.rand6 <= 75 THEN 'NOKIA' 
                     WHEN rs.rand6 <= 90 THEN 'HUAWEI'
                     ELSE 'SAMSUNG' END as VENDOR_NAME,
                'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || (150000 + rs.rn) || 'L2100' as HOSTNAME,
                EVENT_DTTM as TIMESTAMP,
                CASE WHEN SERVICE_CATEGORY = 'VOICE' THEN 30 + (ABS(HASH(rs.rn*22)) % 1770)
                     WHEN SERVICE_CATEGORY = 'GPRS' THEN 60 + (ABS(HASH(rs.rn*22)) % 7140)
                     ELSE 1 + (ABS(HASH(rs.rn*22)) % 9) END as DURATION,
                1 as MANAGED_ELEMENT,
                CASE WHEN rs.rand7 <= 70 THEN 10 ELSE 14 END as ENODEB_FUNCTION,
                DATEADD(MINUTE, -30, EVENT_DTTM) as WINDOW_START_AT,
                DATEADD(MINUTE, 30, EVENT_DTTM) as WINDOW_END_AT,
                'ManagedElement=1,ENodeBFunction=' || (1 + (ABS(HASH(rs.rn*23)) % 20)) || 
                ',EUtranCellFDD=' || CELL_ID || ',UeMeasControl=1,PmUeMeasControl=1' as INDEX,
                1 as UE_MEAS_CONTROL,
                1 as PM_UE_MEAS_CONTROL
            FROM random_seeds rs
            WHERE rs.rn <= 10000
        )
        SELECT 
            base_data.*,
            -- Performance metrics with realistic variations
            CASE WHEN ENODEB_FUNCTION = 10 AND BID_DESCRIPTION LIKE '%(5G)%' THEN 50 + (ABS(HASH(rn*24)) % 70)
                 WHEN ENODEB_FUNCTION = 10 THEN 30 + (ABS(HASH(rn*24)) % 50) 
                 ELSE 10 + (ABS(HASH(rn*24)) % 30) END::DECIMAL(38,2) as PM_ACTIVE_UE_DL_MAX,
            CASE WHEN ENODEB_FUNCTION = 10 THEN 500000 + (ABS(HASH(rn*25)) % 1000000)
                 ELSE 100000 + (ABS(HASH(rn*25)) % 500000) END::DECIMAL(38,2) as PM_ACTIVE_UE_DL_SUM,
            CASE WHEN ENODEB_FUNCTION = 10 THEN 25 + (ABS(HASH(rn*26)) % 45)
                 ELSE 15 + (ABS(HASH(rn*26)) % 35) END::DECIMAL(38,2) as PM_ACTIVE_UE_UL_MAX,
            CASE WHEN ENODEB_FUNCTION = 10 THEN 400000 + (ABS(HASH(rn*27)) % 800000)
                 ELSE 80000 + (ABS(HASH(rn*27)) % 420000) END::DECIMAL(38,2) as PM_ACTIVE_UE_UL_SUM,
            CASE WHEN ENODEB_FUNCTION = 10 THEN 800 + (ABS(HASH(rn*28)) % 700)
                 ELSE 200 + (ABS(HASH(rn*28)) % 400) END::DECIMAL(38,2) as PM_RRC_CONN_MAX,
            -- CRITICAL: Latency varies by vendor and technology  
            CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN 2000000 + (ABS(HASH(rn*29)) % 6000000)
                 WHEN VENDOR_NAME = 'ERICSSON' THEN 3000000 + (ABS(HASH(rn*29)) % 9000000)
                 WHEN VENDOR_NAME = 'NOKIA' THEN 3500000 + (ABS(HASH(rn*29)) % 9500000) 
                 WHEN VENDOR_NAME = 'HUAWEI' THEN 2800000 + (ABS(HASH(rn*29)) % 8200000)
                 ELSE 4000000 + (ABS(HASH(rn*29)) % 11000000) END::DECIMAL(38,2) as PM_PDCP_LAT_TIME_DL,
            400000 + (ABS(HASH(rn*30)) % 800000)::DECIMAL(38,2) as PM_PDCP_LAT_PKT_TRANS_DL,
            TO_VARCHAR(2000000 + (ABS(HASH(rn*31)) % 7000000)) as PM_PDCP_LAT_TIME_UL,
            TO_VARCHAR(300000 + (ABS(HASH(rn*32)) % 600000)) as PM_PDCP_LAT_PKT_TRANS_UL,
            400000 + (ABS(HASH(rn*33)) % 600000)::DECIMAL(38,2) as PM_UE_THP_TIME_DL,
            -- Data volumes vary by location (urban vs rural)
            CASE WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
                     30000000 + (ABS(HASH(rn*34)) % 50000000)
                 WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%MANCHESTER%' THEN
                     10000000 + (ABS(HASH(rn*34)) % 30000000)
                 ELSE 15000000 + (ABS(HASH(rn*34)) % 45000000) END::DECIMAL(38,2) as PM_PDCP_VOL_DL_DRB,
            800000 + (ABS(HASH(rn*35)) % 1200000)::DECIMAL(38,2) as PM_PDCP_VOL_DL_DRB_LAST_TTI,
            -3 + (ABS(HASH(rn*36)) % 6) as PM_UE_MEAS_RSRP_DELTA_INTRA_FREQ1,
            -10 + (ABS(HASH(rn*37)) % 20) as PM_UE_MEAS_RSRP_SERV_INTRA_FREQ1,
            -2 + (ABS(HASH(rn*38)) % 4) as PM_UE_MEAS_RSRQ_DELTA_INTRA_FREQ1, 
            -8 + (ABS(HASH(rn*39)) % 16) as PM_UE_MEAS_RSRQ_SERV_INTRA_FREQ1,
            -- CRITICAL: E-RAB abnormal releases - create clear problem cells
            CASE WHEN (ABS(HASH(rn*40)) % 100) <= 75 THEN 5 + (ABS(HASH(rn*40)) % 35)      -- 75% healthy cells: 5-40
                 WHEN (ABS(HASH(rn*40)) % 100) <= 90 THEN 40 + (ABS(HASH(rn*40)) % 50)     -- 15% problematic: 40-90
                 ELSE 90 + (ABS(HASH(rn*40)) % 60) END::DECIMAL(38,2) as PM_ERAB_REL_ABNORMAL_ENB_ACT, -- 10% severely impacted: 90-150
            CASE WHEN PM_ERAB_REL_ABNORMAL_ENB_ACT < 40 THEN 20 + (ABS(HASH(rn*41)) % 40)
                 WHEN PM_ERAB_REL_ABNORMAL_ENB_ACT < 90 THEN 60 + (ABS(HASH(rn*41)) % 60) 
                 ELSE 120 + (ABS(HASH(rn*41)) % 80) END::DECIMAL(38,2) as PM_ERAB_REL_ABNORMAL_ENB,
            500 + (ABS(HASH(rn*42)) % 1500)::DECIMAL(38,2) as PM_ERAB_REL_NORMAL_ENB,
            50 + (ABS(HASH(rn*43)) % 250)::DECIMAL(38,2) as PM_ERAB_REL_MME,
            -- CRITICAL: RRC Connection Success Rates - varies by vendor and technology
            15000 + (ABS(HASH(rn*44)) % 20000)::DECIMAL(38,2) as PM_RRC_CONN_ESTAB_ATT,
            CASE WHEN VENDOR_NAME = 'ERICSSON' AND BID_DESCRIPTION LIKE '%(5G)%' THEN 
                     ROUND(PM_RRC_CONN_ESTAB_ATT * (92 + (ABS(HASH(rn*45)) % 7)) / 100, 0)  -- 92-99%
                 WHEN VENDOR_NAME = 'ERICSSON' THEN 
                     ROUND(PM_RRC_CONN_ESTAB_ATT * (88 + (ABS(HASH(rn*45)) % 8)) / 100, 0)  -- 88-96%
                 WHEN VENDOR_NAME = 'NOKIA' THEN
                     ROUND(PM_RRC_CONN_ESTAB_ATT * (85 + (ABS(HASH(rn*45)) % 9)) / 100, 0)  -- 85-94%
                 WHEN VENDOR_NAME = 'HUAWEI' THEN
                     ROUND(PM_RRC_CONN_ESTAB_ATT * (82 + (ABS(HASH(rn*45)) % 10)) / 100, 0) -- 82-92%
                 ELSE 
                     ROUND(PM_RRC_CONN_ESTAB_ATT * (75 + (ABS(HASH(rn*45)) % 15)) / 100, 0) -- 75-90%
            END::DECIMAL(38,2) as PM_RRC_CONN_ESTAB_SUCC,
            5000 + (ABS(HASH(rn*46)) % 10000)::DECIMAL(38,2) as PM_RRC_CONN_ESTAB_ATT_REATT,
            12000 + (ABS(HASH(rn*47)) % 13000)::DECIMAL(38,2) as PM_S1_SIG_CONN_ESTAB_ATT,
            ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (85 + (ABS(HASH(rn*48)) % 13)) / 100, 0)::DECIMAL(38,2) as PM_S1_SIG_CONN_ESTAB_SUCC,
            18000 + (ABS(HASH(rn*49)) % 14000)::DECIMAL(38,2) as PM_ERAB_ESTAB_ATT_INIT,
            ROUND(PM_ERAB_ESTAB_ATT_INIT * (80 + (ABS(HASH(rn*50)) % 16)) / 100, 0)::DECIMAL(38,2) as PM_ERAB_ESTAB_SUCC_INIT,
            -- CRITICAL: PRB Utilization - urban vs rural differences
            CASE WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
                     70 + (ABS(HASH(rn*51)) % 30)  -- Urban: 70-100%
                 WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%MANCHESTER%' THEN  
                     15 + (ABS(HASH(rn*51)) % 45)  -- Rural: 15-60%
                 ELSE 30 + (ABS(HASH(rn*51)) % 55) END::DECIMAL(38,2) as PM_PRB_UTIL_DL, -- Suburban: 30-85%
            CASE WHEN PM_PRB_UTIL_DL > 80 THEN 60 + (ABS(HASH(rn*52)) % 35)     -- High DL = High UL
                 WHEN PM_PRB_UTIL_DL > 50 THEN 25 + (ABS(HASH(rn*52)) % 45)
                 ELSE 10 + (ABS(HASH(rn*52)) % 35) END::DECIMAL(38,2) as PM_PRB_UTIL_UL,
            MD5(TO_VARCHAR(rn) || TO_VARCHAR(CELL_ID) || 'unique') as UNIQUE_ID
        FROM base_data
        """
        
        execute_sql(conn, base_data_sql, "Generating 10,000 diverse cell tower records")
        
        # Generate support tickets
        logger.info("=== GENERATING RICH SUPPORT TICKETS DATA (2,000 diverse records) ===")
        
        tickets_sql = """
        INSERT INTO SUPPORT_TICKETS (
            TICKET_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, SERVICE_TYPE, REQUEST, 
            CONTACT_PREFERENCE, CELL_ID, SENTIMENT_SCORE
        )
        WITH RECURSIVE
        ticket_generator AS (
            SELECT 1 as tn
            UNION ALL  
            SELECT tn + 1 FROM ticket_generator WHERE tn < 2000
        ),
        cell_sample AS (
            SELECT CELL_ID, 
                   PM_PRB_UTIL_DL,
                   CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
                       ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100)
                       ELSE 0 END as RRC_FAILURE_RATE,
                   PM_ERAB_REL_ABNORMAL_ENB
            FROM CELL_TOWER 
        ),
        ticket_data AS (
            SELECT 
                tn,
                'TR' || LPAD(TO_VARCHAR(10000 + tn), 5, '0') as TICKET_ID,
                CASE WHEN (ABS(HASH(tn)) % 20) = 1 THEN 'Jennifer'
                     WHEN (ABS(HASH(tn)) % 20) = 2 THEN 'Michael'
                     WHEN (ABS(HASH(tn)) % 20) = 3 THEN 'Sarah'
                     WHEN (ABS(HASH(tn)) % 20) = 4 THEN 'David'
                     WHEN (ABS(HASH(tn)) % 20) = 5 THEN 'Katherine'
                     WHEN (ABS(HASH(tn)) % 20) = 6 THEN 'James'
                     WHEN (ABS(HASH(tn)) % 20) = 7 THEN 'Lisa'
                     WHEN (ABS(HASH(tn)) % 20) = 8 THEN 'Robert'
                     WHEN (ABS(HASH(tn)) % 20) = 9 THEN 'Maria'
                     WHEN (ABS(HASH(tn)) % 20) = 10 THEN 'Christopher'
                     WHEN (ABS(HASH(tn)) % 20) = 11 THEN 'Amanda'
                     WHEN (ABS(HASH(tn)) % 20) = 12 THEN 'Matthew'
                     WHEN (ABS(HASH(tn)) % 20) = 13 THEN 'Jessica'
                     WHEN (ABS(HASH(tn)) % 20) = 14 THEN 'Andrew'
                     WHEN (ABS(HASH(tn)) % 20) = 15 THEN 'Ashley'
                     WHEN (ABS(HASH(tn)) % 20) = 16 THEN 'Daniel'
                     WHEN (ABS(HASH(tn)) % 20) = 17 THEN 'Emily'
                     WHEN (ABS(HASH(tn)) % 20) = 18 THEN 'Joshua'
                     WHEN (ABS(HASH(tn)) % 20) = 19 THEN 'Stephanie'
                     ELSE 'Brian' END as CUSTOMER_NAME,
                CUSTOMER_NAME || CASE WHEN (ABS(HASH(tn*2)) % 10) <= 3 THEN '.smith'
                                      WHEN (ABS(HASH(tn*2)) % 10) <= 6 THEN '.johnson'
                                      WHEN (ABS(HASH(tn*2)) % 10) <= 8 THEN '.williams'
                                      ELSE '.brown' END || '@' ||
                CASE WHEN (ABS(HASH(tn*3)) % 10) <= 3 THEN 'gmail.com'
                     WHEN (ABS(HASH(tn*3)) % 10) <= 6 THEN 'yahoo.com'
                     WHEN (ABS(HASH(tn*3)) % 10) <= 8 THEN 'hotmail.com'
                     ELSE 'outlook.com' END as CUSTOMER_EMAIL,
                CASE WHEN (ABS(HASH(tn*4)) % 100) <= 70 THEN 'Cellular'
                     WHEN (ABS(HASH(tn*4)) % 100) <= 85 THEN 'Home Internet'
                     ELSE 'Business Internet' END as SERVICE_TYPE,
                CASE WHEN (ABS(HASH(tn*5)) % 100) <= 60 THEN 'Email'
                     WHEN (ABS(HASH(tn*5)) % 100) <= 80 THEN 'Phone'
                     ELSE 'Text Message' END as CONTACT_PREFERENCE,
                -- Bias towards problematic cells for realistic correlation
                (SELECT CELL_ID FROM cell_sample 
                 WHERE CASE WHEN (ABS(HASH(tn*6)) % 100) <= 60 THEN RRC_FAILURE_RATE > 8 OR PM_PRB_UTIL_DL > 75 OR PM_ERAB_REL_ABNORMAL_ENB > 60
                            ELSE RRC_FAILURE_RATE <= 8 AND PM_PRB_UTIL_DL <= 75 AND PM_ERAB_REL_ABNORMAL_ENB <= 60 END
                 ORDER BY RANDOM() LIMIT 1) as CELL_ID
            FROM ticket_generator
            WHERE tn <= 2000
        )
        SELECT 
            TICKET_ID,
            CUSTOMER_NAME, 
            CUSTOMER_EMAIL,
            SERVICE_TYPE,
            CASE WHEN SERVICE_TYPE = 'Cellular' THEN
                CASE WHEN (ABS(HASH(tn*7)) % 10) = 1 THEN 
                    'I am experiencing frequent call drops in my area. This has been happening for the past week and is affecting my work calls. Please investigate and resolve this issue.'
                WHEN (ABS(HASH(tn*7)) % 10) = 2 THEN
                    'My data connection is extremely slow, sometimes taking minutes to load basic websites. I have restarted my phone multiple times but the issue persists.'  
                WHEN (ABS(HASH(tn*7)) % 10) = 3 THEN
                    'I traveled internationally and received a $200 roaming charge that was not explained to me. Please review these charges and provide a refund if appropriate.'
                WHEN (ABS(HASH(tn*7)) % 10) = 4 THEN
                    'I cannot make outgoing calls from my location. The calls fail immediately with a busy signal. This started 3 days ago and is still ongoing.'
                WHEN (ABS(HASH(tn*7)) % 10) = 5 THEN
                    'Text messages are being delayed by several hours. I am missing important communications from family and work. Please fix this urgent issue.'
                WHEN (ABS(HASH(tn*7)) % 10) = 6 THEN  
                    'The network coverage in my neighborhood is poor. I only get 1-2 bars and calls often fail. When will you improve coverage in this area?'
                WHEN (ABS(HASH(tn*7)) % 10) = 7 THEN
                    'I want to add a new line to my existing plan for my teenager. Please let me know the process and any additional costs involved.'
                WHEN (ABS(HASH(tn*7)) % 10) = 8 THEN
                    'My monthly bill shows data overage charges but I have been connected to WiFi most of the month. Please review my usage and adjust the charges.'
                WHEN (ABS(HASH(tn*7)) % 10) = 9 THEN
                    'I need to cancel my service as I am moving overseas permanently. What is the cancellation process and are there any early termination fees?'
                ELSE
                    'I am interested in upgrading to a 5G plan. Can you explain the benefits and costs compared to my current LTE plan?'
                END
            WHEN SERVICE_TYPE = 'Home Internet' THEN  
                CASE WHEN (ABS(HASH(tn*8)) % 8) = 1 THEN
                    'My internet connection keeps dropping every few hours. I work from home and this is causing significant disruptions to my productivity.'
                WHEN (ABS(HASH(tn*8)) % 8) = 2 THEN
                    'The internet speed I am receiving is much slower than what I am paying for. Speed tests show only 20% of the promised bandwidth.'
                WHEN (ABS(HASH(tn*8)) % 8) = 3 THEN  
                    'I need technical support to set up my new router. The installation instructions are unclear and I cannot get online.'
                WHEN (ABS(HASH(tn*8)) % 8) = 4 THEN
                    'There has been no internet service in my area for the past 8 hours. When will service be restored and will I receive a credit?'
                WHEN (ABS(HASH(tn*8)) % 8) = 5 THEN
                    'I want to upgrade to a faster internet plan. What options are available and what would be the monthly cost difference?'  
                WHEN (ABS(HASH(tn*8)) % 8) = 6 THEN
                    'My WiFi signal is weak in parts of my house. Can you recommend solutions to improve coverage throughout my home?'
                WHEN (ABS(HASH(tn*8)) % 8) = 7 THEN
                    'I received a bill that is $40 higher than usual with no explanation. Please review the charges and provide a detailed breakdown.'
                ELSE
                    'I am moving to a new address next month. What is the process for transferring my internet service to the new location?'
                END
            ELSE -- Business Internet
                CASE WHEN (ABS(HASH(tn*9)) % 6) = 1 THEN
                    'Our business internet has been unreliable for the past month with frequent outages affecting our operations. We need an immediate solution.'
                WHEN (ABS(HASH(tn*9)) % 6) = 2 THEN  
                    'We need to increase our bandwidth to support additional employees working remotely. What enterprise packages do you offer?'
                WHEN (ABS(HASH(tn*9)) % 6) = 3 THEN
                    'Our current internet speed cannot handle our video conferencing needs. We need an upgrade consultation to determine the right solution.'
                WHEN (ABS(HASH(tn*9)) % 6) = 4 THEN
                    'We require a dedicated support line for our business account. What premium support options are available?'
                WHEN (ABS(HASH(tn*9)) % 6) = 5 THEN
                    'We need a backup internet connection for redundancy. Can you provide information about failover solutions?'
                ELSE  
                    'Our contract is up for renewal next quarter. We would like to discuss our options and negotiate better terms.'
                END
            END as REQUEST,
            CONTACT_PREFERENCE,
            CELL_ID,
            -- Sentiment correlates with issue severity
            CASE WHEN REQUEST LIKE '%frequent call drops%' OR REQUEST LIKE '%extremely slow%' OR REQUEST LIKE '%cannot make%' THEN 
                     (-95 + (ABS(HASH(tn*10)) % 35)) / 100.0  -- Very negative: -0.95 to -0.60
                 WHEN REQUEST LIKE '%roaming charge%' OR REQUEST LIKE '%bill%' OR REQUEST LIKE '%charges%' THEN
                     (-80 + (ABS(HASH(tn*10)) % 50)) / 100.0  -- Negative: -0.80 to -0.30
                 WHEN REQUEST LIKE '%upgrade%' OR REQUEST LIKE '%add%' OR REQUEST LIKE '%interested%' THEN
                     (30 + (ABS(HASH(tn*10)) % 50)) / 100.0   -- Positive: 0.30 to 0.80
                 WHEN REQUEST LIKE '%moving%' OR REQUEST LIKE '%cancel%' OR REQUEST LIKE '%transfer%' THEN  
                     (-20 + (ABS(HASH(tn*10)) % 60)) / 100.0  -- Neutral: -0.20 to 0.40
                 ELSE (-40 + (ABS(HASH(tn*10)) % 60)) / 100.0 END as SENTIMENT_SCORE  -- Slightly negative: -0.40 to 0.20
        FROM ticket_data
        """
        
        execute_sql(conn, tickets_sql, "Generating 2,000 diverse support tickets")
        
        # Final verification
        logger.info("=== FINAL VERIFICATION ===")
        
        verification_sql = """
        SELECT 
            'CELL_TOWER' as TABLE_NAME,
            COUNT(*) as TOTAL_RECORDS,
            COUNT(DISTINCT VENDOR_NAME) as DISTINCT_VENDORS,
            COUNT(DISTINCT BID_DESCRIPTION) as DISTINCT_REGIONS
        FROM CELL_TOWER
        UNION ALL
        SELECT 
            'SUPPORT_TICKETS' as TABLE_NAME,
            COUNT(*) as TOTAL_RECORDS,
            COUNT(DISTINCT SERVICE_TYPE) as DISTINCT_SERVICE_TYPES,
            COUNT(DISTINCT CONTACT_PREFERENCE) as DISTINCT_CONTACT_PREFS
        FROM SUPPORT_TICKETS
        """
        
        verification_results = execute_sql(conn, verification_sql, "Final verification")
        for table_name, total_records, distinct_col2, distinct_col3 in verification_results:
            logger.info(f"✅ {table_name}: {total_records:,} records generated")
        
        # Test key demo metrics
        demo_metrics_sql = """
        SELECT
            'RRC Failure Rate Range' as METRIC,
            ROUND(MIN(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
                ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
                ELSE NULL END), 2) as MIN_VALUE,
            ROUND(MAX(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
                ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
                ELSE NULL END), 2) as MAX_VALUE
        FROM CELL_TOWER
        UNION ALL
        SELECT
            'PRB Utilization DL Range' as METRIC, 
            ROUND(MIN(PM_PRB_UTIL_DL), 2) as MIN_VALUE,
            ROUND(MAX(PM_PRB_UTIL_DL), 2) as MAX_VALUE
        FROM CELL_TOWER
        UNION ALL  
        SELECT
            'Sentiment Score Range' as METRIC,
            ROUND(MIN(SENTIMENT_SCORE), 2) as MIN_VALUE,
            ROUND(MAX(SENTIMENT_SCORE), 2) as MAX_VALUE
        FROM SUPPORT_TICKETS
        """
        
        metrics_results = execute_sql(conn, demo_metrics_sql, "Demo metrics verification")
        
        logger.info("🎯 KEY DEMO METRICS RANGES:")
        for metric, min_val, max_val in metrics_results:
            logger.info(f"   {metric}: {min_val}% to {max_val}%")
        
        # Sample worst performers
        worst_performers_sql = """
        SELECT 
            CELL_ID,
            BID_DESCRIPTION,
            VENDOR_NAME,
            ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE_PERCENT
        FROM CELL_TOWER 
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        ORDER BY FAILURE_RATE_PERCENT DESC
        LIMIT 5
        """
        
        worst_results = execute_sql(conn, worst_performers_sql, "Top 5 worst performers")
        
        logger.info("🚨 TOP 5 WORST RRC CONNECTION FAILURE RATES (Perfect for demos!):")
        for cell_id, region, vendor, failure_rate in worst_results:
            logger.info(f"   Cell {cell_id} ({region}, {vendor}): {failure_rate}% failure rate")
        
        logger.info("🎉 DEMO DATA REGENERATION COMPLETED SUCCESSFULLY!")
        logger.info("📊 Your demo data now has rich variety across all key metrics!")
        logger.info("💾 Original data safely backed up in *_BACKUP tables")
        logger.info("🎯 Ready for compelling AI demonstrations!")
        
    except Exception as e:
        logger.error(f"Fatal error during data regeneration: {str(e)}")
        raise
        
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed")

if __name__ == "__main__":
    main()
