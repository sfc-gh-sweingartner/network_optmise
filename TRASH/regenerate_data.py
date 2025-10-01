#!/usr/bin/env python3
"""
Telco Network Optimization - Demo Data Regeneration Script
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
    logger.info("Starting Telco Network Optimization Demo Data Regeneration")
    
    conn = None
    try:
        # Connect to Snowflake
        conn = get_snowflake_connection()
        
        # Step 1: Backup existing data
        logger.info("=== STEP 1: BACKING UP EXISTING DATA ===")
        
        backup_sql = """
        CREATE OR REPLACE TABLE CELL_TOWER_BACKUP AS 
        SELECT *, CURRENT_TIMESTAMP() AS BACKUP_TIMESTAMP 
        FROM CELL_TOWER
        """
        execute_sql(conn, backup_sql, "Creating CELL_TOWER backup")
        
        backup_tickets_sql = """
        CREATE OR REPLACE TABLE SUPPORT_TICKETS_BACKUP AS 
        SELECT *, CURRENT_TIMESTAMP() AS BACKUP_TIMESTAMP 
        FROM SUPPORT_TICKETS
        """
        execute_sql(conn, backup_tickets_sql, "Creating SUPPORT_TICKETS backup")
        
        # Verify backup counts
        verify_backup_sql = """
        SELECT 'CELL_TOWER_BACKUP' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM CELL_TOWER_BACKUP
        UNION ALL
        SELECT 'SUPPORT_TICKETS_BACKUP' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM SUPPORT_TICKETS_BACKUP
        """
        backup_counts = execute_sql(conn, verify_backup_sql, "Verifying backup counts")
        
        for table_name, count in backup_counts:
            logger.info(f"Backed up {count:,} records from {table_name}")
        
        # Step 2: Truncate existing tables
        logger.info("=== STEP 2: TRUNCATING EXISTING TABLES ===")
        execute_sql(conn, "TRUNCATE TABLE CELL_TOWER", "Truncating CELL_TOWER")
        execute_sql(conn, "TRUNCATE TABLE SUPPORT_TICKETS", "Truncating SUPPORT_TICKETS")
        
        # Step 3: Generate varied cell tower data  
        logger.info("=== STEP 3: GENERATING RICH CELL TOWER DATA ===")
        
        # This is a simplified version - the full complex SQL from the original script
        cell_tower_generation_sql = """
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
        base_data AS (
            SELECT 
                rn,
                30000000 + rn as CELL_ID,
                CASE WHEN UNIFORM(1, 100, RANDOM()) <= 85 THEN 0 ELSE 9 END as CALL_RELEASE_CODE,
                rn as LOOKUP_ID,
                CASE WHEN UNIFORM(1, 100, RANDOM(rn*2)) <= 40 THEN 'CANTS'
                     WHEN UNIFORM(1, 100, RANDOM(rn*2)) <= 75 THEN 'USNYC'
                     ELSE 'GBRCL' END as HOME_NETWORK_TAP_CODE,
                'CANTS' as SERVING_NETWORK_TAP_CODE,
                CASE WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 302
                     WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 310
                     ELSE 234 END as IMSI_PREFIX,
                40000000 + UNIFORM(1, 99999999, RANDOM(rn*3)) as IMEI_PREFIX,
                CASE WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 'TELUS'
                     WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 'VERIZON'
                     ELSE 'EE' END as HOME_NETWORK_NAME,
                CASE WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 'CANADA'
                     WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN 'UNITED STATES'
                     ELSE 'UNITED KINGDOM' END as HOME_NETWORK_COUNTRY,
                44000 + UNIFORM(1, 999, RANDOM(rn*4)) as BID_SERVING_NETWORK,
                CASE WHEN HOME_NETWORK_TAP_CODE = 'CANTS' THEN 
                    CASE WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 30 THEN 'ALBERTA (LTE)'
                         WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 60 THEN 'ONTARIO (5G)'
                         ELSE 'BRITISH COLUMBIA' END
                     WHEN HOME_NETWORK_TAP_CODE = 'USNYC' THEN
                    CASE WHEN UNIFORM(1, 100, RANDOM(rn*5)) <= 50 THEN 'NEW YORK (5G)'
                         ELSE 'CALIFORNIA (LTE)' END
                     ELSE 'LONDON (5G)' END as BID_DESCRIPTION,
                CASE WHEN UNIFORM(1, 100, RANDOM(rn*6)) <= 60 THEN 'VOICE'
                     WHEN UNIFORM(1, 100, RANDOM(rn*6)) <= 85 THEN 'GPRS'
                     ELSE 'SMS' END as SERVICE_CATEGORY,
                'MOBILE ORIGINATED CALL' as CALL_EVENT_DESCRIPTION,
                rn as ORIG_ID,
                DATEADD(DAY, -UNIFORM(1, 90, RANDOM(rn*7)), CURRENT_DATE()) as EVENT_DATE,
                200000000000 + UNIFORM(1, 999999999999, RANDOM(rn*8)) as IMSI_SUFFIX,
                300000 + UNIFORM(1, 999999, RANDOM(rn*9)) as IMEI_SUFFIX,
                11000 + UNIFORM(1, 200, RANDOM(rn*10)) as LOCATION_AREA_CODE,
                UNIFORM(60, 1800, RANDOM(rn*11)) as CHARGED_UNITS,
                9000000000 + UNIFORM(1, 999999999, RANDOM(rn*12)) as MSISDN,
                DATEADD(HOUR, -UNIFORM(1, 2160, RANDOM(rn*13)), CURRENT_TIMESTAMP()) as EVENT_DTTM,
                MD5(TO_VARCHAR(rn) || TO_VARCHAR(CURRENT_TIMESTAMP())) as CALL_ID,
                'NORMAL_CALL_CLEARING' as CAUSE_CODE_SHORT_DESCRIPTION,
                'Call completed successfully without any issues' as CAUSE_CODE_LONG_DESCRIPTION,
                CASE WHEN BID_DESCRIPTION LIKE '%ALBERTA%' THEN 53.0 + (UNIFORM(1, 400, RANDOM(rn*15)) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%ONTARIO%' THEN 45.0 + (UNIFORM(1, 600, RANDOM(rn*15)) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%NEW YORK%' THEN 40.5 + (UNIFORM(1, 200, RANDOM(rn*15)) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%CALIFORNIA%' THEN 34.0 + (UNIFORM(1, 800, RANDOM(rn*15)) / 100.0)
                     ELSE 51.3 + (UNIFORM(1, 200, RANDOM(rn*15)) / 100.0) END as CELL_LATITUDE,
                CASE WHEN BID_DESCRIPTION LIKE '%ALBERTA%' THEN -110.0 - (UNIFORM(1, 400, RANDOM(rn*16)) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%ONTARIO%' THEN -79.0 - (UNIFORM(1, 400, RANDOM(rn*16)) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%NEW YORK%' THEN -74.0 - (UNIFORM(1, 200, RANDOM(rn*16)) / 100.0)
                     WHEN BID_DESCRIPTION LIKE '%CALIFORNIA%' THEN -120.0 - (UNIFORM(1, 600, RANDOM(rn*16)) / 100.0)
                     ELSE -0.1 + (UNIFORM(1, 100, RANDOM(rn*16)) / 100.0) END as CELL_LONGITUDE,
                'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || (150000 + rn) || 'L2100' as SENDER_NAME,
                CASE WHEN UNIFORM(1, 100, RANDOM(rn*17)) <= 45 THEN 'ERICSSON'
                     WHEN UNIFORM(1, 100, RANDOM(rn*17)) <= 75 THEN 'NOKIA'
                     WHEN UNIFORM(1, 100, RANDOM(rn*17)) <= 90 THEN 'HUAWEI'
                     ELSE 'SAMSUNG' END as VENDOR_NAME,
                'SubNetwork=ONRM_ROOT_MO_R,MeContext=' || (150000 + rn) || 'L2100' as HOSTNAME,
                EVENT_DTTM as TIMESTAMP,
                UNIFORM(30, 1800, RANDOM(rn*18)) as DURATION,
                1 as MANAGED_ELEMENT,
                CASE WHEN UNIFORM(1, 100, RANDOM(rn*19)) <= 70 THEN 10 ELSE 14 END as ENODEB_FUNCTION,
                DATEADD(MINUTE, -30, EVENT_DTTM) as WINDOW_START_AT,
                DATEADD(MINUTE, 30, EVENT_DTTM) as WINDOW_END_AT,
                'ManagedElement=1,ENodeBFunction=' || UNIFORM(1, 20, RANDOM(rn*20)) || 
                ',EUtranCellFDD=' || CELL_ID || ',UeMeasControl=1,PmUeMeasControl=1' as INDEX,
                1 as UE_MEAS_CONTROL,
                1 as PM_UE_MEAS_CONTROL
            FROM row_generator
            WHERE rn <= 10000
        )
        SELECT 
            base_data.*,
            CASE WHEN ENODEB_FUNCTION = 10 THEN UNIFORM(30, 80, RANDOM(rn*21))
                 ELSE UNIFORM(10, 40, RANDOM(rn*21)) END::DECIMAL(38,2) as PM_ACTIVE_UE_DL_MAX,
            UNIFORM(500000, 1500000, RANDOM(rn*22))::DECIMAL(38,2) as PM_ACTIVE_UE_DL_SUM,
            UNIFORM(25, 70, RANDOM(rn*23))::DECIMAL(38,2) as PM_ACTIVE_UE_UL_MAX,
            UNIFORM(400000, 1200000, RANDOM(rn*24))::DECIMAL(38,2) as PM_ACTIVE_UE_UL_SUM,
            UNIFORM(800, 1500, RANDOM(rn*25))::DECIMAL(38,2) as PM_RRC_CONN_MAX,
            CASE WHEN VENDOR_NAME = 'ERICSSON' THEN UNIFORM(3000000, 12000000, RANDOM(rn*26))
                 WHEN VENDOR_NAME = 'NOKIA' THEN UNIFORM(3500000, 13000000, RANDOM(rn*26))
                 WHEN VENDOR_NAME = 'HUAWEI' THEN UNIFORM(2800000, 11000000, RANDOM(rn*26))
                 ELSE UNIFORM(4000000, 15000000, RANDOM(rn*26)) END::DECIMAL(38,2) as PM_PDCP_LAT_TIME_DL,
            UNIFORM(400000, 1200000, RANDOM(rn*27))::DECIMAL(38,2) as PM_PDCP_LAT_PKT_TRANS_DL,
            TO_VARCHAR(UNIFORM(2000000, 9000000, RANDOM(rn*28))) as PM_PDCP_LAT_TIME_UL,
            TO_VARCHAR(UNIFORM(300000, 900000, RANDOM(rn*29))) as PM_PDCP_LAT_PKT_TRANS_UL,
            UNIFORM(400000, 1000000, RANDOM(rn*30))::DECIMAL(38,2) as PM_UE_THP_TIME_DL,
            CASE WHEN BID_DESCRIPTION LIKE '%NEW YORK%' OR BID_DESCRIPTION LIKE '%LONDON%' THEN 
                     UNIFORM(30000000, 80000000, RANDOM(rn*31))
                 ELSE UNIFORM(15000000, 60000000, RANDOM(rn*31)) END::DECIMAL(38,2) as PM_PDCP_VOL_DL_DRB,
            UNIFORM(800000, 2000000, RANDOM(rn*32))::DECIMAL(38,2) as PM_PDCP_VOL_DL_DRB_LAST_TTI,
            UNIFORM(-3, 3, RANDOM(rn*33)) as PM_UE_MEAS_RSRP_DELTA_INTRA_FREQ1,
            UNIFORM(-10, 10, RANDOM(rn*34)) as PM_UE_MEAS_RSRP_SERV_INTRA_FREQ1,
            UNIFORM(-2, 2, RANDOM(rn*35)) as PM_UE_MEAS_RSRQ_DELTA_INTRA_FREQ1,
            UNIFORM(-8, 8, RANDOM(rn*36)) as PM_UE_MEAS_RSRQ_SERV_INTRA_FREQ1,
            CASE WHEN UNIFORM(1, 100, RANDOM(rn*37)) <= 80 THEN UNIFORM(5, 40, RANDOM(rn*37))
                 WHEN UNIFORM(1, 100, RANDOM(rn*37)) <= 95 THEN UNIFORM(40, 90, RANDOM(rn*37))
                 ELSE UNIFORM(90, 150, RANDOM(rn*37)) END::DECIMAL(38,2) as PM_ERAB_REL_ABNORMAL_ENB_ACT,
            CASE WHEN PM_ERAB_REL_ABNORMAL_ENB_ACT < 40 THEN UNIFORM(20, 60, RANDOM(rn*38))
                 WHEN PM_ERAB_REL_ABNORMAL_ENB_ACT < 90 THEN UNIFORM(60, 120, RANDOM(rn*38))
                 ELSE UNIFORM(120, 200, RANDOM(rn*38)) END::DECIMAL(38,2) as PM_ERAB_REL_ABNORMAL_ENB,
            UNIFORM(500, 2000, RANDOM(rn*39))::DECIMAL(38,2) as PM_ERAB_REL_NORMAL_ENB,
            UNIFORM(50, 300, RANDOM(rn*40))::DECIMAL(38,2) as PM_ERAB_REL_MME,
            UNIFORM(15000, 35000, RANDOM(rn*41))::DECIMAL(38,2) as PM_RRC_CONN_ESTAB_ATT,
            CASE WHEN VENDOR_NAME = 'ERICSSON' THEN 
                     (PM_RRC_CONN_ESTAB_ATT * UNIFORM(88, 96, RANDOM(rn*42)) / 100)
                 WHEN VENDOR_NAME = 'NOKIA' THEN
                     (PM_RRC_CONN_ESTAB_ATT * UNIFORM(85, 94, RANDOM(rn*42)) / 100)
                 WHEN VENDOR_NAME = 'HUAWEI' THEN
                     (PM_RRC_CONN_ESTAB_ATT * UNIFORM(82, 92, RANDOM(rn*42)) / 100)
                 ELSE 
                     (PM_RRC_CONN_ESTAB_ATT * UNIFORM(75, 90, RANDOM(rn*42)) / 100)
            END::DECIMAL(38,2) as PM_RRC_CONN_ESTAB_SUCC,
            UNIFORM(5000, 15000, RANDOM(rn*43))::DECIMAL(38,2) as PM_RRC_CONN_ESTAB_ATT_REATT,
            UNIFORM(12000, 25000, RANDOM(rn*44))::DECIMAL(38,2) as PM_S1_SIG_CONN_ESTAB_ATT,
            (PM_S1_SIG_CONN_ESTAB_ATT * UNIFORM(85, 98, RANDOM(rn*45)) / 100)::DECIMAL(38,2) as PM_S1_SIG_CONN_ESTAB_SUCC,
            UNIFORM(18000, 32000, RANDOM(rn*46))::DECIMAL(38,2) as PM_ERAB_ESTAB_ATT_INIT,
            (PM_ERAB_ESTAB_ATT_INIT * UNIFORM(80, 96, RANDOM(rn*47)) / 100)::DECIMAL(38,2) as PM_ERAB_ESTAB_SUCC_INIT,
            CASE WHEN BID_DESCRIPTION LIKE '%NEW YORK%' OR BID_DESCRIPTION LIKE '%LONDON%' THEN 
                     UNIFORM(70, 100, RANDOM(rn*48))
                 ELSE UNIFORM(30, 85, RANDOM(rn*48)) END::DECIMAL(38,2) as PM_PRB_UTIL_DL,
            CASE WHEN PM_PRB_UTIL_DL > 80 THEN UNIFORM(60, 95, RANDOM(rn*49))
                 ELSE UNIFORM(10, 45, RANDOM(rn*49)) END::DECIMAL(38,2) as PM_PRB_UTIL_UL,
            MD5(TO_VARCHAR(rn) || TO_VARCHAR(CELL_ID) || 'unique') as UNIQUE_ID
        FROM base_data
        """
        
        execute_sql(conn, cell_tower_generation_sql, "Generating varied cell tower data")
        
        # Step 4: Generate varied support tickets
        logger.info("=== STEP 4: GENERATING RICH SUPPORT TICKETS DATA ===")
        
        tickets_generation_sql = """
        INSERT INTO SUPPORT_TICKETS (
            TICKET_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, SERVICE_TYPE, REQUEST, 
            CONTACT_PREFERENCE, CELL_ID, SENTIMENT_SCORE
        )
        WITH RECURSIVE
        ticket_generator AS (
            SELECT 1 as tn
            UNION ALL  
            SELECT tn + 1 FROM ticket_generator WHERE tn < 1000
        ),
        cell_sample AS (
            SELECT CELL_ID FROM CELL_TOWER SAMPLE(10) LIMIT 500
        ),
        ticket_data AS (
            SELECT 
                tn,
                'TR' || LPAD(TO_VARCHAR(10000 + tn), 5, '0') as TICKET_ID,
                CASE WHEN UNIFORM(1, 10, RANDOM(tn)) = 1 THEN 'Jennifer'
                     WHEN UNIFORM(1, 10, RANDOM(tn)) = 2 THEN 'Michael'
                     WHEN UNIFORM(1, 10, RANDOM(tn)) = 3 THEN 'Sarah'
                     WHEN UNIFORM(1, 10, RANDOM(tn)) = 4 THEN 'David'
                     WHEN UNIFORM(1, 10, RANDOM(tn)) = 5 THEN 'Katherine'
                     WHEN UNIFORM(1, 10, RANDOM(tn)) = 6 THEN 'James'
                     WHEN UNIFORM(1, 10, RANDOM(tn)) = 7 THEN 'Lisa'
                     WHEN UNIFORM(1, 10, RANDOM(tn)) = 8 THEN 'Robert'
                     WHEN UNIFORM(1, 10, RANDOM(tn)) = 9 THEN 'Maria'
                     ELSE 'Christopher' END as CUSTOMER_NAME,
                CUSTOMER_NAME || '.customer@' ||
                CASE WHEN UNIFORM(1, 4, RANDOM(tn*2)) = 1 THEN 'gmail.com'
                     WHEN UNIFORM(1, 4, RANDOM(tn*2)) = 2 THEN 'yahoo.com'
                     WHEN UNIFORM(1, 4, RANDOM(tn*2)) = 3 THEN 'hotmail.com'
                     ELSE 'outlook.com' END as CUSTOMER_EMAIL,
                CASE WHEN UNIFORM(1, 100, RANDOM(tn*3)) <= 70 THEN 'Cellular'
                     WHEN UNIFORM(1, 100, RANDOM(tn*3)) <= 85 THEN 'Home Internet'
                     ELSE 'Business Internet' END as SERVICE_TYPE,
                CASE WHEN UNIFORM(1, 100, RANDOM(tn*4)) <= 60 THEN 'Email'
                     WHEN UNIFORM(1, 100, RANDOM(tn*4)) <= 80 THEN 'Phone'
                     ELSE 'Text Message' END as CONTACT_PREFERENCE,
                (SELECT CELL_ID FROM cell_sample ORDER BY RANDOM() LIMIT 1) as CELL_ID
            FROM ticket_generator
            WHERE tn <= 1000
        )
        SELECT 
            TICKET_ID,
            CUSTOMER_NAME, 
            CUSTOMER_EMAIL,
            SERVICE_TYPE,
            CASE WHEN SERVICE_TYPE = 'Cellular' THEN
                CASE WHEN UNIFORM(1, 5, RANDOM(tn*5)) = 1 THEN 
                    'I am experiencing frequent call drops in my area. This has been happening for the past week and is affecting my work calls.'
                WHEN UNIFORM(1, 5, RANDOM(tn*5)) = 2 THEN
                    'My data connection is extremely slow, sometimes taking minutes to load basic websites.'
                WHEN UNIFORM(1, 5, RANDOM(tn*5)) = 3 THEN
                    'I traveled internationally and received a $200 roaming charge that was not explained to me.'
                WHEN UNIFORM(1, 5, RANDOM(tn*5)) = 4 THEN
                    'I cannot make outgoing calls from my location. The calls fail immediately with a busy signal.'
                ELSE
                    'The network coverage in my neighborhood is poor. I only get 1-2 bars and calls often fail.'
                END
            WHEN SERVICE_TYPE = 'Home Internet' THEN  
                CASE WHEN UNIFORM(1, 4, RANDOM(tn*6)) = 1 THEN
                    'My internet connection keeps dropping every few hours. I work from home and this is disruptive.'
                WHEN UNIFORM(1, 4, RANDOM(tn*6)) = 2 THEN
                    'The internet speed I am receiving is much slower than what I am paying for.'
                WHEN UNIFORM(1, 4, RANDOM(tn*6)) = 3 THEN  
                    'I need technical support to set up my new router. The installation instructions are unclear.'
                ELSE
                    'There has been no internet service in my area for the past 8 hours. When will service be restored?'
                END
            ELSE
                CASE WHEN UNIFORM(1, 3, RANDOM(tn*7)) = 1 THEN
                    'Our business internet has been unreliable with frequent outages affecting our operations.'
                WHEN UNIFORM(1, 3, RANDOM(tn*7)) = 2 THEN  
                    'We need to increase our bandwidth to support additional employees working remotely.'
                ELSE
                    'Our current internet speed cannot handle our video conferencing needs. We need an upgrade.'
                END
            END as REQUEST,
            CONTACT_PREFERENCE,
            CELL_ID,
            CASE WHEN REQUEST LIKE '%call drops%' OR REQUEST LIKE '%extremely slow%' THEN 
                     UNIFORM(-95, -60, RANDOM(tn*8)) / 100.0
                 WHEN REQUEST LIKE '%roaming charge%' OR REQUEST LIKE '%bill%' THEN
                     UNIFORM(-80, -30, RANDOM(tn*8)) / 100.0
                 WHEN REQUEST LIKE '%upgrade%' OR REQUEST LIKE '%increase%' THEN
                     UNIFORM(30, 80, RANDOM(tn*8)) / 100.0
                 ELSE UNIFORM(-40, 20, RANDOM(tn*8)) / 100.0 END as SENTIMENT_SCORE
        FROM ticket_data
        """
        
        execute_sql(conn, tickets_generation_sql, "Generating varied support tickets")
        
        # Step 5: Verification
        logger.info("=== STEP 5: VERIFYING DATA GENERATION ===")
        
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
        
        verification_results = execute_sql(conn, verification_sql, "Verifying data variety")
        
        for table_name, total_records, distinct_col2, distinct_col3 in verification_results:
            logger.info(f"✅ {table_name}: {total_records:,} records, {distinct_col2} distinct values in col2, {distinct_col3} distinct values in col3")
        
        # Test RRC failure rate variety
        rrc_test_sql = """
        SELECT
            'RRC Failure Rate Variety' as TEST,
            MIN(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
                ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
                ELSE NULL END) as MIN_FAILURE_RATE,
            MAX(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
                ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
                ELSE NULL END) as MAX_FAILURE_RATE,
            AVG(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
                ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
                ELSE NULL END) as AVG_FAILURE_RATE
        FROM CELL_TOWER
        """
        
        rrc_results = execute_sql(conn, rrc_test_sql, "Testing RRC failure rate variety")
        
        for test_name, min_rate, max_rate, avg_rate in rrc_results:
            logger.info(f"✅ {test_name}: Min {min_rate:.2f}%, Max {max_rate:.2f}%, Avg {avg_rate:.2f}%")
        
        # Sample top worst performers for demo validation
        demo_test_sql = """
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
        
        demo_results = execute_sql(conn, demo_test_sql, "Sampling worst RRC performers for demo validation")
        
        logger.info("🎯 TOP 5 WORST RRC CONNECTION FAILURE RATES (Perfect for demos!):")
        for cell_id, region, vendor, failure_rate in demo_results:
            logger.info(f"   Cell {cell_id} ({region}, {vendor}): {failure_rate}% failure rate")
        
        logger.info("🎉 DATA REGENERATION COMPLETED SUCCESSFULLY!")
        logger.info("📊 Your demo data now has rich variety for compelling AI demonstrations")
        logger.info("💾 Original data safely backed up in *_BACKUP tables")
        
    except Exception as e:
        logger.error(f"Fatal error during data regeneration: {str(e)}")
        raise
        
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed")

if __name__ == "__main__":
    main()
