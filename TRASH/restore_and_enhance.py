#!/usr/bin/env python3
"""
Restore data from backup and apply demo enhancements
"""

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_connection():
    with open('/Users/sweingartner/.ssh/rsa_key.p8', 'rb') as key:
        p_key = serialization.load_pem_private_key(
            key.read(), 
            password='cLbz!g3hmZGa!Jan'.encode(), 
            backend=default_backend()
        )
    
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return snowflake.connector.connect(
        user='STEPHEN_PYTHON',
        account='rxb32947',
        private_key=pkb,
        warehouse='MYWH',
        database='TELCO_NETWORK_OPTIMIZATION_PROD',
        schema='RAW',
        role='ACCOUNTADMIN'
    )

def execute_sql(conn, sql, description):
    try:
        logger.info(f"Executing: {description}")
        cursor = conn.cursor()
        cursor.execute(sql)
        logger.info(f"✅ Success: {description}")
        return cursor.fetchall() if sql.strip().upper().startswith('SELECT') else None
    except Exception as e:
        logger.error(f"❌ Error in {description}: {str(e)}")
        raise
    finally:
        cursor.close()

def main():
    logger.info("🔄 Restoring data from backup and applying demo enhancements")
    
    conn = get_connection()
    
    try:
        # Step 1: Restore from backup (excluding backup timestamp)
        logger.info("=== STEP 1: RESTORING DATA FROM BACKUP ===")
        
        restore_cell_tower_sql = """
        INSERT INTO CELL_TOWER 
        SELECT CELL_ID, CALL_RELEASE_CODE, LOOKUP_ID, HOME_NETWORK_TAP_CODE, SERVING_NETWORK_TAP_CODE,
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
        FROM CELL_TOWER_BACKUP
        """
        
        execute_sql(conn, restore_cell_tower_sql, "Restoring CELL_TOWER data from backup")
        
        restore_tickets_sql = """
        INSERT INTO SUPPORT_TICKETS
        SELECT TICKET_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, SERVICE_TYPE, REQUEST, 
               CONTACT_PREFERENCE, CELL_ID, SENTIMENT_SCORE
        FROM SUPPORT_TICKETS_BACKUP
        """
        
        execute_sql(conn, restore_tickets_sql, "Restoring SUPPORT_TICKETS data from backup")
        
        # Verify restoration
        check_counts_sql = """
        SELECT 'CELL_TOWER' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM CELL_TOWER
        UNION ALL
        SELECT 'SUPPORT_TICKETS' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM SUPPORT_TICKETS
        """
        
        counts = execute_sql(conn, check_counts_sql, "Verifying data restoration")
        for table_name, count in counts:
            logger.info(f"✅ {table_name}: {count:,} records restored")
        
        # Step 2: Apply all our demo enhancements
        logger.info("=== STEP 2: APPLYING DEMO ENHANCEMENTS ===")
        
        # 2.1: Varied RRC Connection Success Rates
        rrc_update_sql = """
        UPDATE CELL_TOWER 
        SET PM_RRC_CONN_ESTAB_SUCC = 
            CASE 
                WHEN VENDOR_NAME = 'ERICSSON' AND BID_DESCRIPTION LIKE '%(5G)%' THEN 
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.92 + (ABS(HASH(CELL_ID)) % 7) * 0.01), 0)
                WHEN VENDOR_NAME = 'ERICSSON' THEN 
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.88 + (ABS(HASH(CELL_ID)) % 8) * 0.01), 0)
                WHEN VENDOR_NAME = 'NOKIA' THEN
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.85 + (ABS(HASH(CELL_ID)) % 9) * 0.01), 0)
                WHEN VENDOR_NAME = 'HUAWEI' THEN
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.82 + (ABS(HASH(CELL_ID)) % 10) * 0.01), 0)
                ELSE 
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.75 + (ABS(HASH(CELL_ID)) % 15) * 0.01), 0)
            END
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        """
        
        execute_sql(conn, rrc_update_sql, "Creating varied RRC connection success rates")
        
        # 2.2: Varied PRB Utilization
        prb_update_sql = """
        UPDATE CELL_TOWER 
        SET PM_PRB_UTIL_DL = 
            CASE 
                WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
                    70 + (ABS(HASH(CELL_ID*2)) % 30)
                WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%SCOTLAND%' THEN  
                    15 + (ABS(HASH(CELL_ID*2)) % 45)
                ELSE 30 + (ABS(HASH(CELL_ID*2)) % 55) 
            END,
        PM_PRB_UTIL_UL = 
            CASE 
                WHEN PM_PRB_UTIL_DL > 80 THEN 60 + (ABS(HASH(CELL_ID*3)) % 35)
                WHEN PM_PRB_UTIL_DL > 50 THEN 25 + (ABS(HASH(CELL_ID*3)) % 45)
                ELSE 10 + (ABS(HASH(CELL_ID*3)) % 35) 
            END
        """
        
        execute_sql(conn, prb_update_sql, "Creating varied PRB utilization rates")
        
        # 2.3: Varied Vendor Distribution
        vendor_update_sql = """
        UPDATE CELL_TOWER 
        SET VENDOR_NAME = 
            CASE 
                WHEN (ABS(HASH(CELL_ID*7)) % 100) <= 45 THEN 'ERICSSON'
                WHEN (ABS(HASH(CELL_ID*7)) % 100) <= 75 THEN 'NOKIA'
                WHEN (ABS(HASH(CELL_ID*7)) % 100) <= 90 THEN 'HUAWEI'
                ELSE 'SAMSUNG' 
            END
        """
        
        execute_sql(conn, vendor_update_sql, "Creating realistic vendor distribution")
        
        # 2.4: Varied Sentiment Scores
        sentiment_update_sql = """
        UPDATE SUPPORT_TICKETS 
        SET SENTIMENT_SCORE = 
            CASE 
                WHEN REQUEST LIKE '%call drops%' OR REQUEST LIKE '%extremely slow%' OR REQUEST LIKE '%cannot make%' THEN 
                    (-95 + (ABS(HASH(TICKET_ID)) % 35)) / 100.0
                WHEN REQUEST LIKE '%roaming%' OR REQUEST LIKE '%charge%' OR REQUEST LIKE '%bill%' THEN
                    (-80 + (ABS(HASH(TICKET_ID)) % 50)) / 100.0
                WHEN REQUEST LIKE '%upgrade%' OR REQUEST LIKE '%add%' OR REQUEST LIKE '%interested%' THEN
                    (30 + (ABS(HASH(TICKET_ID)) % 50)) / 100.0
                WHEN REQUEST LIKE '%moving%' OR REQUEST LIKE '%cancel%' OR REQUEST LIKE '%transfer%' THEN  
                    (-20 + (ABS(HASH(TICKET_ID)) % 60)) / 100.0
                ELSE (-40 + (ABS(HASH(TICKET_ID)) % 60)) / 100.0 
            END
        """
        
        execute_sql(conn, sentiment_update_sql, "Creating varied sentiment scores")
        
        # Step 3: Final verification with sample data
        logger.info("=== STEP 3: FINAL VERIFICATION ===")
        
        # Top 5 worst RRC performers
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
        
        worst_results = execute_sql(conn, worst_performers_sql, "Sampling top 5 worst RRC performers")
        
        logger.info("🎯 TOP 5 WORST RRC CONNECTION FAILURE RATES (Perfect for your demos!):")
        for i, (cell_id, region, vendor, failure_rate) in enumerate(worst_results, 1):
            logger.info(f"   {i}. Cell {cell_id} ({region}, {vendor}): {failure_rate}% failure rate")
        
        # PRB Utilization samples
        high_prb_sql = """
        SELECT COUNT(*) as HIGH_PRB_COUNT
        FROM CELL_TOWER 
        WHERE PM_PRB_UTIL_DL > 90
        """
        
        high_prb_count = execute_sql(conn, high_prb_sql, "Counting high PRB utilization cells")[0][0]
        logger.info(f"✅ CELLS WITH >90% PRB UTILIZATION: {high_prb_count:,} cells")
        
        # Sentiment variety
        sentiment_check_sql = """
        SELECT 
            COUNT(CASE WHEN SENTIMENT_SCORE < -0.7 THEN 1 END) as VERY_NEGATIVE,
            COUNT(CASE WHEN SENTIMENT_SCORE > 0.5 THEN 1 END) as POSITIVE,
            ROUND(MIN(SENTIMENT_SCORE), 3) as MIN_SENTIMENT,
            ROUND(MAX(SENTIMENT_SCORE), 3) as MAX_SENTIMENT
        FROM SUPPORT_TICKETS
        """
        
        sentiment_result = execute_sql(conn, sentiment_check_sql, "Checking sentiment variety")[0]
        very_neg, positive, min_sent, max_sent = sentiment_result
        logger.info(f"✅ SENTIMENT VARIETY: {min_sent} to {max_sent}")
        logger.info(f"   📊 {very_neg:,} very negative tickets (<-0.7)")
        logger.info(f"   📊 {positive:,} positive tickets (>0.5)")
        
        logger.info("🎉 RESTORATION AND ENHANCEMENT COMPLETED SUCCESSFULLY!")
        logger.info("📊 Your demo data now has the variety needed for compelling AI demonstrations!")
        logger.info("🎯 Ready for demo questions like:")
        logger.info("    • 'What are the top 10 cell towers with the highest RRC connection failure rates?'")
        logger.info("    • 'Which cell towers have PRB utilization above 90% in the downlink?'")
        logger.info("    • 'Show me all support tickets with sentiment scores below -0.7'")
        logger.info("💾 Original data remains safely backed up in *_BACKUP tables")
        
    finally:
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()
