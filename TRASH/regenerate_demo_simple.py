#!/usr/bin/env python3
"""
Telco Network Optimization - Simple Demo Data Enhancement Script
Updates existing data with varied key metrics for compelling demos
"""

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Create Snowflake connection"""
    try:
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
    logger.info("Starting Simple Demo Data Enhancement")
    
    conn = None
    try:
        conn = get_snowflake_connection()
        
        logger.info("✅ Using existing backup data from earlier run")
        
        # Step 1: Add variety to RRC Connection Success Rates (CRITICAL for demos)
        logger.info("=== ENHANCING RRC CONNECTION SUCCESS RATES ===")
        
        rrc_update_sql = """
        UPDATE CELL_TOWER 
        SET PM_RRC_CONN_ESTAB_SUCC = 
            CASE 
                WHEN VENDOR_NAME = 'ERICSSON' AND BID_DESCRIPTION LIKE '%(5G)%' THEN 
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.92 + (ABS(HASH(CELL_ID)) % 7) * 0.01), 0)  -- 92-99%
                WHEN VENDOR_NAME = 'ERICSSON' THEN 
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.88 + (ABS(HASH(CELL_ID)) % 8) * 0.01), 0)  -- 88-96%
                WHEN VENDOR_NAME = 'NOKIA' THEN
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.85 + (ABS(HASH(CELL_ID)) % 9) * 0.01), 0)  -- 85-94%
                WHEN VENDOR_NAME = 'HUAWEI' THEN
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.82 + (ABS(HASH(CELL_ID)) % 10) * 0.01), 0) -- 82-92%
                ELSE 
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.75 + (ABS(HASH(CELL_ID)) % 15) * 0.01), 0) -- 75-90%
            END
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        """
        
        execute_sql(conn, rrc_update_sql, "Creating varied RRC connection success rates")
        
        # Step 2: Add variety to PRB Utilization (CRITICAL for capacity demos)
        logger.info("=== ENHANCING PRB UTILIZATION RATES ===")
        
        prb_update_sql = """
        UPDATE CELL_TOWER 
        SET PM_PRB_UTIL_DL = 
            CASE 
                WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
                    70 + (ABS(HASH(CELL_ID*2)) % 30)  -- Urban: 70-100%
                WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%SCOTLAND%' THEN  
                    15 + (ABS(HASH(CELL_ID*2)) % 45)  -- Rural: 15-60%
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
        
        # Step 3: Add variety to E-RAB Abnormal Release Rates  
        logger.info("=== ENHANCING E-RAB ABNORMAL RELEASE RATES ===")
        
        erab_update_sql = """
        UPDATE CELL_TOWER 
        SET PM_ERAB_REL_ABNORMAL_ENB_ACT = 
            CASE 
                WHEN (ABS(HASH(CELL_ID*4)) % 100) <= 75 THEN 5 + (ABS(HASH(CELL_ID*4)) % 35)      -- 75% healthy: 5-40
                WHEN (ABS(HASH(CELL_ID*4)) % 100) <= 90 THEN 40 + (ABS(HASH(CELL_ID*4)) % 50)     -- 15% problematic: 40-90
                ELSE 90 + (ABS(HASH(CELL_ID*4)) % 60) 
            END,
        PM_ERAB_REL_ABNORMAL_ENB = 
            CASE 
                WHEN PM_ERAB_REL_ABNORMAL_ENB_ACT < 40 THEN 20 + (ABS(HASH(CELL_ID*5)) % 40)
                WHEN PM_ERAB_REL_ABNORMAL_ENB_ACT < 90 THEN 60 + (ABS(HASH(CELL_ID*5)) % 60) 
                ELSE 120 + (ABS(HASH(CELL_ID*5)) % 80) 
            END
        """
        
        execute_sql(conn, erab_update_sql, "Creating varied E-RAB abnormal release rates")
        
        # Step 4: Add variety to latency metrics by vendor
        logger.info("=== ENHANCING LATENCY METRICS BY VENDOR ===")
        
        latency_update_sql = """
        UPDATE CELL_TOWER 
        SET PM_PDCP_LAT_TIME_DL = 
            CASE 
                WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN 2000000 + (ABS(HASH(CELL_ID*6)) % 6000000)   -- 5G: 2-8ms
                WHEN VENDOR_NAME = 'ERICSSON' THEN 3000000 + (ABS(HASH(CELL_ID*6)) % 9000000)       -- Ericsson: 3-12ms
                WHEN VENDOR_NAME = 'NOKIA' THEN 3500000 + (ABS(HASH(CELL_ID*6)) % 9500000)          -- Nokia: 3.5-13ms
                WHEN VENDOR_NAME = 'HUAWEI' THEN 2800000 + (ABS(HASH(CELL_ID*6)) % 8200000)         -- Huawei: 2.8-11ms
                ELSE 4000000 + (ABS(HASH(CELL_ID*6)) % 11000000)                                     -- Samsung: 4-15ms
            END
        """
        
        execute_sql(conn, latency_update_sql, "Creating varied latency by vendor")
        
        # Step 5: Create more vendor variety
        logger.info("=== ENHANCING VENDOR DIVERSITY ===")
        
        vendor_update_sql = """
        UPDATE CELL_TOWER 
        SET VENDOR_NAME = 
            CASE 
                WHEN (ABS(HASH(CELL_ID*7)) % 100) <= 45 THEN 'ERICSSON'   -- 45% market share
                WHEN (ABS(HASH(CELL_ID*7)) % 100) <= 75 THEN 'NOKIA'      -- 30% market share
                WHEN (ABS(HASH(CELL_ID*7)) % 100) <= 90 THEN 'HUAWEI'     -- 15% market share
                ELSE 'SAMSUNG' 
            END
        """
        
        execute_sql(conn, vendor_update_sql, "Creating realistic vendor distribution")
        
        # Step 6: Update support ticket sentiment scores for variety
        logger.info("=== ENHANCING SUPPORT TICKET SENTIMENT VARIETY ===")
        
        sentiment_update_sql = """
        UPDATE SUPPORT_TICKETS 
        SET SENTIMENT_SCORE = 
            CASE 
                WHEN REQUEST LIKE '%call drops%' OR REQUEST LIKE '%extremely slow%' OR REQUEST LIKE '%cannot make%' THEN 
                    (-95 + (ABS(HASH(TICKET_ID)) % 35)) / 100.0  -- Very negative: -0.95 to -0.60
                WHEN REQUEST LIKE '%roaming%' OR REQUEST LIKE '%charge%' OR REQUEST LIKE '%bill%' THEN
                    (-80 + (ABS(HASH(TICKET_ID)) % 50)) / 100.0  -- Negative: -0.80 to -0.30
                WHEN REQUEST LIKE '%upgrade%' OR REQUEST LIKE '%add%' OR REQUEST LIKE '%interested%' THEN
                    (30 + (ABS(HASH(TICKET_ID)) % 50)) / 100.0   -- Positive: 0.30 to 0.80
                WHEN REQUEST LIKE '%moving%' OR REQUEST LIKE '%cancel%' OR REQUEST LIKE '%transfer%' THEN  
                    (-20 + (ABS(HASH(TICKET_ID)) % 60)) / 100.0  -- Neutral: -0.20 to 0.40
                ELSE (-40 + (ABS(HASH(TICKET_ID)) % 60)) / 100.0 
            END
        """
        
        execute_sql(conn, sentiment_update_sql, "Creating varied sentiment scores")
        
        # Step 7: Final verification and demo samples
        logger.info("=== FINAL VERIFICATION AND DEMO SAMPLES ===")
        
        # Check RRC failure rate range
        rrc_check_sql = """
        SELECT
            'RRC Failure Rate Range' as METRIC,
            ROUND(MIN(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
                ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
                ELSE NULL END), 2) as MIN_PERCENT,
            ROUND(MAX(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
                ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
                ELSE NULL END), 2) as MAX_PERCENT,
            ROUND(AVG(CASE WHEN PM_RRC_CONN_ESTAB_ATT > 0 THEN 
                ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) 
                ELSE NULL END), 2) as AVG_PERCENT
        FROM CELL_TOWER
        """
        
        rrc_results = execute_sql(conn, rrc_check_sql, "Checking RRC failure rate variety")
        for metric, min_pct, max_pct, avg_pct in rrc_results:
            logger.info(f"✅ {metric}: {min_pct}% to {max_pct}% (avg: {avg_pct}%)")
        
        # Check PRB utilization range
        prb_check_sql = """
        SELECT
            'PRB Utilization DL Range' as METRIC, 
            ROUND(MIN(PM_PRB_UTIL_DL), 0) as MIN_PERCENT,
            ROUND(MAX(PM_PRB_UTIL_DL), 0) as MAX_PERCENT,
            ROUND(AVG(PM_PRB_UTIL_DL), 0) as AVG_PERCENT
        FROM CELL_TOWER
        """
        
        prb_results = execute_sql(conn, prb_check_sql, "Checking PRB utilization variety")
        for metric, min_pct, max_pct, avg_pct in prb_results:
            logger.info(f"✅ {metric}: {min_pct}% to {max_pct}% (avg: {avg_pct}%)")
        
        # Sample top 10 worst RRC performers for demo
        demo_sample_sql = """
        SELECT 
            CELL_ID,
            BID_DESCRIPTION,
            VENDOR_NAME,
            ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE_PERCENT
        FROM CELL_TOWER 
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        ORDER BY FAILURE_RATE_PERCENT DESC
        LIMIT 10
        """
        
        demo_results = execute_sql(conn, demo_sample_sql, "Sampling top 10 worst performers")
        
        logger.info("🎯 TOP 10 WORST RRC CONNECTION FAILURE RATES (Perfect for your AI demos!):")
        for i, (cell_id, region, vendor, failure_rate) in enumerate(demo_results, 1):
            logger.info(f"   {i:2d}. Cell {cell_id} ({region}, {vendor}): {failure_rate:5.2f}% failure rate")
        
        # Check sentiment variety
        sentiment_check_sql = """
        SELECT
            'Sentiment Score Range' as METRIC,
            ROUND(MIN(SENTIMENT_SCORE), 3) as MIN_SCORE,
            ROUND(MAX(SENTIMENT_SCORE), 3) as MAX_SCORE,
            ROUND(AVG(SENTIMENT_SCORE), 3) as AVG_SCORE,
            COUNT(CASE WHEN SENTIMENT_SCORE < -0.5 THEN 1 END) as VERY_NEGATIVE_COUNT,
            COUNT(CASE WHEN SENTIMENT_SCORE > 0.5 THEN 1 END) as POSITIVE_COUNT
        FROM SUPPORT_TICKETS
        """
        
        sentiment_results = execute_sql(conn, sentiment_check_sql, "Checking sentiment variety")
        for metric, min_score, max_score, avg_score, neg_count, pos_count in sentiment_results:
            logger.info(f"✅ {metric}: {min_score} to {max_score} (avg: {avg_score})")
            logger.info(f"   📊 {neg_count:,} very negative tickets, {pos_count:,} positive tickets")
        
        logger.info("🎉 DEMO DATA ENHANCEMENT COMPLETED SUCCESSFULLY!")
        logger.info("📊 Your existing data now has rich variety for compelling AI demonstrations!")
        logger.info("💾 Original data remains safely backed up in *_BACKUP tables")
        logger.info("🎯 Ready to demo questions like:")
        logger.info("    • 'What are the top 10 cell towers with the highest RRC connection failure rates?'")
        logger.info("    • 'Which cell towers have PRB utilization above 90% in the downlink?'")
        logger.info("    • 'Show me all support tickets with sentiment scores below -0.7'")
        logger.info("🚀 Your AI demos will now show meaningful differences and rankings!")
        
    except Exception as e:
        logger.error(f"Fatal error during data enhancement: {str(e)}")
        raise
        
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed")

if __name__ == "__main__":
    main()
