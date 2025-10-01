#!/usr/bin/env python3
"""
Fix support ticket correlation with problematic towers using a simpler approach
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
    logger.info("🔄 Fixing support ticket correlation with problematic towers")
    
    conn = get_connection()
    
    try:
        # Step 1: Create a temp table of problematic towers for easier correlation
        logger.info("=== STEP 1: CREATING TEMPORARY TABLE OF PROBLEMATIC TOWERS ===")
        
        create_temp_sql = """
        CREATE OR REPLACE TEMPORARY TABLE PROBLEMATIC_TOWERS AS
        SELECT CELL_ID, PERFORMANCE_TIER,
               ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE
        FROM CELL_TOWER 
        WHERE PERFORMANCE_TIER IN ('BAD', 'PROBLEMATIC')
        AND PM_RRC_CONN_ESTAB_ATT > 0
        """
        
        execute_sql(conn, create_temp_sql, "Creating temp table of problematic towers")
        
        # Step 2: Update support tickets to bias towards problematic towers
        logger.info("=== STEP 2: BIASING TICKETS TOWARDS PROBLEMATIC TOWERS ===")
        
        # Update tickets in batches to bias towards problematic towers
        bias_tickets_sql = """
        UPDATE SUPPORT_TICKETS 
        SET CELL_ID = (
            CASE 
                WHEN (ABS(HASH(TICKET_ID || 'bias')) % 10) < 7 THEN 
                    -- 70% of tickets get assigned to problematic towers
                    (SELECT CELL_ID FROM PROBLEMATIC_TOWERS ORDER BY RANDOM() LIMIT 1)
                ELSE 
                    -- 30% get assigned to any tower (including good ones)
                    (SELECT CELL_ID FROM CELL_TOWER ORDER BY RANDOM() LIMIT 1)
            END
        )
        WHERE CELL_ID IS NOT NULL
        """
        
        execute_sql(conn, bias_tickets_sql, "Biasing tickets towards problematic towers")
        
        # Step 3: Update sentiment scores based on tower performance
        logger.info("=== STEP 3: UPDATING SENTIMENT BASED ON TOWER PERFORMANCE ===")
        
        # Create a simpler sentiment correlation using JOINs
        update_sentiment_sql = """
        UPDATE SUPPORT_TICKETS 
        SET SENTIMENT_SCORE = 
            CASE 
                WHEN REQUEST LIKE '%call drops%' OR REQUEST LIKE '%extremely slow%' OR REQUEST LIKE '%cannot make%' THEN 
                    -- Service issues are always very negative, but worse for bad towers
                    CASE WHEN EXISTS(SELECT 1 FROM CELL_TOWER ct WHERE ct.CELL_ID = SUPPORT_TICKETS.CELL_ID AND ct.PERFORMANCE_TIER = 'BAD') THEN
                        (-95 + (ABS(HASH(TICKET_ID || 'bad')) % 15)) / 100.0  -- -0.95 to -0.80 for bad towers
                    ELSE 
                        (-85 + (ABS(HASH(TICKET_ID || 'prob')) % 25)) / 100.0  -- -0.85 to -0.60 for others
                    END
                    
                WHEN REQUEST LIKE '%roaming%' OR REQUEST LIKE '%charge%' OR REQUEST LIKE '%bill%' THEN
                    -- Billing issues
                    (-75 + (ABS(HASH(TICKET_ID || 'bill')) % 45)) / 100.0  -- -0.75 to -0.30
                    
                WHEN REQUEST LIKE '%upgrade%' OR REQUEST LIKE '%add%' OR REQUEST LIKE '%interested%' THEN
                    -- Sales inquiries are positive
                    (30 + (ABS(HASH(TICKET_ID || 'sales')) % 50)) / 100.0   -- 0.30 to 0.80
                    
                WHEN REQUEST LIKE '%moving%' OR REQUEST LIKE '%cancel%' OR REQUEST LIKE '%transfer%' THEN  
                    -- Service changes are neutral
                    (-20 + (ABS(HASH(TICKET_ID || 'change')) % 60)) / 100.0  -- -0.20 to 0.40
                    
                ELSE 
                    -- General support requests - worse sentiment for bad tower tickets
                    CASE WHEN EXISTS(SELECT 1 FROM CELL_TOWER ct WHERE ct.CELL_ID = SUPPORT_TICKETS.CELL_ID AND ct.PERFORMANCE_TIER = 'BAD') THEN
                        (-70 + (ABS(HASH(TICKET_ID || 'gen1')) % 40)) / 100.0  -- -0.70 to -0.30 for bad towers
                    WHEN EXISTS(SELECT 1 FROM CELL_TOWER ct WHERE ct.CELL_ID = SUPPORT_TICKETS.CELL_ID AND ct.PERFORMANCE_TIER = 'PROBLEMATIC') THEN
                        (-50 + (ABS(HASH(TICKET_ID || 'gen2')) % 60)) / 100.0  -- -0.50 to 0.10 for problematic towers
                    ELSE 
                        (-30 + (ABS(HASH(TICKET_ID || 'gen3')) % 60)) / 100.0  -- -0.30 to 0.30 for good towers
                    END
            END
        """
        
        execute_sql(conn, update_sentiment_sql, "Updating sentiment based on tower performance")
        
        # Step 4: Verification and demo samples
        logger.info("=== STEP 4: FINAL VERIFICATION ===")
        
        # Check ticket distribution by tower performance
        ticket_distribution_sql = """
        SELECT 
            ct.PERFORMANCE_TIER,
            COUNT(st.TICKET_ID) as TICKET_COUNT,
            ROUND(COUNT(st.TICKET_ID) * 100.0 / SUM(COUNT(st.TICKET_ID)) OVER(), 1) as TICKET_PERCENTAGE,
            ROUND(AVG(st.SENTIMENT_SCORE), 3) as AVG_SENTIMENT,
            COUNT(CASE WHEN st.SENTIMENT_SCORE < -0.7 THEN 1 END) as VERY_NEGATIVE_TICKETS
        FROM SUPPORT_TICKETS st
        JOIN CELL_TOWER ct ON st.CELL_ID = ct.CELL_ID
        GROUP BY ct.PERFORMANCE_TIER
        ORDER BY AVG_SENTIMENT ASC
        """
        
        distribution_results = execute_sql(conn, ticket_distribution_sql, "Checking ticket distribution")
        
        logger.info("✅ SUPPORT TICKET DISTRIBUTION BY TOWER PERFORMANCE:")
        for tier, ticket_count, ticket_pct, avg_sentiment, very_neg in distribution_results:
            logger.info(f"   {tier} towers: {ticket_count:,} tickets ({ticket_pct}%), avg sentiment: {avg_sentiment}, {very_neg:,} very negative")
        
        # Sample top worst performing towers with their ticket counts
        worst_with_tickets_sql = """
        SELECT 
            ct.CELL_ID,
            ct.BID_DESCRIPTION,
            ct.VENDOR_NAME,
            ct.PERFORMANCE_TIER,
            ROUND(((ct.PM_RRC_CONN_ESTAB_ATT - ct.PM_RRC_CONN_ESTAB_SUCC) / ct.PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE_PERCENT,
            ct.PM_PRB_UTIL_DL as PRB_UTIL_DL,
            COUNT(st.TICKET_ID) as TICKET_COUNT,
            ROUND(AVG(st.SENTIMENT_SCORE), 3) as AVG_TICKET_SENTIMENT
        FROM CELL_TOWER ct
        LEFT JOIN SUPPORT_TICKETS st ON ct.CELL_ID = st.CELL_ID
        WHERE ct.PM_RRC_CONN_ESTAB_ATT > 0
        GROUP BY ct.CELL_ID, ct.BID_DESCRIPTION, ct.VENDOR_NAME, ct.PERFORMANCE_TIER, 
                 ct.PM_RRC_CONN_ESTAB_ATT, ct.PM_RRC_CONN_ESTAB_SUCC, ct.PM_PRB_UTIL_DL
        ORDER BY FAILURE_RATE_PERCENT DESC
        LIMIT 10
        """
        
        worst_results = execute_sql(conn, worst_with_tickets_sql, "Sampling worst towers with ticket correlations")
        
        logger.info("🚨 TOP 10 WORST TOWERS WITH TICKET CORRELATION:")
        for cell_id, region, vendor, tier, failure_rate, prb_util, ticket_count, avg_sentiment in worst_results:
            logger.info(f"   Cell {cell_id} ({tier}, {vendor}): {failure_rate}% failure, {prb_util}% PRB, {ticket_count} tickets (avg sentiment: {avg_sentiment})")
        
        # Overall summary statistics
        summary_sql = """
        SELECT 
            'Overall Statistics' as METRIC,
            COUNT(DISTINCT ct.CELL_ID) as TOTAL_TOWERS,
            COUNT(DISTINCT CASE WHEN ct.PERFORMANCE_TIER = 'BAD' THEN ct.CELL_ID END) as BAD_TOWERS,
            COUNT(DISTINCT st.TICKET_ID) as TOTAL_TICKETS,
            COUNT(DISTINCT CASE WHEN st.SENTIMENT_SCORE < -0.7 THEN st.TICKET_ID END) as VERY_NEGATIVE_TICKETS,
            COUNT(DISTINCT CASE WHEN ct.PM_PRB_UTIL_DL > 90 THEN ct.CELL_ID END) as HIGH_PRB_TOWERS
        FROM CELL_TOWER ct
        LEFT JOIN SUPPORT_TICKETS st ON ct.CELL_ID = st.CELL_ID
        """
        
        summary_results = execute_sql(conn, summary_sql, "Overall summary statistics")
        metric, total_towers, bad_towers, total_tickets, very_neg_tickets, high_prb_towers = summary_results[0]
        
        logger.info("📊 OVERALL DEMO DATA SUMMARY:")
        logger.info(f"   📈 {total_towers:,} total cell towers ({bad_towers:,} bad towers)")
        logger.info(f"   🎫 {total_tickets:,} total support tickets ({very_neg_tickets:,} very negative)")
        logger.info(f"   📡 {high_prb_towers:,} towers with >90% PRB utilization")
        
        logger.info("🎉 SUPPORT TICKET CORRELATION COMPLETED SUCCESSFULLY!")
        logger.info("📊 Your demo data now has:")
        logger.info("    • Varied failure rates across all towers (no more uniform 12.01%!)")
        logger.info("    • Realistic performance tiers with problematic towers")
        logger.info("    • Support tickets biased towards problematic towers")
        logger.info("    • Correlated sentiment scores with tower performance")
        logger.info("🎯 Perfect for AI demonstration questions!")
        logger.info("💾 Original data remains safely backed up in *_BACKUP tables")
        
    finally:
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()
