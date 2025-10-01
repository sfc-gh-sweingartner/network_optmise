#!/usr/bin/env python3
"""
Create truly varied demo data with problematic towers and realistic correlations
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
    logger.info("🎯 Creating varied demo data with problematic towers and realistic performance spread")
    
    conn = get_connection()
    
    try:
        # Step 1: Create a tower performance classification
        logger.info("=== STEP 1: CLASSIFYING TOWERS BY PERFORMANCE TIER ===")
        
        # Add a column to classify towers into performance tiers
        add_tier_column_sql = """
        ALTER TABLE CELL_TOWER ADD COLUMN IF NOT EXISTS PERFORMANCE_TIER VARCHAR(20)
        """
        
        execute_sql(conn, add_tier_column_sql, "Adding performance tier column")
        
        # Classify towers into tiers with different base performance levels
        classify_towers_sql = """
        UPDATE CELL_TOWER 
        SET PERFORMANCE_TIER = 
            CASE 
                WHEN (ABS(HASH(CELL_ID || 'tier')) % 100) < 5 THEN 'BAD'          -- 5% bad towers
                WHEN (ABS(HASH(CELL_ID || 'tier')) % 100) < 20 THEN 'PROBLEMATIC'  -- 15% problematic
                ELSE 'GOOD'                                                         -- 80% good
            END
        """
        
        execute_sql(conn, classify_towers_sql, "Classifying towers into performance tiers")
        
        # Verify tier distribution
        tier_count_sql = """
        SELECT PERFORMANCE_TIER, COUNT(*) as COUNT, 
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as PERCENTAGE
        FROM CELL_TOWER 
        GROUP BY PERFORMANCE_TIER 
        ORDER BY COUNT(*) DESC
        """
        
        tier_results = execute_sql(conn, tier_count_sql, "Verifying tier distribution")
        for tier, count, pct in tier_results:
            logger.info(f"   {tier}: {count:,} towers ({pct}%)")
        
        # Step 2: Create highly varied RRC failure rates based on tiers
        logger.info("=== STEP 2: CREATING VARIED RRC FAILURE RATES ===")
        
        varied_rrc_sql = """
        UPDATE CELL_TOWER 
        SET PM_RRC_CONN_ESTAB_SUCC = 
            CASE PERFORMANCE_TIER
                WHEN 'BAD' THEN 
                    -- Bad towers: 20-35% failure rate
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.65 + (ABS(HASH(CELL_ID || 'rrc1')) % 16) * 0.01), 0)
                WHEN 'PROBLEMATIC' THEN 
                    -- Problematic towers: 10-20% failure rate  
                    ROUND(PM_RRC_CONN_ESTAB_ATT * (0.80 + (ABS(HASH(CELL_ID || 'rrc2')) % 11) * 0.01), 0)
                ELSE 
                    -- Good towers: 1-8% failure rate, with vendor variations
                    CASE 
                        WHEN VENDOR_NAME = 'ERICSSON' THEN 
                            ROUND(PM_RRC_CONN_ESTAB_ATT * (0.94 + (ABS(HASH(CELL_ID || 'rrc3')) % 6) * 0.01), 0)  -- 94-99%
                        WHEN VENDOR_NAME = 'NOKIA' THEN
                            ROUND(PM_RRC_CONN_ESTAB_ATT * (0.93 + (ABS(HASH(CELL_ID || 'rrc4')) % 6) * 0.01), 0)   -- 93-98%
                        WHEN VENDOR_NAME = 'HUAWEI' THEN
                            ROUND(PM_RRC_CONN_ESTAB_ATT * (0.92 + (ABS(HASH(CELL_ID || 'rrc5')) % 7) * 0.01), 0)   -- 92-98%
                        ELSE 
                            ROUND(PM_RRC_CONN_ESTAB_ATT * (0.91 + (ABS(HASH(CELL_ID || 'rrc6')) % 8) * 0.01), 0)   -- 91-98%
                    END
            END
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        """
        
        execute_sql(conn, varied_rrc_sql, "Creating highly varied RRC failure rates by tier")
        
        # Step 3: Create varied PRB utilization with problematic towers having high congestion
        logger.info("=== STEP 3: CREATING VARIED PRB UTILIZATION ===")
        
        varied_prb_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_PRB_UTIL_DL = 
            CASE PERFORMANCE_TIER
                WHEN 'BAD' THEN 
                    -- Bad towers: High congestion 85-99%
                    85 + (ABS(HASH(CELL_ID || 'prb1')) % 15)
                WHEN 'PROBLEMATIC' THEN 
                    -- Problematic towers: Moderate congestion 60-90%
                    60 + (ABS(HASH(CELL_ID || 'prb2')) % 31)
                ELSE 
                    -- Good towers: Low congestion, varies by location
                    CASE 
                        WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
                            40 + (ABS(HASH(CELL_ID || 'prb3')) % 35)  -- Urban: 40-75%
                        WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%SCOTLAND%' THEN  
                            10 + (ABS(HASH(CELL_ID || 'prb4')) % 30)  -- Rural: 10-40%
                        ELSE 20 + (ABS(HASH(CELL_ID || 'prb5')) % 40)  -- Suburban: 20-60%
                    END
            END,
        PM_PRB_UTIL_UL = 
            CASE 
                WHEN PM_PRB_UTIL_DL > 80 THEN 65 + (ABS(HASH(CELL_ID || 'prb6')) % 25)  -- 65-90%
                WHEN PM_PRB_UTIL_DL > 60 THEN 35 + (ABS(HASH(CELL_ID || 'prb7')) % 30)  -- 35-65%
                WHEN PM_PRB_UTIL_DL > 40 THEN 20 + (ABS(HASH(CELL_ID || 'prb8')) % 25)  -- 20-45%
                ELSE 5 + (ABS(HASH(CELL_ID || 'prb9')) % 20)   -- 5-25%
            END
        """
        
        execute_sql(conn, varied_prb_sql, "Creating varied PRB utilization by tier and location")
        
        # Step 4: Create varied E-RAB abnormal release rates
        logger.info("=== STEP 4: CREATING VARIED E-RAB ABNORMAL RELEASE RATES ===")
        
        varied_erab_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_ERAB_REL_ABNORMAL_ENB_ACT = 
            CASE PERFORMANCE_TIER
                WHEN 'BAD' THEN 
                    -- Bad towers: Very high abnormal releases 80-150
                    80 + (ABS(HASH(CELL_ID || 'erab1')) % 71)
                WHEN 'PROBLEMATIC' THEN 
                    -- Problematic towers: High abnormal releases 40-80
                    40 + (ABS(HASH(CELL_ID || 'erab2')) % 41)
                ELSE 
                    -- Good towers: Low abnormal releases 5-25
                    5 + (ABS(HASH(CELL_ID || 'erab3')) % 21)
            END,
        PM_ERAB_REL_ABNORMAL_ENB = 
            CASE PERFORMANCE_TIER
                WHEN 'BAD' THEN 
                    -- Bad towers: Very high 120-200
                    120 + (ABS(HASH(CELL_ID || 'erab4')) % 81)
                WHEN 'PROBLEMATIC' THEN 
                    -- Problematic towers: High 60-120
                    60 + (ABS(HASH(CELL_ID || 'erab5')) % 61)
                ELSE 
                    -- Good towers: Low 20-40
                    20 + (ABS(HASH(CELL_ID || 'erab6')) % 21)
            END
        """
        
        execute_sql(conn, varied_erab_sql, "Creating varied E-RAB abnormal release rates by tier")
        
        # Step 5: Create varied latency by vendor and tier
        logger.info("=== STEP 5: CREATING VARIED LATENCY BY VENDOR AND TIER ===")
        
        varied_latency_sql = """
        UPDATE CELL_TOWER 
        SET PM_PDCP_LAT_TIME_DL = 
            CASE PERFORMANCE_TIER
                WHEN 'BAD' THEN 
                    -- Bad towers: Very high latency 15-25ms
                    15000000 + (ABS(HASH(CELL_ID || 'lat1')) % 10000000)
                WHEN 'PROBLEMATIC' THEN 
                    -- Problematic towers: High latency 8-15ms
                    8000000 + (ABS(HASH(CELL_ID || 'lat2')) % 7000000)
                ELSE 
                    -- Good towers: Low latency varies by vendor and technology
                    CASE 
                        WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN 
                            1000000 + (ABS(HASH(CELL_ID || 'lat3')) % 3000000)   -- 5G: 1-4ms
                        WHEN VENDOR_NAME = 'ERICSSON' THEN 
                            2000000 + (ABS(HASH(CELL_ID || 'lat4')) % 4000000)   -- Ericsson: 2-6ms
                        WHEN VENDOR_NAME = 'NOKIA' THEN
                            2500000 + (ABS(HASH(CELL_ID || 'lat5')) % 4000000)   -- Nokia: 2.5-6.5ms
                        WHEN VENDOR_NAME = 'HUAWEI' THEN
                            2200000 + (ABS(HASH(CELL_ID || 'lat6')) % 3800000)   -- Huawei: 2.2-6ms
                        ELSE 
                            3000000 + (ABS(HASH(CELL_ID || 'lat7')) % 5000000)   -- Samsung: 3-8ms
                    END
            END
        """
        
        execute_sql(conn, varied_latency_sql, "Creating varied latency by vendor, technology and tier")
        
        # Step 6: Update support tickets to correlate with problematic towers
        logger.info("=== STEP 6: CORRELATING SUPPORT TICKETS WITH PROBLEMATIC TOWERS ===")
        
        # First, let's bias ticket assignment towards problematic towers
        correlate_tickets_sql = """
        UPDATE SUPPORT_TICKETS 
        SET CELL_ID = (
            SELECT CELL_ID 
            FROM CELL_TOWER ct
            WHERE CASE 
                -- 70% of tickets go to BAD/PROBLEMATIC towers (even though they're only 20% of towers)
                WHEN (ABS(HASH(SUPPORT_TICKETS.TICKET_ID)) % 10) < 7 THEN 
                    ct.PERFORMANCE_TIER IN ('BAD', 'PROBLEMATIC')
                ELSE 
                    ct.PERFORMANCE_TIER = 'GOOD'
            END
            ORDER BY RANDOM()
            LIMIT 1
        )
        """
        
        execute_sql(conn, correlate_tickets_sql, "Correlating support tickets with problematic towers")
        
        # Update sentiment scores to be worse for tickets associated with bad towers
        correlate_sentiment_sql = """
        UPDATE SUPPORT_TICKETS 
        SET SENTIMENT_SCORE = 
            CASE 
                -- For tickets against BAD towers - make sentiment much worse
                WHEN (SELECT PERFORMANCE_TIER FROM CELL_TOWER WHERE CELL_ID = SUPPORT_TICKETS.CELL_ID) = 'BAD' THEN
                    (-90 + (ABS(HASH(TICKET_ID || 'sent1')) % 25)) / 100.0  -- -0.90 to -0.65
                -- For tickets against PROBLEMATIC towers - make sentiment worse  
                WHEN (SELECT PERFORMANCE_TIER FROM CELL_TOWER WHERE CELL_ID = SUPPORT_TICKETS.CELL_ID) = 'PROBLEMATIC' THEN
                    (-75 + (ABS(HASH(TICKET_ID || 'sent2')) % 45)) / 100.0  -- -0.75 to -0.30
                -- For tickets against GOOD towers - mixed sentiment based on request type
                ELSE
                    CASE 
                        WHEN REQUEST LIKE '%upgrade%' OR REQUEST LIKE '%add%' OR REQUEST LIKE '%interested%' THEN
                            (20 + (ABS(HASH(TICKET_ID || 'sent3')) % 60)) / 100.0   -- 0.20 to 0.80
                        WHEN REQUEST LIKE '%moving%' OR REQUEST LIKE '%cancel%' OR REQUEST LIKE '%transfer%' THEN  
                            (-30 + (ABS(HASH(TICKET_ID || 'sent4')) % 70)) / 100.0  -- -0.30 to 0.40
                        ELSE (-20 + (ABS(HASH(TICKET_ID || 'sent5')) % 50)) / 100.0  -- -0.20 to 0.30
                    END
            END
        """
        
        execute_sql(conn, correlate_sentiment_sql, "Correlating sentiment with tower performance")
        
        # Step 7: Final verification and demo samples
        logger.info("=== STEP 7: FINAL VERIFICATION AND DEMO SAMPLES ===")
        
        # Check RRC failure rate variety by tier
        rrc_variety_sql = """
        SELECT 
            PERFORMANCE_TIER,
            COUNT(*) as TOWER_COUNT,
            ROUND(MIN(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100)), 2) as MIN_FAILURE_RATE,
            ROUND(MAX(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100)), 2) as MAX_FAILURE_RATE,
            ROUND(AVG(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100)), 2) as AVG_FAILURE_RATE
        FROM CELL_TOWER 
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        GROUP BY PERFORMANCE_TIER
        ORDER BY AVG_FAILURE_RATE DESC
        """
        
        rrc_variety_results = execute_sql(conn, rrc_variety_sql, "Checking RRC failure rate variety by tier")
        
        logger.info("✅ RRC FAILURE RATE VARIETY BY TIER:")
        for tier, count, min_rate, max_rate, avg_rate in rrc_variety_results:
            logger.info(f"   {tier}: {count:,} towers, {min_rate}%-{max_rate}% failure rate (avg: {avg_rate}%)")
        
        # Sample top 10 worst performers (should now be varied!)
        worst_performers_sql = """
        SELECT 
            CELL_ID,
            BID_DESCRIPTION,
            VENDOR_NAME,
            PERFORMANCE_TIER,
            ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE_PERCENT,
            PM_PRB_UTIL_DL as PRB_UTIL_DL,
            PM_ERAB_REL_ABNORMAL_ENB_ACT as ERAB_ABNORMAL
        FROM CELL_TOWER 
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        ORDER BY FAILURE_RATE_PERCENT DESC
        LIMIT 10
        """
        
        worst_results = execute_sql(conn, worst_performers_sql, "Sampling top 10 worst RRC performers")
        
        logger.info("🚨 TOP 10 WORST RRC CONNECTION FAILURE RATES (Now with variety!):")
        for i, (cell_id, region, vendor, tier, failure_rate, prb_util, erab_abnormal) in enumerate(worst_results, 1):
            logger.info(f"   {i:2d}. Cell {cell_id} ({tier}, {vendor}): {failure_rate}% failure, {prb_util}% PRB util, {erab_abnormal} ERAB abnormal")
        
        # Check PRB utilization variety
        prb_high_sql = """
        SELECT COUNT(*) as HIGH_PRB_COUNT
        FROM CELL_TOWER 
        WHERE PM_PRB_UTIL_DL > 90
        """
        
        high_prb_count = execute_sql(conn, prb_high_sql, "Counting cells with >90% PRB utilization")[0][0]
        logger.info(f"✅ CELLS WITH >90% PRB UTILIZATION: {high_prb_count:,} cells")
        
        # Check ticket correlation
        ticket_correlation_sql = """
        SELECT 
            ct.PERFORMANCE_TIER,
            COUNT(st.TICKET_ID) as TICKET_COUNT,
            ROUND(AVG(st.SENTIMENT_SCORE), 3) as AVG_SENTIMENT
        FROM SUPPORT_TICKETS st
        JOIN CELL_TOWER ct ON st.CELL_ID = ct.CELL_ID
        GROUP BY ct.PERFORMANCE_TIER
        ORDER BY AVG_SENTIMENT ASC
        """
        
        correlation_results = execute_sql(conn, ticket_correlation_sql, "Checking ticket correlation with tower performance")
        
        logger.info("✅ SUPPORT TICKET CORRELATION:")
        for tier, ticket_count, avg_sentiment in correlation_results:
            logger.info(f"   {tier} towers: {ticket_count:,} tickets, avg sentiment: {avg_sentiment}")
        
        # Sample some individual failure rates to show variety
        sample_rates_sql = """
        SELECT 
            CELL_ID,
            PERFORMANCE_TIER,
            ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE
        FROM CELL_TOWER 
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        ORDER BY RANDOM()
        LIMIT 15
        """
        
        sample_results = execute_sql(conn, sample_rates_sql, "Sampling individual failure rates to show variety")
        
        logger.info("✅ SAMPLE OF INDIVIDUAL FAILURE RATES (showing variety):")
        for cell_id, tier, rate in sample_results:
            logger.info(f"   Cell {cell_id} ({tier}): {rate}%")
        
        logger.info("🎉 VARIED DEMO DATA CREATION COMPLETED SUCCESSFULLY!")
        logger.info("📊 Your demo data now has:")
        logger.info("    • Realistic performance tiers (80% Good, 15% Problematic, 5% Bad)")
        logger.info("    • Highly varied failure rates, PRB utilization, and other metrics")
        logger.info("    • Problematic towers correlated with support tickets")
        logger.info("    • Much more compelling demonstrations possible!")
        logger.info("🎯 Perfect for questions like:")
        logger.info("    • 'What are the top 10 cell towers with the highest RRC connection failure rates?'")
        logger.info("    • 'Which cell towers have PRB utilization above 90%?'") 
        logger.info("    • 'Show me support tickets for the worst performing cell towers'")
        logger.info("💾 Original data remains safely backed up in *_BACKUP tables")
        
    finally:
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()
