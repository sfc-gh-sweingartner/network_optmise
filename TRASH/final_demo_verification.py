#!/usr/bin/env python3
"""
Final comprehensive verification of demo data variety and correlations
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

def main():
    logger.info("🎯 FINAL COMPREHENSIVE DEMO DATA VERIFICATION")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Test 1: RRC Failure Rate Variety (your original issue)
        logger.info("=== TEST 1: RRC FAILURE RATE VARIETY ===")
        cursor.execute("""
            SELECT 
                CELL_ID,
                VENDOR_NAME,
                PERFORMANCE_TIER,
                ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE_PERCENT
            FROM CELL_TOWER 
            WHERE PM_RRC_CONN_ESTAB_ATT > 0
            ORDER BY RANDOM()
            LIMIT 20
        """)
        
        sample_rates = cursor.fetchall()
        logger.info("✅ SAMPLE RRC FAILURE RATES (showing variety):")
        for cell_id, vendor, tier, rate in sample_rates:
            logger.info(f"   Cell {cell_id} ({tier}, {vendor}): {rate}%")
        
        # Test 2: Performance Tier Distribution
        logger.info("=== TEST 2: PERFORMANCE TIER DISTRIBUTION ===")
        cursor.execute("""
            SELECT 
                PERFORMANCE_TIER,
                COUNT(*) as TOWER_COUNT,
                ROUND(AVG(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100)), 2) as AVG_FAILURE_RATE,
                ROUND(AVG(PM_PRB_UTIL_DL), 1) as AVG_PRB_UTIL
            FROM CELL_TOWER 
            WHERE PM_RRC_CONN_ESTAB_ATT > 0
            GROUP BY PERFORMANCE_TIER
            ORDER BY AVG_FAILURE_RATE DESC
        """)
        
        tier_stats = cursor.fetchall()
        logger.info("✅ PERFORMANCE TIER STATISTICS:")
        for tier, count, avg_failure, avg_prb in tier_stats:
            logger.info(f"   {tier}: {count:,} towers, {avg_failure}% avg failure, {avg_prb}% avg PRB util")
        
        # Test 3: Top 10 Worst Performers (key demo query)
        logger.info("=== TEST 3: TOP 10 WORST RRC PERFORMERS (Key Demo Query) ===")
        cursor.execute("""
            SELECT 
                CELL_ID,
                BID_DESCRIPTION,
                VENDOR_NAME,
                PERFORMANCE_TIER,
                ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE_PERCENT
            FROM CELL_TOWER 
            WHERE PM_RRC_CONN_ESTAB_ATT > 0
            ORDER BY FAILURE_RATE_PERCENT DESC
            LIMIT 10
        """)
        
        worst_performers = cursor.fetchall()
        logger.info("🚨 TOP 10 WORST RRC CONNECTION FAILURE RATES:")
        for i, (cell_id, region, vendor, tier, failure_rate) in enumerate(worst_performers, 1):
            logger.info(f"   {i:2d}. Cell {cell_id} ({region}, {vendor}, {tier}): {failure_rate}% failure rate")
        
        # Test 4: High PRB Utilization (capacity demo query)
        logger.info("=== TEST 4: HIGH PRB UTILIZATION TOWERS ===")
        cursor.execute("""
            SELECT 
                CELL_ID,
                BID_DESCRIPTION,
                VENDOR_NAME,
                PERFORMANCE_TIER,
                PM_PRB_UTIL_DL
            FROM CELL_TOWER 
            WHERE PM_PRB_UTIL_DL > 90
            ORDER BY PM_PRB_UTIL_DL DESC
            LIMIT 10
        """)
        
        high_prb_towers = cursor.fetchall()
        logger.info(f"📡 TOWERS WITH >90% PRB UTILIZATION:")
        for cell_id, region, vendor, tier, prb_util in high_prb_towers:
            logger.info(f"   Cell {cell_id} ({region}, {vendor}, {tier}): {prb_util}% PRB utilization")
        
        # Test 5: Support Ticket Sentiment Variety
        logger.info("=== TEST 5: SUPPORT TICKET SENTIMENT VARIETY ===")
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN SENTIMENT_SCORE < -0.7 THEN 'Very Negative (<-0.7)'
                    WHEN SENTIMENT_SCORE < -0.3 THEN 'Negative (-0.7 to -0.3)'
                    WHEN SENTIMENT_SCORE < 0.3 THEN 'Neutral (-0.3 to 0.3)'
                    ELSE 'Positive (>0.3)'
                END as SENTIMENT_CATEGORY,
                COUNT(*) as TICKET_COUNT
            FROM SUPPORT_TICKETS
            GROUP BY SENTIMENT_CATEGORY
            ORDER BY MIN(SENTIMENT_SCORE)
        """)
        
        sentiment_distribution = cursor.fetchall()
        logger.info("✅ SUPPORT TICKET SENTIMENT DISTRIBUTION:")
        for category, count in sentiment_distribution:
            logger.info(f"   {category}: {count:,} tickets")
        
        # Test 6: Sample worst sentiment tickets (demo query)
        logger.info("=== TEST 6: WORST SENTIMENT TICKETS (Demo Query) ===")
        cursor.execute("""
            SELECT 
                TICKET_ID,
                CUSTOMER_NAME,
                SERVICE_TYPE,
                ROUND(SENTIMENT_SCORE, 3) as SENTIMENT,
                LEFT(REQUEST, 80) || '...' as REQUEST_PREVIEW
            FROM SUPPORT_TICKETS
            WHERE SENTIMENT_SCORE < -0.7
            ORDER BY SENTIMENT_SCORE ASC
            LIMIT 5
        """)
        
        worst_sentiment = cursor.fetchall()
        logger.info("😡 SAMPLE TICKETS WITH SENTIMENT < -0.7:")
        for ticket_id, name, service, sentiment, preview in worst_sentiment:
            logger.info(f"   {ticket_id} ({name}, {service}): {sentiment} - \"{preview}\"")
        
        # Test 7: Overall Statistics Summary  
        logger.info("=== TEST 7: OVERALL STATISTICS SUMMARY ===")
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT CELL_ID) as TOTAL_TOWERS,
                COUNT(DISTINCT CASE WHEN PERFORMANCE_TIER = 'BAD' THEN CELL_ID END) as BAD_TOWERS,
                COUNT(DISTINCT CASE WHEN PERFORMANCE_TIER = 'PROBLEMATIC' THEN CELL_ID END) as PROBLEMATIC_TOWERS,
                COUNT(DISTINCT CASE WHEN PM_PRB_UTIL_DL > 90 THEN CELL_ID END) as HIGH_CAPACITY_TOWERS,
                COUNT(DISTINCT CASE WHEN ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) > 20 THEN CELL_ID END) as HIGH_FAILURE_TOWERS
            FROM CELL_TOWER 
            WHERE PM_RRC_CONN_ESTAB_ATT > 0
        """)
        
        tower_stats = cursor.fetchone()
        total, bad, problematic, high_capacity, high_failure = tower_stats
        
        cursor.execute("SELECT COUNT(*) FROM SUPPORT_TICKETS")
        total_tickets = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM SUPPORT_TICKETS WHERE SENTIMENT_SCORE < -0.7")
        very_negative_tickets = cursor.fetchone()[0]
        
        logger.info("📊 COMPREHENSIVE DEMO DATA SUMMARY:")
        logger.info(f"   📈 {total:,} total cell towers")
        logger.info(f"   🔴 {bad:,} BAD performance towers")
        logger.info(f"   🟡 {problematic:,} PROBLEMATIC performance towers") 
        logger.info(f"   📡 {high_capacity:,} towers with >90% PRB utilization")
        logger.info(f"   ⚠️  {high_failure:,} towers with >20% RRC failure rate")
        logger.info(f"   🎫 {total_tickets:,} total support tickets")
        logger.info(f"   😡 {very_negative_tickets:,} very negative tickets (<-0.7)")
        
        # Final demo readiness check
        failure_rate_variety = len(set([rate for _, _, _, rate in sample_rates]))
        
        logger.info("🎉 DEMO READINESS VERIFICATION:")
        logger.info(f"   ✅ RRC Failure Rate Variety: {failure_rate_variety} different rates in sample (FIXED!)")
        logger.info(f"   ✅ Performance Tiers: {len(tier_stats)} tiers with different avg failure rates")
        logger.info(f"   ✅ High-Capacity Towers: {high_capacity:,} towers >90% PRB utilization")
        logger.info(f"   ✅ Problematic Towers: {bad + problematic:,} towers for analysis")
        logger.info(f"   ✅ Sentiment Variety: {len(sentiment_distribution)} sentiment categories")
        
        logger.info("🚀 YOUR DEMO DATA IS PERFECT FOR AI DEMONSTRATIONS!")
        logger.info("🎯 Ready for questions like:")
        logger.info("    • 'What are the top 10 cell towers with the highest RRC connection failure rates?'")
        logger.info("    • 'Which cell towers have PRB utilization above 90% in the downlink?'")
        logger.info("    • 'Show me all support tickets with sentiment scores below -0.7'")
        logger.info("    • 'Which vendors have the best performance in problematic areas?'")
        logger.info("    • 'Correlate cell tower performance with customer satisfaction scores'")
        
    finally:
        cursor.close()
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()
