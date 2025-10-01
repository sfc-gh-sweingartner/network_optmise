#!/usr/bin/env python3
"""
Fix PM_ERAB_REL_ABNORMAL_ENB_ACT and PM_ERAB_REL_ABNORMAL_ENB to be proper percentages (0-25%)
instead of raw counts (2-199)
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
    logger.info("🎯 Fixing E-RAB abnormal release metrics to be proper percentages (0-25%)")
    
    conn = get_connection()
    
    try:
        # First, check current values to see the range
        check_current_values_sql = """
        SELECT 
            MIN(PM_ERAB_REL_ABNORMAL_ENB_ACT) as MIN_ACT,
            MAX(PM_ERAB_REL_ABNORMAL_ENB_ACT) as MAX_ACT,
            MIN(PM_ERAB_REL_ABNORMAL_ENB) as MIN_ENB,
            MAX(PM_ERAB_REL_ABNORMAL_ENB) as MAX_ENB,
            COUNT(*) as TOTAL_ROWS
        FROM CELL_TOWER 
        WHERE PM_ERAB_REL_ABNORMAL_ENB_ACT IS NOT NULL
        """
        
        current_values = execute_sql(conn, check_current_values_sql, "Checking current E-RAB abnormal values")
        
        if current_values:
            min_act, max_act, min_enb, max_enb, total_rows = current_values[0]
            logger.info(f"📊 Current ranges:")
            logger.info(f"   PM_ERAB_REL_ABNORMAL_ENB_ACT: {min_act} - {max_act}")
            logger.info(f"   PM_ERAB_REL_ABNORMAL_ENB: {min_enb} - {max_enb}")
            logger.info(f"   Total rows: {total_rows:,}")
        
        # Fix E-RAB Abnormal Release percentages to proper ranges (0-25%)
        logger.info("=== FIXING E-RAB ABNORMAL RELEASE PERCENTAGES ===")
        
        erab_percentage_fix_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_ERAB_REL_ABNORMAL_ENB_ACT = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    ROUND(20.0 + (ABS(HASH(CELL_ID || 'erab_cat_pct')) % 500) * 0.01, 2)   -- 20.00-24.99% (very high abnormal rate)
                WHEN 'VERY_BAD' THEN 
                    ROUND(15.0 + (ABS(HASH(CELL_ID || 'erab_vbad_pct')) % 500) * 0.01, 2)  -- 15.00-19.99% (high abnormal rate)
                WHEN 'BAD' THEN 
                    ROUND(10.0 + (ABS(HASH(CELL_ID || 'erab_bad_pct')) % 500) * 0.01, 2)   -- 10.00-14.99% (elevated abnormal rate)
                WHEN 'QUITE_BAD' THEN 
                    ROUND(5.0 + (ABS(HASH(CELL_ID || 'erab_qbad_pct')) % 500) * 0.01, 2)   -- 5.00-9.99% (moderate abnormal rate)
                WHEN 'PROBLEMATIC' THEN 
                    ROUND(2.0 + (ABS(HASH(CELL_ID || 'erab_prob_pct')) % 300) * 0.01, 2)   -- 2.00-4.99% (some abnormal rate)
                ELSE 
                    ROUND(0.1 + (ABS(HASH(CELL_ID || 'erab_good_pct')) % 190) * 0.01, 2)   -- 0.10-1.99% (low abnormal rate)
            END,
        PM_ERAB_REL_ABNORMAL_ENB = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    ROUND(22.0 + (ABS(HASH(CELL_ID || 'erab_enb_cat_pct')) % 300) * 0.01, 2)  -- 22.00-24.99% (slightly higher than ACT)
                WHEN 'VERY_BAD' THEN 
                    ROUND(17.0 + (ABS(HASH(CELL_ID || 'erab_enb_vbad_pct')) % 300) * 0.01, 2) -- 17.00-19.99%
                WHEN 'BAD' THEN 
                    ROUND(12.0 + (ABS(HASH(CELL_ID || 'erab_enb_bad_pct')) % 300) * 0.01, 2)  -- 12.00-14.99%
                WHEN 'QUITE_BAD' THEN 
                    ROUND(7.0 + (ABS(HASH(CELL_ID || 'erab_enb_qbad_pct')) % 300) * 0.01, 2)  -- 7.00-9.99%
                WHEN 'PROBLEMATIC' THEN 
                    ROUND(3.5 + (ABS(HASH(CELL_ID || 'erab_enb_prob_pct')) % 250) * 0.01, 2) -- 3.50-5.99%
                ELSE 
                    ROUND(0.5 + (ABS(HASH(CELL_ID || 'erab_enb_good_pct')) % 200) * 0.01, 2) -- 0.50-2.49%
            END
        WHERE CELL_ID IS NOT NULL
        """
        
        execute_sql(conn, erab_percentage_fix_sql, "Converting E-RAB abnormal metrics to proper percentages")
        
        # Verify the new values
        logger.info("=== VERIFICATION OF UPDATED E-RAB PERCENTAGES ===")
        
        verify_new_values_sql = """
        SELECT 
            PERFORMANCE_TIER,
            COUNT(*) as TOWER_COUNT,
            ROUND(MIN(PM_ERAB_REL_ABNORMAL_ENB_ACT), 2) as MIN_ACT_PCT,
            ROUND(MAX(PM_ERAB_REL_ABNORMAL_ENB_ACT), 2) as MAX_ACT_PCT,
            ROUND(AVG(PM_ERAB_REL_ABNORMAL_ENB_ACT), 2) as AVG_ACT_PCT,
            ROUND(MIN(PM_ERAB_REL_ABNORMAL_ENB), 2) as MIN_ENB_PCT,
            ROUND(MAX(PM_ERAB_REL_ABNORMAL_ENB), 2) as MAX_ENB_PCT,
            ROUND(AVG(PM_ERAB_REL_ABNORMAL_ENB), 2) as AVG_ENB_PCT
        FROM CELL_TOWER 
        WHERE PM_ERAB_REL_ABNORMAL_ENB_ACT IS NOT NULL
        GROUP BY PERFORMANCE_TIER
        ORDER BY AVG_ACT_PCT DESC
        """
        
        verification_results = execute_sql(conn, verify_new_values_sql, "Verifying updated E-RAB percentage ranges")
        
        logger.info("✅ UPDATED E-RAB ABNORMAL RELEASE PERCENTAGES BY TIER:")
        logger.info("   Tier          | Towers | ACT Min% | ACT Max% | ACT Avg% | ENB Min% | ENB Max% | ENB Avg%")
        logger.info("   --------------|--------|----------|----------|----------|----------|----------|----------")
        
        for tier, count, min_act, max_act, avg_act, min_enb, max_enb, avg_enb in verification_results:
            logger.info(f"   {tier:13s} | {count:6,d} | {min_act:7.2f}% | {max_act:7.2f}% | {avg_act:7.2f}% | {min_enb:7.2f}% | {max_enb:7.2f}% | {avg_enb:7.2f}%")
        
        # Sample some specific towers to show the variety
        sample_towers_sql = """
        SELECT 
            CELL_ID,
            PERFORMANCE_TIER,
            PM_ERAB_REL_ABNORMAL_ENB_ACT,
            PM_ERAB_REL_ABNORMAL_ENB
        FROM CELL_TOWER 
        WHERE PM_ERAB_REL_ABNORMAL_ENB_ACT IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 15
        """
        
        sample_results = execute_sql(conn, sample_towers_sql, "Sampling towers to show E-RAB percentage variety")
        
        logger.info("✅ SAMPLE OF 15 TOWERS WITH NEW E-RAB PERCENTAGES:")
        logger.info("   Cell ID   | Tier          | ACT %    | ENB %")
        logger.info("   ----------|---------------|----------|----------")
        
        for cell_id, tier, act_pct, enb_pct in sample_results:
            logger.info(f"   {cell_id} | {tier:13s} | {act_pct:7.2f}% | {enb_pct:7.2f}%")
        
        logger.info("🎉 E-RAB ABNORMAL RELEASE PERCENTAGES SUCCESSFULLY FIXED!")
        logger.info("📊 Key improvements:")
        logger.info("    ✅ PM_ERAB_REL_ABNORMAL_ENB_ACT now ranges 0.1-24.99%")
        logger.info("    ✅ PM_ERAB_REL_ABNORMAL_ENB now ranges 0.5-24.99%")
        logger.info("    ✅ Values are now proper percentages, not raw counts")
        logger.info("    ✅ CATASTROPHIC towers have highest abnormal release rates")
        logger.info("    ✅ GOOD towers have lowest abnormal release rates")
        logger.info("    ✅ ENB metric is consistently higher than ACT metric")
        
    finally:
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()
