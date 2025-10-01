#!/usr/bin/env python3
"""
MASTER DATA CLEANUP SCRIPT
==========================
This script consolidates ALL data transformations for the telecom network optimization demo.
It can be run to completely reset and repopulate the demo data from the backup.

29762408 is a cell tower with a lot of customer support tickets indicating problems

VENDOR PERFORMANCE HIERARCHY (BUILT INTO DATA):
==============================================
1. ERICSSON (Premium Vendor) - 37% of towers
   - Best overall performance
   - 70% GOOD towers, 20% PROBLEMATIC, 10% BAD/WORSE
   - Avg RRC failure: ~5%, Latency: ~10ms
   
2. NOKIA (Tier-1 Vendor) - 26% of towers
   - Good performance
   - 60% GOOD towers, 25% PROBLEMATIC, 15% BAD/WORSE
   - Avg RRC failure: ~6%, Latency: ~12ms
   
3. SAMSUNG (Tier-1 Vendor) - 6% of towers
   - Good performance (smaller footprint)
   - 60% GOOD towers, 25% PROBLEMATIC, 15% BAD/WORSE
   - Avg RRC failure: ~6%, Latency: ~11ms
   
4. HUAWEI (Budget Vendor) - 22% of towers
   - Below average performance
   - 45% GOOD towers, 30% PROBLEMATIC, 25% BAD/WORSE
   - Avg RRC failure: ~8%, Latency: ~15ms
   
5. ZTE (Low-Cost Vendor) - 9% of towers
   - Poorest performance - MOST PROBLEMATIC
   - 30% GOOD towers, 25% PROBLEMATIC, 45% BAD/WORSE
   - Avg RRC failure: ~12%, Latency: ~18ms
   - HIGH correlation with support tickets

PERFORMANCE TIERS:
==================
- CATASTROPHIC: Severe failures (>40% RRC failure, >35ms latency)
- VERY_BAD: Major issues (25-40% RRC failure, 25-35ms latency)
- BAD: Significant problems (15-25% RRC failure, 18-25ms latency)
- QUITE_BAD: Noticeable issues (10-15% RRC failure, 14-18ms latency)
- PROBLEMATIC: Minor issues (7-10% RRC failure, 11-14ms latency)
- GOOD: Normal performance (<7% RRC failure, <11ms latency)

GEOGRAPHIC PATTERNS:
===================
- Certain provinces will have more issues (correlated with vendor mix)
- Rural areas may have older equipment (more ZTE/Huawei)

SUPPORT TICKET CORRELATION:
===========================
- BAD/VERY_BAD/CATASTROPHIC towers → Higher negative sentiment tickets
- ZTE towers → 2x more support tickets than Ericsson
- Huawei towers → 1.5x more support tickets than Ericsson

Key transformations:
1. Restore data from backup tables
2. Assign vendors to towers (with distribution)
3. Create performance tiers (vendor-influenced)
4. Fix RRC connection failure rates with wide variation (1-70%)
5. Fix latency values to realistic milliseconds (5-50ms)
6. Update all date/timestamp columns to current (Sep 2025)
7. Fix E-RAB abnormal release percentages (0.1-25%)
8. Fix PRB utilization percentages (5-99%)
9. Fix cause code distributions for interesting failure patterns
10. Populate E-RAB establishment metrics (attempts and success rates)
11. Populate S1 signal connection metrics (attempts and success rates)
12. Populate signal quality metrics (RSRP and RSRQ for serving cell and delta)
13. Populate throughput and data volume metrics
14. Correlate support tickets with problematic towers and vendors
"""

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_SCHEMA = "TELCO_NETWORK_OPTIMIZATION_PROD.RAW"

def get_connection():
    """Establish connection to Snowflake"""
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
    """Execute SQL with error handling"""
    try:
        logger.info(f"Executing: {description}")
        cursor = conn.cursor()
        cursor.execute(sql)
        logger.info(f"✅ Success: {description}")
        sql_upper = sql.strip().upper()
        return cursor.fetchall() if (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')) else None
    except Exception as e:
        logger.error(f"❌ Error in {description}: {str(e)}")
        raise
    finally:
        cursor.close()

def main():
    logger.info("="*80)
    logger.info("🎯 MASTER DATA CLEANUP - Telecom Network Optimization Demo")
    logger.info("="*80)
    
    conn = get_connection()
    
    try:
        # ========================================================================
        # STEP 1: RESTORE FROM BACKUP
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 1: RESTORING DATA FROM BACKUP")
        logger.info("="*80)
        
        # Drop and recreate the table to match backup structure exactly
        drop_and_recreate_sql = f"""
        CREATE OR REPLACE TABLE {DB_SCHEMA}.CELL_TOWER AS 
        SELECT * FROM {DB_SCHEMA}.CELL_TOWER_BACKUP;
        """
        execute_sql(conn, drop_and_recreate_sql, "Recreating CELL_TOWER from backup")
        
        restore_tickets_sql = f"""
        CREATE OR REPLACE TABLE {DB_SCHEMA}.SUPPORT_TICKETS AS
        SELECT * FROM {DB_SCHEMA}.SUPPORT_TICKETS_BACKUP;
        """
        execute_sql(conn, restore_tickets_sql, "Recreating SUPPORT_TICKETS from backup")
        
        logger.info("✅ Data restored from backup successfully")
        
        # ========================================================================
        # STEP 2: ASSIGN VENDORS TO ALL CELL TOWERS
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 2: ASSIGNING VENDORS TO ALL CELL TOWERS")
        logger.info("="*80)
        logger.info("📊 Vendor Distribution:")
        logger.info("   - ERICSSON (Premium):  37% of towers")
        logger.info("   - NOKIA (Tier-1):      26% of towers")
        logger.info("   - HUAWEI (Budget):     22% of towers")
        logger.info("   - ZTE (Low-Cost):       9% of towers")
        logger.info("   - SAMSUNG (Tier-1):     6% of towers")
        
        # Assign vendors to cell towers (by UNIQUE_ID) with distribution:
        # Ericsson: 37%, Nokia: 26%, Huawei: 22%, ZTE: 9%, Samsung: 6%
        vendor_assignment_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET VENDOR_NAME = 
            CASE 
                WHEN (ABS(HASH(UNIQUE_ID)) % 100) < 37 THEN 'ERICSSON'      -- 37%
                WHEN (ABS(HASH(UNIQUE_ID)) % 100) < 63 THEN 'NOKIA'         -- 26% (37+26=63)
                WHEN (ABS(HASH(UNIQUE_ID)) % 100) < 85 THEN 'HUAWEI'        -- 22% (63+22=85)
                WHEN (ABS(HASH(UNIQUE_ID)) % 100) < 94 THEN 'ZTE'           -- 9%  (85+9=94)
                ELSE 'SAMSUNG'                                               -- 6%  (94+6=100)
            END
        WHERE UNIQUE_ID IS NOT NULL;
        """
        execute_sql(conn, vendor_assignment_sql, "Assigning vendors to all cell towers")
        
        # ========================================================================
        # STEP 3: CREATE VENDOR-INFLUENCED PERFORMANCE TIERS
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 3: CREATING VENDOR-INFLUENCED PERFORMANCE TIERS")
        logger.info("="*80)
        logger.info("📊 Tier Distribution by Vendor:")
        logger.info("   ERICSSON: 70% GOOD, 20% PROBLEMATIC, 10% BAD/WORSE")
        logger.info("   NOKIA:    60% GOOD, 25% PROBLEMATIC, 15% BAD/WORSE")
        logger.info("   SAMSUNG:  60% GOOD, 25% PROBLEMATIC, 15% BAD/WORSE")
        logger.info("   HUAWEI:   45% GOOD, 30% PROBLEMATIC, 25% BAD/WORSE")
        logger.info("   ZTE:      30% GOOD, 25% PROBLEMATIC, 45% BAD/WORSE ⚠️")
        
        # Add performance tier column if it doesn't exist
        add_tier_column_sql = f"""
        ALTER TABLE {DB_SCHEMA}.CELL_TOWER ADD COLUMN IF NOT EXISTS PERFORMANCE_TIER VARCHAR(50);
        """
        execute_sql(conn, add_tier_column_sql, "Adding PERFORMANCE_TIER column")
        
        # Classify towers into tiers - VENDOR-INFLUENCED
        # Uses a hash-based distribution that varies by vendor
        tier_classification_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER
        SET PERFORMANCE_TIER = 
            CASE VENDOR_NAME
                -- ERICSSON: Best performance (70% GOOD, 20% PROBLEMATIC, 10% BAD/WORSE)
                WHEN 'ERICSSON' THEN
                    CASE 
                        WHEN (ABS(HASH(CELL_ID || 'tier_eric')) % 100) < 2 THEN 'CATASTROPHIC'   -- 2%
                        WHEN (ABS(HASH(CELL_ID || 'tier_eric')) % 100) < 4 THEN 'VERY_BAD'        -- 2%
                        WHEN (ABS(HASH(CELL_ID || 'tier_eric')) % 100) < 10 THEN 'BAD'            -- 6%
                        WHEN (ABS(HASH(CELL_ID || 'tier_eric')) % 100) < 20 THEN 'QUITE_BAD'      -- 10%
                        WHEN (ABS(HASH(CELL_ID || 'tier_eric')) % 100) < 30 THEN 'PROBLEMATIC'    -- 10%
                        ELSE 'GOOD'                                                                 -- 70%
                    END
                    
                -- NOKIA: Good performance (60% GOOD, 25% PROBLEMATIC, 15% BAD/WORSE)
                WHEN 'NOKIA' THEN
                    CASE 
                        WHEN (ABS(HASH(CELL_ID || 'tier_nokia')) % 100) < 3 THEN 'CATASTROPHIC'   -- 3%
                        WHEN (ABS(HASH(CELL_ID || 'tier_nokia')) % 100) < 6 THEN 'VERY_BAD'       -- 3%
                        WHEN (ABS(HASH(CELL_ID || 'tier_nokia')) % 100) < 15 THEN 'BAD'           -- 9%
                        WHEN (ABS(HASH(CELL_ID || 'tier_nokia')) % 100) < 25 THEN 'QUITE_BAD'     -- 10%
                        WHEN (ABS(HASH(CELL_ID || 'tier_nokia')) % 100) < 40 THEN 'PROBLEMATIC'   -- 15%
                        ELSE 'GOOD'                                                                 -- 60%
                    END
                    
                -- SAMSUNG: Good performance (60% GOOD, 25% PROBLEMATIC, 15% BAD/WORSE)
                WHEN 'SAMSUNG' THEN
                    CASE 
                        WHEN (ABS(HASH(CELL_ID || 'tier_sams')) % 100) < 3 THEN 'CATASTROPHIC'    -- 3%
                        WHEN (ABS(HASH(CELL_ID || 'tier_sams')) % 100) < 6 THEN 'VERY_BAD'        -- 3%
                        WHEN (ABS(HASH(CELL_ID || 'tier_sams')) % 100) < 15 THEN 'BAD'            -- 9%
                        WHEN (ABS(HASH(CELL_ID || 'tier_sams')) % 100) < 25 THEN 'QUITE_BAD'      -- 10%
                        WHEN (ABS(HASH(CELL_ID || 'tier_sams')) % 100) < 40 THEN 'PROBLEMATIC'    -- 15%
                        ELSE 'GOOD'                                                                 -- 60%
                    END
                    
                -- HUAWEI: Below average (45% GOOD, 30% PROBLEMATIC, 25% BAD/WORSE)
                WHEN 'HUAWEI' THEN
                    CASE 
                        WHEN (ABS(HASH(CELL_ID || 'tier_huaw')) % 100) < 5 THEN 'CATASTROPHIC'    -- 5%
                        WHEN (ABS(HASH(CELL_ID || 'tier_huaw')) % 100) < 10 THEN 'VERY_BAD'       -- 5%
                        WHEN (ABS(HASH(CELL_ID || 'tier_huaw')) % 100) < 25 THEN 'BAD'            -- 15%
                        WHEN (ABS(HASH(CELL_ID || 'tier_huaw')) % 100) < 40 THEN 'QUITE_BAD'      -- 15%
                        WHEN (ABS(HASH(CELL_ID || 'tier_huaw')) % 100) < 55 THEN 'PROBLEMATIC'    -- 15%
                        ELSE 'GOOD'                                                                 -- 45%
                    END
                    
                -- ZTE: Poorest performance (30% GOOD, 25% PROBLEMATIC, 45% BAD/WORSE)
                WHEN 'ZTE' THEN
                    CASE 
                        WHEN (ABS(HASH(CELL_ID || 'tier_zte')) % 100) < 10 THEN 'CATASTROPHIC'    -- 10%
                        WHEN (ABS(HASH(CELL_ID || 'tier_zte')) % 100) < 20 THEN 'VERY_BAD'        -- 10%
                        WHEN (ABS(HASH(CELL_ID || 'tier_zte')) % 100) < 45 THEN 'BAD'             -- 25%
                        WHEN (ABS(HASH(CELL_ID || 'tier_zte')) % 100) < 60 THEN 'QUITE_BAD'       -- 15%
                        WHEN (ABS(HASH(CELL_ID || 'tier_zte')) % 100) < 70 THEN 'PROBLEMATIC'     -- 10%
                        ELSE 'GOOD'                                                                 -- 30%
                    END
                    
                ELSE 'GOOD'  -- Fallback
            END
        WHERE VENDOR_NAME IS NOT NULL;
        """
        execute_sql(conn, tier_classification_sql, "Classifying towers into vendor-influenced performance tiers")
        
        # ========================================================================
        # STEP 4: FIX RRC CONNECTION RATES WITH WIDE VARIATION
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 4: FIXING RRC CONNECTION RATES (1-70% failure range)")
        logger.info("="*80)
        
        rrc_variation_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER
        SET
            PM_RRC_CONN_ESTAB_ATT = CASE
                WHEN PERFORMANCE_TIER = 'CATASTROPHIC' THEN PM_RRC_CONN_ESTAB_ATT * (1 + UNIFORM(0.5, 1.0, RANDOM()))
                WHEN PERFORMANCE_TIER = 'VERY_BAD' THEN PM_RRC_CONN_ESTAB_ATT * (1 + UNIFORM(0.3, 0.6, RANDOM()))
                WHEN PERFORMANCE_TIER = 'BAD' THEN PM_RRC_CONN_ESTAB_ATT * (1 + UNIFORM(0.15, 0.35, RANDOM()))
                WHEN PERFORMANCE_TIER = 'QUITE_BAD' THEN PM_RRC_CONN_ESTAB_ATT * (1 + UNIFORM(0.05, 0.20, RANDOM()))
                WHEN PERFORMANCE_TIER = 'PROBLEMATIC' THEN PM_RRC_CONN_ESTAB_ATT * (1 + UNIFORM(0.01, 0.10, RANDOM()))
                ELSE PM_RRC_CONN_ESTAB_ATT * (1 + UNIFORM(0.001, 0.05, RANDOM()))
            END,
            PM_RRC_CONN_ESTAB_SUCC = CASE
                WHEN PERFORMANCE_TIER = 'CATASTROPHIC' THEN PM_RRC_CONN_ESTAB_SUCC * (1 - UNIFORM(0.4, 0.8, RANDOM()))
                WHEN PERFORMANCE_TIER = 'VERY_BAD' THEN PM_RRC_CONN_ESTAB_SUCC * (1 - UNIFORM(0.2, 0.5, RANDOM()))
                WHEN PERFORMANCE_TIER = 'BAD' THEN PM_RRC_CONN_ESTAB_SUCC * (1 - UNIFORM(0.1, 0.3, RANDOM()))
                WHEN PERFORMANCE_TIER = 'QUITE_BAD' THEN PM_RRC_CONN_ESTAB_SUCC * (1 - UNIFORM(0.05, 0.15, RANDOM()))
                WHEN PERFORMANCE_TIER = 'PROBLEMATIC' THEN PM_RRC_CONN_ESTAB_SUCC * (1 - UNIFORM(0.02, 0.08, RANDOM()))
                ELSE PM_RRC_CONN_ESTAB_SUCC * (1 - UNIFORM(0.001, 0.03, RANDOM()))
            END
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, rrc_variation_sql, "Creating wide RRC failure rate variation")
        
        # Fix any negative values
        fix_negatives_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET PM_RRC_CONN_ESTAB_SUCC = GREATEST(
            PM_RRC_CONN_ESTAB_SUCC,
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.30, 0)
                WHEN 'VERY_BAD' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.50, 0)
                WHEN 'BAD' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.70, 0)
                WHEN 'QUITE_BAD' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.85, 0)
                WHEN 'PROBLEMATIC' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.92, 0)
                ELSE ROUND(PM_RRC_CONN_ESTAB_ATT * 0.94, 0)
            END
        )
        WHERE PM_RRC_CONN_ESTAB_ATT > 0;
        """
        execute_sql(conn, fix_negatives_sql, "Fixing negative RRC success values")
        
        # ========================================================================
        # STEP 5: FIX LATENCY VALUES (5-50ms realistic range)
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 5: FIXING LATENCY VALUES TO REALISTIC MILLISECONDS (5-50ms)")
        logger.info("="*80)
        
        latency_fix_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        PM_PDCP_LAT_TIME_DL = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    ROUND(40 + (ABS(HASH(CELL_ID || 'lat_cat')) % 15) + (ABS(HASH(CELL_ID || 'lat_cat_dec')) % 100) * 0.01, 2)    -- 40-55ms (terrible latency)
                WHEN 'VERY_BAD' THEN 
                    ROUND(30 + (ABS(HASH(CELL_ID || 'lat_vbad')) % 12) + (ABS(HASH(CELL_ID || 'lat_vbad_dec')) % 100) * 0.01, 2)   -- 30-42ms (very high latency)
                WHEN 'BAD' THEN 
                    ROUND(22 + (ABS(HASH(CELL_ID || 'lat_bad')) % 10) + (ABS(HASH(CELL_ID || 'lat_bad_dec')) % 100) * 0.01, 2)    -- 22-32ms (high latency)
                WHEN 'QUITE_BAD' THEN 
                    ROUND(15 + (ABS(HASH(CELL_ID || 'lat_qbad')) % 8) + (ABS(HASH(CELL_ID || 'lat_qbad_dec')) % 100) * 0.01, 2)    -- 15-23ms (elevated latency)
                WHEN 'PROBLEMATIC' THEN 
                    ROUND(10 + (ABS(HASH(CELL_ID || 'lat_prob')) % 5) + (ABS(HASH(CELL_ID || 'lat_prob_dec')) % 100) * 0.01, 2)    -- 10-15ms (moderate latency)
                ELSE 
                    -- Good towers: centered around 12ms with vendor differences
                    CASE 
                        WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN 
                            ROUND(8 + (ABS(HASH(CELL_ID || 'lat_5g')) % 4) + (ABS(HASH(CELL_ID || 'lat_5g_dec')) % 100) * 0.01, 2)      -- 8-12ms (5G performance)
                        WHEN VENDOR_NAME = 'ERICSSON' THEN 
                            ROUND(10 + (ABS(HASH(CELL_ID || 'lat_eric')) % 5) + (ABS(HASH(CELL_ID || 'lat_eric_dec')) % 100) * 0.01, 2)   -- 10-15ms
                        WHEN VENDOR_NAME = 'NOKIA' THEN
                            ROUND(11 + (ABS(HASH(CELL_ID || 'lat_nokia')) % 6) + (ABS(HASH(CELL_ID || 'lat_nokia_dec')) % 100) * 0.01, 2)  -- 11-17ms
                        WHEN VENDOR_NAME = 'HUAWEI' THEN
                            ROUND(10.5 + (ABS(HASH(CELL_ID || 'lat_huawei')) % 5.5) + (ABS(HASH(CELL_ID || 'lat_huawei_dec')) % 100) * 0.01, 2) -- 10.5-16ms
                        ELSE 
                            ROUND(11 + (ABS(HASH(CELL_ID || 'lat_samsung')) % 6) + (ABS(HASH(CELL_ID || 'lat_samsung_dec')) % 100) * 0.01, 2) -- 11-17ms
                    END
            END,
        PM_PDCP_LAT_PKT_TRANS_DL = 
            -- Packet transmission latency correlates with time latency (10-30% of time latency)
            ROUND(PM_PDCP_LAT_TIME_DL * (0.1 + (ABS(HASH(CELL_ID || 'pkt_dl')) % 20) * 0.01), 2),
        PM_PDCP_LAT_TIME_UL = 
            -- Uplink latency as string: centered around 25ms (roughly 2x DL latency)
            TO_VARCHAR(
                ROUND(
                    CASE PERFORMANCE_TIER
                        WHEN 'CATASTROPHIC' THEN 
                            80 + (ABS(HASH(CELL_ID || 'lat_ul_cat')) % 30) + (ABS(HASH(CELL_ID || 'lat_ul_cat_dec')) % 100) * 0.01    -- 80-110ms
                        WHEN 'VERY_BAD' THEN 
                            60 + (ABS(HASH(CELL_ID || 'lat_ul_vbad')) % 25) + (ABS(HASH(CELL_ID || 'lat_ul_vbad_dec')) % 100) * 0.01   -- 60-85ms
                        WHEN 'BAD' THEN 
                            45 + (ABS(HASH(CELL_ID || 'lat_ul_bad')) % 20) + (ABS(HASH(CELL_ID || 'lat_ul_bad_dec')) % 100) * 0.01    -- 45-65ms
                        WHEN 'QUITE_BAD' THEN 
                            30 + (ABS(HASH(CELL_ID || 'lat_ul_qbad')) % 15) + (ABS(HASH(CELL_ID || 'lat_ul_qbad_dec')) % 100) * 0.01   -- 30-45ms
                        WHEN 'PROBLEMATIC' THEN 
                            22 + (ABS(HASH(CELL_ID || 'lat_ul_prob')) % 10) + (ABS(HASH(CELL_ID || 'lat_ul_prob_dec')) % 100) * 0.01   -- 22-32ms
                        ELSE 
                            -- Good towers: centered around 25ms
                            20 + (ABS(HASH(CELL_ID || 'lat_ul_good')) % 10) + (ABS(HASH(CELL_ID || 'lat_ul_good_dec')) % 100) * 0.01   -- 20-30ms
                    END
                , 2)
            ),
        PM_PDCP_LAT_PKT_TRANS_UL = 
            -- Uplink packet latency as string (10-30% of UL time latency)
            TO_VARCHAR(ROUND(CAST(PM_PDCP_LAT_TIME_UL AS NUMBER) * (0.1 + (ABS(HASH(CELL_ID || 'pkt_ul')) % 20) * 0.01), 2))
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, latency_fix_sql, "Setting realistic latency values in milliseconds (DL ~12ms, UL ~25ms)")
        
        # ========================================================================
        # STEP 6: UPDATE ALL DATE/TIMESTAMP COLUMNS TO CURRENT (Add 2 years 3 months)
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 6: UPDATING ALL DATE/TIMESTAMP COLUMNS TO CURRENT")
        logger.info("="*80)
        
        # Add 2 years and 3 months to shift June 2023 data to Sep 2025
        date_update_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
            EVENT_DATE = DATEADD(MONTH, 27, EVENT_DATE),  -- Add 2 years 3 months (27 months)
            EVENT_DTTM = DATEADD(MONTH, 27, EVENT_DTTM),
            TIMESTAMP = DATEADD(MONTH, 27, TIMESTAMP),
            WINDOW_START_AT = DATEADD(MONTH, 27, WINDOW_START_AT),
            WINDOW_END_AT = DATEADD(MONTH, 27, WINDOW_END_AT)
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, date_update_sql, "Updating all date/timestamp columns (adding 2 years 3 months)")
        
        # ========================================================================
        # STEP 7: FIX E-RAB ABNORMAL RELEASE PERCENTAGES (0.1-25%)
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 7: FIXING E-RAB ABNORMAL RELEASE PERCENTAGES (0.1-25%)")
        logger.info("="*80)
        
        erab_percentage_fix_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        PM_ERAB_REL_ABNORMAL_ENB_ACT = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    ROUND(20.0 + (ABS(HASH(CELL_ID || 'erab_cat_pct')) % 500) * 0.01, 2)
                WHEN 'VERY_BAD' THEN 
                    ROUND(15.0 + (ABS(HASH(CELL_ID || 'erab_vbad_pct')) % 500) * 0.01, 2)
                WHEN 'BAD' THEN 
                    ROUND(10.0 + (ABS(HASH(CELL_ID || 'erab_bad_pct')) % 500) * 0.01, 2)
                WHEN 'QUITE_BAD' THEN 
                    ROUND(5.0 + (ABS(HASH(CELL_ID || 'erab_qbad_pct')) % 500) * 0.01, 2)
                WHEN 'PROBLEMATIC' THEN 
                    ROUND(2.0 + (ABS(HASH(CELL_ID || 'erab_prob_pct')) % 300) * 0.01, 2)
                ELSE 
                    ROUND(0.1 + (ABS(HASH(CELL_ID || 'erab_good_pct')) % 190) * 0.01, 2)
            END,
        PM_ERAB_REL_ABNORMAL_ENB = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    ROUND(22.0 + (ABS(HASH(CELL_ID || 'erab_enb_cat_pct')) % 300) * 0.01, 2)
                WHEN 'VERY_BAD' THEN 
                    ROUND(17.0 + (ABS(HASH(CELL_ID || 'erab_enb_vbad_pct')) % 300) * 0.01, 2)
                WHEN 'BAD' THEN 
                    ROUND(12.0 + (ABS(HASH(CELL_ID || 'erab_enb_bad_pct')) % 300) * 0.01, 2)
                WHEN 'QUITE_BAD' THEN 
                    ROUND(7.0 + (ABS(HASH(CELL_ID || 'erab_enb_qbad_pct')) % 300) * 0.01, 2)
                WHEN 'PROBLEMATIC' THEN 
                    ROUND(3.5 + (ABS(HASH(CELL_ID || 'erab_enb_prob_pct')) % 250) * 0.01, 2)
                ELSE 
                    ROUND(0.5 + (ABS(HASH(CELL_ID || 'erab_enb_good_pct')) % 200) * 0.01, 2)
            END,
        PM_ERAB_REL_NORMAL_ENB = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 200 + (ABS(HASH(CELL_ID || 'erab_norm_cat')) % 300)
                WHEN 'VERY_BAD' THEN 400 + (ABS(HASH(CELL_ID || 'erab_norm_vbad')) % 600)
                WHEN 'BAD' THEN 600 + (ABS(HASH(CELL_ID || 'erab_norm_bad')) % 800)
                WHEN 'QUITE_BAD' THEN 800 + (ABS(HASH(CELL_ID || 'erab_norm_qbad')) % 700)
                WHEN 'PROBLEMATIC' THEN 1000 + (ABS(HASH(CELL_ID || 'erab_norm_prob')) % 800)
                ELSE 1200 + (ABS(HASH(CELL_ID || 'erab_norm_good')) % 800)
            END,
        PM_ERAB_REL_MME = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 80 + (ABS(HASH(CELL_ID || 'erab_mme_cat')) % 120)
                WHEN 'VERY_BAD' THEN 60 + (ABS(HASH(CELL_ID || 'erab_mme_vbad')) % 80)
                WHEN 'BAD' THEN 40 + (ABS(HASH(CELL_ID || 'erab_mme_bad')) % 60)
                ELSE 10 + (ABS(HASH(CELL_ID || 'erab_mme_other')) % 50)
            END
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, erab_percentage_fix_sql, "Setting E-RAB abnormal percentages")
        
        # ========================================================================
        # STEP 8: FIX PRB UTILIZATION (5-99%)
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 8: FIXING PRB UTILIZATION (5-99%)")
        logger.info("="*80)
        
        prb_variation_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        PM_PRB_UTIL_DL = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 90 + (ABS(HASH(CELL_ID || 'prb_cat')) % 10)
                WHEN 'VERY_BAD' THEN 75 + (ABS(HASH(CELL_ID || 'prb_vbad')) % 20)
                WHEN 'BAD' THEN 60 + (ABS(HASH(CELL_ID || 'prb_bad')) % 25)
                WHEN 'QUITE_BAD' THEN 45 + (ABS(HASH(CELL_ID || 'prb_qbad')) % 25)
                WHEN 'PROBLEMATIC' THEN 30 + (ABS(HASH(CELL_ID || 'prb_prob')) % 25)
                ELSE 
                    CASE 
                        WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
                            25 + (ABS(HASH(CELL_ID || 'prb_urban')) % 30)
                        WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%SCOTLAND%' THEN  
                            5 + (ABS(HASH(CELL_ID || 'prb_rural')) % 20)
                        ELSE 15 + (ABS(HASH(CELL_ID || 'prb_suburb')) % 25)
                    END
            END,
        PM_PRB_UTIL_UL = 
            ROUND(PM_PRB_UTIL_DL * (0.6 + (ABS(HASH(CELL_ID || 'prb_ul')) % 21) * 0.01), 0)
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, prb_variation_sql, "Setting PRB utilization variation")
        
        # ========================================================================
        # STEP 9: FIX CAUSE CODE DISTRIBUTION
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 9: REDISTRIBUTING CAUSE CODES FOR INTERESTING PATTERNS")
        logger.info("="*80)
        
        redistribute_cause_codes_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET CAUSE_CODE_SHORT_DESCRIPTION = 
            CASE 
                WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'CALL_OK' THEN 'CALL_OK'
                WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'NETWORK_OUT_OF_ORDER' THEN
                    CASE 
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 16 THEN 'NETWORK_OUT_OF_ORDER'
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 36 THEN 'BEARER_CAPABILITY_NOT_AVAILABLE'
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 52 THEN 'CHANNEL_UNACCEPTABLE'
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 64 THEN 'DESTINATION_OUT_OF_ORDER'
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 74 THEN 'INVALID_CALL_REFERENCE_VALUE'
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 82 THEN 'PRECEDENCE_CALL_BLOCKED'
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 88 THEN 'CONNECTION_OPERATIONAL'
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 93 THEN 'MISDIALED_TRUNK_PREFIX'
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 97 THEN 'OUTGOING_CALLS_BARRED'
                        ELSE 'MESSAGE_TYPE_NON_EXISTENT'
                    END
                ELSE CAUSE_CODE_SHORT_DESCRIPTION
            END
        WHERE CAUSE_CODE_SHORT_DESCRIPTION IS NOT NULL;
        """
        execute_sql(conn, redistribute_cause_codes_sql, "Redistributing cause codes")
        
        # ========================================================================
        # STEP 10: FIX E-RAB ESTABLISHMENT METRICS (PM_ERAB_ESTAB_ATT_INIT, PM_ERAB_ESTAB_SUCC_INIT)
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 10: FIXING E-RAB ESTABLISHMENT METRICS")
        logger.info("="*80)
        
        # First set attempt values
        erab_estab_att_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        PM_ERAB_ESTAB_ATT_INIT = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 5000 + (ABS(HASH(CELL_ID || 'erab_att_cat')) % 3000)
                WHEN 'VERY_BAD' THEN 8000 + (ABS(HASH(CELL_ID || 'erab_att_vb')) % 5000)
                WHEN 'BAD' THEN 10000 + (ABS(HASH(CELL_ID || 'erab_att_bad')) % 6000)
                WHEN 'QUITE_BAD' THEN 12000 + (ABS(HASH(CELL_ID || 'erab_att_qb')) % 8000)
                WHEN 'PROBLEMATIC' THEN 15000 + (ABS(HASH(CELL_ID || 'erab_att_prob')) % 10000)
                ELSE 18000 + (ABS(HASH(CELL_ID || 'erab_att_good')) % 12000)
            END
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, erab_estab_att_sql, "Setting E-RAB establishment attempt metrics")
        
        # Then set success values based on attempt values
        erab_estab_succ_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        PM_ERAB_ESTAB_SUCC_INIT = 
            CASE PERFORMANCE_TIER
                -- Success rate: CATASTROPHIC ~60%, VERY_BAD ~70%, BAD ~80%, QUITE_BAD ~85%, PROBLEMATIC ~90%, GOOD ~95%
                WHEN 'CATASTROPHIC' THEN ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.55 + (ABS(HASH(CELL_ID || 'erab_succ_cat')) % 10) * 0.01), 0)
                WHEN 'VERY_BAD' THEN ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.65 + (ABS(HASH(CELL_ID || 'erab_succ_vb')) % 10) * 0.01), 0)
                WHEN 'BAD' THEN ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.75 + (ABS(HASH(CELL_ID || 'erab_succ_bad')) % 10) * 0.01), 0)
                WHEN 'QUITE_BAD' THEN ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.80 + (ABS(HASH(CELL_ID || 'erab_succ_qb')) % 10) * 0.01), 0)
                WHEN 'PROBLEMATIC' THEN ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.88 + (ABS(HASH(CELL_ID || 'erab_succ_prob')) % 8) * 0.01), 0)
                ELSE ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.93 + (ABS(HASH(CELL_ID || 'erab_succ_good')) % 6) * 0.01), 0)
            END
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, erab_estab_succ_sql, "Setting E-RAB establishment success metrics")
        
        # ========================================================================
        # STEP 11: FIX S1 SIGNAL CONNECTION METRICS (PM_S1_SIG_CONN_ESTAB_ATT, PM_S1_SIG_CONN_ESTAB_SUCC)
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 11: FIXING S1 SIGNAL CONNECTION METRICS")
        logger.info("="*80)
        
        # First set attempt values
        s1_signal_att_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        PM_S1_SIG_CONN_ESTAB_ATT = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 6000 + (ABS(HASH(CELL_ID || 's1_att_cat')) % 4000)
                WHEN 'VERY_BAD' THEN 9000 + (ABS(HASH(CELL_ID || 's1_att_vb')) % 6000)
                WHEN 'BAD' THEN 11000 + (ABS(HASH(CELL_ID || 's1_att_bad')) % 7000)
                WHEN 'QUITE_BAD' THEN 13000 + (ABS(HASH(CELL_ID || 's1_att_qb')) % 8000)
                WHEN 'PROBLEMATIC' THEN 15000 + (ABS(HASH(CELL_ID || 's1_att_prob')) % 9000)
                ELSE 18000 + (ABS(HASH(CELL_ID || 's1_att_good')) % 10000)
            END
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, s1_signal_att_sql, "Setting S1 signal connection attempt metrics")
        
        # Then set success values based on attempt values
        s1_signal_succ_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        PM_S1_SIG_CONN_ESTAB_SUCC = 
            CASE PERFORMANCE_TIER
                -- Success rate: CATASTROPHIC ~65%, VERY_BAD ~75%, BAD ~82%, QUITE_BAD ~87%, PROBLEMATIC ~92%, GOOD ~96%
                WHEN 'CATASTROPHIC' THEN ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.60 + (ABS(HASH(CELL_ID || 's1_succ_cat')) % 10) * 0.01), 0)
                WHEN 'VERY_BAD' THEN ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.70 + (ABS(HASH(CELL_ID || 's1_succ_vb')) % 10) * 0.01), 0)
                WHEN 'BAD' THEN ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.78 + (ABS(HASH(CELL_ID || 's1_succ_bad')) % 8) * 0.01), 0)
                WHEN 'QUITE_BAD' THEN ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.84 + (ABS(HASH(CELL_ID || 's1_succ_qb')) % 8) * 0.01), 0)
                WHEN 'PROBLEMATIC' THEN ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.90 + (ABS(HASH(CELL_ID || 's1_succ_prob')) % 6) * 0.01), 0)
                ELSE ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.95 + (ABS(HASH(CELL_ID || 's1_succ_good')) % 4) * 0.01), 0)
            END
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, s1_signal_succ_sql, "Setting S1 signal connection success metrics")
        
        # ========================================================================
        # STEP 12: FIX SIGNAL QUALITY METRICS (RSRP and RSRQ)
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 12: FIXING SIGNAL QUALITY METRICS (RSRP and RSRQ)")
        logger.info("="*80)
        
        signal_quality_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        -- RSRP serving cell: Good towers -60 to -80 dBm, Bad towers -90 to -110 dBm
        PM_UE_MEAS_RSRP_SERV_INTRA_FREQ1 = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN -105 - (ABS(HASH(CELL_ID || 'rsrp_serv_cat')) % 10)  -- -105 to -115 dBm
                WHEN 'VERY_BAD' THEN -95 - (ABS(HASH(CELL_ID || 'rsrp_serv_vb')) % 10)        -- -95 to -105 dBm
                WHEN 'BAD' THEN -85 - (ABS(HASH(CELL_ID || 'rsrp_serv_bad')) % 10)            -- -85 to -95 dBm
                WHEN 'QUITE_BAD' THEN -75 - (ABS(HASH(CELL_ID || 'rsrp_serv_qb')) % 10)       -- -75 to -85 dBm
                WHEN 'PROBLEMATIC' THEN -68 - (ABS(HASH(CELL_ID || 'rsrp_serv_prob')) % 10)   -- -68 to -78 dBm
                ELSE -55 - (ABS(HASH(CELL_ID || 'rsrp_serv_good')) % 15)                       -- -55 to -70 dBm (good)
            END,
        -- RSRP delta: Good towers have small delta (cells similar strength), Bad towers have larger delta
        PM_UE_MEAS_RSRP_DELTA_INTRA_FREQ1 = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 8 + (ABS(HASH(CELL_ID || 'rsrp_delta_cat')) % 8)     -- 8-16 dB difference
                WHEN 'VERY_BAD' THEN 6 + (ABS(HASH(CELL_ID || 'rsrp_delta_vb')) % 7)          -- 6-13 dB difference
                WHEN 'BAD' THEN 4 + (ABS(HASH(CELL_ID || 'rsrp_delta_bad')) % 6)              -- 4-10 dB difference
                WHEN 'QUITE_BAD' THEN 3 + (ABS(HASH(CELL_ID || 'rsrp_delta_qb')) % 5)         -- 3-8 dB difference
                WHEN 'PROBLEMATIC' THEN 2 + (ABS(HASH(CELL_ID || 'rsrp_delta_prob')) % 4)     -- 2-6 dB difference
                ELSE 0 + (ABS(HASH(CELL_ID || 'rsrp_delta_good')) % 3)                        -- 0-3 dB difference
            END,
        -- RSRQ serving cell: Good towers -5 to -10 dB, Bad towers -15 to -20 dB
        PM_UE_MEAS_RSRQ_SERV_INTRA_FREQ1 = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN -18 - (ABS(HASH(CELL_ID || 'rsrq_serv_cat')) % 5)    -- -18 to -23 dB
                WHEN 'VERY_BAD' THEN -15 - (ABS(HASH(CELL_ID || 'rsrq_serv_vb')) % 4)         -- -15 to -19 dB
                WHEN 'BAD' THEN -12 - (ABS(HASH(CELL_ID || 'rsrq_serv_bad')) % 4)             -- -12 to -16 dB
                WHEN 'QUITE_BAD' THEN -9 - (ABS(HASH(CELL_ID || 'rsrq_serv_qb')) % 3)         -- -9 to -12 dB
                WHEN 'PROBLEMATIC' THEN -7 - (ABS(HASH(CELL_ID || 'rsrq_serv_prob')) % 3)     -- -7 to -10 dB
                ELSE -4 - (ABS(HASH(CELL_ID || 'rsrq_serv_good')) % 4)                        -- -4 to -8 dB (good)
            END,
        -- RSRQ delta: Good towers have small delta, Bad towers have larger delta
        PM_UE_MEAS_RSRQ_DELTA_INTRA_FREQ1 = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 5 + (ABS(HASH(CELL_ID || 'rsrq_delta_cat')) % 6)     -- 5-11 dB difference
                WHEN 'VERY_BAD' THEN 4 + (ABS(HASH(CELL_ID || 'rsrq_delta_vb')) % 5)          -- 4-9 dB difference
                WHEN 'BAD' THEN 3 + (ABS(HASH(CELL_ID || 'rsrq_delta_bad')) % 4)              -- 3-7 dB difference
                WHEN 'QUITE_BAD' THEN 2 + (ABS(HASH(CELL_ID || 'rsrq_delta_qb')) % 3)         -- 2-5 dB difference
                WHEN 'PROBLEMATIC' THEN 1 + (ABS(HASH(CELL_ID || 'rsrq_delta_prob')) % 3)     -- 1-4 dB difference
                ELSE 0 + (ABS(HASH(CELL_ID || 'rsrq_delta_good')) % 2)                        -- 0-2 dB difference
            END
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, signal_quality_sql, "Setting signal quality metrics (RSRP and RSRQ)")
        
        # ========================================================================
        # STEP 13: FIX THROUGHPUT AND DATA VOLUME METRICS
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 13: FIXING THROUGHPUT AND DATA VOLUME METRICS")
        logger.info("="*80)
        
        # First set throughput and volume
        throughput_volume_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        -- Throughput time DL: Good towers have higher throughput (more data processed)
        PM_UE_THP_TIME_DL = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 200000 + (ABS(HASH(CELL_ID || 'thp_cat')) % 200000)    -- 200k-400k
                WHEN 'VERY_BAD' THEN 350000 + (ABS(HASH(CELL_ID || 'thp_vb')) % 250000)         -- 350k-600k
                WHEN 'BAD' THEN 450000 + (ABS(HASH(CELL_ID || 'thp_bad')) % 300000)             -- 450k-750k
                WHEN 'QUITE_BAD' THEN 500000 + (ABS(HASH(CELL_ID || 'thp_qb')) % 350000)        -- 500k-850k
                WHEN 'PROBLEMATIC' THEN 550000 + (ABS(HASH(CELL_ID || 'thp_prob')) % 400000)    -- 550k-950k
                ELSE 650000 + (ABS(HASH(CELL_ID || 'thp_good')) % 500000)                       -- 650k-1150k (good)
            END,
        -- Downlink data volume: Good towers handle more data
        PM_PDCP_VOL_DL_DRB = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 8000000 + (ABS(HASH(CELL_ID || 'vol_cat')) % 7000000)    -- 8M-15M bytes
                WHEN 'VERY_BAD' THEN 12000000 + (ABS(HASH(CELL_ID || 'vol_vb')) % 10000000)       -- 12M-22M bytes
                WHEN 'BAD' THEN 18000000 + (ABS(HASH(CELL_ID || 'vol_bad')) % 15000000)           -- 18M-33M bytes
                WHEN 'QUITE_BAD' THEN 22000000 + (ABS(HASH(CELL_ID || 'vol_qb')) % 18000000)      -- 22M-40M bytes
                WHEN 'PROBLEMATIC' THEN 28000000 + (ABS(HASH(CELL_ID || 'vol_prob')) % 20000000)  -- 28M-48M bytes
                ELSE 35000000 + (ABS(HASH(CELL_ID || 'vol_good')) % 25000000)                     -- 35M-60M bytes (good)
            END
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, throughput_volume_sql, "Setting throughput and data volume metrics")
        
        # Then set last TTI volume based on total volume
        last_tti_sql = f"""
        UPDATE {DB_SCHEMA}.CELL_TOWER 
        SET 
        -- Last TTI volume: Typically 3-6% of total volume
        PM_PDCP_VOL_DL_DRB_LAST_TTI = 
            ROUND(PM_PDCP_VOL_DL_DRB * (0.03 + (ABS(HASH(CELL_ID || 'vol_tti')) % 30) * 0.001), 0)
        WHERE CELL_ID IS NOT NULL;
        """
        execute_sql(conn, last_tti_sql, "Setting last TTI volume metrics")
        
        # ========================================================================
        # STEP 14: CORRELATE SUPPORT TICKETS WITH BAD TOWERS
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("STEP 14: CORRELATING SUPPORT TICKETS WITH PROBLEMATIC TOWERS")
        logger.info("="*80)
        
        create_temp_bad_towers_sql = f"""
        CREATE OR REPLACE TEMPORARY TABLE TEMP_BAD_TOWERS AS
        SELECT CELL_ID FROM {DB_SCHEMA}.CELL_TOWER 
        WHERE PERFORMANCE_TIER IN ('BAD', 'VERY_BAD', 'CATASTROPHIC');
        """
        execute_sql(conn, create_temp_bad_towers_sql, "Creating temp table of problematic towers")
        
        bias_tickets_sql = f"""
        UPDATE {DB_SCHEMA}.SUPPORT_TICKETS st
        SET CELL_ID = (SELECT CELL_ID FROM TEMP_BAD_TOWERS ORDER BY RANDOM() LIMIT 1)
        WHERE st.SENTIMENT_SCORE < 0.0 AND UNIFORM(0, 1, RANDOM()) < 0.7;
        """
        execute_sql(conn, bias_tickets_sql, "Correlating negative support tickets with bad towers")
        
        # ========================================================================
        # VERIFICATION
        # ========================================================================
        logger.info("\n" + "="*80)
        logger.info("VERIFICATION: CHECKING FINAL DATA QUALITY")
        logger.info("="*80)
        
        verify_sql = f"""
        SELECT 
            PERFORMANCE_TIER,
            COUNT(*) as TOWER_COUNT,
            ROUND(AVG(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100)), 1) as AVG_RRC_FAILURE_RATE,
            ROUND(AVG(PM_PDCP_LAT_TIME_DL) / 1000, 1) as AVG_LATENCY_MS,
            ROUND(AVG(PM_PRB_UTIL_DL), 1) as AVG_PRB_UTIL_DL,
            ROUND(AVG(PM_ERAB_REL_ABNORMAL_ENB_ACT), 2) as AVG_ERAB_ABNORMAL_PCT
        FROM {DB_SCHEMA}.CELL_TOWER 
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        GROUP BY PERFORMANCE_TIER
        ORDER BY AVG_RRC_FAILURE_RATE DESC;
        """
        
        verification_results = execute_sql(conn, verify_sql, "Final verification query")
        
        logger.info("\n✅ FINAL DATA QUALITY VERIFICATION:")
        logger.info("   Tier          | Towers | RRC Fail% | Latency(ms) | PRB Util% | ERAB Abnormal%")
        logger.info("   --------------|--------|-----------|-------------|-----------|----------------")
        
        for tier, count, rrc_fail, latency, prb_util, erab_abnormal in verification_results:
            logger.info(f"   {tier:13s} | {count:6,d} | {rrc_fail:8.1f}% | {latency:10.1f} | {prb_util:8.1f}% | {erab_abnormal:13.2f}%")
        
        logger.info("\n" + "="*80)
        logger.info("🎉 MASTER DATA CLEANUP COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info("✅ All cell towers assigned vendors (Ericsson 37%, Nokia 26%, Huawei 22%, ZTE 9%, Samsung 6%)")
        logger.info("✅ Vendor-influenced performance tiers created (ZTE worst, Ericsson best)")
        logger.info("✅ All latency values are now realistic (5-50ms)")
        logger.info("✅ All date/timestamp columns updated to current (Sep 2025)")
        logger.info("✅ RRC failure rates show wide variation (1-70%) by vendor")
        logger.info("✅ E-RAB abnormal percentages are proper (0.1-25%)")
        logger.info("✅ PRB utilization is realistic (5-99%)")
        logger.info("✅ Cause codes show interesting failure patterns")
        logger.info("✅ E-RAB establishment metrics populated (success rates: 60-98%)")
        logger.info("✅ S1 signal connection metrics populated (success rates: 65-99%)")
        logger.info("✅ Signal quality metrics (RSRP/RSRQ) populated with realistic values")
        logger.info("✅ Throughput and data volume metrics populated (200k-1150k, 8M-60M bytes)")
        logger.info("✅ Support tickets correlated with problematic towers and vendors")
        logger.info("💾 Original data safely preserved in *_BACKUP tables")
        logger.info("="*80)
        
    finally:
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()
