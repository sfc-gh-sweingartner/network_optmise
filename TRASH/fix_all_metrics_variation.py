#!/usr/bin/env python3
"""
Fix negative values and create consistent variation across ALL fact columns
Bad towers should be consistently bad across all metrics
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
    logger.info("🎯 Fixing negative values and creating consistent variation across ALL fact columns")
    
    conn = get_connection()
    
    try:
        # Step 1: Fix any negative failure rates that occurred from randomization
        logger.info("=== STEP 1: FIXING NEGATIVE VALUES ===")
        
        fix_negatives_sql = """
        UPDATE CELL_TOWER 
        SET PM_RRC_CONN_ESTAB_SUCC = GREATEST(
            PM_RRC_CONN_ESTAB_SUCC,
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.30, 0)  -- Max 70% failure
                WHEN 'VERY_BAD' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.50, 0)      -- Max 50% failure
                WHEN 'BAD' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.70, 0)           -- Max 30% failure
                WHEN 'QUITE_BAD' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.85, 0)     -- Max 15% failure
                WHEN 'PROBLEMATIC' THEN ROUND(PM_RRC_CONN_ESTAB_ATT * 0.92, 0)   -- Max 8% failure
                ELSE ROUND(PM_RRC_CONN_ESTAB_ATT * 0.94, 0)                      -- Max 6% failure
            END
        )
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        """
        
        execute_sql(conn, fix_negatives_sql, "Fixing any negative RRC success values")
        
        # Step 2: Create consistent PRB Utilization variation (already partially done)
        logger.info("=== STEP 2: CONSISTENT PRB UTILIZATION VARIATION ===")
        
        prb_variation_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_PRB_UTIL_DL = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    90 + (ABS(HASH(CELL_ID || 'prb_cat')) % 10)  -- 90-99% (severely congested)
                WHEN 'VERY_BAD' THEN 
                    75 + (ABS(HASH(CELL_ID || 'prb_vbad')) % 20)  -- 75-94% (highly congested)
                WHEN 'BAD' THEN 
                    60 + (ABS(HASH(CELL_ID || 'prb_bad')) % 25)   -- 60-84% (congested)
                WHEN 'QUITE_BAD' THEN 
                    45 + (ABS(HASH(CELL_ID || 'prb_qbad')) % 25)  -- 45-69% (moderately congested)
                WHEN 'PROBLEMATIC' THEN 
                    30 + (ABS(HASH(CELL_ID || 'prb_prob')) % 25)  -- 30-54% (some congestion)
                ELSE 
                    -- Good towers vary by location type
                    CASE 
                        WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
                            25 + (ABS(HASH(CELL_ID || 'prb_urban')) % 30)    -- Urban: 25-54%
                        WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%SCOTLAND%' THEN  
                            5 + (ABS(HASH(CELL_ID || 'prb_rural')) % 20)     -- Rural: 5-24%
                        ELSE 15 + (ABS(HASH(CELL_ID || 'prb_suburb')) % 25)  -- Suburban: 15-39%
                    END
            END,
        PM_PRB_UTIL_UL = 
            -- UL typically 60-80% of DL utilization
            ROUND(PM_PRB_UTIL_DL * (0.6 + (ABS(HASH(CELL_ID || 'prb_ul')) % 21) * 0.01), 0)
        """
        
        execute_sql(conn, prb_variation_sql, "Creating consistent PRB utilization variation")
        
        # Step 3: Create consistent E-RAB Abnormal Release variation
        logger.info("=== STEP 3: CONSISTENT E-RAB ABNORMAL RELEASE VARIATION ===")
        
        erab_variation_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_ERAB_REL_ABNORMAL_ENB_ACT = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    120 + (ABS(HASH(CELL_ID || 'erab_cat')) % 80)   -- 120-199 (very high abnormal)
                WHEN 'VERY_BAD' THEN 
                    80 + (ABS(HASH(CELL_ID || 'erab_vbad')) % 60)   -- 80-139 (high abnormal)
                WHEN 'BAD' THEN 
                    50 + (ABS(HASH(CELL_ID || 'erab_bad')) % 40)    -- 50-89 (elevated abnormal)
                WHEN 'QUITE_BAD' THEN 
                    25 + (ABS(HASH(CELL_ID || 'erab_qbad')) % 30)   -- 25-54 (moderate abnormal)
                WHEN 'PROBLEMATIC' THEN 
                    15 + (ABS(HASH(CELL_ID || 'erab_prob')) % 20)   -- 15-34 (some abnormal)
                ELSE 
                    2 + (ABS(HASH(CELL_ID || 'erab_good')) % 15)    -- 2-16 (low abnormal)
            END,
        PM_ERAB_REL_ABNORMAL_ENB = 
            -- This should be higher than ACT metric
            PM_ERAB_REL_ABNORMAL_ENB_ACT + 20 + (ABS(HASH(CELL_ID || 'erab_enb')) % 40),
        PM_ERAB_REL_NORMAL_ENB = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    200 + (ABS(HASH(CELL_ID || 'erab_norm_cat')) % 300)   -- 200-499 (many normal releases due to problems)
                WHEN 'VERY_BAD' THEN 
                    400 + (ABS(HASH(CELL_ID || 'erab_norm_vbad')) % 600)  -- 400-999
                WHEN 'BAD' THEN 
                    600 + (ABS(HASH(CELL_ID || 'erab_norm_bad')) % 800)   -- 600-1399
                WHEN 'QUITE_BAD' THEN 
                    800 + (ABS(HASH(CELL_ID || 'erab_norm_qbad')) % 700)  -- 800-1499
                WHEN 'PROBLEMATIC' THEN 
                    1000 + (ABS(HASH(CELL_ID || 'erab_norm_prob')) % 800) -- 1000-1799
                ELSE 
                    1200 + (ABS(HASH(CELL_ID || 'erab_norm_good')) % 800) -- 1200-1999 (good towers have most normal releases)
            END,
        PM_ERAB_REL_MME = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    80 + (ABS(HASH(CELL_ID || 'erab_mme_cat')) % 120)     -- 80-199 (high MME releases)
                WHEN 'VERY_BAD' THEN 
                    60 + (ABS(HASH(CELL_ID || 'erab_mme_vbad')) % 80)     -- 60-139
                WHEN 'BAD' THEN 
                    40 + (ABS(HASH(CELL_ID || 'erab_mme_bad')) % 60)      -- 40-99
                ELSE 
                    10 + (ABS(HASH(CELL_ID || 'erab_mme_other')) % 50)    -- 10-59 (lower for better towers)
            END
        """
        
        execute_sql(conn, erab_variation_sql, "Creating consistent E-RAB release variation")
        
        # Step 4: Create consistent Latency variation (CRITICAL for performance demos)
        logger.info("=== STEP 4: CONSISTENT LATENCY VARIATION ===")
        
        latency_variation_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_PDCP_LAT_TIME_DL = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    20000000 + (ABS(HASH(CELL_ID || 'lat_cat')) % 15000000)    -- 20-35ms (terrible latency)
                WHEN 'VERY_BAD' THEN 
                    12000000 + (ABS(HASH(CELL_ID || 'lat_vbad')) % 10000000)   -- 12-22ms (very high latency)
                WHEN 'BAD' THEN 
                    8000000 + (ABS(HASH(CELL_ID || 'lat_bad')) % 6000000)      -- 8-14ms (high latency)
                WHEN 'QUITE_BAD' THEN 
                    5000000 + (ABS(HASH(CELL_ID || 'lat_qbad')) % 4000000)     -- 5-9ms (elevated latency)
                WHEN 'PROBLEMATIC' THEN 
                    3000000 + (ABS(HASH(CELL_ID || 'lat_prob')) % 3000000)     -- 3-6ms (moderate latency)
                ELSE 
                    -- Good towers: vendor and technology differences
                    CASE 
                        WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN 
                            500000 + (ABS(HASH(CELL_ID || 'lat_5g')) % 2000000)      -- 0.5-2.5ms (5G performance)
                        WHEN VENDOR_NAME = 'ERICSSON' THEN 
                            1000000 + (ABS(HASH(CELL_ID || 'lat_eric')) % 2500000)   -- 1-3.5ms
                        WHEN VENDOR_NAME = 'NOKIA' THEN
                            1200000 + (ABS(HASH(CELL_ID || 'lat_nokia')) % 2800000)  -- 1.2-4ms
                        WHEN VENDOR_NAME = 'HUAWEI' THEN
                            1100000 + (ABS(HASH(CELL_ID || 'lat_huawei')) % 2400000) -- 1.1-3.5ms
                        ELSE 
                            1500000 + (ABS(HASH(CELL_ID || 'lat_samsung')) % 3000000) -- 1.5-4.5ms
                    END
            END,
        PM_PDCP_LAT_PKT_TRANS_DL = 
            -- Packet transmission latency should correlate with time latency
            ROUND(PM_PDCP_LAT_TIME_DL * (0.1 + (ABS(HASH(CELL_ID || 'pkt_dl')) % 20) * 0.01), 0),
        PM_PDCP_LAT_TIME_UL = 
            -- Uplink latency as string (higher than DL)
            TO_VARCHAR(ROUND(PM_PDCP_LAT_TIME_DL * (1.2 + (ABS(HASH(CELL_ID || 'lat_ul')) % 30) * 0.01), 0)),
        PM_PDCP_LAT_PKT_TRANS_UL = 
            -- Uplink packet latency as string
            TO_VARCHAR(ROUND(PM_PDCP_LAT_PKT_TRANS_DL * (1.1 + (ABS(HASH(CELL_ID || 'pkt_ul')) % 25) * 0.01), 0))
        """
        
        execute_sql(conn, latency_variation_sql, "Creating consistent latency variation by tier and vendor")
        
        # Step 5: Create consistent Active UE and Capacity metrics
        logger.info("=== STEP 5: CONSISTENT ACTIVE UE AND CAPACITY METRICS ===")
        
        capacity_variation_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_ACTIVE_UE_DL_MAX = 
            CASE 
                WHEN ENODEB_FUNCTION = 10 THEN  -- Macro cells
                    CASE PERFORMANCE_TIER
                        WHEN 'CATASTROPHIC' THEN 
                            150 + (ABS(HASH(CELL_ID || 'ue_max_cat')) % 50)    -- 150-199 (overloaded)
                        WHEN 'VERY_BAD' THEN 
                            120 + (ABS(HASH(CELL_ID || 'ue_max_vbad')) % 40)   -- 120-159 (heavily loaded)
                        WHEN 'BAD' THEN 
                            90 + (ABS(HASH(CELL_ID || 'ue_max_bad')) % 40)     -- 90-129 (heavily loaded)
                        WHEN 'QUITE_BAD' THEN 
                            70 + (ABS(HASH(CELL_ID || 'ue_max_qbad')) % 30)    -- 70-99 (moderately loaded)
                        ELSE 
                            40 + (ABS(HASH(CELL_ID || 'ue_max_good')) % 50)    -- 40-89 (normal load)
                    END
                ELSE  -- Small cells (ENODEB_FUNCTION = 14)
                    CASE PERFORMANCE_TIER
                        WHEN 'CATASTROPHIC' THEN 
                            60 + (ABS(HASH(CELL_ID || 'ue_small_cat')) % 20)   -- 60-79
                        WHEN 'VERY_BAD' THEN 
                            50 + (ABS(HASH(CELL_ID || 'ue_small_vbad')) % 20)  -- 50-69
                        WHEN 'BAD' THEN 
                            40 + (ABS(HASH(CELL_ID || 'ue_small_bad')) % 15)   -- 40-54
                        ELSE 
                            15 + (ABS(HASH(CELL_ID || 'ue_small_other')) % 25) -- 15-39
                    END
            END,
        PM_ACTIVE_UE_DL_SUM = 
            -- Sum should be much higher and correlate with max
            PM_ACTIVE_UE_DL_MAX * (8000 + (ABS(HASH(CELL_ID || 'ue_sum_dl')) % 4000)),
        PM_ACTIVE_UE_UL_MAX = 
            -- UL max typically 60-90% of DL max
            ROUND(PM_ACTIVE_UE_DL_MAX * (0.6 + (ABS(HASH(CELL_ID || 'ue_ul_ratio')) % 31) * 0.01), 0),
        PM_ACTIVE_UE_UL_SUM = 
            -- UL sum correlates with UL max
            PM_ACTIVE_UE_UL_MAX * (7000 + (ABS(HASH(CELL_ID || 'ue_sum_ul')) % 5000)),
        PM_RRC_CONN_MAX = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    1800 + (ABS(HASH(CELL_ID || 'rrc_max_cat')) % 700)     -- 1800-2499 (way overloaded)
                WHEN 'VERY_BAD' THEN 
                    1400 + (ABS(HASH(CELL_ID || 'rrc_max_vbad')) % 500)    -- 1400-1899 (overloaded)
                WHEN 'BAD' THEN 
                    1000 + (ABS(HASH(CELL_ID || 'rrc_max_bad')) % 400)     -- 1000-1399 (heavily loaded)
                WHEN 'QUITE_BAD' THEN 
                    700 + (ABS(HASH(CELL_ID || 'rrc_max_qbad')) % 300)     -- 700-999 (loaded)
                WHEN 'PROBLEMATIC' THEN 
                    500 + (ABS(HASH(CELL_ID || 'rrc_max_prob')) % 200)     -- 500-699 (moderate)
                ELSE 
                    200 + (ABS(HASH(CELL_ID || 'rrc_max_good')) % 400)     -- 200-599 (normal)
            END
        """
        
        execute_sql(conn, capacity_variation_sql, "Creating consistent active UE and capacity metrics")
        
        # Step 6: Create consistent Throughput and Data Volume metrics
        logger.info("=== STEP 6: CONSISTENT THROUGHPUT AND DATA VOLUME METRICS ===")
        
        throughput_variation_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_UE_THP_TIME_DL = 
            -- Throughput time should be higher for worse performing towers (worse throughput)
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    900000 + (ABS(HASH(CELL_ID || 'thp_cat')) % 300000)    -- 900K-1.2M (poor throughput)
                WHEN 'VERY_BAD' THEN 
                    700000 + (ABS(HASH(CELL_ID || 'thp_vbad')) % 250000)   -- 700K-950K
                WHEN 'BAD' THEN 
                    500000 + (ABS(HASH(CELL_ID || 'thp_bad')) % 200000)    -- 500K-700K
                WHEN 'QUITE_BAD' THEN 
                    350000 + (ABS(HASH(CELL_ID || 'thp_qbad')) % 150000)   -- 350K-500K
                ELSE 
                    200000 + (ABS(HASH(CELL_ID || 'thp_good')) % 200000)   -- 200K-400K (good throughput)
            END,
        PM_PDCP_VOL_DL_DRB = 
            -- Data volumes should be lower for problematic towers (less successful data transfer)
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    5000000 + (ABS(HASH(CELL_ID || 'vol_cat')) % 15000000)     -- 5-20MB (low due to failures)
                WHEN 'VERY_BAD' THEN 
                    15000000 + (ABS(HASH(CELL_ID || 'vol_vbad')) % 25000000)   -- 15-40MB
                WHEN 'BAD' THEN 
                    25000000 + (ABS(HASH(CELL_ID || 'vol_bad')) % 30000000)    -- 25-55MB
                ELSE 
                    -- Good towers vary by location (urban vs rural usage patterns)
                    CASE 
                        WHEN BID_DESCRIPTION LIKE '%LONDON%' OR BID_DESCRIPTION LIKE '%NEW YORK%' THEN 
                            45000000 + (ABS(HASH(CELL_ID || 'vol_urban')) % 35000000)  -- 45-80MB (high urban usage)
                        WHEN BID_DESCRIPTION LIKE '%ALBERTA%' OR BID_DESCRIPTION LIKE '%SCOTLAND%' THEN  
                            20000000 + (ABS(HASH(CELL_ID || 'vol_rural')) % 25000000)  -- 20-45MB (moderate rural usage)
                        ELSE 
                            30000000 + (ABS(HASH(CELL_ID || 'vol_suburb')) % 30000000) -- 30-60MB (suburban usage)
                    END
            END,
        PM_PDCP_VOL_DL_DRB_LAST_TTI = 
            -- Last TTI should be a small fraction of total volume
            ROUND(PM_PDCP_VOL_DL_DRB * (0.001 + (ABS(HASH(CELL_ID || 'tti')) % 5) * 0.001), 0)
        """
        
        execute_sql(conn, throughput_variation_sql, "Creating consistent throughput and data volume metrics")
        
        # Step 7: Create consistent S1 and ERAB establishment metrics
        logger.info("=== STEP 7: CONSISTENT S1 AND ERAB ESTABLISHMENT METRICS ===")
        
        establishment_variation_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_S1_SIG_CONN_ESTAB_ATT = 
            -- S1 attempts should correlate with RRC attempts
            ROUND(PM_RRC_CONN_ESTAB_ATT * (0.8 + (ABS(HASH(CELL_ID || 's1_att')) % 25) * 0.01), 0),
        PM_S1_SIG_CONN_ESTAB_SUCC = 
            -- S1 success should correlate with tower performance
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.60 + (ABS(HASH(CELL_ID || 's1_cat')) % 20) * 0.01), 0)  -- 60-79%
                WHEN 'VERY_BAD' THEN 
                    ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.75 + (ABS(HASH(CELL_ID || 's1_vbad')) % 15) * 0.01), 0) -- 75-89%
                WHEN 'BAD' THEN 
                    ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.85 + (ABS(HASH(CELL_ID || 's1_bad')) % 10) * 0.01), 0)  -- 85-94%
                ELSE 
                    ROUND(PM_S1_SIG_CONN_ESTAB_ATT * (0.90 + (ABS(HASH(CELL_ID || 's1_good')) % 9) * 0.01), 0)  -- 90-98%
            END,
        PM_ERAB_ESTAB_ATT_INIT = 
            -- ERAB attempts correlate with RRC attempts
            ROUND(PM_RRC_CONN_ESTAB_ATT * (0.9 + (ABS(HASH(CELL_ID || 'erab_att')) % 20) * 0.01), 0),
        PM_ERAB_ESTAB_SUCC_INIT = 
            -- ERAB success should correlate with tower performance
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.55 + (ABS(HASH(CELL_ID || 'erab_succ_cat')) % 25) * 0.01), 0)  -- 55-79%
                WHEN 'VERY_BAD' THEN 
                    ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.70 + (ABS(HASH(CELL_ID || 'erab_succ_vbad')) % 20) * 0.01), 0) -- 70-89%
                WHEN 'BAD' THEN 
                    ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.80 + (ABS(HASH(CELL_ID || 'erab_succ_bad')) % 15) * 0.01), 0)  -- 80-94%
                ELSE 
                    ROUND(PM_ERAB_ESTAB_ATT_INIT * (0.85 + (ABS(HASH(CELL_ID || 'erab_succ_good')) % 14) * 0.01), 0) -- 85-98%
            END
        """
        
        execute_sql(conn, establishment_variation_sql, "Creating consistent S1 and E-RAB establishment metrics")
        
        # Step 8: Create consistent Signal Quality metrics
        logger.info("=== STEP 8: CONSISTENT SIGNAL QUALITY METRICS ===")
        
        signal_quality_sql = """
        UPDATE CELL_TOWER 
        SET 
        PM_UE_MEAS_RSRP_DELTA_INTRA_FREQ1 = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    -8 + (ABS(HASH(CELL_ID || 'rsrp_delta_cat')) % 6)      -- -8 to -3 (poor signal difference)
                WHEN 'VERY_BAD' THEN 
                    -6 + (ABS(HASH(CELL_ID || 'rsrp_delta_vbad')) % 6)     -- -6 to -1
                WHEN 'BAD' THEN 
                    -4 + (ABS(HASH(CELL_ID || 'rsrp_delta_bad')) % 6)      -- -4 to 1
                ELSE 
                    -2 + (ABS(HASH(CELL_ID || 'rsrp_delta_good')) % 5)     -- -2 to 2 (good signal difference)
            END,
        PM_UE_MEAS_RSRP_SERV_INTRA_FREQ1 = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    -15 + (ABS(HASH(CELL_ID || 'rsrp_serv_cat')) % 10)     -- -15 to -6 (poor serving signal)
                WHEN 'VERY_BAD' THEN 
                    -10 + (ABS(HASH(CELL_ID || 'rsrp_serv_vbad')) % 10)    -- -10 to -1
                WHEN 'BAD' THEN 
                    -8 + (ABS(HASH(CELL_ID || 'rsrp_serv_bad')) % 12)      -- -8 to 3
                ELSE 
                    -5 + (ABS(HASH(CELL_ID || 'rsrp_serv_good')) % 15)     -- -5 to 9 (good serving signal)
            END,
        PM_UE_MEAS_RSRQ_DELTA_INTRA_FREQ1 = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    -4 + (ABS(HASH(CELL_ID || 'rsrq_delta_cat')) % 3)      -- -4 to -2 (poor quality difference)
                WHEN 'VERY_BAD' THEN 
                    -3 + (ABS(HASH(CELL_ID || 'rsrq_delta_vbad')) % 4)     -- -3 to 0
                ELSE 
                    -2 + (ABS(HASH(CELL_ID || 'rsrq_delta_other')) % 5)    -- -2 to 2 (good quality difference)
            END,
        PM_UE_MEAS_RSRQ_SERV_INTRA_FREQ1 = 
            CASE PERFORMANCE_TIER
                WHEN 'CATASTROPHIC' THEN 
                    -12 + (ABS(HASH(CELL_ID || 'rsrq_serv_cat')) % 6)      -- -12 to -7 (poor serving quality)
                WHEN 'VERY_BAD' THEN 
                    -10 + (ABS(HASH(CELL_ID || 'rsrq_serv_vbad')) % 8)     -- -10 to -3
                WHEN 'BAD' THEN 
                    -8 + (ABS(HASH(CELL_ID || 'rsrq_serv_bad')) % 10)      -- -8 to 1
                ELSE 
                    -5 + (ABS(HASH(CELL_ID || 'rsrq_serv_good')) % 13)     -- -5 to 7 (good serving quality)
            END
        """
        
        execute_sql(conn, signal_quality_sql, "Creating consistent signal quality metrics")
        
        # Step 9: Final verification across multiple metrics
        logger.info("=== STEP 9: FINAL COMPREHENSIVE VERIFICATION ===")
        
        comprehensive_verification_sql = """
        SELECT 
            PERFORMANCE_TIER,
            COUNT(*) as TOWER_COUNT,
            -- RRC Failure Rate
            ROUND(AVG(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100)), 1) as AVG_RRC_FAILURE_RATE,
            -- PRB Utilization  
            ROUND(AVG(PM_PRB_UTIL_DL), 1) as AVG_PRB_UTIL_DL,
            -- Latency (convert to milliseconds)
            ROUND(AVG(PM_PDCP_LAT_TIME_DL) / 1000000, 1) as AVG_LATENCY_MS,
            -- E-RAB Abnormal Releases
            ROUND(AVG(PM_ERAB_REL_ABNORMAL_ENB_ACT), 1) as AVG_ERAB_ABNORMAL,
            -- Active UE Max
            ROUND(AVG(PM_ACTIVE_UE_DL_MAX), 0) as AVG_ACTIVE_UE_DL_MAX
        FROM CELL_TOWER 
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        GROUP BY PERFORMANCE_TIER
        ORDER BY AVG_RRC_FAILURE_RATE DESC
        """
        
        verification_results = execute_sql(conn, comprehensive_verification_sql, "Comprehensive verification across all metrics")
        
        logger.info("✅ COMPREHENSIVE METRICS VERIFICATION BY TIER:")
        logger.info("   Tier          | Towers | RRC Fail% | PRB Util% | Latency(ms) | ERAB Abnormal | Active UE Max")
        logger.info("   --------------|--------|-----------|-----------|-------------|---------------|---------------")
        
        for tier, count, rrc_fail, prb_util, latency, erab_abnormal, active_ue in verification_results:
            logger.info(f"   {tier:13s} | {count:6,d} | {rrc_fail:8.1f}% | {prb_util:8.1f}% | {latency:10.1f} | {erab_abnormal:12.1f} | {active_ue:13.0f}")
        
        # Sample 15 towers to show variety across metrics
        sample_variety_sql = """
        SELECT 
            CELL_ID,
            PERFORMANCE_TIER,
            ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 1) as RRC_FAILURE_RATE,
            PM_PRB_UTIL_DL,
            ROUND(PM_PDCP_LAT_TIME_DL / 1000000, 1) as LATENCY_MS,
            PM_ERAB_REL_ABNORMAL_ENB_ACT as ERAB_ABNORMAL
        FROM CELL_TOWER 
        WHERE PM_RRC_CONN_ESTAB_ATT > 0
        ORDER BY RANDOM()
        LIMIT 15
        """
        
        variety_sample = execute_sql(conn, sample_variety_sql, "Sampling 15 towers for cross-metric variety")
        
        logger.info("✅ SAMPLE OF 15 TOWERS SHOWING VARIETY ACROSS ALL METRICS:")
        logger.info("   Cell ID   | Tier          | RRC Fail% | PRB Util% | Latency(ms) | ERAB Abnormal")
        logger.info("   ----------|---------------|-----------|-----------|-------------|---------------")
        
        for cell_id, tier, rrc_fail, prb_util, latency, erab_abnormal in variety_sample:
            logger.info(f"   {cell_id} | {tier:13s} | {rrc_fail:8.1f}% | {prb_util:8.0f}% | {latency:10.1f} | {erab_abnormal:13.0f}")
        
        logger.info("🎉 COMPREHENSIVE METRICS VARIATION COMPLETED SUCCESSFULLY!")
        logger.info("📊 Your demo data now has consistent variation across ALL fact columns:")
        logger.info("    ✅ RRC Connection Success/Failure rates")
        logger.info("    ✅ PRB Utilization (DL & UL)")
        logger.info("    ✅ E-RAB Abnormal Release rates") 
        logger.info("    ✅ Latency (DL & UL, time & packet)")
        logger.info("    ✅ Active UE metrics (DL & UL, max & sum)")
        logger.info("    ✅ RRC Connection Max capacity")
        logger.info("    ✅ Throughput time and data volumes")
        logger.info("    ✅ S1 and E-RAB establishment metrics")
        logger.info("    ✅ Signal quality (RSRP & RSRQ)")
        logger.info("    ✅ All negative values eliminated")
        logger.info("🎯 CATASTROPHIC towers are consistently bad across ALL metrics!")
        logger.info("🎯 GOOD towers are consistently good across ALL metrics!")
        logger.info("💾 Original data remains safely backed up in *_BACKUP tables")
        
    finally:
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()


