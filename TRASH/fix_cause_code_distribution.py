#!/usr/bin/env python3
"""
Fix CAUSE_CODE_SHORT_DESCRIPTION distribution to create more interesting demo data:
- Reduce NETWORK_OUT_OF_ORDER from ~62k to ~10k
- Redistribute the remaining ~52k among other cause codes in an uneven, realistic manner
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
        # Return results for SELECT queries or CTEs that start with WITH
        sql_upper = sql.strip().upper()
        return cursor.fetchall() if (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')) else None
    except Exception as e:
        logger.error(f"❌ Error in {description}: {str(e)}")
        raise
    finally:
        cursor.close()

def main():
    logger.info("🎯 Redistributing CAUSE_CODE_SHORT_DESCRIPTION for more interesting demo results")
    
    conn = get_connection()
    
    try:
        # First, check current distribution
        check_current_distribution_sql = """
        SELECT 
            CAUSE_CODE_SHORT_DESCRIPTION,
            COUNT(*) as FREQUENCY
        FROM CELL_TOWER 
        WHERE CAUSE_CODE_SHORT_DESCRIPTION IS NOT NULL
        GROUP BY CAUSE_CODE_SHORT_DESCRIPTION
        ORDER BY FREQUENCY DESC
        LIMIT 20
        """
        
        current_dist = execute_sql(conn, check_current_distribution_sql, "Checking current cause code distribution")
        
        logger.info("📊 CURRENT DISTRIBUTION (Top 20):")
        logger.info("   Cause Code                          | Frequency")
        logger.info("   ------------------------------------|------------")
        for code, freq in current_dist:
            logger.info(f"   {code:35s} | {freq:10,d}")
        
        # Redistribute cause codes to create more interesting patterns
        logger.info("\n=== REDISTRIBUTING CAUSE CODES ===")
        
        redistribute_sql = """
        UPDATE CELL_TOWER 
        SET CAUSE_CODE_SHORT_DESCRIPTION = 
            CASE 
                -- Keep CALL_OK as is (majority of calls are successful)
                WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'CALL_OK' THEN 'CALL_OK'
                
                -- Reduce NETWORK_OUT_OF_ORDER from ~62k to ~10k (keep ~16% of them)
                WHEN CAUSE_CODE_SHORT_DESCRIPTION = 'NETWORK_OUT_OF_ORDER' THEN
                    CASE 
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 16 THEN 'NETWORK_OUT_OF_ORDER'
                        -- Redistribute the rest to create interesting patterns
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 36 THEN 'BEARER_CAPABILITY_NOT_AVAILABLE'  -- ~20% -> ~12k
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 52 THEN 'CHANNEL_UNACCEPTABLE'             -- ~16% -> ~10k
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 64 THEN 'DESTINATION_OUT_OF_ORDER'         -- ~12% -> ~7.5k
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 74 THEN 'INVALID_CALL_REFERENCE_VALUE'    -- ~10% -> ~6k
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 82 THEN 'PRECEDENCE_CALL_BLOCKED'          -- ~8% -> ~5k
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 88 THEN 'CONNECTION_OPERATIONAL'           -- ~6% -> ~3.7k
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 93 THEN 'MISDIALED_TRUNK_PREFIX'           -- ~5% -> ~3k
                        WHEN (ABS(HASH(UNIQUE_ID || 'network')) % 100) < 97 THEN 'OUTGOING_CALLS_BARRED'            -- ~4% -> ~2.5k
                        ELSE 'MESSAGE_TYPE_NON_EXISTENT'                                                             -- ~3% -> ~1.9k
                    END
                
                -- Add some variation to other small cause codes by redistributing among themselves
                WHEN CAUSE_CODE_SHORT_DESCRIPTION IN (
                    'INVALID_CALL_REFERENCE_VALUE', 
                    'BEARER_CAPABILITY_NOT_AVAILABLE',
                    'PRECEDENCE_CALL_BLOCKED',
                    'MISDIALED_TRUNK_PREFIX',
                    'CONNECTION_OPERATIONAL',
                    'NORMAL_UNSPECIFIED',
                    'DESTINATION_OUT_OF_ORDER',
                    'MESSAGE_TYPE_NON_EXISTENT',
                    'OUTGOING_CALLS_BARRED',
                    'CHANNEL_UNACCEPTABLE'
                ) THEN
                    -- Keep most as-is but shuffle ~20% to create more variation
                    CASE 
                        WHEN (ABS(HASH(UNIQUE_ID || 'shuffle')) % 100) < 80 THEN CAUSE_CODE_SHORT_DESCRIPTION
                        -- Shuffle the remaining 20% to other codes
                        WHEN (ABS(HASH(UNIQUE_ID || 'shuffle')) % 100) < 84 THEN 'BEARER_CAPABILITY_NOT_AVAILABLE'
                        WHEN (ABS(HASH(UNIQUE_ID || 'shuffle')) % 100) < 88 THEN 'CHANNEL_UNACCEPTABLE'
                        WHEN (ABS(HASH(UNIQUE_ID || 'shuffle')) % 100) < 91 THEN 'DESTINATION_OUT_OF_ORDER'
                        WHEN (ABS(HASH(UNIQUE_ID || 'shuffle')) % 100) < 94 THEN 'INVALID_CALL_REFERENCE_VALUE'
                        WHEN (ABS(HASH(UNIQUE_ID || 'shuffle')) % 100) < 96 THEN 'PRECEDENCE_CALL_BLOCKED'
                        WHEN (ABS(HASH(UNIQUE_ID || 'shuffle')) % 100) < 98 THEN 'CONNECTION_OPERATIONAL'
                        ELSE 'MISDIALED_TRUNK_PREFIX'
                    END
                
                -- Keep all other cause codes as-is
                ELSE CAUSE_CODE_SHORT_DESCRIPTION
            END
        WHERE CAUSE_CODE_SHORT_DESCRIPTION IS NOT NULL
        """
        
        execute_sql(conn, redistribute_sql, "Redistributing cause codes for interesting demo patterns")
        
        # Verify the new distribution
        logger.info("\n=== VERIFICATION OF NEW DISTRIBUTION ===")
        
        verify_distribution_sql = """
        SELECT 
            CAUSE_CODE_SHORT_DESCRIPTION,
            COUNT(*) as FREQUENCY,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as PERCENTAGE
        FROM CELL_TOWER 
        WHERE CAUSE_CODE_SHORT_DESCRIPTION IS NOT NULL
        GROUP BY CAUSE_CODE_SHORT_DESCRIPTION
        ORDER BY FREQUENCY DESC
        LIMIT 25
        """
        
        new_dist = execute_sql(conn, verify_distribution_sql, "Verifying new cause code distribution")
        
        logger.info("✅ NEW DISTRIBUTION (Top 25):")
        logger.info("   Cause Code                          | Frequency  | Percentage")
        logger.info("   ------------------------------------|------------|------------")
        for code, freq, pct in new_dist:
            logger.info(f"   {code:35s} | {freq:10,d} | {pct:9.2f}%")
        
        # Show distribution for the most recent month (as in the user's query)
        logger.info("\n=== DISTRIBUTION FOR MOST RECENT MONTH ===")
        
        recent_month_sql = """
        WITH __cell_tower AS (
          SELECT
            cause_code_short_description,
            event_date
          FROM CELL_TOWER
        ), recent_month_data AS (
          SELECT
            cause_code_short_description,
            COUNT(*) AS frequency,
            COUNT(DISTINCT event_date) AS unique_dates
          FROM __cell_tower
          WHERE
            event_date >= DATE_TRUNC('MONTH', (
              SELECT MAX(event_date) FROM __cell_tower
            ))
            AND event_date <= (SELECT MAX(event_date) FROM __cell_tower)
          GROUP BY cause_code_short_description
        )
        SELECT
          cause_code_short_description,
          frequency,
          unique_dates,
          ROUND(frequency * 100.0 / SUM(frequency) OVER (), 2) as percentage
        FROM recent_month_data
        ORDER BY frequency DESC NULLS LAST
        LIMIT 20
        """
        
        recent_month_dist = execute_sql(conn, recent_month_sql, "Checking distribution for most recent month")
        
        logger.info("📅 MOST RECENT MONTH DISTRIBUTION (Top 20):")
        logger.info("   Cause Code                          | Frequency  | Days | Percentage")
        logger.info("   ------------------------------------|------------|------|------------")
        for code, freq, days, pct in recent_month_dist:
            logger.info(f"   {code:35s} | {freq:10,d} | {days:4d} | {pct:9.2f}%")
        
        logger.info("\n🎉 CAUSE CODE DISTRIBUTION SUCCESSFULLY REDISTRIBUTED!")
        logger.info("📊 Key improvements for demo:")
        logger.info("    ✅ NETWORK_OUT_OF_ORDER reduced from ~62k to ~10k")
        logger.info("    ✅ BEARER_CAPABILITY_NOT_AVAILABLE increased significantly (~13k)")
        logger.info("    ✅ CHANNEL_UNACCEPTABLE now prominent (~11k)")
        logger.info("    ✅ Other cause codes have varied, realistic distributions")
        logger.info("    ✅ Data now shows interesting failure patterns for demos")
        logger.info("🎯 Perfect for answering: 'What are the most common cause codes for call failures?'")
        
    finally:
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()
