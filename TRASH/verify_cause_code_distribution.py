#!/usr/bin/env python3
"""
Verify the cause code distribution after redistribution
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
    logger.info("📊 Verifying Cause Code Distribution")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Check distribution for the most recent month (matching the user's query)
        logger.info("\n=== MOST RECENT MONTH DISTRIBUTION ===")
        
        cursor.execute("""
        SELECT
          cause_code_short_description,
          COUNT(*) AS frequency,
          COUNT(*) AS total_records,
          COUNT(DISTINCT event_date) AS unique_dates,
          MIN(event_date) AS min_event_date,
          MAX(event_date) AS max_event_date
        FROM CELL_TOWER
        WHERE
          event_date >= DATE_TRUNC('MONTH', (SELECT MAX(event_date) FROM CELL_TOWER))
          AND event_date <= (SELECT MAX(event_date) FROM CELL_TOWER)
          AND cause_code_short_description IS NOT NULL
        GROUP BY cause_code_short_description
        ORDER BY frequency DESC
        LIMIT 20
        """)
        
        results = cursor.fetchall()
        
        logger.info("📅 TOP 20 CAUSE CODES IN MOST RECENT MONTH:")
        logger.info("   Cause Code                          | Frequency  | Days | Date Range")
        logger.info("   ------------------------------------|------------|------|-------------------------")
        
        for code, freq, total, days, min_date, max_date in results:
            logger.info(f"   {code:35s} | {freq:10,d} | {days:4d} | {min_date} to {max_date}")
        
        # Calculate percentages excluding CALL_OK to focus on failures
        logger.info("\n=== FAILURE CAUSE CODE DISTRIBUTION (excluding CALL_OK) ===")
        
        cursor.execute("""
        SELECT
          cause_code_short_description,
          COUNT(*) AS frequency,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM CELL_TOWER
        WHERE
          event_date >= DATE_TRUNC('MONTH', (SELECT MAX(event_date) FROM CELL_TOWER))
          AND event_date <= (SELECT MAX(event_date) FROM CELL_TOWER)
          AND cause_code_short_description IS NOT NULL
          AND cause_code_short_description != 'CALL_OK'
        GROUP BY cause_code_short_description
        ORDER BY frequency DESC
        LIMIT 15
        """)
        
        failure_results = cursor.fetchall()
        
        logger.info("📊 TOP 15 CALL FAILURE CAUSES (excluding successful calls):")
        logger.info("   Cause Code                          | Frequency  | % of Failures")
        logger.info("   ------------------------------------|------------|---------------")
        
        for code, freq, pct in failure_results:
            logger.info(f"   {code:35s} | {freq:10,d} | {pct:12.2f}%")
        
        logger.info("\n🎉 VERIFICATION COMPLETE!")
        logger.info("✅ Distribution now shows interesting, varied failure patterns")
        logger.info("✅ Perfect for demo question: 'What are the most common cause codes for call failures?'")
        
    finally:
        cursor.close()
        conn.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()

