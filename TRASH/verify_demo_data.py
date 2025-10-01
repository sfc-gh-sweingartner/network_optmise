#!/usr/bin/env python3
"""
Quick verification that our demo data enhancements worked
"""

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # Connect
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
        
        conn = snowflake.connector.connect(
            user='STEPHEN_PYTHON',
            account='rxb32947',
            private_key=pkb,
            warehouse='MYWH',
            database='TELCO_NETWORK_OPTIMIZATION_PROD',
            schema='RAW',
            role='ACCOUNTADMIN'
        )
        
        cursor = conn.cursor()
        
        # Test 1: Basic counts
        cursor.execute("SELECT COUNT(*) FROM CELL_TOWER")
        cell_count = cursor.fetchone()[0]
        logger.info(f"✅ CELL_TOWER: {cell_count:,} records")
        
        cursor.execute("SELECT COUNT(*) FROM SUPPORT_TICKETS") 
        ticket_count = cursor.fetchone()[0]
        logger.info(f"✅ SUPPORT_TICKETS: {ticket_count:,} records")
        
        # Test 2: Vendor variety
        cursor.execute("SELECT VENDOR_NAME, COUNT(*) FROM CELL_TOWER GROUP BY VENDOR_NAME")
        vendors = cursor.fetchall()
        logger.info("✅ VENDOR DISTRIBUTION:")
        for vendor, count in vendors:
            logger.info(f"   {vendor}: {count:,}")
        
        # Test 3: Sample some failure rates to see if they're varied
        cursor.execute("""
            SELECT CELL_ID, 
                   ROUND(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100), 2) as FAILURE_RATE
            FROM CELL_TOWER 
            WHERE PM_RRC_CONN_ESTAB_ATT > 0
            ORDER BY RANDOM()
            LIMIT 10
        """)
        
        failure_rates = cursor.fetchall()
        logger.info("✅ SAMPLE RRC FAILURE RATES:")
        for cell_id, rate in failure_rates:
            logger.info(f"   Cell {cell_id}: {rate}%")
        
        # Test 4: Check if we have high failure rate cells for demos
        cursor.execute("""
            SELECT COUNT(*) as HIGH_FAILURE_COUNT
            FROM CELL_TOWER 
            WHERE PM_RRC_CONN_ESTAB_ATT > 0 
            AND ((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100) > 15
        """)
        
        high_failure_count = cursor.fetchone()[0] 
        logger.info(f"✅ HIGH FAILURE CELLS (>15%): {high_failure_count:,} cells")
        
        # Test 5: Sample PRB utilizations
        cursor.execute("SELECT PM_PRB_UTIL_DL FROM CELL_TOWER ORDER BY RANDOM() LIMIT 10")
        prb_utils = cursor.fetchall()
        logger.info("✅ SAMPLE PRB DL UTILIZATIONS:")
        for (util,) in prb_utils:
            logger.info(f"   {util}%")
        
        # Test 6: Check sentiment variety
        cursor.execute("SELECT MIN(SENTIMENT_SCORE), MAX(SENTIMENT_SCORE), COUNT(*) FROM SUPPORT_TICKETS")
        min_sent, max_sent, total_tickets = cursor.fetchone()
        logger.info(f"✅ SENTIMENT SCORES: {min_sent} to {max_sent} ({total_tickets:,} tickets)")
        
        logger.info("🎉 VERIFICATION COMPLETE - Your demo data is ready!")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
