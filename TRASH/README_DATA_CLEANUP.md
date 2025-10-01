# Master Data Cleanup Script

## Overview
This repository contains a consolidated master script (`master_data_cleanup.py`) that manages all synthetic data transformations for the Telecom Network Optimization demo.

## Key Features Fixed

### ✅ Latency Values (CRITICAL FIX)
- **Before**: 3,000,000+ ms (3 million milliseconds = 50 minutes!)
- **After**: 5-50 ms (realistic mobile network latency)
- **Columns Fixed**:
  - `PM_PDCP_LAT_TIME_DL`: 3-50ms depending on tower performance
  - `PM_PDCP_LAT_PKT_TRANS_DL`: 10-30% of time latency
  - `PM_PDCP_LAT_TIME_UL`: 110-150% of DL latency
  - `PM_PDCP_LAT_PKT_TRANS_UL`: Correlates with DL packet latency

### ✅ RRC Connection Failure Rates  
- Wide variation: 1-70% failure rates
- Performance tiers: CATASTROPHIC (50-70%), VERY_BAD (30-50%), BAD (20-30%), GOOD (1-6%)

### ✅ E-RAB Abnormal Release Percentages
- Proper percentages: 0.1-25%
- Not raw counts (was 2-199)

### ✅ PRB Utilization
- Realistic ranges: 5-99%
- Urban towers: Higher utilization (25-54%)
- Rural towers: Lower utilization (5-24%)

### ✅ Cause Code Distribution
- Interesting failure patterns
- Top failure causes stand out
- Realistic distribution (not uniform ~1k each)

### ✅ Support Ticket Correlation
- Negative sentiment tickets (70%) correlated with problematic towers

## Usage

### Running the Master Script

```bash
python3 master_data_cleanup.py
```

This single script will:
1. Restore data from backup tables
2. Create performance tiers
3. Fix all latency values to realistic ranges
4. Apply wide variation to RRC failure rates
5. Set proper E-RAB abnormal percentages
6. Apply PRB utilization variation
7. Redistribute cause codes for interesting patterns
8. Correlate support tickets with bad towers
9. Verify final data quality

**Expected Runtime**: ~35-40 seconds

### Output
The script provides comprehensive logging showing:
- Each transformation step
- Final verification with statistics by performance tier
- Confirmation of realistic ranges for all metrics

## Data Backup

Your original data is always preserved in backup tables:
- `CELL_TOWER_BACKUP`
- `SUPPORT_TICKETS_BACKUP`

The master script recreates the working tables from these backups each time it runs, ensuring you can always reset to a clean state.

## Verification Queries

### Check Latency Values (Should be 5-50ms)
```sql
SELECT 
    BID_DESCRIPTION as PROVINCE,
    COUNT(*) as TOWER_COUNT,
    ROUND(AVG(PM_PDCP_LAT_TIME_DL) / 1000, 2) as AVG_LATENCY_MS,
    ROUND(MIN(PM_PDCP_LAT_TIME_DL) / 1000, 2) as MIN_LATENCY_MS,
    ROUND(MAX(PM_PDCP_LAT_TIME_DL) / 1000, 2) as MAX_LATENCY_MS
FROM CELL_TOWER
WHERE BID_DESCRIPTION LIKE '%ALBERTA%' 
   OR BID_DESCRIPTION LIKE '%BRITISH COLUMBIA%'
GROUP BY BID_DESCRIPTION
ORDER BY AVG_LATENCY_MS DESC;
```

### Check Cause Code Distribution
```sql
SELECT
  cause_code_short_description,
  COUNT(*) AS frequency
FROM CELL_TOWER
WHERE event_date >= DATE_TRUNC('MONTH', (SELECT MAX(event_date) FROM CELL_TOWER))
  AND cause_code_short_description IS NOT NULL
  AND cause_code_short_description != 'CALL_OK'
GROUP BY cause_code_short_description
ORDER BY frequency DESC
LIMIT 15;
```

### Check Performance Tier Distribution
```sql
SELECT 
    PERFORMANCE_TIER,
    COUNT(*) as TOWER_COUNT,
    ROUND(AVG(((PM_RRC_CONN_ESTAB_ATT - PM_RRC_CONN_ESTAB_SUCC) / PM_RRC_CONN_ESTAB_ATT * 100)), 1) as AVG_RRC_FAILURE_RATE,
    ROUND(AVG(PM_PDCP_LAT_TIME_DL) / 1000, 1) as AVG_LATENCY_MS
FROM CELL_TOWER 
WHERE PM_RRC_CONN_ESTAB_ATT > 0
GROUP BY PERFORMANCE_TIER
ORDER BY AVG_RRC_FAILURE_RATE DESC;
```

## Old Scripts (TRASH Directory)

All individual scripts have been moved to the `TRASH/` directory:
- `restore_and_enhance.py`
- `create_varied_demo_data.py`
- `fix_ticket_correlation.py`
- `fix_all_metrics_variation.py`
- `create_wide_variation.py`
- `fix_erab_percentages.py`
- `fix_cause_code_distribution.py`
- `verify_cause_code_distribution.py`
- `final_demo_verification.py`
- And other experimental scripts

These are kept for reference but are no longer needed. The `master_data_cleanup.py` script consolidates all their functionality.

## Troubleshooting

### Script Fails on Restore
If you get column count mismatch errors, the backup tables may have been modified. Contact admin to restore from original source.

### Latency Values Still Too High
Ensure you're dividing by 1000 in your queries:
```sql
PM_PDCP_LAT_TIME_DL / 1000  -- Converts microseconds to milliseconds
```

### Need to Reset Everything
Just run the master script again:
```bash
python3 master_data_cleanup.py
```

It's idempotent and will recreate everything from backup.

## Contact
For issues or questions, contact the demo data team.
