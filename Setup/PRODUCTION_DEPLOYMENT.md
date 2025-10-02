# Production Deployment - Data Generators

**Date:** October 2, 2025  
**Status:** ✅ PRODUCTION READY

---

## 🎯 Summary

The data generators have been successfully deployed to **production**. They now write directly to:
- `TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER`
- `TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS`

---

## ✅ Deployment Steps Completed

### 1. Data Quality Fixes ✅
- Fixed PM_PDCP_LAT_PKT_TRANS_DL (now 400K-1.2M, matches production)
- Fixed PM_PDCP_LAT_PKT_TRANS_UL (now NULL, matches production)
- Fixed PM_PRB_UTIL_UL (now skewed toward zero, matches production)

### 2. Production Configuration ✅
- Updated procedures to write to `RAW.CELL_TOWER` and `RAW.SUPPORT_TICKETS`
- Dropped test tables (`CELL_TOWER_TEST`, `SUPPORT_TICKETS_TEST`)
- Verified data generation with production tables

### 3. File Cleanup ✅
Moved obsolete files to `TRASH/Setup_Archive_20251002/`:
- `update_generators_to_hourly.sql` (integrated into main script)
- `fix_null_timestamps.sql` (one-time fix, already executed)
- `cell_tower.csv` / `support_tickets.csv` (old exports)
- `load_from_csv_backup.sql` (backup script, not needed)
- `README_BACKUP.md` / `README_DATA_GENERATORS.md` (old documentation)
- `HOURLY_GENERATOR_SUMMARY.md` (superseded by current docs)

---

## 📁 Production Files

### Essential Scripts
| File | Purpose | When to Use |
|------|---------|-------------|
| `setup_data_generators.sql` | Initial setup | **Run once** to create procedures and tasks |
| `START_DEMO.sql` | Start generators | Run to **begin** data generation |
| `STOP_DEMO.sql` | Stop generators | Run to **suspend** data generation |
| `manage_data_generators.sql` | Control & monitor | Advanced monitoring and control |

### Data Setup
| File | Purpose | When to Use |
|------|---------|-------------|
| `create_tables.sql` | Create table schemas | Run once for initial database setup |
| `regenerate_demo_data.sql` | Generate initial data | Run once to populate initial dataset |
| `master_data_cleanup.py` | Data quality fixes | Run after regenerate_demo_data.sql |

### Configuration
| File | Purpose | When to Use |
|------|---------|-------------|
| `mapbox_access_setup.sql` | Mapbox integration | For map visualizations |
| `connectMapBoxNoKey.sql` | Mapbox connection | For map visualizations |

### Documentation
| File | Purpose |
|------|---------|
| `DEMO_STREAMING_SUMMARY.md` | Complete technical documentation |
| `QUICK_START_DEMO_STREAMING.md` | Quick reference guide |
| `DATA_ANALYSIS_FINDINGS.md` | Data quality analysis |
| `FIXES_VERIFICATION.md` | Verification report |
| `PRODUCTION_DEPLOYMENT.md` | This file |

---

## 🚀 How to Use

### For a Colleague Setting Up From Scratch

1. **Create Database and Tables**
   ```sql
   @Setup/create_tables.sql
   ```

2. **Generate Initial Data**
   ```sql
   @Setup/regenerate_demo_data.sql
   ```

3. **Clean Up Data Quality**
   ```bash
   python Setup/master_data_cleanup.py
   ```

4. **Set Up Data Generators**
   ```sql
   @Setup/setup_data_generators.sql
   ```

5. **Start Demo Streaming**
   ```sql
   @Setup/START_DEMO.sql
   ```

6. **Stop When Done**
   ```sql
   @Setup/STOP_DEMO.sql
   ```

### For Daily Use (Demo Already Set Up)

**Start:**
```sql
@Setup/START_DEMO.sql
```

**Stop:**
```sql
@Setup/STOP_DEMO.sql
```

---

## 📊 What Gets Generated

| Component | Frequency | Volume | Details |
|-----------|-----------|--------|---------|
| Cell Tower Data | Every 1 MINUTE | ~14,000 records | One per CELL_ID, timestamp +1 HOUR |
| Support Tickets | Every 1 MINUTE | 1 ticket | Correlated with problematic towers |

**Demo Effect:** 1 minute of real time = 1 hour of data time (60x speed!)

---

## ⚙️ Technical Details

### Configuration
- **Compute Model:** Serverless (Snowflake manages resources)
- **Schedule:** 1 MINUTE intervals
- **Tasks:** 
  - `GENERATE.TASK_GENERATE_CELL_TOWER_DATA`
  - `GENERATE.TASK_GENERATE_SUPPORT_TICKET`

### Data Pattern
- **TIMESTAMP:** Top of hour + .001ms (e.g., `22:00:00.001`)
- **Increment:** +1 HOUR per execution
- **EVENT_DTTM:** Random time within the hour
- **WINDOW_START_AT:** 30 minutes before timestamp
- **WINDOW_END_AT:** 30 minutes after timestamp

### Reference Data
Stored in `GENERATE` schema:
- `REF_CELL_TOWER_ATTRIBUTES` - Cell tower properties
- `REF_COMPLAINT_TEXTS` - Sample support ticket texts
- `REF_CUSTOMER_NAMES` - Sample first names
- `REF_CUSTOMER_SURNAMES` - Sample last names
- `REF_EMAIL_DOMAINS` - Sample email domains

---

## 🔒 Safety Features

1. **Incremental Timestamps**
   - Reads `MAX(TIMESTAMP)` from production table
   - Always generates next hour (+1) to avoid duplicates

2. **Sequential Ticket IDs**
   - Reads `MAX(TICKET_ID)` from production table
   - Generates next sequential ID

3. **Suspend by Default**
   - Tasks are created in suspended state
   - Must explicitly RESUME to start generation

---

## 📈 Monitoring

### Check Task Status
```sql
SHOW TASKS LIKE 'TASK_GENERATE%' IN SCHEMA GENERATE;
```

### View Recent Data
```sql
SELECT TIMESTAMP, COUNT(*) 
FROM RAW.CELL_TOWER 
GROUP BY TIMESTAMP 
ORDER BY TIMESTAMP DESC 
LIMIT 10;
```

### View Task History
```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_GENERATE_CELL_TOWER_DATA'
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
```

---

## 💰 Cost Implications

**Serverless Compute:**
- ~2 seconds of compute per minute
- XSMALL serverless credits
- Approximately $0.01 per hour of demo streaming

**For typical demo:**
- 1 hour demo: ~$0.60
- Full day simulation: ~$14.40

---

## 🎬 Demo Scenarios

### Quick Demo (5 minutes)
- **Duration:** 5 minutes
- **Data Generated:** 5 hours of telco data
- **Use Case:** Show real-time dashboard updates

### Half-Day Analysis (12 minutes)
- **Duration:** 12 minutes
- **Data Generated:** 12 hours of telco data
- **Use Case:** Morning/afternoon trend analysis

### Full Day Simulation (24 minutes)
- **Duration:** 24 minutes
- **Data Generated:** 24 hours of telco data
- **Use Case:** Daily pattern analysis, peak hours

### Week Simulation (2.8 hours)
- **Duration:** 168 minutes
- **Data Generated:** 1 week of telco data
- **Use Case:** Weekly trend analysis, capacity planning

---

## ⚠️ Important Notes

1. **Production Data:** Generators now write to production tables. Use caution.

2. **Timestamp Continuity:** Each execution increments by 1 hour from the latest timestamp in the table.

3. **No Duplicates:** The generator checks existing timestamps to avoid duplicates.

4. **Always Suspend:** Remember to suspend tasks when not demoing to avoid unnecessary data generation.

---

## 🆘 Troubleshooting

### Tasks Not Running?
```sql
-- Check task state
SHOW TASKS LIKE 'TASK_GENERATE%';

-- If suspended, resume them
ALTER TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA RESUME;
ALTER TASK GENERATE.TASK_GENERATE_SUPPORT_TICKET RESUME;
```

### No New Data?
```sql
-- Check task execution history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_GENERATE_CELL_TOWER_DATA'
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;
```

### Data Quality Issues?
- Refer to `DATA_ANALYSIS_FINDINGS.md` for expected distributions
- Refer to `FIXES_VERIFICATION.md` for verification methods

---

## 📞 Support

**Documentation:**
- Quick Start: `QUICK_START_DEMO_STREAMING.md`
- Technical Details: `DEMO_STREAMING_SUMMARY.md`
- Data Quality: `DATA_ANALYSIS_FINDINGS.md`

**Key Scripts:**
- Setup: `setup_data_generators.sql`
- Control: `manage_data_generators.sql`
- Quick Start: `START_DEMO.sql`
- Quick Stop: `STOP_DEMO.sql`

---

**Deployment Date:** October 2, 2025  
**Deployed By:** Data Engineering Team  
**Status:** ✅ Production Ready  
**Version:** 1.0 - Demo Streaming Mode

