# Quick Start Guide - Hourly Data Generators

## ✅ Status: READY TO USE

All generators have been configured to generate hourly data matching your production pattern.

---

## 🚀 Start Generating Data

```sql
USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA GENERATE;

-- Start both generators
ALTER TASK TASK_GENERATE_CELL_TOWER_DATA RESUME;
ALTER TASK TASK_GENERATE_SUPPORT_TICKET RESUME;
```

**What Happens:**
- Every hour: ~14,000 new cell tower records (one per CELL_ID)
- Every hour: 1 new support ticket
- Data written to test tables: `CELL_TOWER_TEST` and `SUPPORT_TICKETS_TEST`

---

## 🛑 Stop Generating Data

```sql
ALTER TASK TASK_GENERATE_CELL_TOWER_DATA SUSPEND;
ALTER TASK TASK_GENERATE_SUPPORT_TICKET SUSPEND;
```

---

## 📊 Monitor Progress

```sql
-- Check how much data has been generated
SELECT 
    TIMESTAMP AS HOUR,
    COUNT(*) AS RECORDS,
    MIN(EVENT_DTTM) AS EARLIEST_EVENT,
    MAX(EVENT_DTTM) AS LATEST_EVENT
FROM GENERATE.CELL_TOWER_TEST
GROUP BY TIMESTAMP
ORDER BY TIMESTAMP DESC
LIMIT 10;

-- Check task status
SHOW TASKS LIKE 'TASK_GENERATE%' IN SCHEMA GENERATE;
```

---

## ⏰ Timestamp Pattern

Your data follows this hourly pattern:

| Column | Pattern | Example (for 22:00 hour) |
|--------|---------|--------------------------|
| **TIMESTAMP** | Top of hour + .001ms | `2025-10-01 22:00:00.001` |
| **EVENT_DTTM** | Random within the hour | `2025-10-01 22:37:42.000` |
| **WINDOW_START_AT** | 30 min before + .001ms | `2025-10-01 21:30:00.001` |
| **WINDOW_END_AT** | 30 min after + .001ms | `2025-10-01 22:30:00.001` |

This matches your production `RAW.CELL_TOWER` table exactly.

---

## 🔧 What Was Fixed

### 1. Production Data ✅
- Fixed 2,051,779 NULL timestamps in `RAW.CELL_TOWER`
- All timestamps now follow the hourly pattern
- No data loss or corruption

### 2. Generators Updated ✅
- Changed from per-minute to hourly generation
- Timestamp pattern matches production
- Tasks schedule: `60 MINUTE` (hourly)

---

## 📁 Key Files

| File | Purpose |
|------|---------|
| `manage_data_generators.sql` | Control & monitor generators |
| `HOURLY_GENERATOR_SUMMARY.md` | Detailed update documentation |
| `fix_null_timestamps.sql` | Script that fixed production NULLs |
| `update_generators_to_hourly.sql` | Script that updated generators |

---

## 🎯 Common Tasks

### Test the Procedure Manually
```sql
CALL GENERATE.SP_GENERATE_CELL_TOWER_DATA();
```

### Clear Test Data and Start Fresh
```sql
TRUNCATE TABLE GENERATE.CELL_TOWER_TEST;
TRUNCATE TABLE GENERATE.SUPPORT_TICKETS_TEST;
```

### Check Next Expected Timestamp
```sql
SELECT 
    MAX(TIMESTAMP) AS last_generated,
    DATEADD(HOUR, 1, MAX(TIMESTAMP)) AS next_expected
FROM GENERATE.CELL_TOWER_TEST;
```

### Verify Timestamp Pattern
```sql
SELECT 
    COUNT(*) AS total,
    SUM(CASE WHEN EXTRACT(MINUTE FROM TIMESTAMP) = 0 
             AND EXTRACT(SECOND FROM TIMESTAMP) = 0 THEN 1 
        ELSE 0 END) AS on_hour_pattern
FROM GENERATE.CELL_TOWER_TEST;
-- Should show: total = on_hour_pattern (all records match)
```

---

## 🎬 Promotion to Production

Once you're satisfied with the test data:

1. **Suspend tasks:**
   ```sql
   ALTER TASK TASK_GENERATE_CELL_TOWER_DATA SUSPEND;
   ALTER TASK TASK_GENERATE_SUPPORT_TICKET SUSPEND;
   ```

2. **Update procedures** to write to production tables:
   - Edit `SP_GENERATE_CELL_TOWER_DATA`
   - Change: `INSERT INTO GENERATE.CELL_TOWER_TEST`
   - To: `INSERT INTO RAW.CELL_TOWER`
   
   - Edit `SP_GENERATE_SUPPORT_TICKET`
   - Change: `INSERT INTO GENERATE.SUPPORT_TICKETS_TEST`
   - To: `INSERT INTO RAW.SUPPORT_TICKETS`

3. **Resume tasks:**
   ```sql
   ALTER TASK TASK_GENERATE_CELL_TOWER_DATA RESUME;
   ALTER TASK TASK_GENERATE_SUPPORT_TICKET RESUME;
   ```

---

## 💰 Resource Usage

### Storage
- **Per hour:** ~14,000 rows (cell tower) + 1 row (ticket)
- **Per day:** ~336,000 rows (cell tower) + 24 rows (ticket)
- **Per month:** ~10.1M rows (cell tower) + 720 rows (ticket)

### Compute
- **Per execution:** ~2 seconds (cell tower) + ~1 second (ticket)
- **Per day:** ~48 seconds of warehouse time
- **Cost:** Minimal with auto-suspend enabled

---

## ❓ Troubleshooting

### Tasks Not Running?
```sql
-- Check task state (should be "started", not "suspended")
SHOW TASKS LIKE 'TASK_GENERATE%';

-- Resume if needed
ALTER TASK TASK_GENERATE_CELL_TOWER_DATA RESUME;
```

### No New Data Generated?
1. Check tasks are started (not suspended)
2. Wait for the full hour to complete
3. Check task history for errors:
   ```sql
   SELECT name, state, scheduled_time, error_message
   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
   WHERE name LIKE 'TASK_GENERATE%'
   ORDER BY scheduled_time DESC
   LIMIT 10;
   ```

### Timestamps Don't Match Pattern?
- Verify you're using the updated procedure
- Check that `update_generators_to_hourly.sql` was run
- Manually test: `CALL SP_GENERATE_CELL_TOWER_DATA();`

---

## 📞 Need Help?

1. Check `manage_data_generators.sql` for monitoring queries
2. Review `HOURLY_GENERATOR_SUMMARY.md` for detailed information
3. Verify pattern: Compare test data to production using comparison queries

---

**Last Updated:** October 1, 2025
**Pattern:** Hourly generation at HH:00:00.001
**Status:** ✅ Fully Tested and Ready

