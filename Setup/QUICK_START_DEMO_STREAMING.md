# Quick Start Guide - Demo Streaming Mode

## ✅ Status: READY TO USE

All generators have been configured for **DEMO STREAMING MODE** - tasks run every minute, generating data with timestamps that increment by 1 hour.

**Demo Effect:** 1 minute of real time = 1 hour of data time (perfect for demos!)

---

## 🚀 Start Generating Data

```sql
USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA GENERATE;

-- Start both generators (SERVERLESS tasks)
ALTER TASK TASK_GENERATE_CELL_TOWER_DATA RESUME;
ALTER TASK TASK_GENERATE_SUPPORT_TICKET RESUME;
```

**What Happens:**
- Every MINUTE: ~14,000 new cell tower records (one per CELL_ID, timestamp +1 HOUR)
- Every MINUTE: 1 new support ticket
- Data written to test tables: `CELL_TOWER_TEST` and `SUPPORT_TICKETS_TEST`
- Serverless compute automatically managed by Snowflake

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

Your data uses timestamps that increment by 1 HOUR:

| Column | Pattern | Example (for 22:00 hour) |
|--------|---------|--------------------------|
| **TIMESTAMP** | Top of hour + .001ms | `2025-10-01 22:00:00.001` |
| **EVENT_DTTM** | Random within the hour | `2025-10-01 22:37:42.000` |
| **WINDOW_START_AT** | 30 min before + .001ms | `2025-10-01 21:30:00.001` |
| **WINDOW_END_AT** | 30 min after + .001ms | `2025-10-01 22:30:00.001` |

**Demo Mode:** Tasks generate these hourly timestamps every MINUTE (1 min real time = 1 hour data time).

---

## 🔧 Configuration History

### Production Data Fixed ✅
- Fixed 2,051,779 NULL timestamps in `RAW.CELL_TOWER`
- All timestamps now follow the hourly increment pattern
- No data loss or corruption

### Generators Configured for Demo Mode ✅
- Tasks run every MINUTE (demo streaming)
- Timestamps increment by 1 HOUR per execution
- Serverless compute (no warehouse needed)
- Effect: Fast-forward time for demos (60x speed)

---

## 📁 Key Files

| File | Purpose |
|------|---------|
| `setup_data_generators.sql` | Initial setup (run once) |
| `manage_data_generators.sql` | Control & monitor generators |
| `HOURLY_GENERATOR_SUMMARY.md` | Historical update documentation |

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
- Verify you're using the latest `setup_data_generators.sql`
- Manually test: `CALL SP_GENERATE_CELL_TOWER_DATA();`
- Check that timestamps increment by exactly 1 hour

### Tasks Not Running?
- Confirm they're resumed: `SHOW TASKS LIKE 'TASK_GENERATE%';`
- Check for EXECUTE MANAGED TASK privilege
- Review task history for errors

---

## 📞 Need Help?

1. Check `manage_data_generators.sql` for monitoring queries
2. Review `HOURLY_GENERATOR_SUMMARY.md` for historical information
3. Verify pattern: Compare test data to production using comparison queries

---

**Last Updated:** October 2, 2025
**Mode:** Demo Streaming (1 minute = 1 hour of data)
**Compute:** Serverless
**Status:** ✅ Fully Tested and Ready

