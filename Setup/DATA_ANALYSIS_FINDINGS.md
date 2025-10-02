# Data Generation Issues - Analysis Report

**Date:** October 2, 2025  
**Issue:** Generated test data doesn't match production data distributions

---

## 📊 Comparison: Production vs Test Data

### Analysis of 5,000 Random Rows

| Metric | Production (RAW.CELL_TOWER) | Test (GENERATE.CELL_TOWER_TEST) | Status |
|--------|---------------------------|----------------------------------|--------|
| **PM_PDCP_LAT_PKT_TRANS_DL** | Min: 69,900<br>Max: 2,521,426<br>Avg: 931,816<br>Median: 862,128 | Min: 1<br>Max: 15.91<br>Avg: 3.68<br>Median: 3.07 | ❌ **CRITICAL** |
| **PM_PDCP_LAT_TIME_UL** | Min: 21<br>Max: 103<br>Avg: 31<br>Median: 26 | Min: 20<br>Max: 111<br>Avg: 36<br>Median: 29 | ✅ ACCEPTABLE |
| **PM_PDCP_LAT_PKT_TRANS_UL** | Min: NULL<br>Max: NULL<br>Count: 0 (not populated) | Min: 2<br>Max: 33<br>Avg: 7<br>Median: 6 | ⚠️ MISMATCH |
| **PM_PRB_UTIL_UL** | Min: 0<br>Max: 72<br>Avg: 8<br>Median: 0 | Min: 3<br>Max: 81<br>Avg: 28<br>Median: 23 | ❌ WRONG DISTRIBUTION |
| **PM_PRB_UTIL_DL** | Min: 9<br>Max: 96<br>Avg: 33<br>Median: 23 | Min: 5<br>Max: 100<br>Avg: 40<br>Median: 33 | ✅ ACCEPTABLE |
| **PM_PDCP_LAT_TIME_DL** | Min: 10.24<br>Max: 49.36<br>Avg: 17<br>Median: 14 | *(need to check)* | ⚠️ TO VERIFY |

---

## 🐛 Root Cause Analysis

### Issue 1: PM_PDCP_LAT_PKT_TRANS_DL (CRITICAL)

**Current Code (WRONG):**
```sql
ROUND(lat_dl * (0.1 + (UNIFORM(0, 20, RANDOM()) * 0.01)), 2)
```
- Calculates as: lat_dl (8-55) × (0.1-0.3) = 0.8 to 16.5
- **This is completely wrong!**

**Correct Code (from regenerate_demo_data.sql):**
```sql
CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN UNIFORM(300000, 800000, RANDOM())
     ELSE UNIFORM(400000, 1200000, RANDOM()) END::DECIMAL(38,2)
```
- Should be: 300,000 to 1,200,000 (independent value)
- **Not derived from lat_dl at all!**

---

### Issue 2: PM_PDCP_LAT_PKT_TRANS_UL (MISMATCH)

**Current Code:**
```sql
TO_VARCHAR(ROUND(CAST(lat_ul AS NUMBER) * (0.1 + (UNIFORM(0, 20, RANDOM()) * 0.01)), 2))
```
- Calculates as: lat_ul (20-111) × (0.1-0.3) = 2 to 33
- **Produces values, but production has NULL**

**Correct Code (from regenerate_demo_data.sql):**
```sql
CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN TO_VARCHAR(UNIFORM(200000, 600000, RANDOM()))
     ELSE TO_VARCHAR(UNIFORM(300000, 900000, RANDOM())) END
```
- Should be: TO_VARCHAR(200,000 to 900,000)
- **But production data shows NULL values**

**Decision:** Since production is NULL, we should either:
- Option A: Set to NULL to match production exactly
- Option B: Generate large values (200K-900K) as strings for future use

---

### Issue 3: PM_PRB_UTIL_UL (WRONG DISTRIBUTION)

**Current Code:**
```sql
ROUND(prb_dl * (0.6 + (UNIFORM(0, 21, RANDOM()) * 0.01)), 0)
```
- Calculates as: prb_dl (5-100) × (0.6-0.81) = 3 to 81
- Average: 28, Median: 23
- **Distribution is too high!**

**Production Reality:**
- Min: 0, Max: 72
- Average: 8, **Median: 0** ← Most values are very low or zero!
- Distribution is heavily skewed toward low values

**Correct Approach:**
The production data shows that uplink utilization is much lower than downlink and heavily skewed toward zero. We need a formula that:
1. Allows zeros (common)
2. Creates a skewed distribution (median 0, average 8)
3. Occasionally has higher values (up to 72)

**Proposed Fix:**
```sql
CASE 
    WHEN UNIFORM(1, 100, RANDOM()) <= 50 THEN 0  -- 50% are zero
    WHEN UNIFORM(1, 100, RANDOM()) <= 80 THEN UNIFORM(1, 15, RANDOM())  -- 30% low values
    WHEN UNIFORM(1, 100, RANDOM()) <= 95 THEN UNIFORM(15, 40, RANDOM()) -- 15% medium
    ELSE UNIFORM(40, 72, RANDOM())  -- 5% high
END::DECIMAL(38,2)
```

---

## 🔧 Required Fixes

### Priority 1: PM_PDCP_LAT_PKT_TRANS_DL (CRITICAL)
Change from calculated to independent large value:
```sql
-- OLD (WRONG):
ROUND(lat_dl * (0.1 + (UNIFORM(0, 20, RANDOM()) * 0.01)), 2)

-- NEW (CORRECT):
CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN UNIFORM(300000, 800000, RANDOM())
     ELSE UNIFORM(400000, 1200000, RANDOM()) END::DECIMAL(38,2)
```

### Priority 2: PM_PRB_UTIL_UL (HIGH)
Change to skewed distribution with many zeros:
```sql
-- OLD (WRONG):
ROUND(prb_dl * (0.6 + (UNIFORM(0, 21, RANDOM()) * 0.01)), 0)

-- NEW (CORRECT):
CASE 
    WHEN UNIFORM(1, 100, RANDOM()) <= 50 THEN 0
    WHEN UNIFORM(1, 100, RANDOM()) <= 80 THEN UNIFORM(1, 15, RANDOM())
    WHEN UNIFORM(1, 100, RANDOM()) <= 95 THEN UNIFORM(15, 40, RANDOM())
    ELSE UNIFORM(40, 72, RANDOM())
END::DECIMAL(38,2)
```

### Priority 3: PM_PDCP_LAT_PKT_TRANS_UL (MEDIUM)
Set to NULL to match production:
```sql
-- OLD:
TO_VARCHAR(ROUND(CAST(lat_ul AS NUMBER) * (0.1 + (UNIFORM(0, 20, RANDOM()) * 0.01)), 2))

-- NEW (OPTION A - Match Production):
NULL

-- NEW (OPTION B - Generate for future):
CASE WHEN BID_DESCRIPTION LIKE '%(5G)%' THEN TO_VARCHAR(UNIFORM(200000, 600000, RANDOM()))
     ELSE TO_VARCHAR(UNIFORM(300000, 900000, RANDOM())) END
```

**Recommendation:** Use Option A (NULL) to match current production exactly.

---

## 📝 Implementation Plan

1. **Update setup_data_generators.sql**
   - Fix PM_PDCP_LAT_PKT_TRANS_DL calculation
   - Fix PM_PRB_UTIL_UL distribution
   - Set PM_PDCP_LAT_PKT_TRANS_UL to NULL

2. **Test Changes**
   - Truncate CELL_TOWER_TEST
   - Run procedure multiple times
   - Verify distributions match production

3. **Validate**
   - Compare 5,000 rows again
   - Ensure min/max/avg/median match production

4. **Document**
   - Update generator documentation
   - Note why certain fields are NULL

---

## ✅ Verification Checklist

After fixes:
- [ ] PM_PDCP_LAT_PKT_TRANS_DL: 300K-1.2M range
- [ ] PM_PRB_UTIL_UL: Median near 0, average near 8
- [ ] PM_PDCP_LAT_PKT_TRANS_UL: NULL values
- [ ] Run full comparison again
- [ ] Verify with user

---

**Status:** Analysis Complete - Ready for Implementation  
**Next Step:** Apply fixes to setup_data_generators.sql

