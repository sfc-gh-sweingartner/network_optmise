# Data Generator System

## Overview

This system provides automated data generation for the Telco Network Optimization project. It creates realistic cell tower performance data and customer support tickets that match the patterns and distributions of your existing production data.

## Architecture

### Components

1. **GENERATE Schema** - Dedicated schema for all generator components
2. **Test Tables** - `CELL_TOWER_TEST` and `SUPPORT_TICKETS_TEST` for safe testing
3. **Reference Tables** - Store reusable data patterns from production
4. **Stored Procedures** - Generate new data records
5. **Snowflake Tasks** - Automated execution every minute

### Data Generation Rates

- **Cell Tower Data**: One row per CELL_ID per minute (~50,000 rows/minute)
- **Support Tickets**: One ticket per minute

## Setup Instructions

### Step 1: Run Setup Script

```sql
-- Execute the setup script (only needs to be run once)
-- File: Setup/setup_data_generators.sql
```

This will create:
- `GENERATE` schema
- `CELL_TOWER_TEST` and `SUPPORT_TICKETS_TEST` tables
- Reference tables populated from production data
- Two stored procedures
- Two Snowflake tasks (in suspended state)

### Step 2: Verify Setup

Check that all components were created:

```sql
USE DATABASE TELCO_NETWORK_OPTIMIZATION_PROD;
USE SCHEMA GENERATE;

-- Check tables
SHOW TABLES IN GENERATE;

-- Check procedures
SHOW PROCEDURES IN GENERATE;

-- Check tasks
SHOW TASKS IN GENERATE;

-- Check reference data
SELECT COUNT(*) FROM GENERATE.REF_CELL_TOWER_ATTRIBUTES;
SELECT COUNT(*) FROM GENERATE.REF_COMPLAINT_TEXTS;
```

## Usage

### Starting Data Generation

```sql
-- Start both generators
ALTER TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA RESUME;
ALTER TASK GENERATE.TASK_GENERATE_SUPPORT_TICKET RESUME;
```

### Stopping Data Generation

```sql
-- Stop both generators
ALTER TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA SUSPEND;
ALTER TASK GENERATE.TASK_GENERATE_SUPPORT_TICKET SUSPEND;
```

### Monitoring Progress

Use the `manage_data_generators.sql` script to:
- Check task status
- View generated data
- Validate data quality
- Compare to production data

```sql
-- Check how much data has been generated
SELECT 
    'CELL_TOWER_TEST' AS TABLE_NAME,
    COUNT(*) AS TOTAL_RECORDS,
    MIN(EVENT_DTTM) AS EARLIEST_TIMESTAMP,
    MAX(EVENT_DTTM) AS LATEST_TIMESTAMP
FROM GENERATE.CELL_TOWER_TEST;

SELECT 
    'SUPPORT_TICKETS_TEST' AS TABLE_NAME,
    COUNT(*) AS TOTAL_RECORDS
FROM GENERATE.SUPPORT_TICKETS_TEST;
```

### Manual Testing

Test procedures manually before starting tasks:

```sql
-- Generate one batch of cell tower data
CALL GENERATE.SP_GENERATE_CELL_TOWER_DATA();

-- Generate one support ticket
CALL GENERATE.SP_GENERATE_SUPPORT_TICKET();
```

## Data Quality Features

### Cell Tower Data

The generator preserves these characteristics from production data:

1. **Vendor Distribution**
   - ERICSSON: 37%
   - NOKIA: 26%
   - HUAWEI: 22%
   - ZTE: 9%
   - SAMSUNG: 6%

2. **Performance Tiers**
   - CATASTROPHIC, VERY_BAD, BAD, QUITE_BAD, PROBLEMATIC, GOOD
   - Distribution varies by vendor (ZTE worst, Ericsson best)

3. **Geographic Patterns**
   - Realistic coordinates by region
   - Urban vs. rural performance differences

4. **Performance Metrics**
   - RRC connection failure rates (1-70%)
   - Latency values (5-50ms)
   - E-RAB abnormal release percentages (0.1-25%)
   - PRB utilization (5-99%)
   - Signal quality (RSRP/RSRQ)

5. **Timestamp Management**
   - Continues incrementally from latest existing timestamp
   - New data generated every minute

### Support Ticket Data

The generator creates:

1. **Realistic Complaints**
   - 26 different complaint templates
   - Categorized by service type (Cellular, Home Internet, Business Internet)
   - Appropriate sentiment scores

2. **Customer Information**
   - 30 first names, 20 surnames
   - Realistic email addresses
   - Contact preferences

3. **Cell Tower Correlation**
   - 70% of tickets linked to problematic towers (BAD/VERY_BAD/CATASTROPHIC)
   - 30% linked to healthy towers
   - Realistic sentiment distribution

## Reference Tables

### REF_CELL_TOWER_ATTRIBUTES

Stores static attributes for each cell tower:
- CELL_ID, VENDOR_NAME, PERFORMANCE_TIER
- Geographic information (latitude, longitude, region)
- Network topology (location area code, eNodeB function)

Populated from: `RAW.CELL_TOWER` (grouped by CELL_ID)

### REF_COMPLAINT_TEXTS

26 pre-written complaint templates with:
- SERVICE_TYPE
- COMPLAINT_TEXT
- SENTIMENT_MIN and SENTIMENT_MAX ranges

### REF_CUSTOMER_NAMES / REF_CUSTOMER_SURNAMES / REF_EMAIL_DOMAINS

Name and email components for generating customer profiles.

## Stored Procedures

### SP_GENERATE_CELL_TOWER_DATA()

**Purpose**: Generates one row per cell ID with current timestamp + 1 minute

**Process**:
1. Gets latest timestamp from `CELL_TOWER_TEST`
2. Adds 1 minute
3. Generates rows for ALL cell IDs (from `REF_CELL_TOWER_ATTRIBUTES`)
4. Applies performance tier-based variations
5. Returns count of rows inserted

**Execution Time**: ~10-30 seconds (depending on cell count)

### SP_GENERATE_SUPPORT_TICKET()

**Purpose**: Generates one support ticket

**Process**:
1. Gets next ticket ID (sequential: TR10001, TR10002, etc.)
2. Randomly selects customer name, service type, complaint text
3. Biases cell tower selection (70% problematic towers)
4. Generates appropriate sentiment score
5. Returns ticket ID

**Execution Time**: <1 second

## Snowflake Tasks

### TASK_GENERATE_CELL_TOWER_DATA

- **Schedule**: Every 1 minute
- **Warehouse**: MYWH
- **Action**: Calls `SP_GENERATE_CELL_TOWER_DATA()`
- **Default State**: SUSPENDED

### TASK_GENERATE_SUPPORT_TICKET

- **Schedule**: Every 1 minute
- **Warehouse**: MYWH
- **Action**: Calls `SP_GENERATE_SUPPORT_TICKET()`
- **Default State**: SUSPENDED

## Troubleshooting

### Tasks Not Running

Check task state:
```sql
SHOW TASKS IN GENERATE;
```

If state is "suspended", resume them:
```sql
ALTER TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA RESUME;
```

### Task Execution Errors

View error history:
```sql
SELECT 
    name,
    state,
    scheduled_time,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE name LIKE 'TASK_GENERATE%'
ORDER BY scheduled_time DESC
LIMIT 10;
```

### No Data Being Generated

1. Verify tasks are resumed (not suspended)
2. Check warehouse is running: `SHOW WAREHOUSES;`
3. Verify reference tables are populated
4. Test procedures manually

### Data Quality Issues

Compare test data to production:
```sql
-- Use queries in manage_data_generators.sql
-- Section: ADVANCED: COMPARE TEST DATA TO PRODUCTION DATA
```

## Promotion to Production

Once satisfied with test data quality:

### Option 1: Modify Procedures (Recommended)

1. Suspend both tasks:
   ```sql
   ALTER TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA SUSPEND;
   ALTER TASK GENERATE.TASK_GENERATE_SUPPORT_TICKET SUSPEND;
   ```

2. Update procedures to write to production tables:
   - Change `GENERATE.CELL_TOWER_TEST` → `RAW.CELL_TOWER`
   - Change `GENERATE.SUPPORT_TICKETS_TEST` → `RAW.SUPPORT_TICKETS`

3. Resume tasks:
   ```sql
   ALTER TASK GENERATE.TASK_GENERATE_CELL_TOWER_DATA RESUME;
   ALTER TASK GENERATE.TASK_GENERATE_SUPPORT_TICKET RESUME;
   ```

### Option 2: Copy Test Data to Production

```sql
-- Copy validated test data to production
INSERT INTO RAW.CELL_TOWER
SELECT * FROM GENERATE.CELL_TOWER_TEST;

INSERT INTO RAW.SUPPORT_TICKETS
SELECT * FROM GENERATE.SUPPORT_TICKETS_TEST;
```

## Maintenance

### Clearing Test Data

```sql
TRUNCATE TABLE GENERATE.CELL_TOWER_TEST;
TRUNCATE TABLE GENERATE.SUPPORT_TICKETS_TEST;
```

### Updating Reference Data

If production data changes significantly:

```sql
-- Refresh cell tower attributes
CREATE OR REPLACE TABLE GENERATE.REF_CELL_TOWER_ATTRIBUTES AS
SELECT 
    CELL_ID,
    HOME_NETWORK_TAP_CODE,
    HOME_NETWORK_NAME,
    HOME_NETWORK_COUNTRY,
    BID_DESCRIPTION,
    VENDOR_NAME,
    CELL_LATITUDE,
    CELL_LONGITUDE,
    ENODEB_FUNCTION,
    LOCATION_AREA_CODE,
    PERFORMANCE_TIER
FROM RAW.CELL_TOWER
GROUP BY [all columns];
```

### Warehouse Considerations

- Tasks use warehouse `MYWH`
- Cell tower generation (~50K rows/minute) needs medium warehouse
- Consider auto-suspend settings to control costs
- Monitor warehouse usage in Snowflake console

## Cost Considerations

### Compute Costs
- Tasks run every minute = 1,440 executions/day
- Each cell tower generation: ~10-30 seconds compute
- Each ticket generation: <1 second compute
- Daily compute: ~6-12 hours of warehouse time

### Storage Costs
- Cell tower: ~50K rows/minute = ~72M rows/day
- Support tickets: 1 row/minute = ~1,440 rows/day
- Monitor table sizes and implement retention policies

### Optimization Tips
1. Use auto-suspend warehouse (1 minute idle)
2. Consider longer intervals (5 or 15 minutes) if appropriate
3. Implement data retention (delete old test data)
4. Schedule tasks during off-peak hours if testing

## Files

- `setup_data_generators.sql` - One-time setup script
- `manage_data_generators.sql` - Control and monitoring queries
- `README_DATA_GENERATORS.md` - This documentation

## Support

For issues or questions:
1. Check task execution history
2. Test procedures manually
3. Verify reference table data
4. Review Snowflake warehouse status
5. Check existing production data patterns

