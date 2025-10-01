-- Examine CELL_TOWER table basics
SELECT 
  COUNT(*) as total_records,
  MIN(EVENT_DATE) as earliest_date,
  MAX(EVENT_DATE) as latest_date,
  COUNT(DISTINCT CELL_ID) as unique_cells,
  COUNT(DISTINCT HOME_NETWORK_COUNTRY) as countries,
  COUNT(DISTINCT BID_DESCRIPTION) as regions,
  COUNT(DISTINCT SERVICE_CATEGORY) as service_types,
  COUNT(DISTINCT VENDOR_NAME) as vendors
FROM CELL_TOWER;

-- Sample service categories and countries
SELECT SERVICE_CATEGORY, COUNT(*) as count
FROM CELL_TOWER 
WHERE SERVICE_CATEGORY IS NOT NULL
GROUP BY SERVICE_CATEGORY
ORDER BY count DESC;

SELECT HOME_NETWORK_COUNTRY, COUNT(*) as count  
FROM CELL_TOWER
WHERE HOME_NETWORK_COUNTRY IS NOT NULL
GROUP BY HOME_NETWORK_COUNTRY
ORDER BY count DESC
LIMIT 10;

-- Sample BID descriptions (regions)
SELECT BID_DESCRIPTION, COUNT(*) as count
FROM CELL_TOWER
WHERE BID_DESCRIPTION IS NOT NULL  
GROUP BY BID_DESCRIPTION
ORDER BY count DESC
LIMIT 10;

-- Support tickets overview
SELECT 
  COUNT(*) as total_tickets,
  COUNT(DISTINCT CELL_ID) as cells_with_tickets,
  COUNT(DISTINCT SERVICE_TYPE) as service_types,
  AVG(SENTIMENT_SCORE) as avg_sentiment,
  MIN(SENTIMENT_SCORE) as min_sentiment,
  MAX(SENTIMENT_SCORE) as max_sentiment
FROM SUPPORT_TICKETS;

-- Support ticket service types
SELECT SERVICE_TYPE, COUNT(*) as ticket_count, AVG(SENTIMENT_SCORE) as avg_sentiment
FROM SUPPORT_TICKETS
GROUP BY SERVICE_TYPE
ORDER BY ticket_count DESC;

-- Performance metrics samples - connection success rates
SELECT 
  COUNT(*) as records_with_data,
  AVG(PM_RRC_CONN_ESTAB_SUCC) as avg_rrc_success,
  AVG(PM_RRC_CONN_ESTAB_ATT) as avg_rrc_attempts,
  AVG(PM_S1_SIG_CONN_ESTAB_SUCC) as avg_s1_success,
  AVG(PM_ERAB_ESTAB_SUCC_INIT) as avg_erab_success
FROM CELL_TOWER
WHERE PM_RRC_CONN_ESTAB_SUCC IS NOT NULL;

-- PRB Utilization overview
SELECT 
  AVG(PM_PRB_UTIL_DL) as avg_dl_util,
  AVG(PM_PRB_UTIL_UL) as avg_ul_util,
  MAX(PM_PRB_UTIL_DL) as max_dl_util,
  MAX(PM_PRB_UTIL_UL) as max_ul_util,
  COUNT(*) as records_with_util_data
FROM CELL_TOWER  
WHERE PM_PRB_UTIL_DL IS NOT NULL OR PM_PRB_UTIL_UL IS NOT NULL;
