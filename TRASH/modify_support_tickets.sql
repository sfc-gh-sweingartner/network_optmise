--the sample data has the identical number of tickets for each cell tower
-- this script modifies the data to skew based on the sentiment.  Negative sentiment
-- is more likely to be associated with a higher number of tickets

select cell_id, count(1),  avg(sentiment_score) from support_tickets
group by cell_id
order by 2 desc;

select count(1) from  support_tickets;

-- Create a new table with redistributed data
CREATE OR REPLACE TABLE TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS_REDISTRIBUTED AS
WITH 
-- Calculate average sentiment score per cell_id
cell_sentiments AS (
  SELECT 
    CELL_ID, 
    AVG(SENTIMENT_SCORE) AS avg_sentiment,
    COUNT(1) AS record_count
  FROM TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS
  GROUP BY CELL_ID
),
-- Classify cells as high or low sentiment
cell_classifications AS (
  SELECT 
    CELL_ID, 
    avg_sentiment,
    record_count,
    CASE WHEN avg_sentiment < 0 THEN 'LOW' ELSE 'HIGH' END AS sentiment_class
  FROM cell_sentiments
),
-- Identify donor and recipient cells for redistribution
donor_cells AS (
  SELECT CELL_ID 
  FROM cell_classifications 
  WHERE sentiment_class = 'HIGH'
),
recipient_cells AS (
  SELECT CELL_ID, 
         ROW_NUMBER() OVER (ORDER BY CELL_ID) AS recipient_row_num
  FROM cell_classifications 
  WHERE sentiment_class = 'LOW'
),
recipient_count AS (
  SELECT COUNT(*) AS total_recipients FROM recipient_cells
),
-- Select records from high sentiment cells that will be moved
records_to_move AS (
  SELECT 
    st.*,
    cc.record_count,
    ROW_NUMBER() OVER (PARTITION BY st.CELL_ID ORDER BY st.SENTIMENT_SCORE DESC) AS rn
  FROM TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS st
  JOIN cell_classifications cc ON st.CELL_ID = cc.CELL_ID
  WHERE cc.sentiment_class = 'HIGH'
),
-- Prepare for redistribution
records_with_recipient AS (
  SELECT 
    r.*,
    rc.total_recipients,
    MOD(r.rn, rc.total_recipients) + 1 AS recipient_index
  FROM records_to_move r
  CROSS JOIN recipient_count rc
  WHERE r.rn <= FLOOR(r.record_count * 0.4) -- Move about 40% of records from high sentiment cells
),
-- Assign these records to low sentiment cells
reassigned_records AS (
  SELECT 
    r.TICKET_ID,
    r.CUSTOMER_NAME,
    r.CUSTOMER_EMAIL,
    r.SERVICE_TYPE,
    r.REQUEST,
    r.CONTACT_PREFERENCE,
    c.CELL_ID,
    r.SENTIMENT_SCORE
  FROM records_with_recipient r
  JOIN recipient_cells c ON c.recipient_row_num = r.recipient_index
)
-- Combine original records (except moved ones) with reassigned records
SELECT 
  TICKET_ID,
  CUSTOMER_NAME,
  CUSTOMER_EMAIL,
  SERVICE_TYPE,
  REQUEST,
  CONTACT_PREFERENCE,
  CELL_ID,
  SENTIMENT_SCORE
FROM TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS t
WHERE NOT EXISTS (
  SELECT 1 FROM reassigned_records r WHERE t.TICKET_ID = r.TICKET_ID
)
UNION ALL
SELECT * FROM reassigned_records;

select cell_id, count(1),  avg(sentiment_score) from SUPPORT_TICKETS_REDISTRIBUTED
group by cell_id
order by 2 desc;


truncate table support_tickets;

insert into support_tickets select * from SUPPORT_TICKETS_REDISTRIBUTED;