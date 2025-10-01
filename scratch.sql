select * from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER limit 100;

select home_network_name, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by home_network_name;

select home_network_country, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by home_network_country;

select bid_description, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by bid_description;

select service_category, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by service_category;

select vendor_name, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by vendor_name;

select call_event_description, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by call_event_description;

select cause_code_short_description, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by cause_code_short_description;

select cause_code_long_description, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by cause_code_long_description;

select sender_name, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by sender_name;

select hostname, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by hostname;

select timestamp, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by timestamp;

select duration, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by duration;

select pm_pdcp_lat_time_ul, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by pm_pdcp_lat_time_ul;

select pm_pdcp_lat_pkt_trans_ul, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by pm_pdcp_lat_pkt_trans_ul;

select pm_ue_meas_control, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by pm_ue_meas_control;

select unique_id, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by unique_id;

select pm_pdcp_vol_dl_drb, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by pm_pdcp_vol_dl_drb;

select pm_active_ue_dl_max, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER group by pm_active_ue_dl_max;


select * from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS limit 100;

select contact_preference, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS group by contact_preference;  

select call_id, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER  group by call_id having count(*) > 1;

select unique_id, count(*) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER  group by unique_id having count(*) > 1;


WITH __cell_tower AS (
  SELECT
    cell_id,
    pm_rrc_conn_estab_att,
    pm_rrc_conn_estab_succ
  FROM telco_network_optimization_prod.raw.cell_tower
)
SELECT
  cell_id,
  (
    COALESCE(pm_rrc_conn_estab_att, 0) - COALESCE(pm_rrc_conn_estab_succ, 0)
  ) / NULLIF(NULLIF(COALESCE(pm_rrc_conn_estab_att, 0), 0), 0) * 100 AS rrc_conn_failure_rate
FROM __cell_tower
ORDER BY
  rrc_conn_failure_rate asc NULLS LAST
LIMIT 300;


select min(PM_ERAB_REL_ABNORMAL_ENB), max(PM_ERAB_REL_ABNORMAL_ENB), avg(PM_ERAB_REL_ABNORMAL_ENB) from TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER;
