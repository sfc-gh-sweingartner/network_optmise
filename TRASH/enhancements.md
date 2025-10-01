Based on your network optimization app and the data available in your tables, here are some ideas for creating more engaging and insightful visualizations.  These enhancements would provide more actionable insights for Telco employees while making your application more visually appealing and informative.

The way that this will be built is that there will be a main page that allows the user to go to any of these pages.  

The technical stack is Streamlit in Snowflake.  The list of approved python libraries that this technology allows can be found here:  https://repo.anaconda.com/pkgs/snowflake/  (Look at the LINUX-64 column)  The two underlying tables are in Snowflake and held in tables as can be seen in the semantic_model.yaml file and also in the create_tables.sql file.

Design
**1. Main Page** 
A landing page with side bar navigation to multiple pages
**2. Cell Tower Lookup**
The existing network optimisation page works quite well.  A user can click a cell tower and find out information about it.  Lets leave this as it is.  
**3. Geospatial Analysis**
Create a heatmap overlay of various network metrics and support ticket metrics 
Color-code areas based on sentiment scores from SUPPORT_TICKETS to quickly identify regions with negative customer experiences
Allow toggling between different metrics (failure rate, ticket count, sentiment score)
**4. Correlation Analytics**
 This page is devoted to helping a user learn about coorelations in the data.   All the same data / metrics are available as in the Heatmap Overlay.  Except that rather than visually look at a map, a user will be presented with information that tells then about coorelations they should beware of.  
The functionality is that a user identifies one metric they care about.  The defauly one is Customer Support Ticket count.  Then the user is presented charts and other information that informs them how other metrics are coorelated.   Cursor has tended to use WebGL to present coorelations however that does not work with Streamlit in Snowflake.  Charting tools like altair work well as they are in our anaconda channel.  
Questions from Cursor: 
Would you prefer a simpler interface with fewer visualization types, or the comprehensive approach I outlined? The users are sophisticated so I prefer the comprehensive approach you outlined. 
Do you want the page to include statistical significance testing for correlations? Yes
Should the page include filtering capabilities (by region, time period, etc.) before calculating correlations? Yes
Would you like any specific insights highlighted automatically (e.g., "Top 3 factors affecting customer complaints")? yes
**5. Customer Impact Dashboard**
Display correlation between technical metrics and customer complaints
Create scatter plots showing relationship between cell tower performance metrics (PM_ACTIVE_UE_DL_MAX, PM_PRB_UTIL_DL, etc.) and sentiment scores
Highlight cells that have both poor technical performance and high negative sentiment
**6. Loyalty Status Impact View**
Map showing distribution of customers by loyalty status (Bronze/Silver/Gold) affected by problematic towers
Bar charts comparing performance metrics with percentage of high-value customers affected
**7. Time-Series Analysis**
Track cell performance over time using EVENT_DATE and TIMESTAMP data
Show trends of support tickets and corresponding network performance
Identify recurring patterns in network degradation
**8. Service Type Performance Breakdown**
Create visualizations filtered by SERVICE_TYPE from support tickets
Compare metrics across different service offerings (Cellular, Business Internet, Home Internet)
**9. Issue Prioritization Matrix**
Develop a quadrant chart positioning cells by technical severity vs. customer impact
Help technicians quickly identify high-impact, easy-to-fix issues
**10. Problematic Cell Towers** 
High Ticket Volume: Filter SUPPORT_TICKETS for CELL_IDs that have a high volume of tickets.
Negative Sentiment: Analyze SENTIMENT_SCORE in SUPPORT_TICKETS to pinpoint CELL_IDs associated with highly dissatisfied customers.
Specific Complaints: Parse the REQUEST field in SUPPORT_TICKETS (using NLP if necessary) to categorize issues (e.g., dropped calls, slow data, no service) and map them to CELL_IDs.
Link to Performance Metrics: Join SUPPORT_TICKETS with CELL_TOWER on CELL_ID. For the problematic cell towers identified above, examine the performance metrics in the CELL_TOWER table around the EVENT_DTTM of the tickets:
Latency Issues: Check PM_PDCP_LAT_TIME_UL, PM_PDCP_LAT_PKT_TRANS_UL, PM_PDCP_LAT_TIME_DL, PM_PDCP_LAT_PKT_TRANS_DL. High latency reported by customers can be validated with these metrics.
Connection Problems: Look at PM_RRC_CONN_ESTAB_SUCC, PM_RRC_CONN_ESTAB_ATT (Radio Resource Control connection success rates), PM_S1_SIG_CONN_ESTAB_SUCC, PM_S1_SIG_CONN_ESTAB_ATT (S1 signaling connection success rates), and PM_ERAB_ESTAB_SUCC_INIT, PM_ERAB_ESTAB_ATT_INIT (E-RAB setup success rates). Low success rates here directly impact user experience.
Dropped Calls/Sessions: Analyze PM_ERAB_REL_ABNORMAL_ENB_ACT, PM_ERAB_REL_ABNORMAL_ENB (abnormal E-RAB releases). CAUSE_CODE_SHORT_DESCRIPTION and CAUSE_CODE_LONG_DESCRIPTION can provide reasons for call drops or failures originating from the network.
Throughput/Speed Issues: Examine PM_ACTIVE_UE_DL_MAX, PM_ACTIVE_UE_UL_MAX, PM_UE_THP_TIME_DL, PM_PDCP_VOL_DL_DRB. Low throughput values or high utilization (PM_PRB_UTIL_DL, PM_PRB_UTIL_UL) might indicate congestion.
Signal Quality: While direct RSRP/RSRQ values for all UEs aren't in the summary facts (those like PM_UE_MEAS_RSRP_SERV_INTRA_FREQ1 seem to be specific measurements), trends in connection drops or low throughput for a cell might imply underlying signal issues.
**11. Proactive Network Adjustments**
Trend Analysis: Monitor the CELL_TOWER performance metrics over time (EVENT_DTTM, TIMESTAMP, WINDOW_START_AT, WINDOW_END_AT). Identify degrading trends (e.g., increasing latency, decreasing success rates, rising utilization) before they lead to a surge in support tickets.
Anomaly Detection: Implement anomaly detection on key performance indicators (KPIs) derived from the CELL_TOWER facts. For example, a sudden spike in PM_ERAB_REL_ABNORMAL_ENB or a drop in PM_RRC_CONN_ESTAB_SUCC could indicate an emerging fault.
Predictive Maintenance: Correlate specific CAUSE_CODE_LONG_DESCRIPTION patterns or performance degradation patterns with potential hardware failures or configuration issues. This can inform a predictive maintenance schedule.
Parameter Optimization: For cells showing suboptimal performance but not outright failure (e.g., borderline latency, slightly low throughput affecting a subset of users), network engineers can use this data to fine-tune cell parameters (e.g., handover parameters, power settings, antenna tilts – though these actions are outside the data itself, the data guides these decisions).
3. Strategic Cell Tower Investments:

**12. Capacity Planning**
High Utilization: CELL_IDs consistently showing high PM_PRB_UTIL_DL or PM_PRB_UTIL_UL (Physical Resource Block utilization) are candidates for capacity upgrades (e.g., adding more carriers, new sectors).
High Active Users: PM_ACTIVE_UE_DL_SUM, PM_ACTIVE_UE_UL_SUM, and PM_RRC_CONN_MAX indicate user load. Consistently high numbers suggest a need for more capacity.
Growth Areas: Analyze trends in these metrics over time along with CELL_LATITUDE and CELL_LONGITUDE to identify geographic areas with growing demand.
Coverage Expansion/Optimization:
Support Tickets from Specific Areas: If SUPPORT_TICKETS can be geo-tagged (even roughly, perhaps by cross-referencing CUSTOMER_NAME with CUSTOMER_LOYALTY.ADDRESS if that table contains location data, though the current schema doesn't explicitly link tickets to customer addresses beyond name/email), this can identify coverage holes or weak spots.
"No User Responding" or QoS Issues: Frequent CAUSE_CODE_SHORT_DESCRIPTION like NO_USER_RESPONDING or QOS_NOT_AVAILABLE in areas with expected coverage might indicate a need for a new tower or optimization of existing ones.
New Technology Deployment (e.g., 5G):
Investment decisions for new technologies can be prioritized in areas with high data demand (PM_PDCP_VOL_DL_DRB), high user density, or areas where existing technology is struggling to meet performance targets.
Benchmarking and Vendor Performance:
If VENDOR_NAME is populated, you can compare the performance and reliability of equipment from different vendors across various cells, which can inform future procurement decisions.
4. Enhancing Customer Experience:

**13. Root Cause Analysis for Complaints** 
When a customer lodges a complaint via a SUPPORT_TICKETS entry, linking to the CELL_TOWER data for the associated CELL_ID around the time of the incident provides invaluable context for customer service agents and technical teams to understand the potential network contribution to the issue.
Setting Realistic Expectations: If network data indicates a known issue or congestion in an area, support teams can provide more accurate information to customers.
Steps to Implement Analysis:

**14.Data Integration and Quality**
Ensure the data in both tables is clean, accurate, and that the CELL_ID linkage is reliable. Timestamps are crucial for correlation.
Define Key Performance Indicators (KPIs): From the raw metrics, calculate relevant KPIs like Call Drop Rate, RRC Setup Success Rate, E-RAB Setup Success Rate, Average Latency, Cell Availability, etc.
Visualization: Use BI tools or dashboards to visualize KPIs geographically (on a map using CELL_LATITUDE, CELL_LONGITUDE) and over time. Overlay support ticket density or sentiment scores.
Alerting: Set up alerts for KPI breaches or significant negative trends.
Advanced Analytics: Employ statistical analysis, machine learning for predictive modeling (e.g., predict cell failures, predict high ticket volumes based on performance trends), and NLP on ticket descriptions for deeper insights.
By systematically analyzing these interconnected datasets, a telecom operator can move from reactive problem-solving to proactive network management and make data-driven investment decisions to optimize performance and customer satisfaction.

