import streamlit as st
import snowflake.snowpark.context

# Page configuration - must be the first Streamlit command
st.set_page_config(
    page_title="Telco Network Optimization Suite",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize Snowflake session
@st.cache_resource
def init_session():
    return snowflake.snowpark.context.get_active_session()

session = init_session()

# Main page content
st.title("📡 Telco Network Optimization Suite")
st.markdown("""
### Welcome to the Telco Network Optimization Dashboard

This application provides interactive visualizations and analytics to help identify 
and prioritize network issues that impact customer satisfaction.

Use the sidebar to navigate between different analysis views.
""")

# Display some key network stats on the home page
st.markdown("""
## Available Analysis Tools

- **Cell Tower Lookup**: Examine individual cell tower performance metrics
- **Geospatial Analysis**: Visualize network metrics and support ticket data on maps
- **Correlation Analytics**: Discover relationships between network metrics and customer experience
- **Customer Impact Dashboard TBC**: Correlate technical metrics with customer complaints
- **Loyalty Status Impact View TBC**: Analyze how network issues affect customers by loyalty tier
- **Time-Series Analysis TBC**: Track performance metrics over time to identify patterns
- **Service Type Performance Breakdown TBC**: Compare metrics across different service offerings
- **Issue Prioritization Matrix TBC**: Identify high-impact, easy-to-fix network issues
- **Problematic Cell Towers TBC**: Identify and diagnose towers with technical or customer issues
- **Proactive Network Adjustments TBC**: Monitor trends to detect issues before they affect customers
- **Capacity Planning TBC**: Identify areas needing infrastructure upgrades or expansion
- **Root Cause Analysis for Complaints TBC**: Link customer complaints to network performance issues
- **Data Integration and Quality TBC**: Ensure data accuracy and reliability for decision-making

### Getting Started
Select a page from the sidebar to begin your analysis.
""")

# Display some key network stats on the home page
col1, col2, col3 = st.columns(3)

# Count of cell towers
total_cells = session.sql("""
    SELECT COUNT(DISTINCT cell_id) as total_cells 
    FROM TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER
""").collect()[0]["TOTAL_CELLS"]

# Average failure rate
avg_failure = session.sql("""
    SELECT ROUND(AVG(CASE WHEN call_release_code != 0 THEN 1 ELSE 0 END) * 100, 2) as failure_rate
    FROM TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER
""").collect()[0]["FAILURE_RATE"]

# Count of support tickets
ticket_count = session.sql("""
    SELECT COUNT(*) as ticket_count 
    FROM TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS
""").collect()[0]["TICKET_COUNT"]

col1.metric("Total Cell Towers", f"{total_cells:,}")
col2.metric("Average Failure Rate", f"{avg_failure}%")
col3.metric("Support Tickets", f"{ticket_count:,}") 