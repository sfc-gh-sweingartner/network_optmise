# Telco Network Optimization Suite

A multi-page Streamlit application for visualizing and analyzing cell tower performance and customer support data.

## Features

* **Home Dashboard**: Overview of network statistics and navigation
* **Cell Tower Lookup**: Interactive map for examining individual cell tower performance metrics
* **Heatmap Overlay**: Visualize support ticket density and sentiment data
* **Additional Analysis Pages**: For in-depth network performance analysis

## Recent Updates

The application has been updated to work without requiring a personal Mapbox API key. This leverages Snowflake's built-in support for Mapbox tiles in Streamlit applications.

### Key Changes:

1. Removed Mapbox API key requirement from Cell Tower Lookup page
2. Added `connectMapBoxNoKey.sql` script to set up the necessary network access without API key dependency
3. Simplified map configuration to use default Mapbox access

## Setup Instructions

### 1. Configure Snowflake Network Access

Run the `connectMapBoxNoKey.sql` script to set up the necessary network access integration:

```sql
-- Execute the script in Snowflake
-- Be sure to update the app name in the ALTER STREAMLIT command with your actual Streamlit app name
```

### 2. App Structure

The application follows Snowflake's multi-page Streamlit app structure:

```
/
├── main.py                       # Main landing page
├── pages/                        # Subdirectory for individual pages
│   ├── 2_Cell_Tower_Lookup.py    # Cell tower lookup page (updated to work without API key)
│   ├── 3_Geospatial_Analysis.py  # Geospatial analysis page
│   └── [Additional pages]        # Other analysis pages
└── README.md                     # Documentation
```

## Troubleshooting

If you encounter issues with maps not displaying:

1. Verify the external access integration is properly configured with `SHOW EXTERNAL ACCESS INTEGRATIONS`
2. Check your Streamlit app configuration with `SHOW STREAMLITS` and ensure it references only the integration, not any secrets
3. If needed, update your app configuration with:
   ```sql
   ALTER STREAMLIT YOURAPP.YOURSCHEMA.YOUR_APP_ID
     SET EXTERNAL_ACCESS_INTEGRATIONS = (map_access_int);
   ```

## Multi-Page Navigation

The application uses Streamlit's built-in multi-page support, which automatically adds navigation in the sidebar. According to Snowflake's documentation:

* The main.py file serves as the landing page
* Files in the pages/ directory are displayed as navigation options
* File naming with numbers (e.g., 1_Cell_Tower_Lookup.py) controls the order
* Each page can be accessed directly via URL paths

## Key Features of the Heatmap Overlay

The Heatmap Overlay page provides several powerful visualizations:

1. **Interactive Heatmap** with options to view:
   - Cell Tower Failure Rate
   - Support Ticket Density
   - Customer Sentiment Distribution
   - Combined Issue Severity

2. **Correlation Analysis** showing the relationship between:
   - Failure Rate vs. Support Ticket Count
   - Failure Rate vs. Sentiment Score

3. **Key Statistics** tables displaying:
   - Top 5 worst performing cell towers
   - Areas with the most support tickets

4. **Priority Areas** highlighting the most problematic locations based on a combination of technical and customer impact metrics


## Installation

### Prerequisites: Data Files

Due to GitHub file size limits, the large CSV data files are not included in this repository. You need to obtain them separately:

**Required Data Files:**
- `Setup/cell_tower.csv` (1.8 GB)
- `Setup/support_tickets.csv` (73 MB)

**How to get the data files:**
- Contact stephen.weingartner@snowflake.com for access to the data files
- Once obtained, place them in the `Setup/` directory before proceeding with installation

### Installation Steps

1. Download the data to your laptop:
   - Support Tickets: https://drive.google.com/file/d/1OfWzNgwg2GdJ0xBuJCdWaoX_4gMQ8MPm/view?usp=sharing 
   - Network Data: https://drive.google.com/file/d/1tDZZqXD1Xfb7N0nHb82YEVClQAb5XHyi/view?usp=sharing
   - If you have problems with access, contact stephen.weingartner@snowflake.com 
2. Run through the Setup / README_BACKUP.sql file to create the tables and upload the data into you environment.  
3. In Snowsight, open a SQL worksheet and run this with ACCOUNTADMIN to allow your env to see this GIT project: CREATE OR REPLACE API INTEGRATION git_sweingartner API_PROVIDER = git_https_api API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-sweingartner') ENABLED = TRUE;
4. click Projects > Streamlit
5. Tick the drop downbox next to the blue "+ Streamlit App" and select "create from repository"
6. Click "Create Git Repository"
7. In the Repository URL field, enter: https://github.com/sfc-gh-sweingartner/network_optmise
8. You can leave the repository name as the default
9. In the API Integration drop down box, choose GIT_SWEINGARTNER
10. Deploy it into the TELCO_NETWORK_OPTIMIZATION_PROD database and RAW schema, and use any WH
11. Click Home.py then "Select File"
12. Choose the db TELCO_NETWORK_OPTIMIZATION_PROD and schema RAW
13. Name the app whatever you like
14. Choose any warehouse you want (maybe small or above) and click create
15. Open the code editor panel and add the following packages via the drop down box above the code: altair, branca, h3-py, matplotlib, numpy, pandas, plotly, pydeck, scipy 
16. Run the script connectMapBoxNoKey.sql (note that the script shows you will need to find the app name and add it to the SQL)
17. Reopen your app (or Run should work)


## Troubleshooting
If you hit any issues, ask a vibe coding tool to assist or contact stephen.weingartner@snowflake.com 