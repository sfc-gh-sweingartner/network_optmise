import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session
import _snowflake
import branca.colormap as cm
import h3

# Define Branca colormap color lists globally
colors_yellow_blue = ['#fafa6e','#e1f46e','#caee70','#b3e773','#9ddf77','#89d77b','#75cf7f','#62c682',
                       '#51bd86','#40b488','#31aa89','#24a08a','#199689','#138c87','#138284','#17787f',
                       '#1d6e79','#226472','#265b6b','#285162','#2a4858']
colors_yellow_red = ['#ffff00', '#ffdd00', '#ffbb00', '#ff9900', '#ff5500', '#ff0000']
colors_blue_green = ['#0000ff', '#0044ff', '#0088ff', '#00ccff', '#00ee99', '#00ff00']
colors_white_blue = ['#ffffff', '#ddddff', '#bbbbff', '#9999ff', '#7777ff', '#1F00FF']
colors_white_red  = ['#ffffff', '#ffdddd', '#ffbbbb', '#ff9999', '#ff7777', '#FF1F00']
colors_white_green = ['#ffffff', '#ddffdd', '#bbffbb', '#99ff99', '#77ff77', '#00FF1F']

# Page configuration - must be the first Streamlit command
st.set_page_config(
    page_title="Geospatial Analysis",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Page header
st.title("🗺️ Cell Tower and Support Geospatial Analysis")
st.markdown("""
This page visualizes cell tower data overlaid with support ticket information, 
enabling you to identify areas with both technical issues and customer complaints.
""")

# Debugging section - will only appear when debug is enabled
debug_mode = st.sidebar.checkbox("Enable Debug Mode", value=False, key="debug_mode")
debug_container = st.container()

# Function to show debug info
def show_debug(message, data=None):
    if debug_mode:
        with debug_container:
            st.write(f"DEBUG: {message}")
            if data is not None:
                if isinstance(data, pd.DataFrame):
                    st.write(f"DataFrame shape: {data.shape}")
                    st.dataframe(data.head())
                else:
                    st.write(data)
                    
# Initialize Snowpark session
@st.cache_resource
def init_session():
    return get_active_session()

session = init_session()

# Sidebar options
st.sidebar.header("Visualization Options")

# Add a clear cache button
if st.sidebar.button("🔄 Clear Data Cache", help="Refresh all data from the database"):
    # Clear all cached data
    st.cache_data.clear()
    # Show a message instead of using experimental_rerun
    st.sidebar.success("Cache cleared! Please refresh the page manually.")
    # Add instructions for manual refresh
    st.sidebar.info("Please click the 'Refresh' button in your browser or press F5 to load fresh data.")

# Heatmap type selector - replacing radio button with multiselect
available_metrics = [
    "Failure Rate", 
    "Support Ticket Count", 
    "Sentiment Score",
    "Downlink Latency",
    "Connection Success Rate",
    "Abnormal Drop Rate",
    "Downlink Speed",
    "Uplink Speed",
    "Resource Utilization Downlink",
    "Resource Utilization Uplink",
    "Signal Connection Success Rate"
]
selected_metrics = st.sidebar.multiselect(
    "Select Metrics to Display",
    available_metrics,
    default=["Failure Rate"],  # Default to showing failure rate
    help="Select one or more metrics to display on the map. Multiple metrics will be overlaid."
)

# Ensure at least one metric is selected
if not selected_metrics:
    st.sidebar.warning("Please select at least one metric to display.")
    selected_metrics = ["Failure Rate"]  # Default if nothing selected

# Add metric definitions
with st.sidebar.expander("📊 Metric Definitions"):
    st.markdown("""
    ### Failure Rate
    
    **Definition:** Percentage of calls that failed out of total calls processed by each cell tower.
    
    **Formula:** `(Number of calls with call_release_code != 0) * 100 / Total calls`
    
    **Range:** 0% to 100% (higher is worse)
    """)
    
    st.markdown("""
    ### Support Ticket Count
    
    **Definition:** Total number of customer support tickets associated with each cell tower.
    
    **Formula:** `COUNT(*) from SUPPORT_TICKETS table, grouped by cell_id`
    
    **Range:** 0 to unlimited (higher indicates more customer issues)
    """)
    
    st.markdown("""
    ### Sentiment Score
    
    **Definition:** Average sentiment score from support tickets for each cell tower.
    
    **Formula:** `AVG(sentiment_score) from SUPPORT_TICKETS table, grouped by cell_id`
    
    **Range:** -1 (extremely negative) to +1 (extremely positive)
    """)
    
    st.markdown("""
    ### Downlink Latency
    
    **Definition:** Average downlink latency time in Packet Data Convergence Protocol.
    
    **Formula:** `AVG(PM_PDCP_LAT_TIME_DL) from CELL_TOWER table, grouped by cell_id`
    
    **Range:** Higher values indicate worse performance (longer delays)
    """)
    
    st.markdown("""
    ### Connection Success Rate
    
    **Definition:** Percentage of successful RRC connection establishment attempts.
    
    **Formula:** `(PM_RRC_CONN_ESTAB_SUCC / PM_RRC_CONN_ESTAB_ATT) * 100`
    
    **Range:** 0% to 100% (higher is better)
    """)
    
    st.markdown("""
    ### Abnormal Drop Rate
    
    **Definition:** Number of abnormal E-RAB releases by the Evolved Node B.
    
    **Formula:** `AVG(PM_ERAB_REL_ABNORMAL_ENB) from CELL_TOWER table`
    
    **Range:** Higher values indicate worse performance (more abnormal drops)
    """)
    
    st.markdown("""
    ### Downlink Speed
    
    **Definition:** Maximum downlink data rate for active User Equipment.
    
    **Formula:** `AVG(PM_ACTIVE_UE_DL_MAX) from CELL_TOWER table`
    
    **Range:** Higher values indicate better performance (faster speeds)
    """)
    
    st.markdown("""
    ### Uplink Speed
    
    **Definition:** Maximum uplink data rate for active User Equipment.
    
    **Formula:** `AVG(PM_ACTIVE_UE_UL_MAX) from CELL_TOWER table`
    
    **Range:** Higher values indicate better performance (faster speeds)
    """)
    
    st.markdown("""
    ### Resource Utilization Downlink
    
    **Definition:** Percentage of downlink Physical Resource Block utilization.
    
    **Formula:** `AVG(PM_PRB_UTIL_DL) from CELL_TOWER table`
    
    **Range:** 0% to 100% (higher indicates higher resource usage, potentially congestion)
    """)
    
    st.markdown("""
    ### Resource Utilization Uplink
    
    **Definition:** Percentage of uplink Physical Resource Block utilization.
    
    **Formula:** `AVG(PM_PRB_UTIL_UL) from CELL_TOWER table`
    
    **Range:** 0% to 100% (higher indicates higher resource usage, potentially congestion)
    """)
    
    st.markdown("""
    ### Signal Connection Success Rate
    
    **Definition:** Percentage of successful S1 signal connection establishment attempts.
    
    **Formula:** `(PM_S1_SIG_CONN_ESTAB_SUCC / PM_S1_SIG_CONN_ESTAB_ATT) * 100`
    
    **Range:** 0% to 100% (higher is better)
    """)

# Create layer configuration for each selected metric
layer_configs = {}

# Move layer configuration to a function to reduce code duplication
def get_layer_config(metric_name, index):
    with st.sidebar.expander(f"⚙️ {metric_name} Layer Settings", expanded=(index == 0)):
        # Resolution selector for H3 level
        resolution = st.slider(
            f"{metric_name} H3 Resolution (Grid Size)",
            min_value=4,  # Coarser
            max_value=11, # Finer
            value=6,      # Default H3 resolution (Changed from 7 to 6)
            key=f"h3_resolution_{metric_name}",
            help="Higher values mean smaller, more numerous hexagons (4=Largest, 11=Smallest). Level 6 is a good default."
        )

        # Opacity selector
        opacity = st.slider(
            f"{metric_name} Opacity",
            min_value=0.0, 
            max_value=1.0,
            value=0.5,  # Default opacity (Changed from 0.6 to 0.5)
            step=0.05,
            key=f"opacity_{metric_name}"
        )

        # Set default color scheme based on index
        default_index = 0  # Default to White-Blue
        if index == 1:
            default_index = 1  # White-Red for second metric
        elif index == 2:
            default_index = 2  # White-Green for third metric
        elif index > 2:
            default_index = index % 3  # Cycle through the first three schemes

        # Style options (color schemes)
        color_options = ("White-Blue", "White-Red", "White-Green", "Yellow Blue", "Yellow-Red", "Blue-Green")
        style_option = st.selectbox(
            f"{metric_name} Color Scheme",
            color_options,
            index=default_index,
            key=f"style_{metric_name}"
        )
        
        return {
            "resolution": resolution, 
            "opacity": opacity,
            "style_option": style_option
        }

# Get configuration for each selected metric
for i, metric in enumerate(selected_metrics):
    layer_configs[metric] = get_layer_config(metric, i)

# Add the 3D Height Control section - always show it regardless of metric count
st.sidebar.markdown("### 3D Height Control")

# If multiple metrics are selected, let user choose which one controls height
if len(selected_metrics) > 1:
    height_metric = st.sidebar.radio(
        "Which metric should control the hexagon height?",
        options=selected_metrics,
        index=0,
        key="height_control"
    )
else:
    # If only one metric is selected, it controls the height by default
    height_metric = selected_metrics[0] if selected_metrics else None

# Height multiplier slider - same for single or multiple metrics
height_multiplier = st.sidebar.slider(
    "Height Multiplier",
    min_value=1,
    max_value=200,
    value=100,
    step=5,
    help="Increase this value to make the height differences more pronounced."
)

# Add option to normalize height values - set default to False
normalize_heights = st.sidebar.checkbox(
    "Normalize Height Values", 
    value=True,
    help="Normalize values to range from 0 to 100, making height differences more visible for metrics with small values or little variation."
)

# Fetch cell tower data
@st.cache_data(ttl="1h")
def get_cell_data():
    query = """
    SELECT
        cell_id,
        ROUND(cell_latitude, 4) AS latitude, 
        ROUND(cell_longitude, 4) AS longitude, 
        SUM(CASE WHEN call_release_code = 0 THEN 1 ELSE 0 END) AS total_success, 
        COUNT(*) AS total_calls, 
        ROUND((SUM(CASE WHEN call_release_code != 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) AS failure_rate,
        AVG(PM_PDCP_LAT_TIME_DL) AS avg_dl_latency,
        SUM(PM_RRC_CONN_ESTAB_SUCC) AS total_conn_succ,
        SUM(PM_RRC_CONN_ESTAB_ATT) AS total_conn_att,
        CASE 
            WHEN SUM(PM_RRC_CONN_ESTAB_ATT) > 0 
            THEN ROUND((SUM(PM_RRC_CONN_ESTAB_SUCC) * 100.0 / SUM(PM_RRC_CONN_ESTAB_ATT)), 2)
            ELSE NULL
        END AS conn_success_rate,
        AVG(PM_ERAB_REL_ABNORMAL_ENB) AS avg_abnormal_drop,
        AVG(PM_ACTIVE_UE_DL_MAX) AS avg_dl_speed,
        AVG(PM_ACTIVE_UE_UL_MAX) AS avg_ul_speed,
        AVG(PM_PRB_UTIL_DL) AS avg_dl_util,
        AVG(PM_PRB_UTIL_UL) AS avg_ul_util,
        SUM(PM_S1_SIG_CONN_ESTAB_SUCC) AS total_sig_conn_succ,
        SUM(PM_S1_SIG_CONN_ESTAB_ATT) AS total_sig_conn_att,
        CASE 
            WHEN SUM(PM_S1_SIG_CONN_ESTAB_ATT) > 0 
            THEN ROUND((SUM(PM_S1_SIG_CONN_ESTAB_SUCC) * 100.0 / SUM(PM_S1_SIG_CONN_ESTAB_ATT)), 2)
            ELSE NULL
        END AS sig_conn_success_rate
    FROM TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER
    GROUP BY cell_id, latitude, longitude
    """
    df = session.sql(query).to_pandas()
    # Convert column names to lowercase for consistent access
    df.columns = df.columns.str.lower()
    return df

# Fetch support ticket data
@st.cache_data(ttl="1h")
def get_ticket_data():
    query = """
    SELECT 
        st.cell_id,
        COUNT(DISTINCT st.ticket_id) AS ticket_count,  -- Count distinct ticket IDs to avoid duplicates
        AVG(st.sentiment_score) AS avg_sentiment,
        ROUND(c.cell_latitude, 4) AS latitude,
        ROUND(c.cell_longitude, 4) AS longitude,
        COUNT(DISTINCT CASE WHEN st.service_type = 'Cellular' THEN st.ticket_id END) AS cellular_tickets,
        COUNT(DISTINCT CASE WHEN st.service_type = 'Business Internet' THEN st.ticket_id END) AS business_tickets,
        COUNT(DISTINCT CASE WHEN st.service_type = 'Home Internet' THEN st.ticket_id END) AS home_tickets
    FROM 
        TELCO_NETWORK_OPTIMIZATION_PROD.RAW.SUPPORT_TICKETS st
    JOIN 
        TELCO_NETWORK_OPTIMIZATION_PROD.RAW.CELL_TOWER c ON st.cell_id = c.cell_id
    GROUP BY st.cell_id, latitude, longitude
    """
    df = session.sql(query).to_pandas()
    # Convert column names to lowercase for consistent access
    df.columns = df.columns.str.lower()
    return df

# Fetch and process our data
cell_data = get_cell_data()
ticket_data = get_ticket_data()

# Merge data for combined view
merged_data = pd.merge(
    cell_data,
    ticket_data[['cell_id', 'ticket_count', 'avg_sentiment']],
    on='cell_id',
    how='left'
)
merged_data['ticket_count'] = merged_data['ticket_count'].fillna(0)
merged_data['avg_sentiment'] = merged_data['avg_sentiment'].fillna(0)

# Normalize values for combined score
if len(merged_data) > 0:
    # We keep the merged data for showing raw data but remove combined score calculations 
    pass

# Helper function to determine center of map
def get_map_center(data):
    if len(data) > 0:
        return data['latitude'].mean(), data['longitude'].mean()
    else:
        # Default to San Diego if no data
        return 32.7157, -117.1611

# Function to get quantiles for colormap
@st.cache_data
def get_quantiles(df_column, num_quantiles=20): # Default to 20 quantiles for smoother gradients
    return df_column.quantile(np.linspace(0, 1, num_quantiles + 1))

# Function to get RGBA colors based on values
@st.cache_data
def calculate_rgba_color(df_column, colors_hex_list, quantiles, opacity, reverse=False):
    if reverse:
        colors_hex_list = colors_hex_list[::-1]
    
    colormap = cm.LinearColormap(colors_hex_list, vmin=quantiles.min(), vmax=quantiles.max(), index=quantiles)
    
    rgba_colors = []
    for val in df_column:
        if pd.isna(val):
            rgba_colors.append([0,0,0,0]) # Transparent for NaNs
            continue
        rgb_hex = colormap(val) # Gets hex string like #RRGGBB
        r = int(rgb_hex[1:3], 16)
        g = int(rgb_hex[3:5], 16)
        b = int(rgb_hex[5:7], 16)
        rgba_colors.append([r, g, b, int(opacity * 255)])
    
    # Add debugging
    show_debug(f"calculate_rgba_color output (first few):", rgba_colors[:3] if rgba_colors else "Empty")
    return rgba_colors

# Replace the blend_colors function with a more robust version
@st.cache_data(show_spinner=False)
def blend_colors(color_arrays):
    """Blend multiple RGBA color arrays by averaging their values"""
    show_debug("blend_colors received:", color_arrays)
    
    if not color_arrays or len(color_arrays) == 0:
        return [0, 0, 0, 0]  # Return transparent if empty
    
    # Normalize all colors to a standard format
    normalized_colors = []
    for color in color_arrays:
        if isinstance(color, list):
            # If it's already a list of integers (a single RGBA)
            if len(color) == 4 and all(isinstance(c, int) for c in color):
                normalized_colors.append(color)
            # If it's a list of lists (multiple RGBAs) - take the first one
            elif len(color) > 0 and isinstance(color[0], list):
                normalized_colors.append(color[0])
        # Skip anything else
    
    show_debug("normalized_colors:", normalized_colors)
    
    if not normalized_colors:
        return [0, 0, 0, 0]  # Return transparent if no valid colors
    
    # Initialize with zeros
    blended = [0, 0, 0, 0]
    
    # Sum all components
    for rgba in normalized_colors:
        for j in range(min(4, len(rgba))):  # Ensure we don't go out of bounds
            blended[j] += rgba[j]
    
    # Average RGB components
    num_colors = len(normalized_colors)
    for j in range(3):  # RGB only
        blended[j] = min(255, blended[j] // num_colors)
    
    # Use max alpha
    blended[3] = min(255, max(rgba[3] for rgba in normalized_colors if len(rgba) > 3))
    
    show_debug("Final blended color:", blended)
    return blended

# Modified function to prepare visualization data
def prepare_visualization_data(metric_name, config):
    # Determine source dataframe and value column
    if metric_name == "Failure Rate":
        df = cell_data.copy()
        value_column = 'failure_rate'
        agg_method = 'mean' # Average failure rate
    elif metric_name == "Support Ticket Count":
        df = ticket_data.copy()
        value_column = 'ticket_count'
        agg_method = 'sum' # Sum of tickets
    elif metric_name == "Sentiment Score":
        df = ticket_data.copy()
        value_column = 'avg_sentiment'
        agg_method = 'mean' # Average sentiment
    elif metric_name == "Downlink Latency":
        df = cell_data.copy()
        value_column = 'avg_dl_latency'
        agg_method = 'mean'
    elif metric_name == "Connection Success Rate":
        df = cell_data.copy()
        value_column = 'conn_success_rate'
        agg_method = 'mean'
    elif metric_name == "Abnormal Drop Rate":
        df = cell_data.copy()
        value_column = 'avg_abnormal_drop'
        agg_method = 'mean'
    elif metric_name == "Downlink Speed":
        df = cell_data.copy()
        value_column = 'avg_dl_speed'
        agg_method = 'mean'
    elif metric_name == "Uplink Speed":
        df = cell_data.copy()
        value_column = 'avg_ul_speed'
        agg_method = 'mean'
    elif metric_name == "Resource Utilization Downlink":
        df = cell_data.copy()
        value_column = 'avg_dl_util'
        agg_method = 'mean'
    elif metric_name == "Resource Utilization Uplink":
        df = cell_data.copy()
        value_column = 'avg_ul_util'
        agg_method = 'mean'
    elif metric_name == "Signal Connection Success Rate":
        df = cell_data.copy()
        value_column = 'sig_conn_success_rate'
        agg_method = 'mean'
    
    title = metric_name # Title can just be the metric name
    
    essential_cols = ['latitude', 'longitude', value_column, 'cell_id']
    if not all(col in df.columns for col in essential_cols):
        st.sidebar.error(f"Missing essential columns ({essential_cols}) for {metric_name}.")
        return pd.DataFrame(), 0, 0, title, value_column # Return empty df

    df = df.dropna(subset=essential_cols).copy() # Use .copy() to avoid SettingWithCopyWarning
    if df.empty:
        st.sidebar.warning(f"No valid data after NaNs dropped for {metric_name}.")
        return pd.DataFrame(), 0, 0, title, value_column

    # --- Stage 1: Prepare per-cell data --- 
    df['h3_actual_index'] = df.apply(lambda row: h3.geo_to_h3(float(row['latitude']), float(row['longitude']), config['resolution']), axis=1)
    df['numeric_metric_value'] = pd.to_numeric(df[value_column], errors='coerce')
    df = df.dropna(subset=['numeric_metric_value', 'h3_actual_index'])
    df['cell_id_str'] = df['cell_id'].astype(str) # Ensure cell_id is string for aggregation

    if df.empty:
        st.sidebar.warning(f"No valid data after H3 generation for {metric_name}.")
        return pd.DataFrame(), 0, 0, title, value_column

    # --- Stage 2: Aggregate by H3 index --- 
    aggregation_dict = {
        'numeric_metric_value': agg_method, 
        'cell_id_str': lambda x: ", ".join(x) # Aggregate cell IDs into a string
    }
    # Add lat/lon mean if needed for centering (optional)
    # aggregation_dict['latitude'] = 'mean'
    # aggregation_dict['longitude'] = 'mean'

    aggregated_df = df.groupby('h3_actual_index').agg(aggregation_dict).reset_index()

    # Rename aggregated value column for clarity
    aggregated_df = aggregated_df.rename(columns={'numeric_metric_value': 'agg_numeric_value'})
    
    # --- Stage 3: Add display columns to aggregated data --- 
    aggregated_df['metric_name_for_tooltip'] = metric_name
    
    # Create cell tower display string (limit length)
    def format_cell_ids(ids_str, limit=3):
        ids = ids_str.split(", ")
        if len(ids) > limit:
            return f"Cell Tower(s): {', '.join(ids[:limit])}, ... ({len(ids)} total)"
        else:
            return f"Cell Tower(s): {', '.join(ids)}"
    aggregated_df['cell_towers_display'] = aggregated_df['cell_id_str'].apply(format_cell_ids)

    # Format aggregated value for display
    if metric_name in ["Failure Rate", "Connection Success Rate", "Signal Connection Success Rate", "Resource Utilization Downlink", "Resource Utilization Uplink"]:
        aggregated_df['aggregated_value_display'] = aggregated_df['agg_numeric_value'].round(2).astype(str) + "%"
    elif metric_name == "Sentiment Score":
        aggregated_df['aggregated_value_display'] = aggregated_df['agg_numeric_value'].round(2).astype(str)
    elif metric_name in ["Downlink Speed", "Uplink Speed"]:
        aggregated_df['aggregated_value_display'] = aggregated_df['agg_numeric_value'].round(2).astype(str) + " Mbps"
    elif metric_name == "Downlink Latency":
        aggregated_df['aggregated_value_display'] = aggregated_df['agg_numeric_value'].round(2).astype(str) + " ms"
    else: # Ticket Count, Abnormal Drop Rate, etc.
        aggregated_df['aggregated_value_display'] = aggregated_df['agg_numeric_value'].round(0).astype(int).astype(str)

    # --- Stage 4: Calculate RGBA Color based on aggregated value --- 
    # For Connection Success Rate and Signal Connection Success Rate, higher is better
    reverse_colormap = (metric_name in ["Sentiment Score", "Connection Success Rate", "Downlink Speed", "Uplink Speed", "Signal Connection Success Rate"])
    opacity = config['opacity']
    style = config['style_option']
    colors_hex_list = [] 
    # (Assume color definitions like colors_yellow_blue etc. are still available globally or defined earlier)
    if style == "Yellow Blue": colors_hex_list = colors_yellow_blue
    elif style == "Yellow-Red": colors_hex_list = colors_yellow_red
    elif style == "Blue-Green": colors_hex_list = colors_blue_green
    elif style == "White-Blue": colors_hex_list = colors_white_blue
    elif style == "White-Red": colors_hex_list = colors_white_red
    elif style == "White-Green": colors_hex_list = colors_white_green
    # Fixed colors removed

    if colors_hex_list and not aggregated_df.empty: 
        if aggregated_df['agg_numeric_value'].nunique() > 1: 
            quantiles = get_quantiles(aggregated_df['agg_numeric_value'], num_quantiles=len(colors_hex_list) - 1)
            aggregated_df['rgba_color'] = calculate_rgba_color(aggregated_df['agg_numeric_value'], colors_hex_list, quantiles, opacity, reverse=reverse_colormap)
        else: 
            mid_color_hex = colors_hex_list[len(colors_hex_list) // 2] if len(colors_hex_list) > 0 else '#808080'
            r = int(mid_color_hex[1:3], 16); g = int(mid_color_hex[3:5], 16); b = int(mid_color_hex[5:7], 16)
            aggregated_df['rgba_color'] = [[r,g,b, int(opacity*255)]] * len(aggregated_df)
    elif not aggregated_df.empty:
         aggregated_df['rgba_color'] = [[128, 128, 128, int(opacity*255)]] * len(aggregated_df) # Grey
    else: # Handle case where aggregated_df might be empty
        aggregated_df['rgba_color'] = []

    # Calculate center based on original data (more stable than aggregated means)
    center_lat, center_lon = get_map_center(df)
    
    # Return the aggregated dataframe
    return aggregated_df, center_lat, center_lon, title, value_column # Note: value_column here is original, might not be directly used later

# Update the create_layer function to normalize single metric elevations
def create_layer(metric_name, df, value_column, config, z_index=0):
    layer_id = f"h3_layer_{metric_name}_{config['resolution']}".lower().replace(" ", "_") 
    
    # Create a copy of the dataframe to avoid modifying the original
    df_copy = df.copy()
    
    # If this is the height metric and normalization is enabled, add a normalized value
    if metric_name == height_metric and normalize_heights and 'agg_numeric_value' in df_copy.columns:
        min_val = df_copy['agg_numeric_value'].min()
        max_val = df_copy['agg_numeric_value'].max()
        
        # Only normalize if we have different values
        if min_val != max_val:
            df_copy['normalized_elevation'] = ((df_copy['agg_numeric_value'] - min_val) / (max_val - min_val)) * 100
            elevation_column = 'normalized_elevation'
        else:
            elevation_column = 'agg_numeric_value'
    else:
        elevation_column = 'agg_numeric_value'
    
    return pdk.Layer(
        "H3HexagonLayer",
        data=df_copy,
        id=layer_id,
        pickable=True,
        stroked=True,
        filled=True,
        get_hexagon="h3_actual_index",
        get_fill_color="rgba_color", 
        extruded=(metric_name == height_metric),
        # Use the appropriate elevation column
        get_elevation=elevation_column if (metric_name == height_metric) else 0,
        elevation_scale=height_multiplier if (metric_name == height_metric) else 0, 
        z_index=z_index
    )

# Instead of creating one layer per metric, we'll create a combined layer
combined_df = None
all_h3_indices = set()
data_for_metric = {}
metric_info = {}

# First pass: collect all H3 indices and create individual dataframes
for i, metric_name in enumerate(selected_metrics):
    config = layer_configs[metric_name]
    aggregated_df, lat, lon, title, _ = prepare_visualization_data(metric_name, config)
    
    if not aggregated_df.empty:
        # Store data for this metric
        data_for_metric[metric_name] = aggregated_df
        metric_info[metric_name] = {
            "title": title,
            "value_column": None,
            "center_lat": lat,
            "center_lon": lon,
            "config": config
        }
        # Collect all H3 indices
        all_h3_indices.update(aggregated_df['h3_actual_index'].tolist())
    else:
        st.sidebar.warning(f"Could not generate layer for {metric_name} due to lack of valid data.")

# Second pass: create a combined dataframe with blended colors
if all_h3_indices and len(selected_metrics) > 1:
    # Create a new dataframe with all unique H3 indices
    combined_df = pd.DataFrame({'h3_actual_index': list(all_h3_indices)})
    
    # For each H3 index, collect colors from all metrics and blend them
    color_data = []
    show_debug("Starting to process H3 indices for color blending", f"Total indices: {len(all_h3_indices)}")

    # First pass to find min/max values for height metric if normalization is enabled
    height_min_val = None
    height_max_val = None

    if normalize_heights and height_metric in data_for_metric:
        height_data = data_for_metric[height_metric]
        if 'agg_numeric_value' in height_data.columns and not height_data.empty:
            height_min_val = height_data['agg_numeric_value'].min()
            height_max_val = height_data['agg_numeric_value'].max()
            show_debug("Height metric normalization range:", f"Min: {height_min_val}, Max: {height_max_val}")

    for h3_index in all_h3_indices:
        colors_for_cell = []
        tooltip_parts = []
        cell_towers = []
        agg_value = 0
        
        for metric_name in selected_metrics:
            if metric_name in data_for_metric:
                metric_df = data_for_metric[metric_name]
                if h3_index in metric_df['h3_actual_index'].values:
                    row = metric_df[metric_df['h3_actual_index'] == h3_index].iloc[0]
                    # Add a check for the type of color data
                    if 'rgba_color' in row:
                        color_value = row['rgba_color']
                        # Skip None or empty values
                        if color_value is not None and color_value != []:
                            colors_for_cell.append(color_value)
                    tooltip_parts.append(f"{row['metric_name_for_tooltip']}: {row['aggregated_value_display']}")
                    if len(cell_towers) == 0 and 'cell_towers_display' in row:
                        cell_towers = [row['cell_towers_display']]
                    if metric_name == height_metric and 'agg_numeric_value' in row:
                        # If normalization is enabled and we have min/max values, normalize the value
                        if normalize_heights and height_min_val is not None and height_max_val is not None and height_min_val != height_max_val:
                            agg_value = ((row['agg_numeric_value'] - height_min_val) / (height_max_val - height_min_val)) * 100
                        else:
                            agg_value = row['agg_numeric_value']
        
        # Blend colors if we have multiple
        if len(colors_for_cell) > 1:
            show_debug(f"Blending colors for h3_index {h3_index}", colors_for_cell)
            blended_color = blend_colors(colors_for_cell)
            show_debug("Resulting blended color:", blended_color)
        elif len(colors_for_cell) == 1:
            blended_color = colors_for_cell[0]
        else:
            blended_color = [0, 0, 0, 0]  # Transparent if no data
        
        color_data.append({
            'h3_actual_index': h3_index,
            'rgba_color': blended_color,
            'tooltip_text': "\n".join(tooltip_parts),
            'cell_towers_display': cell_towers[0] if cell_towers else "No cell tower data",
            'agg_numeric_value': agg_value
        })
    
    # Create the combined dataframe
    combined_df = pd.DataFrame(color_data)
    
    # Create a single layer with blended colors
    blended_layer = pdk.Layer(
        "H3HexagonLayer",
        data=combined_df,
        id="h3_blended_layer",
        pickable=True,
        stroked=True,
        filled=True,
        get_hexagon="h3_actual_index",
        get_fill_color="rgba_color",
        extruded=(height_metric in selected_metrics),
        get_elevation="agg_numeric_value" if height_metric in selected_metrics else 0,
        elevation_scale=height_multiplier if height_metric in selected_metrics else 0
    )
    
    # Replace all individual layers with our blended layer
    layers = [blended_layer]
else:
    # If we only have one metric or no data, use the original approach
    layers = []
    for i, metric_name in enumerate(selected_metrics):
        if metric_name in data_for_metric:
            config = layer_configs[metric_name]
            layer = create_layer(metric_name, data_for_metric[metric_name], None, config, z_index=i)
            layers.append(layer)

# Calculate the overall map center from all selected metrics
if selected_metrics and metric_info: 
    # Filter out metrics not in metric_info (e.g., if prepare_visualization_data returned empty for them)
    valid_metrics_for_center = [m for m in selected_metrics if m in metric_info and metric_info[m].get("center_lat") is not None]
    if valid_metrics_for_center:
        center_lat = sum(metric_info[m]["center_lat"] for m in valid_metrics_for_center) / len(valid_metrics_for_center)
        center_lon = sum(metric_info[m]["center_lon"] for m in valid_metrics_for_center) / len(valid_metrics_for_center)
    else:
        center_lat, center_lon = 32.7157, -117.1611 # Default if no valid metrics
else:
    center_lat, center_lon = 32.7157, -117.1611

# Create a full-width layout for the map and move statistics below
st.markdown("""
<style>
.element-container:has(iframe.deckgl-ui) {
height: 600px;
}
iframe.deckgl-ui {
height: 600px !important;
}
</style>
""", unsafe_allow_html=True)

# Create and display the map with PyDeck - full width
st.subheader("🗺️ Multi-Metric Geospatial Analysis")

# Always render the main visualization using H3HexagonLayers
st.pydeck_chart(
    pdk.Deck(
        map_provider="mapbox",
        map_style="mapbox://styles/mapbox/light-v9", 
        initial_view_state=pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=5, 
            pitch=45 if any(metric == height_metric for metric in selected_metrics) else 0,
            bearing=0,
            height=600  
        ),
        layers=layers, 
        tooltip={
            "text": "{cell_towers_display}\n{tooltip_text}" if len(selected_metrics) > 1 else "{cell_towers_display}\n{metric_name_for_tooltip}: {aggregated_value_display}",
            "style": {"backgroundColor": "rgb(14, 17, 23)", "color": "white"}
        }
    ),
    use_container_width=True,
    height=600,
    key=f"map_main_h3_{'_'.join(selected_metrics)}_{hash(str(layer_configs))}"
)

# Statistics section - Now using tabs for better organization
st.subheader("📈 Data Analysis")
tab1, tab2 = st.tabs(["Key Statistics", "Raw Data"])

with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        # Cell Tower Statistics
        st.subheader("Cell Tower Statistics")
        
        # Top 5 worst performing cells
        st.markdown("#### Top 5 Worst Performing Cell Towers")
        worst_cells = cell_data.sort_values(by='failure_rate', ascending=False).head(5)
        st.dataframe(
            worst_cells[['cell_id', 'failure_rate', 'total_calls']].rename(columns={
                'cell_id': 'Cell ID',
                'failure_rate': 'Failure Rate (%)',
                'total_calls': 'Total Calls'
            }),
            use_container_width=True
        )
        
        # Connection Statistics
        st.markdown("#### Connection Quality Statistics")
        # Filter out rows with null values for these metrics
        conn_data = cell_data.dropna(subset=['conn_success_rate', 'sig_conn_success_rate'])
        # Only display if we have data
        if len(conn_data) > 0:
            conn_stats = pd.DataFrame({
                'Metric': ['Avg Connection Success Rate (%)', 'Avg Signal Connection Success Rate (%)', 'Worst Connection Success Rate (%)'],
                'Value': [
                    f"{conn_data['conn_success_rate'].mean():.2f}%",
                    f"{conn_data['sig_conn_success_rate'].mean():.2f}%",
                    f"{conn_data['conn_success_rate'].min():.2f}%"
                ]
            })
            st.dataframe(conn_stats, use_container_width=True, hide_index=True)
        else:
            st.info("No connection quality data available")

    with col2:
        # Support Ticket Statistics
        st.subheader("Support Ticket Statistics")
        
        # Areas with most tickets
        st.markdown("#### Areas with Most Support Tickets")
        most_tickets = ticket_data.sort_values(by='ticket_count', ascending=False).head(5)
        st.dataframe(
            most_tickets[['cell_id', 'ticket_count', 'avg_sentiment']].rename(columns={
                'cell_id': 'Cell ID',
                'ticket_count': 'Ticket Count',
                'avg_sentiment': 'Avg Sentiment'
            }),
            use_container_width=True
        )
        
        # Performance Statistics
        st.markdown("#### Network Performance Statistics")
        # Filter out rows with null values for these metrics
        perf_data = cell_data.dropna(subset=['avg_dl_speed', 'avg_ul_speed', 'avg_dl_latency'])
        # Only display if we have data
        if len(perf_data) > 0:
            perf_stats = pd.DataFrame({
                'Metric': ['Avg Downlink Speed (Mbps)', 'Avg Uplink Speed (Mbps)', 'Avg Downlink Latency (ms)'],
                'Value': [
                    f"{perf_data['avg_dl_speed'].mean():.2f}",
                    f"{perf_data['avg_ul_speed'].mean():.2f}",
                    f"{perf_data['avg_dl_latency'].mean():.2f}"
                ]
            })
            st.dataframe(perf_stats, use_container_width=True, hide_index=True)
        else:
            st.info("No performance data available")
    
    # Create a new row for utilization and drop statistics
    col3, col4 = st.columns(2)
    
    with col3:
        # Resource Utilization Statistics
        st.markdown("#### Resource Utilization Statistics")
        # Filter out rows with null values for these metrics
        util_data = cell_data.dropna(subset=['avg_dl_util', 'avg_ul_util'])
        # Only display if we have data
        if len(util_data) > 0:
            # Find cells with high utilization (potential congestion)
            high_util_threshold = 80
            high_dl_util = util_data[util_data['avg_dl_util'] > high_util_threshold]
            high_ul_util = util_data[util_data['avg_ul_util'] > high_util_threshold]
            
            util_stats = pd.DataFrame({
                'Metric': [
                    'Avg Downlink Utilization (%)', 
                    'Avg Uplink Utilization (%)',
                    f'Cells with DL Util > {high_util_threshold}%',
                    f'Cells with UL Util > {high_util_threshold}%'
                ],
                'Value': [
                    f"{util_data['avg_dl_util'].mean():.2f}%",
                    f"{util_data['avg_ul_util'].mean():.2f}%",
                    f"{len(high_dl_util)} ({len(high_dl_util)/len(util_data)*100:.1f}%)",
                    f"{len(high_ul_util)} ({len(high_ul_util)/len(util_data)*100:.1f}%)"
                ]
            })
            st.dataframe(util_stats, use_container_width=True, hide_index=True)
        else:
            st.info("No utilization data available")
    
    with col4:
        # Abnormal Drop Statistics
        st.markdown("#### Abnormal Drop Statistics")
        # Filter out rows with null values for these metrics
        drop_data = cell_data.dropna(subset=['avg_abnormal_drop'])
        # Only display if we have data
        if len(drop_data) > 0:
            # Find cells with high drop rates
            high_drop_threshold = 50
            high_drop_cells = drop_data[drop_data['avg_abnormal_drop'] > high_drop_threshold]
            
            drop_stats = pd.DataFrame({
                'Metric': [
                    'Avg Abnormal Drop Rate', 
                    'Max Abnormal Drop Rate',
                    f'Cells with Drop Rate > {high_drop_threshold}'
                ],
                'Value': [
                    f"{drop_data['avg_abnormal_drop'].mean():.2f}",
                    f"{drop_data['avg_abnormal_drop'].max():.2f}",
                    f"{len(high_drop_cells)} ({len(high_drop_cells)/len(drop_data)*100:.1f}%)"
                ]
            })
            st.dataframe(drop_stats, use_container_width=True, hide_index=True)
        else:
            st.info("No abnormal drop data available")
    
    # Key Insights section (ensure this is also under tab1, but not cols)
    st.subheader("Key Insights")
    
    # Find cells with issues
    if len(merged_data) > 0:
        # Show the cell with highest failure rate instead
        worst_failure_cell = cell_data.loc[cell_data['failure_rate'].idxmax()]
        # Show the cell with most tickets
        most_tickets_cell = ticket_data.sort_values(by='ticket_count', ascending=False).iloc[0] if len(ticket_data) > 0 else None
        
        # Add insights for new metrics only if they exist in the data
        insights_list = [f"- Cell Tower **{worst_failure_cell['cell_id']}** shows the highest failure rate: {worst_failure_cell['failure_rate']}%"]
        
        if most_tickets_cell is not None:
            insights_list.append(f"- Cell Tower **{most_tickets_cell['cell_id']}** has the most support tickets: {most_tickets_cell['ticket_count']} tickets with average sentiment: {most_tickets_cell['avg_sentiment']:.2f}")
        
        # Add new insights for the additional metrics
        if 'conn_success_rate' in cell_data.columns and not cell_data['conn_success_rate'].isna().all():
            worst_conn_cell = cell_data.dropna(subset=['conn_success_rate']).sort_values(by='conn_success_rate').iloc[0]
            insights_list.append(f"- Cell Tower **{worst_conn_cell['cell_id']}** has the lowest connection success rate: {worst_conn_cell['conn_success_rate']}%")
        
        if 'avg_dl_latency' in cell_data.columns and not cell_data['avg_dl_latency'].isna().all():
            worst_latency_cell = cell_data.dropna(subset=['avg_dl_latency']).sort_values(by='avg_dl_latency', ascending=False).iloc[0]
            insights_list.append(f"- Cell Tower **{worst_latency_cell['cell_id']}** has the highest downlink latency: {worst_latency_cell['avg_dl_latency']:.2f} ms")
        
        if 'avg_abnormal_drop' in cell_data.columns and not cell_data['avg_abnormal_drop'].isna().all():
            worst_drop_cell = cell_data.dropna(subset=['avg_abnormal_drop']).sort_values(by='avg_abnormal_drop', ascending=False).iloc[0]
            insights_list.append(f"- Cell Tower **{worst_drop_cell['cell_id']}** has the highest abnormal drop rate: {worst_drop_cell['avg_abnormal_drop']:.2f}")
        
        st.markdown("#### Priority Areas:")
        for insight in insights_list:
            st.markdown(insight)

with tab2:
    # Show raw data
    raw_data_tabs = st.tabs(["Cell Tower Data", "Support Ticket Data", "Combined Data"])
    
    with raw_data_tabs[0]:
        st.dataframe(cell_data)
    
    with raw_data_tabs[1]:
        st.dataframe(ticket_data)
    
    with raw_data_tabs[2]:
        st.dataframe(merged_data) 

# Cell Data Explorer remains as a useful tool
with st.expander("Cell Data Explorer", expanded=False):
    try:
        all_cell_ids = []
        if not cell_data.empty:
            all_cell_ids.extend(cell_data['cell_id'].astype(str).unique())
        if not ticket_data.empty:
            all_cell_ids.extend(ticket_data['cell_id'].astype(str).unique())
        if not merged_data.empty:
            all_cell_ids.extend(merged_data['cell_id'].astype(str).unique())
        
        all_cell_ids = sorted(list(set(all_cell_ids)))
        if all_cell_ids:
            selected_cell_explorer = st.selectbox(
                "Select Cell ID for Explorer", 
                options=all_cell_ids, 
                index=0, 
                key="cell_explorer_selectbox"
            )
            
            st.write(f"### Data for Cell ID: {selected_cell_explorer}")
            
            # Create tabs for different metric categories
            metric_tabs = st.tabs(["Basic Metrics", "Performance Metrics", "Connection Quality", "Resource Utilization"])
            
            with metric_tabs[0]:
                col1_exp, col2_exp = st.columns(2)
                
                with col1_exp:
                    # Basic cell tower metrics
                    if "Failure Rate" in selected_metrics and not cell_data.empty:
                        cell_info = cell_data[cell_data['cell_id'].astype(str) == selected_cell_explorer]
                        if not cell_info.empty:
                            st.metric(
                                label="Failure Rate", 
                                value=f"{cell_info['failure_rate'].values[0]:.2f}%",
                                delta=None
                            )
                            st.write(f"Total Calls: {cell_info['total_calls'].values[0]:,}")
                            st.write(f"Lat: {cell_info['latitude'].values[0]:.4f}, Lon: {cell_info['longitude'].values[0]:.4f}")
                
                with col2_exp:
                    # Support ticket metrics
                    if "Support Ticket Count" in selected_metrics and not ticket_data.empty:
                        cell_info = ticket_data[ticket_data['cell_id'].astype(str) == selected_cell_explorer]
                        if not cell_info.empty:
                            st.metric(
                                label="Ticket Count",
                                value=f"{int(cell_info['ticket_count'].values[0]):,}",
                                delta=None
                            )
                            st.metric(
                                label="Sentiment Score",
                                value=f"{cell_info['avg_sentiment'].values[0]:.2f}",
                                delta=None
                            )
            
            with metric_tabs[1]:
                if not cell_data.empty:
                    cell_info = cell_data[cell_data['cell_id'].astype(str) == selected_cell_explorer]
                    if not cell_info.empty:
                        col1_perf, col2_perf = st.columns(2)
                        
                        with col1_perf:
                            # Downlink metrics
                            if 'avg_dl_speed' in cell_info.columns and not pd.isna(cell_info['avg_dl_speed'].values[0]):
                                st.metric(
                                    label="Downlink Speed",
                                    value=f"{cell_info['avg_dl_speed'].values[0]:.2f} Mbps",
                                    delta=None
                                )
                            
                            if 'avg_dl_latency' in cell_info.columns and not pd.isna(cell_info['avg_dl_latency'].values[0]):
                                st.metric(
                                    label="Downlink Latency",
                                    value=f"{cell_info['avg_dl_latency'].values[0]:.2f} ms",
                                    delta=None
                                )
                        
                        with col2_perf:
                            # Uplink metrics
                            if 'avg_ul_speed' in cell_info.columns and not pd.isna(cell_info['avg_ul_speed'].values[0]):
                                st.metric(
                                    label="Uplink Speed",
                                    value=f"{cell_info['avg_ul_speed'].values[0]:.2f} Mbps",
                                    delta=None
                                )
            
            with metric_tabs[2]:
                if not cell_data.empty:
                    cell_info = cell_data[cell_data['cell_id'].astype(str) == selected_cell_explorer]
                    if not cell_info.empty:
                        col1_conn, col2_conn = st.columns(2)
                        
                        with col1_conn:
                            # Connection success metrics
                            if 'conn_success_rate' in cell_info.columns and not pd.isna(cell_info['conn_success_rate'].values[0]):
                                st.metric(
                                    label="Connection Success Rate",
                                    value=f"{cell_info['conn_success_rate'].values[0]:.2f}%",
                                    delta=None
                                )
                            
                            if 'sig_conn_success_rate' in cell_info.columns and not pd.isna(cell_info['sig_conn_success_rate'].values[0]):
                                st.metric(
                                    label="Signal Connection Success Rate",
                                    value=f"{cell_info['sig_conn_success_rate'].values[0]:.2f}%",
                                    delta=None
                                )
                        
                        with col2_conn:
                            # Abnormal drops
                            if 'avg_abnormal_drop' in cell_info.columns and not pd.isna(cell_info['avg_abnormal_drop'].values[0]):
                                st.metric(
                                    label="Abnormal Drop Rate",
                                    value=f"{cell_info['avg_abnormal_drop'].values[0]:.2f}",
                                    delta=None
                                )
            
            with metric_tabs[3]:
                if not cell_data.empty:
                    cell_info = cell_data[cell_data['cell_id'].astype(str) == selected_cell_explorer]
                    if not cell_info.empty:
                        col1_util, col2_util = st.columns(2)
                        
                        with col1_util:
                            # Downlink utilization
                            if 'avg_dl_util' in cell_info.columns and not pd.isna(cell_info['avg_dl_util'].values[0]):
                                dl_util = cell_info['avg_dl_util'].values[0]
                                st.metric(
                                    label="Downlink Utilization",
                                    value=f"{dl_util:.2f}%",
                                    # Show warning if utilization is high (>80%)
                                    delta="High - Possible Congestion" if dl_util > 80 else None,
                                    delta_color="inverse"
                                )
                        
                        with col2_util:
                            # Uplink utilization
                            if 'avg_ul_util' in cell_info.columns and not pd.isna(cell_info['avg_ul_util'].values[0]):
                                ul_util = cell_info['avg_ul_util'].values[0]
                                st.metric(
                                    label="Uplink Utilization",
                                    value=f"{ul_util:.2f}%",
                                    # Show warning if utilization is high (>80%)
                                    delta="High - Possible Congestion" if ul_util > 80 else None,
                                    delta_color="inverse"
                                )
        else:
            st.write("No cell IDs available to explore. Ensure data is loaded.")
            
    except Exception as e:
        st.error(f"Error in Cell Data Explorer: {str(e)}") 