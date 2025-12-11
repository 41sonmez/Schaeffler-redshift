import streamlit as st
import pandas as pd
import psycopg2 
import altair as alt 
from typing import Optional, Dict

# TARGET_TABLE remains here as it is not sensitive
TARGET_TABLE = "schema_f540_fst_ohd_camera_results.material_processes_count_test"

# ⚠️ --- CONNECTION SETTINGS (SECRETS) ---
# Attempt to read Redshift connection parameters from st.secrets.
try:
    # When deployed on Streamlit Cloud, these details are read securely from st.secrets["redshift"].
    DB_HOST = st.secrets["redshift"]["host"]
    DB_NAME = st.secrets["redshift"]["dbname"]
    DB_USER = st.secrets["redshift"]["user"]
    DB_PASS = st.secrets["redshift"]["password"]
    DB_PORT = st.secrets["redshift"]["port"]
except KeyError:
    # Error handling for local testing or when secrets are not yet defined in the cloud.
    st.error("❌ Redshift connection details not found in st.secrets. Please check your configuration file.")
    
    # Assign None values to prevent connection attempts if secrets are missing
    DB_HOST = None
    DB_NAME = None
    DB_USER = None
    DB_PASS = None
    DB_PORT = 5439

# 1. Redshift Connection Function
# @st.cache_resource caches the connection object (conn) for the application's lifespan.
@st.cache_resource
def get_redshift_connection():
    """Establishes and caches the Redshift connection."""
    # Do not attempt connection if essential secrets are missing
    if not DB_HOST:
        return None
        
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=int(DB_PORT) # Ensure port number is an integer
        )
        return conn
    except Exception as e:
        st.error(f"❌ Redshift Connection Error: {e}")
        return None

# 2. Main Data Fetch Function
# Caches the DataFrame data for 1800 seconds (30 minutes).
@st.cache_data(ttl=1800)
def get_data_from_redshift(_conn, table_name):
    """Fetches all data from Redshift and returns it as a DataFrame."""
    if _conn is None:
        return pd.DataFrame()
    
    sql_query = f"""
    SELECT 
        part_number, 
        production_variant, 
        development_variant, 
        production_step, 
        quantity
    FROM {table_name}
    """
    try:
        df = pd.read_sql_query(sql_query, _conn)
        return df
    except Exception as e:
        st.error(f"❌ Data Fetch Error (SQL): {e}")
        return pd.DataFrame()
    
# 3. Streamlit Application Flow
def main():
    # Set page configuration (wide layout)
    st.set_page_config(layout="wide") 
    st.title("🏭 Material Process and Variant Analysis")
    
    conn = get_redshift_connection()
    if conn is None:
        return # Stop the application if connection cannot be established

    df_data = get_data_from_redshift(conn, TARGET_TABLE)
    
    if not df_data.empty:
        
        # ----------------------------------------------------
        # 1. SIDEBAR FILTERS 🎚️
        # ----------------------------------------------------
        st.sidebar.header("🔍 Filtering Options")
        
        all_variants = df_data['production_variant'].unique()
        selected_prod_variant = st.sidebar.multiselect(
            "Select Production Variant:",
            options=all_variants,
            default=all_variants
        )

        all_steps = df_data['production_step'].unique()
        selected_step = st.sidebar.multiselect(
            "Select Production Step:",
            options=all_steps,
            default=all_steps
        )
        
        df_filtered = df_data[
            (df_data['production_variant'].isin(selected_prod_variant)) &
            (df_data['production_step'].isin(selected_step))
        ]
        
        # Refresh button to clear cache and fetch new data
        if st.sidebar.button("🔄 Update 🔄", type="secondary"):
            
            # 1. Clears Data Cache
            get_data_from_redshift.clear()
            # 2. Reruns the page to fetch updated data
            st.rerun() 
            
        # Information about data freshness
        st.sidebar.markdown("---")
        st.sidebar.info("✨ Data loaded successfully. Check the database timestamp for freshness.") 

        # ----------------------------------------------------
        # 2. TOP METRICS 🔢
        # ----------------------------------------------------
        total_parts = len(df_filtered['part_number'].unique())
        total_quantity = df_filtered['quantity'].sum()
        total_steps = len(df_filtered['production_step'].unique())
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Unique Part Number", total_parts)
        col2.metric("Total Filtered Stock (Quantity)", f"{total_quantity:,}")
        col3.metric("Number of Filtered Production Steps", total_steps)

        # ----------------------------------------------------
        # 3. VISUALIZATION (Cleaned Altair Chart) 🎨
        # ----------------------------------------------------
        st.subheader("Stock Distribution by Production Steps")
        
        # Filter out records where 'production_step' is empty
        df_chart_data = df_filtered[df_filtered['production_step'] != ''].copy()
        
        df_group_step = df_chart_data.groupby('production_step')['quantity'].sum().sort_values(ascending=False).reset_index()
        
        # Total Stock number displayed above the chart
        chart_total_quantity = df_group_step['quantity'].sum()
        st.markdown(f"**Total Displayed Stock (All Steps):** **<span style='font-size: 24px;'>{chart_total_quantity: ,}</span>**", unsafe_allow_html=True)
        st.markdown("---") 

        # Altair Bar Chart
        chart = alt.Chart(df_group_step).mark_bar().encode(
            x=alt.X('quantity', title=None), # X-Axis Title removed
            y=alt.Y('production_step', title=None, sort='-x', 
                     axis=alt.Axis(labelAlign='right')), # Y-Axis Title removed, labels right-aligned
            tooltip=['production_step', alt.Tooltip('quantity', format=',')] 
        ).properties(
            title='' # Chart title suppressed
        ).interactive() 

        st.altair_chart(chart, use_container_width=True)


        # ----------------------------------------------------
        # 4. DUAL TABLE LAYOUT 📚
        # ----------------------------------------------------
        st.subheader("🔍 Filtered Raw Data Table Layout")

        # 1. Complete Records (where all relevant fields are non-empty)
        df_full = df_filtered[
            (df_filtered['production_step'] != '') &
            (df_filtered['production_variant'] != '') &
            (df_filtered['development_variant'] != '')
        ]

        # 2. Part Number and Quantity Summary
        df_summary = df_filtered[['part_number', 'quantity']]

        col_full, col_summary = st.columns(2)

        with col_full:
            st.markdown("##### ✅ Complete & Full Records")
            st.dataframe(df_full, use_container_width=True)
            st.caption(f"Number of complete records shown: {len(df_full)} / {len(df_filtered)}")

        with col_summary:
            st.markdown("##### 🔢 Part Number and Quantity Summary (All Filtered Records)")
            st.dataframe(df_summary, use_container_width=True)
            st.caption(f"Total number of records shown: {len(df_summary)}")
        
        st.success(f"✅ Successfully filtered {len(df_filtered)} records from {len(df_data)} total records.")
    else:
        st.warning("⚠️ Data table is empty or could not be fetched.")
    

if __name__ == "__main__":
    main()