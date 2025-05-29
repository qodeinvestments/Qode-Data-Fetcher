import streamlit as st
from database import get_table_metadata, get_table_sample_data
from utils import parse_table_name, describe_table_type

def data_explorer(query_engine):
    st.header("Data Explorer")
    
    st.subheader("Table Naming Convention")
    
    st.markdown("""
    **Table Name Format:** `EXCHANGE_INSTRUMENT_UNDERLYING_[ADDITIONAL_PARAMS]`
    
    **Examples:**
    - **Index:** `NSE_Index_NIFTY`, `BSE_Index_SENSEX`
    - **Options:** `NSE_Options_NIFTY_20240125_21000_CE`, `BSE_Options_BANKNIFTY_20240201_45000_PE`
    - **Futures:** `NSE_Futures_NIFTY_I`, `MCX_Futures_GOLD_II`, `NCDEX_Futures_WHEAT_III`
    - **Stocks:** `NSE_Stocks_RELIANCE`, `BSE_Stocks_TCS`
    
    **Parameter Guide:**
    - **Exchange:** NSE, BSE, MCX, NCDEX, etc.
    - **Instrument:** Index, Options, Futures, Stocks
    - **Underlying:** NIFTY, BANKNIFTY, SENSEX, stock symbols, commodity names
    - **Options Additional:** YYYYMMDD (expiry), strike price, CE/PE (call/put)
    - **Futures Additional:** I/II/III (delivery cycles - near/middle/far month)
    """)
    
    st.markdown("---")
    
    st.subheader("Table Information")
    
    search_input = st.text_input(
        "Enter exact table name:",
        placeholder="e.g., NSE_Index_NIFTY",
        help="Enter the complete table name exactly as shown in the naming convention"
    )
    
    if search_input:
        search_input = search_input.strip()
        
        show_table_details(query_engine, search_input)


def show_table_details(query_engine, table_name):
    st.subheader("Table Details")
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown(f"**Table:** `{table_name}`")
        st.markdown(f"**Description:** {describe_table_type(table_name)}")
    
    with col2:
        parsed_info = parse_table_name(table_name)
        if parsed_info['exchange']:
            st.markdown(f"**Exchange:** {parsed_info['exchange']}")
        if parsed_info['instrument_type']:
            st.markdown(f"**Type:** {parsed_info['instrument_type']}")
    
    with st.spinner("Loading table information..."):
        metadata = get_table_metadata(query_engine.disk_conn, table_name)
        first_10, last_10 = get_table_sample_data(query_engine.disk_conn, table_name)
    
    if metadata:
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Rows", f"{metadata['total_rows']:,}")
        with col2:
            st.metric("Columns", metadata['total_columns'])
        with col3:
            st.metric("Frequency", metadata['frequency'])
        
        if metadata['earliest_timestamp'] and metadata['latest_timestamp']:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Earliest Data", str(metadata['earliest_timestamp'])[:19])
            with col2:
                st.metric("Latest Data", str(metadata['latest_timestamp'])[:19])
        
        st.markdown("---")
        
        tab1, tab2 = st.tabs(["First 10 Rows", "Last 10 Rows"])
        
        with tab1:
            if not first_10.empty:
                st.dataframe(first_10, use_container_width=True)
            else:
                st.info("No data available")
        
        with tab2:
            if not last_10.empty:
                st.dataframe(last_10, use_container_width=True)
            else:
                st.info("No data available")
    
    else:
        st.error("Unable to load table metadata")