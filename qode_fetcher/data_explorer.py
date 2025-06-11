import streamlit as st
import time
from database import (
    get_table_metadata, 
    get_table_sample_data, 
    get_all_tables, 
    fuzzy_search_tables,
    search_tables_by_pattern
)
from utils import parse_table_name, describe_table_type

def data_explorer(query_engine):
    st.header("Data Explorer")
    
    if 'all_tables' not in st.session_state:
        with st.spinner("Loading table list... This may take a moment."):
            st.session_state.all_tables = get_all_tables(query_engine.disk_conn)
            st.success(f"Loaded {len(st.session_state.all_tables):,} tables")
    
    st.subheader("Table Naming Convention")
    
    with st.expander("View Table Naming Guidelines", expanded=False):
        st.markdown("""
        **Table Name Format:** `EXCHANGE_INSTRUMENT_UNDERLYING_[ADDITIONAL_PARAMS]`
        
        **Examples:**
        - **Index:** `NSE_Index_NIFTY`, `BSE_Index_SENSEX`
        - **Options:** `NSE_Options_NIFTY_20240125_21000_call`, `BSE_Options_BANKNIFTY_20240201_45000_put`
        - **Futures:** `NSE_Futures_NIFTY`, `BSE_Futures_BANKEX`
        - **Stocks:** `NSE_Stocks_RELIANCE`, `BSE_Stocks_TCS`
        
        **Parameter Guide:**
        - **Exchange:** NSE, BSE, CBOE, etc.
        - **Instrument:** Index, Options, Futures, Stocks
        - **Underlying:** NIFTY, BANKNIFTY, SENSEX, stock symbols
        - **Options Additional:** YYYYMMDD (expiry), strike price, (call/put)
        """)
    
    st.markdown("---")
    
    st.subheader("Search and Filter Tables")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_query = st.text_input(
            "Search tables:",
            placeholder="e.g., NSE, NIFTY, Options, RELIANCE...",
            key="table_search"
        )
    
    with col2:
        exchange_filter = st.selectbox(
            "Exchange:",
            options=["", "NSE", "BSE", "CBOE", "MCX"]
        )
    
    with col3:
        instrument_filter = st.selectbox(
            "Instrument:",
            options=["", "Index", "Options", "Futures", "Stocks"]
        )
    
    matching_tables = []
    
    if search_query or exchange_filter or instrument_filter:
        start_time = time.time()
        
        if search_query and (exchange_filter or instrument_filter):
            fuzzy_results = fuzzy_search_tables(
                st.session_state.all_tables, 
                search_query, 
                limit=200
            )
            matching_tables = search_tables_by_pattern(
                fuzzy_results,
                exchange=exchange_filter,
                instrument=instrument_filter,
                limit=25
            )
        elif search_query:
            matching_tables = fuzzy_search_tables(
                st.session_state.all_tables, 
                search_query, 
                limit=25
            )
        else:
            matching_tables = search_tables_by_pattern(
                st.session_state.all_tables,
                exchange=exchange_filter,
                instrument=instrument_filter,
                limit=25
            )
        
        search_time = time.time() - start_time
        
        if matching_tables:
            st.success(f"Found {len(matching_tables)} matches in {search_time:.3f}s")
            
            selected_table = st.selectbox(
                "Select a table:",
                options=[""] + matching_tables,
                format_func=lambda x: x if x else "-- Select a table --",
                key="selected_table"
            )
            
            if selected_table:
                show_table_details(query_engine, selected_table)
        else:
            st.warning("No matching tables found. Try a different search term or filter.")
    else:
        st.info(f"Total tables available: {len(st.session_state.all_tables):,}")

def show_table_details(query_engine, table_name):
    st.markdown("---")
    st.subheader(f"Table Details: `{table_name}`")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown(f"**Description:** {describe_table_type(table_name)}")
    
    with col2:
        parsed_info = parse_table_name(table_name)
        if parsed_info['exchange']:
            st.markdown(f"**Exchange:** {parsed_info['exchange']}")
        if parsed_info['instrument_type']:
            st.markdown(f"**Type:** {parsed_info['instrument_type']}")
    
    try:
        with st.spinner("Loading table information..."):
            metadata = get_table_metadata(query_engine.disk_conn, table_name)
            first_10, last_10 = get_table_sample_data(query_engine.disk_conn, table_name)
        
        if metadata:
            st.markdown("### Table Statistics")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Rows", f"{metadata['total_rows']:,}")
            with col2:
                st.metric("Columns", metadata['total_columns'])
            with col3:
                st.metric("Frequency", metadata['frequency'])
            with col4:
                st.metric("Missing Timestamps", metadata['missing_timestamps'] if metadata['missing_timestamps'] is not None else "Unknown")

            if metadata['earliest_timestamp'] and metadata['latest_timestamp']:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Earliest Data", str(metadata['earliest_timestamp'])[:19])
                with col2:
                    st.metric("Latest Data", str(metadata['latest_timestamp'])[:19])
            
            st.markdown("### Sample Data")
            
            tab1, tab2 = st.tabs(["First 10 Rows", "Last 10 Rows"])
            
            with tab1:
                if not first_10.empty:
                    st.dataframe(first_10, use_container_width=True, height=400)
                else:
                    st.info("No data available in this table")
            
            with tab2:
                if not last_10.empty:
                    st.dataframe(last_10, use_container_width=True, height=400)
                else:
                    st.info("No data available in this table")
        else:
            st.error("Unable to load table metadata. The table might not exist or there might be a connection issue.")
    
    except Exception as e:
        st.error(f"Error loading table details: {str(e)}")
        st.info("Please check if the table name is correct and the database connection is working.")