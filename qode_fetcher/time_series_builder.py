import streamlit as st
from datetime import datetime, timedelta
from data_utils import get_underlyings, get_table_name, get_table_columns
from query_builder import build_query
from chart_renderer import has_candlestick_columns, render_candlestick_chart

def time_series_query_builder(query_engine):
    st.subheader("Time Series Query Builder")
    
    exchange_options = ["NSE", "BSE", "CBOE"]
    instrument_options = ["Index", "Futures", "Stocks"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        selected_exchange = st.selectbox("Exchange:", exchange_options)
    
    with col2:
        selected_instrument = st.selectbox("Instrument:", instrument_options)
    
    underlying_options = get_underlyings(query_engine, selected_exchange, selected_instrument)
    
    if underlying_options:
        selected_underlying = st.selectbox("Underlying:", underlying_options)
    else:
        st.warning(f"No {selected_instrument.lower()} data available for {selected_exchange}")
        return
    
    delivery_cycle = None
    if selected_instrument == "Futures":
        delivery_cycle = st.selectbox("Delivery Cycle:", ["I"])
    
    st.markdown("### Time Range")
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input(
            "Start Date:",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now().date()
        )
        start_time = st.time_input("Start Time:", value=datetime.strptime("09:15:00", "%H:%M:%S").time())
    
    with col2:
        end_date = st.date_input(
            "End Date:",
            value=datetime.now().date(),
            max_value=datetime.now().date()
        )
        end_time = st.time_input("End Time:", value=datetime.strptime("15:30:00", "%H:%M:%S").time())
    
    start_datetime = datetime.combine(start_date, start_time)
    end_datetime = datetime.combine(end_date, end_time)
    
    if start_datetime >= end_datetime:
        st.error("Start datetime must be before end datetime")
        return
    
    st.markdown("### Data Configuration")
    
    resample_options = ["Raw Data", "1s", "1m", "5m", "15m", "30m", "1h", "1d"]
    selected_resample = st.selectbox("Resample Interval:", resample_options)
    
    table_name = get_table_name(selected_exchange, selected_instrument, selected_underlying, delivery_cycle)
    
    if table_name:
        available_columns = get_table_columns(query_engine, table_name)
        
        if available_columns:
            st.markdown("### Select Columns")
            
            col1, col2 = st.columns([1, 3])
            
            with col1:
                select_all = st.checkbox("Select All")
                
            with col2:
                if select_all:
                    selected_columns = st.multiselect(
                        "Columns:",
                        available_columns,
                        default=available_columns
                    )
                else:
                    default_cols = get_default_columns(available_columns, selected_instrument)
                    selected_columns = st.multiselect(
                        "Columns:",
                        available_columns,
                        default=[col for col in default_cols if col in available_columns]
                    )
            
            if not selected_columns:
                st.warning("Please select at least one column")
                return
            
            st.markdown("### Generated Query")
            
            generated_query = build_query(
                table_name, 
                selected_columns, 
                start_datetime, 
                end_datetime, 
                selected_resample,
                selected_instrument
            )
            
            generated_query = st.text_area(
                "Generated Query",
                value=generated_query.strip(),
                height=len(generated_query.splitlines()) * 20 + 40,
            )
            
            col1, col2 = st.columns([1, 4])
            
            with col1:
                execute_button = st.button("Execute Query", type="primary", key="ts_execute")
            
            if execute_button:
                execute_time_series_query(query_engine, generated_query)
        else:
            st.error(f"Unable to fetch columns for table: {table_name}")
    else:
        st.error("Unable to determine table name for selected options")

def get_default_columns(available_columns, instrument_type):
    if instrument_type == "Futures":
        return ["timestamp", "o", "h", "l", "c", "v", "oi", "symbol"]
    elif instrument_type == "Stocks":
        return ["timestamp", "o", "h", "l", "c", "v", "symbol"]
    else:
        return ["timestamp", "o", "h", "l", "c", "v"]

def execute_time_series_query(query_engine, query):
    with st.spinner("Executing query..."):
        result, exec_time, error = query_engine.execute_query(query)
        
        if error:
            st.error(f"Query Error: {error}")
        else:
            st.success(f"Query executed successfully in {exec_time:.2f} seconds")
            
            if len(result) > 0:
                st.write(f"**Results: {len(result)} rows**")
                st.dataframe(result)
                
                col1, col2 = st.columns(2)
                with col1:
                    csv = result.to_csv(index=False)
                    st.download_button("Download as CSV", csv, "ts_query_results.csv")
                
                if has_candlestick_columns(result):
                    st.subheader("Candlestick Preview")
                    render_candlestick_chart(result)
            else:
                st.info("Query returned no results")