import streamlit as st
from datetime import datetime, timedelta
import zipfile
import io
import pandas as pd
from data_utils import get_underlyings, get_table_name, get_table_columns, get_option_expiry_dates, get_option_strikes, get_option_tables_by_moneyness, get_option_tables_by_premium_percentage, event_days_filter_ui
from query_builder import build_query
from chart_renderer import has_candlestick_columns, render_candlestick_chart

def get_table_timestamp_info(query_engine, table_name):
    try:
        timestamp_query = f"SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest, COUNT(*) as total_rows FROM market_data.{table_name}"
        result, _, error = query_engine.execute_query(timestamp_query)
        
        if error or len(result) == 0:
            return None, None, None
        
        earliest = pd.to_datetime(result.iloc[0]['earliest'])
        latest = pd.to_datetime(result.iloc[0]['latest'])
        total_rows = result.iloc[0]['total_rows']
        
        return earliest, latest, total_rows
    except Exception:
        return None, None, None

def calculate_data_statistics(earliest, latest, total_rows):
    if not earliest or not latest or not total_rows:
        return None, None
    
    time_diff = latest - earliest
    total_seconds = time_diff.total_seconds()
    
    if total_rows <= 1 or total_seconds <= 0:
        return None, None
    
    average_interval = total_seconds / (total_rows - 1)
    
    expected_1s_points = int(total_seconds) + 1
    missing_percentage = max(0, (expected_1s_points - total_rows) / expected_1s_points * 100)
    
    return average_interval, missing_percentage

def format_interval(seconds):
    if seconds < 1:
        return f"{seconds*1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}min"
    else:
        return f"{seconds/3600:.1f}hrs"

def time_series_query_builder(query_engine):
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 2rem;
        text-align: center;
        border-bottom: 3px solid #3b82f6;
        padding-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">Time Series Query Builder</div>', unsafe_allow_html=True)
    
    exchange_options = ["NSE", "BSE", "CBOE", "MCX"]
    instrument_options = ["Index", "Futures", "Stocks", "Options"]
    
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
    
    if selected_instrument == "Options":
        handle_options_configuration(query_engine, selected_exchange, selected_underlying)
        return
    
    table_name = get_table_name(selected_exchange, selected_instrument, selected_underlying, delivery_cycle)
    
    st.markdown("### Time Range")
    col1, col2 = st.columns(2)
    
    earliest, latest, _ = get_table_timestamp_info(query_engine, table_name)

    with col1:
        start_date = st.date_input(
            "Start Date:",
            value=earliest.date() if earliest else (datetime.now() - timedelta(days=30)).date(),
            min_value=datetime(2000, 1, 1).date(),
            max_value=datetime.now().date()
        )
        start_time = st.time_input(
            "Start Time:",
            value=earliest.time() if earliest else datetime.strptime("09:15:00", "%H:%M:%S").time()
        )

    with col2:
        end_date = st.date_input(
            "End Date:",
            value=latest.date() if latest else datetime.now().date(),
            max_value=datetime.now().date()
        )
        end_time = st.time_input(
            "End Time:",
            value=latest.time() if latest else datetime.strptime("15:30:00", "%H:%M:%S").time()
        )
    
    start_datetime = datetime.combine(start_date, start_time)
    end_datetime = datetime.combine(end_date, end_time)
    
    if start_datetime >= end_datetime:
        st.error("Start datetime must be before end datetime")
        return
    
    filter_option, filtered_event_days = event_days_filter_ui(key1="time series", key2="time series multi")
    event_dates = [e['date'] for e in filtered_event_days]
    
    st.markdown("### Data Configuration")
    
    resample_options = ["Raw Data", "1s", "1m", "5m", "15m", "30m", "1h", "1d"]
    selected_resample = st.selectbox("Resample Interval:", resample_options)
    
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
                execute_time_series_query(query_engine, generated_query, filter_option, event_dates)
        else:
            st.error(f"Unable to fetch columns for table: {table_name}")
    else:
        st.error("Unable to determine table name for selected options")

def handle_options_configuration(query_engine, selected_exchange, selected_underlying):
    st.markdown("### Options Configuration")
    
    option_methods = ["Single Instrument View", "Moneyness of an option", "Premium as a Percentage of Underlying"]
    selected_method = st.selectbox("Select Method:", option_methods)
    
    if selected_method == "Single Instrument View":
        handle_single_instrument_view_with_info(query_engine, selected_exchange, selected_underlying)
    elif selected_method == "Moneyness of an option":
        handle_moneyness_view_with_info(query_engine, selected_exchange, selected_underlying)
    elif selected_method == "Premium as a Percentage of Underlying":
        handle_premium_percentage_view_with_info(query_engine, selected_exchange, selected_underlying)

def handle_single_instrument_view_with_info(query_engine, selected_exchange, selected_underlying):
    st.markdown("### Single Instrument Configuration")
    
    expiry_dates = get_option_expiry_dates(query_engine, selected_exchange, selected_underlying)
    
    expiry_dates = [pd.to_datetime(date).strftime("%d %b %Y") for date in expiry_dates]
    expiry_dates.sort(key=lambda x: datetime.strptime(x, "%d %b %Y"))
    
    if not expiry_dates:
        st.warning("No expiry dates available for this underlying")
        return
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_expiry = st.selectbox("Expiry Date:", expiry_dates)
        selected_expiry = datetime.strptime(selected_expiry, "%d %b %Y").date()
        selected_expiry = selected_expiry.strftime("%Y%m%d")

    with col2:
        strike_prices = get_option_strikes(query_engine, selected_exchange, selected_underlying, selected_expiry)
        if not strike_prices:
            st.warning("No strike prices available for this expiry")
            return
        selected_strike = st.selectbox("Strike Price:", strike_prices)
    
    with col3:
        option_type = st.selectbox("Option Type:", ["Put", "Call"])
    
    table_name = f"{selected_exchange}_Options_{selected_underlying}_{selected_expiry}_{int(selected_strike)}_{option_type.lower()}"
    
    handle_options_time_range_and_execution(query_engine, table_name, "single")

def handle_moneyness_view_with_info(query_engine, selected_exchange, selected_underlying):
    st.markdown("### Moneyness Configuration")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        moneyness_type = st.selectbox("Moneyness Type:", ["ATM", "OTM", "ITM"])
    
    with col2:
        if moneyness_type == "ATM":
            percentage_value = 0.0
            st.number_input("Percentage:", value=0.0, disabled=True, key="moneyness_pct")
        else:
            percentage_value = st.number_input("Percentage:", min_value=0.0, max_value=100.0, value=5.0, step=0.1, key="moneyness_pct")
    
    with col3:
        option_type = st.selectbox("Option Type:", ["Call", "Put"], key="moneyness_option_type")
    
    st.markdown("### Time Range")
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input(
            "Start Date:",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now().date(),
            key="moneyness_start_date"
        )
        start_time = st.time_input("Start Time:", value=datetime.strptime("09:15:00", "%H:%M:%S").time(), key="moneyness_start_time")
    
    with col2:
        end_date = st.date_input(
            "End Date:",
            value=datetime.now().date(),
            max_value=datetime.now().date(),
            key="moneyness_end_date"
        )
        end_time = st.time_input("End Time:", value=datetime.strptime("15:30:00", "%H:%M:%S").time(), key="moneyness_end_time")
    
    start_datetime = datetime.combine(start_date, start_time)
    end_datetime = datetime.combine(end_date, end_time)
    
    if start_datetime >= end_datetime:
        st.error("Start datetime must be before end datetime")
        return
    
    st.markdown("### Data Configuration")
    
    resample_options = ["Raw Data", "1s", "1m", "5m", "15m", "30m", "1h", "1d"]
    selected_resample = st.selectbox("Resample Interval:", resample_options, key="moneyness_resample")
    
    table_names = get_option_tables_by_moneyness(query_engine, selected_exchange, selected_underlying, moneyness_type, percentage_value, option_type, start_datetime, end_datetime)
    
    if not table_names:
        st.warning("No option tables found for the specified moneyness criteria")
        return
    
    st.info(f"Found {len(table_names)} option instruments matching criteria")
    
    sample_table = table_names[0] if table_names else None
    available_columns = get_table_columns(query_engine, sample_table) if sample_table else []
    
    if available_columns:
        st.markdown("### Select Columns")
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            select_all = st.checkbox("Select All", key="moneyness_select_all")
            
        with col2:
            if select_all:
                selected_columns = st.multiselect(
                    "Columns:",
                    available_columns,
                    default=available_columns,
                    key="moneyness_columns"
                )
            else:
                default_cols = get_default_columns(available_columns, "Options")
                selected_columns = st.multiselect(
                    "Columns:",
                    available_columns,
                    default=[col for col in default_cols if col in available_columns],
                    key="moneyness_columns"
                )
        
        if not selected_columns:
            st.warning("Please select at least one column")
            return
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            execute_button = st.button("Download All Tables as ZIP", type="primary", key="moneyness_execute")
        
        if execute_button:
            execute_multiple_tables_query(query_engine, table_names, selected_columns, start_datetime, end_datetime, selected_resample, f"moneyness_{moneyness_type}_{percentage_value}pct")
    else:
        st.error("Unable to fetch columns for option tables")

def handle_premium_percentage_view_with_info(query_engine, selected_exchange, selected_underlying):
    st.markdown("### Premium Percentage Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        premium_percentage = st.number_input("Premium as % of Underlying:", min_value=0.0, max_value=100.0, value=2.0, step=0.1, key="premium_pct")
    
    with col2:
        option_type = st.selectbox("Option Type:", ["Call", "Put"], key="premium_option_type")
    
    st.markdown("### Time Range")
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input(
            "Start Date:",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now().date(),
            key="premium_start_date"
        )
        start_time = st.time_input("Start Time:", value=datetime.strptime("09:15:00", "%H:%M:%S").time(), key="premium_start_time")
    
    with col2:
        end_date = st.date_input(
            "End Date:",
            value=datetime.now().date(),
            max_value=datetime.now().date(),
            key="premium_end_date"
        )
        end_time = st.time_input("End Time:", value=datetime.strptime("15:30:00", "%H:%M:%S").time(), key="premium_end_time")
    
    start_datetime = datetime.combine(start_date, start_time)
    end_datetime = datetime.combine(end_date, end_time)
    
    if start_datetime >= end_datetime:
        st.error("Start datetime must be before end datetime")
        return
    
    st.markdown("### Data Configuration")
    
    resample_options = ["Raw Data", "1s", "1m", "5m", "15m", "30m", "1h", "1d"]
    selected_resample = st.selectbox("Resample Interval:", resample_options, key="premium_resample")
    
    table_names = get_option_tables_by_premium_percentage(query_engine, selected_exchange, selected_underlying, premium_percentage, option_type, start_datetime, end_datetime)
    
    if not table_names:
        st.warning("No option tables found for the specified premium percentage criteria")
        return
    
    st.info(f"Found {len(table_names)} option instruments matching criteria")
    
    sample_table = table_names[0] if table_names else None
    available_columns = get_table_columns(query_engine, sample_table) if sample_table else None
    
    if available_columns:
        st.markdown("### Select Columns")
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            select_all = st.checkbox("Select All", key="premium_select_all")
            
        with col2:
            if select_all:
                selected_columns = st.multiselect(
                    "Columns:",
                    available_columns,
                    default=available_columns,
                    key="premium_columns"
                )
            else:
                default_cols = get_default_columns(available_columns, "Options")
                selected_columns = st.multiselect(
                    "Columns:",
                    available_columns,
                    default=[col for col in default_cols if col in available_columns],
                    key="premium_columns"
                )
        
        if not selected_columns:
            st.warning("Please select at least one column")
            return
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            execute_button = st.button("Download All Tables as ZIP", type="primary", key="premium_execute")
        
        if execute_button:
            execute_multiple_tables_query(query_engine, table_names, selected_columns, start_datetime, end_datetime, selected_resample, f"premium_{premium_percentage}pct")
    else:
        st.error("Unable to fetch columns for option tables")

def handle_options_time_range_and_execution(query_engine, table_name, method_key):
    st.markdown("### Time Range")
    col1, col2 = st.columns(2)
    
    earliest, latest, _ = get_table_timestamp_info(query_engine, table_name)

    with col1:
        start_date = st.date_input(
            "Start Date:",
            value=earliest.date() if earliest else (datetime.now() - timedelta(days=30)).date(),
            min_value=datetime(2000, 1, 1).date(),
            max_value=datetime.now().date(),
            key=f"{method_key}_start_date"
        )
        start_time = st.time_input(
            "Start Time:",
            value=earliest.time() if earliest else datetime.strptime("09:15:00", "%H:%M:%S").time(),
            key=f"{method_key}_start_time"
        )

    with col2:
        end_date = st.date_input(
            "End Date:",
            value=latest.date() if latest else datetime.now().date(),
            max_value=datetime.now().date(),
            key=f"{method_key}_end_date"
        )
        end_time = st.time_input(
            "End Time:",
            value=latest.time() if latest else datetime.strptime("15:30:00", "%H:%M:%S").time(),
            key=f"{method_key}_end_time"
        )
    
    start_datetime = datetime.combine(start_date, start_time)
    end_datetime = datetime.combine(end_date, end_time)
    
    if start_datetime >= end_datetime:
        st.error("Start datetime must be before end datetime")
        return
    
    st.markdown("### Data Configuration")
    
    resample_options = ["Raw Data", "1s", "1m", "5m", "15m", "30m", "1h", "1d"]
    selected_resample = st.selectbox("Resample Interval:", resample_options, key=f"{method_key}_resample")
    
    available_columns = get_table_columns(query_engine, table_name)
    
    if available_columns:
        st.markdown("### Select Columns")
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            select_all = st.checkbox("Select All", key=f"{method_key}_select_all")
            
        with col2:
            if select_all:
                selected_columns = st.multiselect(
                    "Columns:",
                    available_columns,
                    default=available_columns,
                    key=f"{method_key}_columns"
                )
            else:
                default_cols = get_default_columns(available_columns, "Options")
                selected_columns = st.multiselect(
                    "Columns:",
                    available_columns,
                    default=[col for col in default_cols if col in available_columns],
                    key=f"{method_key}_columns"
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
            "Options"
        )
        
        generated_query = st.text_area(
            "Generated Query",
            value=generated_query.strip(),
            height=len(generated_query.splitlines()) * 20 + 40,
            key=f"{method_key}_query"
        )
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            execute_button = st.button("Execute Query", type="primary", key=f"{method_key}_execute")
        
        if execute_button:
            execute_time_series_query(query_engine, generated_query)
    else:
        st.error(f"Unable to fetch columns for table: {table_name}")

def get_default_columns(available_columns, instrument_type):
    if instrument_type == "Futures":
        return ["timestamp", "o", "h", "l", "c", "v", "oi", "symbol"]
    elif instrument_type == "Stocks":
        return ["timestamp", "o", "h", "l", "c", "v", "symbol"]
    elif instrument_type == "Options":
        return ["timestamp", "o", "h", "l", "c", "v", "oi", "symbol", "strike", "expiry"]
    else:
        return ["timestamp", "o", "h", "l", "c", "v"]

def execute_time_series_query(query_engine, query, filter_option, event_dates):
    with st.spinner("Executing query..."):
        result, exec_time, error = query_engine.execute_query(query)
        
        if not error and "timestamp" in result.columns:
            result['date'] = pd.to_datetime(result['timestamp']).dt.date.astype(str)
            if filter_option == "Exclude Event Days":
                result = result[~result['date'].isin(event_dates)]
            elif filter_option == "Only Event Days":
                result = result[result['date'].isin(event_dates)]
            result = result.drop(columns=["date"])
        
        if error:
            st.error(f"Query Error: {error}")
        else:
            st.success(f"Query executed successfully in {exec_time:.2f} seconds")
            
            if len(result) > 0:
                st.write(f"**Results: {len(result)} rows**")
                st.dataframe(result)
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    csv = result.to_csv(index=False)
                    st.download_button("Download as CSV", csv, "adv_query_results.csv")
                
                with col2:
                    json_data = result.to_json(orient='records')
                    st.download_button("Download as JSON", json_data, "adv_query_results.json")
                    
                with col3:
                    parquet_file = result.to_parquet()
                    st.download_button("Download as Parquet", parquet_file, "adv_query_results.parquet")
                    
                with col4:
                    gzip_file = result.to_csv(compression='gzip')
                    st.download_button("Download as Gzip CSV", gzip_file, "adv_query_results.csv.gz", mime="application/gzip")
                
                if has_candlestick_columns(result) and len(result) > 0:
                    st.subheader("Candlestick Preview")
                    render_candlestick_chart(result)
            else:
                st.info("Query returned no results")

def execute_multiple_tables_query(query_engine, table_names, selected_columns, start_datetime, end_datetime, selected_resample, file_prefix):
    with st.spinner(f"Executing queries for {len(table_names)} tables..."):
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, table_name in enumerate(table_names):
                generated_query = build_query(
                    table_name, 
                    selected_columns, 
                    start_datetime, 
                    end_datetime, 
                    selected_resample,
                    "Options"
                )
                
                result, exec_time, error = query_engine.execute_query(generated_query)
                
                if error:
                    st.warning(f"Error in table {table_name}: {error}")
                    continue
                
                if len(result) > 0:
                    csv_data = result.to_csv(index=False)
                    zip_file.writestr(f"{table_name}.csv", csv_data)
                
                if i % 10 == 0:
                    st.write(f"Processed {i+1}/{len(table_names)} tables...")
        
        zip_buffer.seek(0)
        
        st.success(f"Successfully processed {len(table_names)} tables")
        st.download_button(
            label="Download ZIP file",
            data=zip_buffer.getvalue(),
            file_name=f"{file_prefix}_options_data.zip",
            mime="application/zip"
        )