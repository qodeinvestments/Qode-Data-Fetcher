import streamlit as st
from datetime import datetime, timedelta
import zipfile
import io
import pandas as pd
from data_utils import get_underlyings, get_table_name, get_table_columns, get_option_expiry_dates, get_option_strikes, event_days_filter_ui
from query_builder import build_query
from chart_renderer import has_candlestick_columns, has_line_chart_columns, render_appropriate_chart

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

def get_valid_columns_for_table(query_engine, table_name, requested_columns):
    available_columns = get_table_columns(query_engine, table_name)
    if not available_columns:
        return []
    
    valid_columns = [col for col in requested_columns if col in available_columns]
    return valid_columns

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
    
    option_methods = [
        "Single Instrument View",
        "Moneyness of an option",
        "Premium as a Percentage of Underlying",
        "Delta of an Option",
        "Download All Options Master Data"
    ]
    selected_method = st.selectbox("Select Method:", option_methods)
    
    if selected_method == "Single Instrument View":
        handle_single_instrument_view_with_info(query_engine, selected_exchange, selected_underlying)
    elif selected_method == "Moneyness of an option":
        handle_moneyness_view_with_info(query_engine, selected_exchange, selected_underlying)
    elif selected_method == "Premium as a Percentage of Underlying":
        handle_premium_percentage_view_with_info(query_engine, selected_exchange, selected_underlying)
    elif selected_method == "Delta of an Option":
        handle_delta_of_option_view_with_info(query_engine, selected_exchange, selected_underlying)
    elif selected_method == "Download All Options Master Data":
        handle_download_all_options_master_data(query_engine, selected_exchange, selected_underlying)

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
        option_type = st.selectbox("Option Type:", ["Put", "Call", "Both"])
    
    if option_type == "Both":
        table_name_call = f"{selected_exchange}_Options_{selected_underlying}_{selected_expiry}_{int(selected_strike)}_call"
        table_name_put = f"{selected_exchange}_Options_{selected_underlying}_{selected_expiry}_{int(selected_strike)}_put"
        handle_options_time_range_and_execution(query_engine, [table_name_call, table_name_put], "single")
    else:
        table_name = f"{selected_exchange}_Options_{selected_underlying}_{selected_expiry}_{int(selected_strike)}_{option_type.lower()}"
        handle_options_time_range_and_execution(query_engine, table_name, "single")

def handle_moneyness_view_with_info(query_engine, selected_exchange, selected_underlying):
    st.markdown("### Moneyness Configuration")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        moneyness_type = st.selectbox("Moneyness Type:", ["ATM", "OTM", "ITM"])
    
    with col2:
        if moneyness_type == "ATM":
            strike_diff_pct = st.number_input(
                "Strike Difference %:",
                min_value=0.0,
                max_value=100.0,
                value=5.0,
                step=0.1,
                key="atm_strike_diff_pct"
            )
            percentage_value = None  # Not used for ATM
        else:
            percentage_value = st.number_input("Percentage:", min_value=0.0, max_value=100.0, value=5.0, step=0.1, key="moneyness_pct")
            strike_diff_pct = None
    
    with col3:
        option_type = st.selectbox("Option Type:", ["Call", "Put", "Both"], key="moneyness_option_type")
    
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
    
    master_table = f"market_data.{selected_exchange}_Options_{selected_underlying}_Master"
    available_columns = get_table_columns(query_engine, master_table.replace("market_data.", ""))
    
    if not available_columns:
        st.error(f"Unable to fetch columns for master table: {master_table}")
        return
    
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
    
    if moneyness_type == "ATM":
        moneyness_condition = f"ABS((strike - c) / c * 100) <= {strike_diff_pct}"
    elif moneyness_type == "OTM":
        if option_type.lower() == "call":
            moneyness_condition = f"((strike - c) / c * 100) BETWEEN 0 AND {percentage_value}"
        else:
            moneyness_condition = f"((c - strike) / c * 100) BETWEEN 0 AND {percentage_value}"
    else:
        if option_type.lower() == "call":
            moneyness_condition = f"((c - strike) / c * 100) BETWEEN 0 AND {percentage_value}"
        else:
            moneyness_condition = f"((strike - c) / c * 100) BETWEEN 0 AND {percentage_value}"
    
    columns_str = ", ".join(selected_columns)
    option_type_filter = option_type.lower()
    if selected_resample == "Raw Data":
        generated_query = f"""
        SELECT {columns_str}
        FROM {master_table}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        if option_type != "Both":
            generated_query += f"AND option_type = '{option_type.lower()}'\n"
        generated_query += f"AND {moneyness_condition}\nORDER BY timestamp"
    else:
        agg_columns = []
        for col in selected_columns:
            if col == "timestamp":
                agg_columns.append(f"time_bucket(INTERVAL '{selected_resample}', timestamp) as timestamp")
            elif col in ["open", "high", "low", "close", "c"]:
                if col == "open":
                    agg_columns.append("FIRST(open) as open")
                elif col == "high":
                    agg_columns.append("MAX(high) as high")
                elif col == "low":
                    agg_columns.append("MIN(low) as low")
                elif col == "close":
                    agg_columns.append("LAST(close) as close")
                elif col == "c":
                    agg_columns.append("LAST(c) as c")
            elif col in ["symbol", "underlying", "expiry", "strike", "option_type"]:
                agg_columns.append(f"FIRST({col}) as {col}")
            else:
                agg_columns.append(f"FIRST({col}) as {col}")
        agg_columns_str = ", ".join(agg_columns)
        generated_query = f"""
        SELECT {agg_columns_str}
        FROM {master_table}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        if option_type != "Both":
            generated_query += f"AND option_type = '{option_type.lower()}'\n"
        generated_query += f"AND {moneyness_condition}\nGROUP BY time_bucket(INTERVAL '{selected_resample}', timestamp), symbol, strike, expiry\nORDER BY timestamp"
    
    st.markdown("### Generated Query")
    generated_query = st.text_area(
        "Generated Query",
        value=generated_query.strip(),
        height=len(generated_query.splitlines()) * 20 + 40,
        key="moneyness_query"
    )
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        execute_button = st.button("Execute Query", type="primary", key="moneyness_execute")
    
    if execute_button:
        # For ATM, use strike_diff_pct in the file name, for others use percentage_value
        suffix = f"atm_{strike_diff_pct}pct" if moneyness_type == "ATM" else f"{moneyness_type}_{percentage_value}pct"
        execute_master_table_query(query_engine, generated_query, f"moneyness_{suffix}")

def handle_premium_percentage_view_with_info(query_engine, selected_exchange, selected_underlying):
    st.markdown("### Premium Percentage Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        premium_percentage = st.number_input("Premium as % of Underlying:", min_value=0.0, max_value=100.0, value=2.0, step=0.1, key="premium_pct")
    
    with col2:
        option_type = st.selectbox("Option Type:", ["Call", "Put", "Both"], key="premium_option_type")
    
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
    
    master_table = f"market_data.{selected_exchange}_Options_{selected_underlying}_Master"
    available_columns = get_table_columns(query_engine, master_table.replace("market_data.", ""))
    
    if not available_columns:
        st.error(f"Unable to fetch columns for master table: {master_table}")
        return
    
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
    
    premium_condition = f"(close / c * 100) BETWEEN {premium_percentage - 0.1} AND {premium_percentage + 0.1}"
    
    columns_str = ", ".join(selected_columns)
    option_type_filter = option_type.lower()
    if selected_resample == "Raw Data":
        generated_query = f"""
        SELECT {columns_str}
        FROM {master_table}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        if option_type != "Both":
            generated_query += f"AND option_type = '{option_type.lower()}'\n"
        generated_query += f"AND {premium_condition}\nORDER BY timestamp"
    else:
        agg_columns = []
        for col in selected_columns:
            if col == "timestamp":
                agg_columns.append(f"time_bucket(INTERVAL '{selected_resample}', timestamp) as timestamp")
            elif col in ["open", "high", "low", "close", "c"]:
                if col == "open":
                    agg_columns.append("FIRST(open) as open")
                elif col == "high":
                    agg_columns.append("MAX(high) as high")
                elif col == "low":
                    agg_columns.append("MIN(low) as low")
                elif col == "close":
                    agg_columns.append("LAST(close) as close")
                elif col == "c":
                    agg_columns.append("LAST(c) as c")
            elif col in ["symbol", "underlying", "expiry", "strike", "option_type"]:
                agg_columns.append(f"FIRST({col}) as {col}")
            else:
                agg_columns.append(f"FIRST({col}) as {col}")
        agg_columns_str = ", ".join(agg_columns)
        generated_query = f"""
        SELECT {agg_columns_str}
        FROM {master_table}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        if option_type != "Both":
            generated_query += f"AND option_type = '{option_type.lower()}'\n"
        generated_query += f"AND {premium_condition}\nGROUP BY time_bucket(INTERVAL '{selected_resample}', timestamp), symbol, strike, expiry\nORDER BY timestamp"
    
    st.markdown("### Generated Query")
    generated_query = st.text_area(
        "Generated Query",
        value=generated_query.strip(),
        height=len(generated_query.splitlines()) * 20 + 40,
        key="premium_query"
    )
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        execute_button = st.button("Execute Query", type="primary", key="premium_execute")
    
    if execute_button:
        execute_master_table_query(query_engine, generated_query, f"premium_{premium_percentage}pct")

def handle_delta_of_option_view_with_info(query_engine, selected_exchange, selected_underlying):
    st.markdown("### Delta of an Option Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        delta_value = st.number_input("Delta Value:", min_value=-1.0, max_value=1.0, value=0.5, step=0.01, key="delta_value")
    with col2:
        option_type = st.selectbox("Option Type:", ["Call", "Put", "Both"], key="delta_option_type")
    
    st.markdown("### Time Range")
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input(
            "Start Date:",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now().date(),
            key="delta_start_date"
        )
        start_time = st.time_input("Start Time:", value=datetime.strptime("09:15:00", "%H:%M:%S").time(), key="delta_start_time")
    with col2:
        end_date = st.date_input(
            "End Date:",
            value=datetime.now().date(),
            max_value=datetime.now().date(),
            key="delta_end_date"
        )
        end_time = st.time_input("End Time:", value=datetime.strptime("15:30:00", "%H:%M:%S").time(), key="delta_end_time")
    
    start_datetime = datetime.combine(start_date, start_time)
    end_datetime = datetime.combine(end_date, end_time)
    
    if start_datetime >= end_datetime:
        st.error("Start datetime must be before end datetime")
        return
    
    st.markdown("### Data Configuration")
    resample_options = ["Raw Data", "1s", "1m", "5m", "15m", "30m", "1h", "1d"]
    selected_resample = st.selectbox("Resample Interval:", resample_options, key="delta_resample")
    
    master_table = f"market_data.{selected_exchange}_Options_{selected_underlying}_Master"
    available_columns = get_table_columns(query_engine, master_table.replace("market_data.", ""))
    
    if not available_columns:
        st.error(f"Unable to fetch columns for master table: {master_table}")
        return
    
    if "delta" not in available_columns:
        st.warning("The selected table does not have a 'delta' column. Please choose another method or table.")
        return
    
    st.markdown("### Select Columns")
    col1, col2 = st.columns([1, 3])
    with col1:
        select_all = st.checkbox("Select All", key="delta_select_all")
    with col2:
        if select_all:
            selected_columns = st.multiselect(
                "Columns:",
                available_columns,
                default=available_columns,
                key="delta_columns"
            )
        else:
            default_cols = get_default_columns(available_columns, "Options")
            selected_columns = st.multiselect(
                "Columns:",
                available_columns,
                default=[col for col in default_cols if col in available_columns],
                key="delta_columns"
            )
    
    if not selected_columns:
        st.warning("Please select at least one column")
        return
    
    delta_condition = f"ABS(delta - {delta_value}) <= 0.01"
    columns_str = ", ".join(selected_columns)
    if selected_resample == "Raw Data":
        generated_query = f"""
        SELECT {columns_str}
        FROM {master_table}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        if option_type != "Both":
            generated_query += f"AND option_type = '{option_type.lower()}'\n"
        generated_query += f"AND {delta_condition}\nORDER BY timestamp"
    else:
        agg_columns = []
        for col in selected_columns:
            if col == "timestamp":
                agg_columns.append(f"time_bucket(INTERVAL '{selected_resample}', timestamp) as timestamp")
            elif col in ["open", "high", "low", "close", "c"]:
                if col == "open":
                    agg_columns.append("FIRST(open) as open")
                elif col == "high":
                    agg_columns.append("MAX(high) as high")
                elif col == "low":
                    agg_columns.append("MIN(low) as low")
                elif col == "close":
                    agg_columns.append("LAST(close) as close")
                elif col == "c":
                    agg_columns.append("LAST(c) as c")
            elif col in ["symbol", "underlying", "expiry", "strike", "option_type"]:
                agg_columns.append(f"FIRST({col}) as {col}")
            else:
                agg_columns.append(f"FIRST({col}) as {col}")
        agg_columns_str = ", ".join(agg_columns)
        generated_query = f"""
        SELECT {agg_columns_str}
        FROM {master_table}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        if option_type != "Both":
            generated_query += f"AND option_type = '{option_type.lower()}'\n"
        generated_query += f"AND {delta_condition}\nGROUP BY time_bucket(INTERVAL '{selected_resample}', timestamp), symbol, strike, expiry\nORDER BY timestamp"
    
    st.markdown("### Generated Query")
    generated_query = st.text_area(
        "Generated Query",
        value=generated_query.strip(),
        height=len(generated_query.splitlines()) * 20 + 40,
        key="delta_query"
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("Execute Query", type="primary", key="delta_execute")
    if execute_button:
        execute_master_table_query(query_engine, generated_query, f"delta_{delta_value}")

def execute_master_table_query(query_engine, query, file_prefix):
    with st.spinner("Executing query..."):
        result, exec_time, error = query_engine.execute_query(query)
        
        if error:
            st.error(f"Query Error: {error}")
        else:
            st.success(f"Query executed successfully in {exec_time:.2f} seconds")
            
            if len(result) > 0:
                st.write(f"**Results: {len(result)} rows**")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    csv = result.to_csv(index=False)
                    st.download_button(f"Download as CSV", csv, f"{file_prefix}_options_data.csv")
                
                with col2:
                    json_data = result.to_json(orient='records')
                    st.download_button("Download as JSON", json_data, f"{file_prefix}_options_data.json")
                    
                with col3:
                    parquet_file = result.to_parquet()
                    st.download_button("Download as Parquet", parquet_file, f"{file_prefix}_options_data.parquet")
                    
                with col4:
                    gzip_file = result.to_csv(compression='gzip')
                    st.download_button("Download as Gzip CSV", gzip_file, f"{file_prefix}_options_data.csv.gz", mime="application/gzip")
                
                st.dataframe(result)
                
                if has_candlestick_columns(result) or has_line_chart_columns(result):
                    render_appropriate_chart(result)
            else:
                st.info("Query returned no results")

def handle_options_time_range_and_execution(query_engine, table_name, method_key):
    # If table_name is a list (Both), create a single concatenated query
    if isinstance(table_name, list):
        # Get UI parameters from both tables to find earliest start and latest end
        earliest_start = None
        latest_end = None
        
        for tname in table_name:
            earliest, latest, _ = get_table_timestamp_info(query_engine, tname)
            if earliest and latest:
                if earliest_start is None or earliest < earliest_start:
                    earliest_start = earliest
                if latest_end is None or latest > latest_end:
                    latest_end = latest
        
        # If no valid timestamps found, use defaults
        if earliest_start is None:
            earliest_start = datetime.now() - timedelta(days=30)
        if latest_end is None:
            latest_end = datetime.now()
        
        available_columns = get_table_columns(query_engine, table_name[0])
        
        if not available_columns:
            st.error(f"Unable to fetch columns for table: {table_name[0]}")
            return
        
        st.markdown("### Time Range")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date:",
                value=earliest_start.date(),
                min_value=datetime(2000, 1, 1).date(),
                max_value=datetime.now().date(),
                key=f"{method_key}_start_date"
            )
            start_time = st.time_input(
                "Start Time:",
                value=earliest_start.time(),
                key=f"{method_key}_start_time"
            )
        with col2:
            end_date = st.date_input(
                "End Date:",
                value=latest_end.date(),
                max_value=datetime.now().date(),
                key=f"{method_key}_end_date"
            )
            end_time = st.time_input(
                "End Time:",
                value=latest_end.time(),
                key=f"{method_key}_end_time"
            )
        
        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)
        
        if start_datetime >= end_datetime:
            st.error("Start datetime must be before end datetime")
            return
        
        filter_option, filtered_event_days = event_days_filter_ui(key1=f"{method_key} time series", key2=f"{method_key} time series multi")
        event_dates = [e['date'] for e in filtered_event_days]
        
        st.markdown("### Data Configuration")
        resample_options = ["Raw Data", "1s", "1m", "5m", "15m", "30m", "1h", "1d"]
        selected_resample = st.selectbox("Resample Interval:", resample_options, key=f"{method_key}_resample")
        
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
        
        st.markdown("### Generated Query (Call & Put Combined)")
        
        # Create a single concatenated query with UNION ALL
        if selected_resample == "Raw Data":
            # For raw data, use UNION ALL to concatenate both tables
            call_table = table_name[0]  # call table
            put_table = table_name[1]   # put table
            
            columns_with_type_call = ", ".join([f'"{col}"' for col in selected_columns]) + ", 'call' as option_type"
            columns_with_type_put = ", ".join([f'"{col}"' for col in selected_columns]) + ", 'put' as option_type"
            
            generated_query = f"""
            SELECT {columns_with_type_call}
            FROM market_data.{call_table}
            WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
            UNION ALL
            SELECT {columns_with_type_put}
            FROM market_data.{put_table}
            WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
            ORDER BY timestamp
            """
        else:
            # For resampled data, need to handle aggregation properly
            call_table = table_name[0]  # call table
            put_table = table_name[1]   # put table
            
            # Build aggregation columns for both tables
            agg_columns = []
            for col in selected_columns:
                if col == "timestamp":
                    agg_columns.append(f"time_bucket(INTERVAL '{selected_resample}', timestamp) as timestamp")
                elif col in ["open", "high", "low", "close", "c"]:
                    if col == "open":
                        agg_columns.append("FIRST(open) as open")
                    elif col == "high":
                        agg_columns.append("MAX(high) as high")
                    elif col == "low":
                        agg_columns.append("MIN(low) as low")
                    elif col == "close":
                        agg_columns.append("LAST(close) as close")
                    elif col == "c":
                        agg_columns.append("LAST(c) as c")
                elif col in ["symbol", "underlying", "expiry", "strike"]:
                    agg_columns.append(f"FIRST({col}) as {col}")
                else:
                    agg_columns.append(f"FIRST({col}) as {col}")
            
            agg_columns_str = ", ".join(agg_columns)
            
            generated_query = f"""
            SELECT {agg_columns_str}, 'call' as option_type
            FROM market_data.{call_table}
            WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
            GROUP BY time_bucket(INTERVAL '{selected_resample}', timestamp)
            UNION ALL
            SELECT {agg_columns_str}, 'put' as option_type
            FROM market_data.{put_table}
            WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
            GROUP BY time_bucket(INTERVAL '{selected_resample}', timestamp)
            ORDER BY timestamp
            """
        
        st.text_area(
            "Generated Query (Call & Put Combined)",
            value=generated_query.strip(),
            height=len(generated_query.splitlines()) * 20 + 40,
            key=f"{method_key}_query"
        )
        
        col1, col2 = st.columns([1, 4])
        with col1:
            execute_button = st.button("Execute Query", type="primary", key=f"{method_key}_execute")
        
        if execute_button:
            with st.spinner("Executing combined query..."):
                result, exec_time, error = query_engine.execute_query(generated_query)
                
                if error:
                    st.error(f"Query Error: {error}")
                else:
                    st.success(f"Query executed successfully in {exec_time:.2f} seconds")
                    
                    if len(result) > 0:
                        st.write(f"**Results: {len(result)} rows (Call & Put combined)**")
                        st.dataframe(result)
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            csv = result.to_csv(index=False)
                            st.download_button("Download as CSV", csv, "adv_query_results_both.csv")
                        with col2:
                            json_data = result.to_json(orient='records')
                            st.download_button("Download as JSON", json_data, "adv_query_results_both.json")
                        with col3:
                            parquet_file = result.to_parquet()
                            st.download_button("Download as Parquet", parquet_file, "adv_query_results_both.parquet")
                        with col4:
                            gzip_file = result.to_csv(compression='gzip')
                            st.download_button("Download as Gzip CSV", gzip_file, "adv_query_results_both.csv.gz", mime="application/gzip")
                        
                        if has_candlestick_columns(result) or has_line_chart_columns(result):
                            render_appropriate_chart(result)
                    else:
                        st.info("Query returned no results for either Call or Put.")
    else:
        # Original logic for single table
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
        
        filter_option, filtered_event_days = event_days_filter_ui(key1=f"{method_key} time series", key2=f"{method_key} time series multi")
        event_dates = [e['date'] for e in filtered_event_days]
        
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
                execute_time_series_query(query_engine, generated_query, filter_option, event_dates)
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
                
                if has_candlestick_columns(result) or has_line_chart_columns(result):
                    render_appropriate_chart(result)
            else:
                st.info("Query returned no results")

def execute_multiple_tables_query(query_engine, table_names, selected_columns, start_datetime, end_datetime, selected_resample, file_prefix):
    total_tables = len(table_names)
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    successful_tables = 0
    failed_tables = 0
    tables_with_no_data = 0
    error_log = []
    
    with st.spinner(f"Processing {total_tables} tables..."):
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, table_name in enumerate(table_names):
                try:
                    progress_percent = (i + 1) / total_tables
                    progress_bar.progress(progress_percent)
                    status_text.text(f"Processing table {i+1}/{total_tables}: {table_name}")
                    
                    valid_columns = get_valid_columns_for_table(query_engine, table_name, selected_columns)
                    
                    if not valid_columns:
                        error_msg = f"No valid columns found for table {table_name}"
                        error_log.append(error_msg)
                        failed_tables += 1
                        continue
                    
                    missing_columns = set(selected_columns) - set(valid_columns)
                    if missing_columns:
                        error_log.append(f"Table {table_name}: Missing columns {', '.join(missing_columns)}")
                    
                    generated_query = build_query(
                        table_name, 
                        valid_columns, 
                        start_datetime, 
                        end_datetime, 
                        selected_resample,
                        "Options"
                    )
                    
                    result, exec_time, error = query_engine.execute_query(generated_query)
                    
                    if error:
                        error_msg = f"Query error for {table_name}: {error}"
                        error_log.append(error_msg)
                        failed_tables += 1
                        continue
                    
                    if len(result) == 0:
                        error_log.append(f"No data returned for table {table_name}")
                        tables_with_no_data += 1
                        continue
                    
                    csv_data = result.to_csv(index=False)
                    zip_file.writestr(f"{table_name}.csv", csv_data)
                    successful_tables += 1
                    
                except Exception as e:
                    error_msg = f"Unexpected error processing {table_name}: {str(e)}"
                    error_log.append(error_msg)
                    failed_tables += 1
        
        zip_buffer.seek(0)
    
    progress_bar.progress(1.0)
    status_text.text("Processing complete!")
    
    st.markdown("### Processing Summary")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Tables", total_tables)
    with col2:
        st.metric("Successful", successful_tables, delta=None if successful_tables == 0 else f"{successful_tables/total_tables*100:.1f}%")
    with col3:
        st.metric("Failed", failed_tables, delta=None if failed_tables == 0 else f"-{failed_tables/total_tables*100:.1f}%")
    with col4:
        st.metric("No Data", tables_with_no_data, delta=None if tables_with_no_data == 0 else f"{tables_with_no_data/total_tables*100:.1f}%")
    
    if error_log:
        with st.expander(f"View Errors and Warnings ({len(error_log)} items)", expanded=False):
            for error in error_log:
                st.text(error)
    
    if successful_tables > 0:
        st.success(f"Successfully processed {successful_tables} out of {total_tables} tables")
        st.download_button(
            label=f"Download ZIP file ({successful_tables} files)",
            data=zip_buffer.getvalue(),
            file_name=f"{file_prefix}_options_data.zip",
            mime="application/zip"
        )
    else:
        st.error("No tables were successfully processed")

def handle_download_all_options_master_data(query_engine, selected_exchange, selected_underlying):
    st.markdown("### Download All Options Master Data")
    master_table = f"market_data.{selected_exchange}_Options_{selected_underlying}_Master"
    available_columns = get_table_columns(query_engine, master_table.replace("market_data.", ""))
    if not available_columns:
        st.error(f"Unable to fetch columns for master table: {master_table}")
        return

    # Time Range
    st.markdown("### Time Range")
    earliest, latest, _ = get_table_timestamp_info(query_engine, master_table.replace("market_data.", ""))
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date:",
            value=earliest.date() if earliest else (datetime.now() - timedelta(days=30)).date(),
            min_value=datetime(2000, 1, 1).date(),
            max_value=datetime.now().date(),
            key="download_master_start_date"
        )
        start_time = st.time_input(
            "Start Time:",
            value=earliest.time() if earliest else datetime.strptime("09:15:00", "%H:%M:%S").time(),
            key="download_master_start_time"
        )
    with col2:
        end_date = st.date_input(
            "End Date:",
            value=latest.date() if latest else datetime.now().date(),
            max_value=datetime.now().date(),
            key="download_master_end_date"
        )
        end_time = st.time_input(
            "End Time:",
            value=latest.time() if latest else datetime.strptime("15:30:00", "%H:%M:%S").time(),
            key="download_master_end_time"
        )
    start_datetime = datetime.combine(start_date, start_time)
    end_datetime = datetime.combine(end_date, end_time)
    if start_datetime >= end_datetime:
        st.error("Start datetime must be before end datetime")
        return

    # Data Configuration
    st.markdown("### Data Configuration")
    resample_options = ["Raw Data", "1s", "1m", "5m", "15m", "30m", "1h", "1d"]
    selected_resample = st.selectbox("Resample Interval:", resample_options, key="download_master_resample")

    # Select Columns
    st.markdown("### Select Columns")
    col1, col2 = st.columns([1, 3])
    with col1:
        select_all = st.checkbox("Select All", key="download_master_select_all")
    with col2:
        if select_all:
            selected_columns = st.multiselect(
                "Columns:",
                available_columns,
                default=available_columns,
                key="download_master_columns"
            )
        else:
            default_cols = get_default_columns(available_columns, "Options")
            selected_columns = st.multiselect(
                "Columns:",
                available_columns,
                default=[col for col in default_cols if col in available_columns],
                key="download_master_columns"
            )
    if not selected_columns:
        st.warning("Please select at least one column")
        return

    # Generated Query
    st.markdown("### Generated Query")
    if selected_resample == "Raw Data":
        columns_str = ", ".join([f'"{col}"' for col in selected_columns])
        generated_query = f"""
        SELECT {columns_str}
        FROM {master_table}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        ORDER BY timestamp
        """
    else:
        agg_columns = []
        for col in selected_columns:
            if col == "timestamp":
                agg_columns.append(f"time_bucket(INTERVAL '{selected_resample}', timestamp) as timestamp")
            elif col in ["open", "high", "low", "close", "c"]:
                if col == "open":
                    agg_columns.append("FIRST(open) as open")
                elif col == "high":
                    agg_columns.append("MAX(high) as high")
                elif col == "low":
                    agg_columns.append("MIN(low) as low")
                elif col == "close":
                    agg_columns.append("LAST(close) as close")
                elif col == "c":
                    agg_columns.append("LAST(c) as c")
            elif col in ["symbol", "underlying", "expiry", "strike", "option_type"]:
                agg_columns.append(f"FIRST({col}) as {col}")
            else:
                agg_columns.append(f"FIRST({col}) as {col}")
        agg_columns_str = ", ".join(agg_columns)
        generated_query = f"""
        SELECT {agg_columns_str}
        FROM {master_table}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        GROUP BY time_bucket(INTERVAL '{selected_resample}', timestamp), symbol, strike, expiry
        ORDER BY timestamp
        """
    generated_query = st.text_area(
        "Generated Query",
        value=generated_query.strip(),
        height=len(generated_query.splitlines()) * 20 + 40,
        key="download_master_query"
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("Execute Query", type="primary", key="download_master_execute")

    if execute_button:
        with st.spinner("Executing query..."):
            result, exec_time, error = query_engine.execute_query(generated_query)
            if error:
                st.error(f"Query Error: {error}")
            elif len(result) == 0:
                st.info("No data found in master table for selected range.")
            else:
                st.success(f"Query executed successfully in {exec_time:.2f} seconds")
                st.write(f"**Results: {len(result)} rows**")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    csv = result.to_csv(index=False)
                    st.download_button("Download as CSV", csv, f"{selected_exchange}_{selected_underlying}_Options_Master.csv")
                with col2:
                    json_data = result.to_json(orient='records')
                    st.download_button("Download as JSON", json_data, f"{selected_exchange}_{selected_underlying}_Options_Master.json")
                with col3:
                    parquet_file = result.to_parquet()
                    st.download_button("Download as Parquet", parquet_file, f"{selected_exchange}_{selected_underlying}_Options_Master.parquet")
                with col4:
                    gzip_file = result.to_csv(compression='gzip')
                    st.download_button("Download as Gzip CSV", gzip_file, f"{selected_exchange}_{selected_underlying}_Options_Master.csv.gz", mime="application/gzip")
                st.dataframe(result)
