import streamlit as st
import json
from pathlib import Path

def get_underlyings(query_engine, exchange, instrument):
    try:
        pattern = f"{exchange}_{instrument}_"
        
        if 'all_tables' not in st.session_state:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'market_data'
            ORDER BY table_name
            """
            result = query_engine.execute_query(query)
            if result[2] is None:
                st.session_state.all_tables = result[0]['table_name'].tolist()
            else:
                return []
        
        matching_tables = [table for table in st.session_state.all_tables if table.startswith(pattern)]
        
        underlyings = []
        for table in matching_tables:
            parts = table.split('_')
            if len(parts) >= 3:
                underlying = parts[2]
                if underlying not in underlyings:
                    underlyings.append(underlying)
        
        return sorted(underlyings)
    
    except Exception as e:
        st.error(f"Error fetching underlyings: {e}")
        return []

def get_table_name(exchange, instrument, underlying, delivery_cycle=None):
    base_name = f"{exchange}_{instrument}_{underlying}"
    
    if instrument == "Futures":
        return f"{base_name}"
    
    return base_name

def get_table_columns(query_engine, table_name):
    try:
        query = f"DESCRIBE SELECT * FROM market_data.{table_name} LIMIT 0"
        result, _, error = query_engine.execute_query(query)
        
        if error:
            return None
        
        return result['column_name'].tolist()
    
    except Exception as e:
        st.error(f"Error fetching columns: {e}")
        return None

def parse_option_table_name(table_name):
    """
    Parse option table name to extract metadata
    Format: EXCHANGE_Options_UNDERLYING_YYYYMMDD_STRIKE_CALL/PUT
    """
    parts = table_name.split('_')
    if len(parts) >= 6 and parts[1].lower() == 'options':
        try:
            return {
                'exchange': parts[0],
                'underlying': parts[2],
                'expiry_date': parts[3],
                'strike_price': float(parts[4]),
                'option_type': parts[5].upper()
            }
        except (ValueError, IndexError):
            return None
    return None

def get_option_expiry_dates(query_engine, exchange, underlying):
    try:
        if 'all_tables' not in st.session_state:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'market_data'
            ORDER BY table_name
            """
            result = query_engine.execute_query(query)
            if result[2] is None:
                st.session_state.all_tables = result[0]['table_name'].tolist()
            else:
                return []
        
        pattern = f"{exchange}_Options_{underlying}_"
        option_tables = [table for table in st.session_state.all_tables if table.startswith(pattern)]
        
        expiry_dates = set()
        for table in option_tables:
            parsed = parse_option_table_name(table)
            if parsed:
                expiry_dates.add(parsed['expiry_date'])
        
        return sorted(list(expiry_dates))
    
    except Exception as e:
        st.error(f"Error fetching expiry dates: {e}")
        return []

def get_option_strikes(query_engine, exchange, underlying, expiry_date):
    try:
        if 'all_tables' not in st.session_state:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'market_data'
            ORDER BY table_name
            """
            result = query_engine.execute_query(query)
            if result[2] is None:
                st.session_state.all_tables = result[0]['table_name'].tolist()
            else:
                return []
        
        pattern = f"{exchange}_Options_{underlying}_{expiry_date}_"
        option_tables = [table for table in st.session_state.all_tables if table.startswith(pattern)]
        
        strikes = set()
        for table in option_tables:
            parsed = parse_option_table_name(table)
            if parsed:
                strikes.add(parsed['strike_price'])
        
        return sorted(list(strikes))
    
    except Exception as e:
        st.error(f"Error fetching strikes: {e}")
        return []

def get_option_tables_by_moneyness(query_engine, exchange, underlying, moneyness_type, percentage_value, option_type, start_datetime, end_datetime):
    try:
        if 'all_tables' not in st.session_state:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'market_data'
            ORDER BY table_name
            """
            result = query_engine.execute_query(query)
            if result[2] is None:
                st.session_state.all_tables = result[0]['table_name'].tolist()
            else:
                return []
        
        pattern = f"{exchange}_Options_{underlying}_"
        option_tables = [table for table in st.session_state.all_tables if table.startswith(pattern)]
        
        filtered_tables = []
        for table in option_tables:
            parsed = parse_option_table_name(table)
            if parsed and parsed['option_type'] == option_type.upper():
                filtered_tables.append((table, parsed))
        
        if not filtered_tables:
            return []
        
        underlying_query = f"""
        SELECT timestamp, c as underlying_price
        FROM market_data.{exchange}_Index_{underlying}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        underlying_result, _, underlying_error = query_engine.execute_query(underlying_query)
        
        if underlying_error or len(underlying_result) == 0:
            return []
        
        underlying_prices = {}
        for row in underlying_result.itertuples():
            underlying_prices[row.timestamp] = row.underlying_price
        
        matching_tables = []
        
        for table_name, parsed_info in filtered_tables:
            strike_price = parsed_info['strike_price']
            
            option_query = f"""
            SELECT DISTINCT timestamp 
            FROM market_data.{table_name}
            WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
            LIMIT 10
            """
            option_result, _, option_error = query_engine.execute_query(option_query)
            
            if option_error or len(option_result) == 0:
                continue
            
            matches_criteria = False
            for row in option_result.itertuples():
                timestamp = row.timestamp
                if timestamp in underlying_prices:
                    underlying_price = underlying_prices[timestamp]
                    
                    if moneyness_type == "ATM":
                        moneyness = abs((strike_price - underlying_price) / underlying_price * 100)
                        if moneyness <= 0.5:
                            matches_criteria = True
                            break
                    elif moneyness_type == "OTM":
                        if option_type.upper() == "CALL":
                            moneyness = (strike_price - underlying_price) / underlying_price * 100
                        else:
                            moneyness = (underlying_price - strike_price) / underlying_price * 100
                        if percentage_value - 0.5 <= moneyness <= percentage_value + 0.5:
                            matches_criteria = True
                            break
                    else:
                        if option_type.upper() == "CALL":
                            moneyness = (underlying_price - strike_price) / underlying_price * 100
                        else:
                            moneyness = (strike_price - underlying_price) / underlying_price * 100
                        if percentage_value - 0.5 <= moneyness <= percentage_value + 0.5:
                            matches_criteria = True
                            break
            
            if matches_criteria:
                matching_tables.append(table_name)
        
        return matching_tables
    
    except Exception as e:
        st.error(f"Error fetching option tables by moneyness: {e}")
        return []

def get_option_tables_by_premium_percentage(query_engine, exchange, underlying, premium_percentage, option_type, start_datetime, end_datetime):
    try:
        if 'all_tables' not in st.session_state:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'market_data'
            ORDER BY table_name
            """
            result = query_engine.execute_query(query)
            if result[2] is None:
                st.session_state.all_tables = result[0]['table_name'].tolist()
            else:
                return []
        
        pattern = f"{exchange}_Options_{underlying}_"
        option_tables = [table for table in st.session_state.all_tables if table.startswith(pattern)]
        
        filtered_tables = []
        for table in option_tables:
            parsed = parse_option_table_name(table)
            if parsed and parsed['option_type'] == option_type.upper():
                filtered_tables.append(table)
        
        if not filtered_tables:
            return []
        
        underlying_query = f"""
        SELECT timestamp, c as underlying_price
        FROM market_data.{exchange}_Index_{underlying}
        WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
        """
        underlying_result, _, underlying_error = query_engine.execute_query(underlying_query)
        
        if underlying_error or len(underlying_result) == 0:
            return []
        
        matching_tables = []
        
        for table_name in filtered_tables:
            option_query = f"""
            WITH underlying_prices AS (
                SELECT timestamp, c as underlying_price
                FROM market_data.{exchange}_Index_{underlying}
                WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
            )
            SELECT COUNT(*) as match_count
            FROM market_data.{table_name} opt
            JOIN underlying_prices up
                ON DATE_TRUNC('minute', opt.timestamp) = DATE_TRUNC('minute', up.timestamp)
            WHERE opt.timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
                AND ABS((opt.c / up.underlying_price * 100) - {premium_percentage}) <= 0.1
            """
            
            result, _, error = query_engine.execute_query(option_query)
            if not error and len(result) > 0 and result.iloc[0]['match_count'] > 0:
                matching_tables.append(table_name)
        
        return matching_tables
    
    except Exception as e:
        st.error(f"Error fetching option tables by premium percentage: {e}")
        return []
    
def get_available_expiries(query_engine, exchange, underlying, target_datetime):
    query = f"""
    SELECT DISTINCT expiry
    FROM market_data.{exchange}_Options_{underlying}_Master 
    WHERE timestamp = '{target_datetime}'
    ORDER BY expiry
    """
    
    result, _, error = query_engine.execute_query(query)
    
    if error or len(result) == 0:
        return []
    
    return result['expiry'].tolist()

def get_spot_price(query_engine, exchange, underlying, target_datetime):
    underlying_table = f"{exchange}_Index_{underlying}"
    
    query = f"""
    SELECT c
    FROM market_data.{underlying_table}
    WHERE timestamp = '{target_datetime}'
    ORDER BY timestamp DESC
    LIMIT 1
    """
    
    result, _, error = query_engine.execute_query(query)
    
    if error or len(result) == 0:
        return None
    
    return float(result.iloc[0]['c'])

def build_option_chain_query(exchange, underlying, target_datetime, selected_expiry, option_type_filter, spot_price, strike_range, available_columns):
    base_columns = """
        timestamp,
        expiry,
        symbol,
        strike,
        option_type,
        underlying,
        open,
        high,
        low,
        close,
        volume,
        open_interest
    """
    
    optional_columns = []
    if 'iv' in available_columns:
        optional_columns.append('iv')
    if 'delta' in available_columns:
        optional_columns.append('delta')
    if 'gamma' in available_columns:
        optional_columns.append('gamma')
    if 'theta' in available_columns:
        optional_columns.append('theta')
    if 'vega' in available_columns:
        optional_columns.append('vega')
    if 'rho' in available_columns:
        optional_columns.append('rho')
    
    all_columns = base_columns
    if optional_columns:
        all_columns += ",\n        " + ",\n        ".join(optional_columns)
    
    base_query = f"""
    SELECT 
        {all_columns}
    FROM market_data.{exchange}_Options_{underlying}_Master WHERE timestamp = '{target_datetime}'
    """
    
    if selected_expiry:
        base_query += f" AND expiry = '{selected_expiry}'"
    
    if option_type_filter != "All":
        base_query += f" AND option_type = '{option_type_filter.lower()}'"
    
    if spot_price and strike_range:
        lower_bound = spot_price * (1 - strike_range / 100)
        upper_bound = spot_price * (1 + strike_range / 100)
        base_query += f" AND strike BETWEEN {lower_bound} AND {upper_bound}"
    
    final_columns = """
        expiry,
        symbol,
        strike,
        option_type,
        underlying,
        open,
        high,
        low,
        close,
        volume,
        open_interest,
        timestamp
    """
    
    if optional_columns:
        final_columns += ",\n        " + ",\n        ".join(optional_columns)
    
    final_query = f"""
    WITH latest_data AS (
        {base_query}
    ),
    ranked_data AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY expiry, strike, option_type ORDER BY timestamp DESC) as rn
        FROM latest_data
    )
    SELECT 
        {final_columns}
    FROM ranked_data
    WHERE rn = 1
    ORDER BY expiry, strike, option_type
    """
    
    return final_query

def load_event_days():
    event_days_path = Path(__file__).parent / "event_days.json"
    with open(event_days_path, "r") as f:
        event_days = json.load(f)
    return event_days

def event_days_filter_ui(key1, key2):
    event_days = load_event_days()
    
    event_titles = [f"{e['title']} ({e['date']})" for e in event_days]
    st.markdown("#### Event Days Filter")
    filter_option = st.selectbox(
        "How to handle Event Days in Data Time Range?",
        ["Include Event Days", "Exclude Event Days", "Only Event Days"],
        key=key1
    )
    removed = st.multiselect(
        "Temporarily remove specific Event Days:",
        event_titles,
        key=key2
    )
    filtered_event_days = [
        e for i, e in enumerate(event_days) if event_titles[i] not in removed
    ]
    return filter_option, filtered_event_days