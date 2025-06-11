import streamlit as st

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