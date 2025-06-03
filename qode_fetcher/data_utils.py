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