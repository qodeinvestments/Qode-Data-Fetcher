import streamlit as st
import pandas as pd
import duckdb
import datetime
import json
import os
import io
import time
import uuid
import re
from typing import List, Dict, Any, Optional
import traceback

# Page configuration
st.set_page_config(
    page_title="Qode Data Fetcher",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'user_id' not in st.session_state:
    st.session_state['user_id'] = str(uuid.uuid4())[:8]

if 'user_folder' not in st.session_state:
    st.session_state['user_folder'] = f"user_queries/{st.session_state['user_id']}"
    os.makedirs(st.session_state['user_folder'], exist_ok=True)

# Database connection
@st.cache_resource
def get_database_connection():
    """Initialize DuckDB connection"""
    try:
        conn = duckdb.connect("qode_edw.db", read_only=True)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

class QueryEngine:
    def __init__(self, disk_conn):
        self.disk_conn = disk_conn

    def _is_read_only_query(self, query):
        """Check if query is read-only"""
        query = re.sub(r'--.*?(\n|$)|/\*.*?\*/', '', query, flags=re.DOTALL)
        modify_pattern = r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|UPSERT|MERGE|COPY|GRANT|REVOKE)\b'
        return not re.search(modify_pattern, query, re.IGNORECASE)

    def execute_query(self, sql_query):
        """Execute SQL query with safety checks"""
        if not self._is_read_only_query(sql_query):
            return None, 0, "ERROR: Only SELECT queries are allowed."

        start_time = time.time()
        try:
            result = self.disk_conn.execute(sql_query).fetchdf()
            if not isinstance(result, pd.DataFrame):
                result = pd.DataFrame(result)
            if result.empty:
                result = pd.DataFrame(columns=["No results found"])
            
            execution_time = time.time() - start_time
            return result, execution_time, None
        except Exception as e:
            execution_time = time.time() - start_time
            return None, execution_time, str(e)

    def get_available_tables(self):
        """Get list of available tables"""
        try:
            query = """
            SELECT table_name, table_schema 
            FROM information_schema.tables 
            WHERE table_schema = 'market_data'
            ORDER BY table_name
            """
            result = self.disk_conn.execute(query).fetchdf()
            return result
        except Exception as e:
            st.error(f"Error fetching tables: {e}")
            return pd.DataFrame()

    def get_table_schema(self, table_name):
        """Get schema for a specific table"""
        try:
            query = f"DESCRIBE SELECT * FROM {table_name} LIMIT 0"
            result = self.disk_conn.execute(query).fetchdf()
            return result
        except Exception as e:
            st.error(f"Error fetching schema for {table_name}: {e}")
            return pd.DataFrame()

def parse_table_name(table_name: str) -> Dict[str, str]:
    """Parse table name to extract instrument details"""
    parts = table_name.replace('market_data.', '').split('_')
    
    info = {
        'exchange': parts[0] if len(parts) > 0 else '',
        'instrument_type': parts[1] if len(parts) > 1 else '',
        'underlying': parts[2] if len(parts) > 2 else ''
    }
    
    if info['instrument_type'].lower() == 'options':
        if len(parts) >= 6:
            info['expiry'] = parts[3]
            info['strike'] = parts[4]
            info['option_type'] = parts[5]
    elif info['instrument_type'].lower() == 'futures':
        if len(parts) >= 4:
            info['expiry'] = parts[3] if len(parts) > 3 else 'continuous'
    
    return info

def main():
    st.title("🎯 Qode Data Fetcher")
    st.markdown("---")
    
    # Initialize database connection
    conn = get_database_connection()
    if conn is None:
        st.error("Unable to connect to database. Please check your connection.")
        return
    
    query_engine = QueryEngine(conn)
    
    # Sidebar for navigation
    st.sidebar.title("Navigation")
    tab_selection = st.sidebar.radio(
        "Select Interface:",
        ["Time Series Builder", "SQL Query Interface", "Data Explorer"]
    )
    
    if tab_selection == "Time Series Builder":
        time_series_builder(query_engine)
    elif tab_selection == "SQL Query Interface":
        sql_query_interface(query_engine)
    else:
        data_explorer(query_engine)

def time_series_builder(query_engine):
    st.header("📈 Time Series Builder")
    
    # Date range selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.date.today() - datetime.timedelta(days=30)
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime.date.today()
        )
    
    st.markdown("---")
    
    # Instrument selection
    st.subheader("🎯 Instrument Selection")
    
    instrument_type = st.selectbox(
        "Select Instrument Type:",
        ["Index", "Options", "Futures", "Stocks"]
    )
    
    # Get available tables for context
    tables_df = query_engine.get_available_tables()
    
    if instrument_type == "Index":
        index_builder(query_engine, tables_df, start_date, end_date)
    elif instrument_type == "Options":
        options_builder(query_engine, tables_df, start_date, end_date)
    elif instrument_type == "Futures":
        futures_builder(query_engine, tables_df, start_date, end_date)
    else:
        stocks_builder(query_engine, tables_df, start_date, end_date)

def index_builder(query_engine, tables_df, start_date, end_date):
    st.subheader("📊 Index Configuration")
    
    # Extract available indices from table names
    index_tables = tables_df[tables_df['table_name'].str.contains('_Index_', case=False)]
    available_indices = []
    
    for table in index_tables['table_name']:
        parsed = parse_table_name(table)
        if parsed['underlying']:
            available_indices.append(parsed['underlying'])
    
    available_indices = list(set(available_indices))
    
    if available_indices:
        selected_index = st.selectbox("Select Index:", available_indices)
        
        # Base time interval
        time_interval = st.selectbox(
            "Base Time Interval:",
            ["1min", "5min", "15min", "1hour", "1day"]
        )
        
        if st.button("Generate Index Time Series"):
            generate_index_time_series(query_engine, selected_index, start_date, end_date, time_interval)
    else:
        st.warning("No index data found in the database.")

def options_builder(query_engine, tables_df, start_date, end_date):
    st.subheader("🎲 Options Configuration")
    
    # Extract available underlyings for options
    options_tables = tables_df[tables_df['table_name'].str.contains('_Options_', case=False)]
    available_underlyings = []
    
    for table in options_tables['table_name']:
        parsed = parse_table_name(table)
        if parsed['underlying']:
            available_underlyings.append(parsed['underlying'])
    
    available_underlyings = list(set(available_underlyings))
    
    if available_underlyings:
        col1, col2 = st.columns(2)
        
        with col1:
            underlying = st.selectbox("Select Underlying:", available_underlyings)
            
            # Expiry configuration
            st.subheader("Expiry Configuration")
            expiry_method = st.selectbox(
                "Expiry Method:",
                ["Specific Date", "Interval + Offset"]
            )
            
            if expiry_method == "Specific Date":
                expiry_date = st.date_input("Expiry Date")
            else:
                interval = st.selectbox("Interval:", ["D", "W", "M", "Q", "Y"])
                if interval == "D":
                    offset = st.selectbox("Offset:", [1, 2, 3, 4, 5])
                elif interval == "W":
                    offset = st.selectbox("Offset:", [1, 2, 3])
                elif interval == "M":
                    offset = st.selectbox("Offset:", [1, 2, 3])
                elif interval == "Q":
                    offset = st.selectbox("Offset:", [1, 2, 3, 4])
                else:  # Y
                    offset = st.selectbox("Offset:", [1, 2, 3])
        
        with col2:
            # Strike price configuration
            st.subheader("Strike Price Configuration")
            strike_method = st.selectbox(
                "Strike Method:",
                ["% Moneyness", "% Premium", "Delta", "Vol Adjusted"]
            )
            
            if strike_method == "% Moneyness":
                moneyness = st.number_input("Moneyness %", value=0.0, step=0.1)
                moneyness_type = st.selectbox("Type:", ["ATM", "ITM", "OTM"])
            elif strike_method == "% Premium":
                premium_pct = st.number_input("Premium %", value=10.0, step=0.1)
            elif strike_method == "Delta":
                delta_value = st.number_input("Delta Value", value=0.5, step=0.01)
            else:  # Vol Adjusted
                vol_method = st.selectbox("Volatility Method:", ["IV", "HV"])
                vol_divisor = st.number_input("Divisor", value=15.875, step=0.001)
            
            # Call/Put selection
            option_type = st.selectbox("Option Type:", ["CE", "PE"])
        
        # Base time interval
        time_interval = st.selectbox(
            "Base Time Interval:",
            ["1min", "5min", "15min", "1hour", "1day"]
        )
        
        if st.button("Generate Options Time Series"):
            generate_options_time_series(
                query_engine, underlying, strike_method, option_type, 
                start_date, end_date, time_interval
            )
    else:
        st.warning("No options data found in the database.")

def futures_builder(query_engine, tables_df, start_date, end_date):
    st.subheader("🚀 Futures Configuration")
    
    # Extract available underlyings for futures
    futures_tables = tables_df[tables_df['table_name'].str.contains('_Futures_', case=False)]
    available_underlyings = []
    
    for table in futures_tables['table_name']:
        parsed = parse_table_name(table)
        if parsed['underlying']:
            available_underlyings.append(parsed['underlying'])
    
    available_underlyings = list(set(available_underlyings))
    
    if available_underlyings:
        underlying = st.selectbox("Select Underlying:", available_underlyings)
        
        # Expiry offset (delivery cycle)
        expiry_offset = st.selectbox("Delivery Cycle:", ["I", "II", "III"])
        
        # Base time interval
        time_interval = st.selectbox(
            "Base Time Interval:",
            ["1min", "5min", "15min", "1hour", "1day"]
        )
        
        if st.button("Generate Futures Time Series"):
            generate_futures_time_series(
                query_engine, underlying, expiry_offset, 
                start_date, end_date, time_interval
            )
    else:
        st.warning("No futures data found in the database.")

def stocks_builder(query_engine, tables_df, start_date, end_date):
    st.subheader("📈 Stocks Configuration")
    
    # Stock identification methods
    stock_method = st.selectbox(
        "Identification Method:",
        ["Symbol", "ISIN", "Name Search"]
    )
    
    if stock_method == "Symbol":
        exchange = st.selectbox("Exchange:", ["BSE", "NSE"])
        symbol = st.text_input("Stock Symbol:")
    elif stock_method == "ISIN":
        isin = st.text_input("ISIN Number:")
    else:  # Name Search
        stock_name = st.text_input("Stock Name:")
    
    # Base time interval
    time_interval = st.selectbox(
        "Base Time Interval:",
        ["1min", "5min", "15min", "1hour", "1day"]
    )
    
    if st.button("Generate Stock Time Series"):
        st.info("Stock time series generation will be implemented based on available stock data.")

def generate_index_time_series(query_engine, index_name, start_date, end_date, interval):
    """Generate time series for index data"""
    try:
        # Find matching table
        tables_df = query_engine.get_available_tables()
        matching_table = None
        
        for table in tables_df['table_name']:
            if index_name.lower() in table.lower() and 'index' in table.lower():
                matching_table = f"market_data.{table.replace('market_data.', '')}"
                break
        
        if matching_table:
            query = f"""
            SELECT timestamp, o, h, l, c, v
            FROM {matching_table}
            WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'
            ORDER BY timestamp
            """
            
            result, exec_time, error = query_engine.execute_query(query)
            
            if error:
                st.error(f"Query failed: {error}")
            else:
                st.success(f"Retrieved {len(result)} records in {exec_time:.2f}s")
                st.dataframe(result.head(100))
                
                # Download options
                col1, col2, col3 = st.columns(3)
                with col1:
                    csv = result.to_csv(index=False)
                    st.download_button("Download CSV", csv, f"{index_name}_timeseries.csv")
                with col2:
                    excel_buffer = io.BytesIO()
                    result.to_excel(excel_buffer, index=False)
                    st.download_button("Download Excel", excel_buffer.getvalue(), f"{index_name}_timeseries.xlsx")
        else:
            st.error(f"No data found for index: {index_name}")
            
    except Exception as e:
        st.error(f"Error generating time series: {e}")

def generate_options_time_series(query_engine, underlying, strike_method, option_type, start_date, end_date, interval):
    """Generate time series for options data"""
    st.info(f"Generating options time series for {underlying} {option_type} using {strike_method} method")
    
    # This would implement the complex options selection logic
    # For now, showing available options tables
    tables_df = query_engine.get_available_tables()
    options_tables = [t for t in tables_df['table_name'] if underlying.lower() in t.lower() and 'options' in t.lower()]
    
    if options_tables:
        st.write("Available Options Tables:")
        for table in options_tables[:10]:  # Show first 10
            st.write(f"- {table}")
    else:
        st.warning(f"No options data found for {underlying}")

def generate_futures_time_series(query_engine, underlying, expiry_offset, start_date, end_date, interval):
    """Generate time series for futures data"""
    st.info(f"Generating futures time series for {underlying} (Cycle {expiry_offset})")
    
    # Implementation would select appropriate futures contract
    tables_df = query_engine.get_available_tables()
    futures_tables = [t for t in tables_df['table_name'] if underlying.lower() in t.lower() and 'futures' in t.lower()]
    
    if futures_tables:
        st.write("Available Futures Tables:")
        for table in futures_tables:
            st.write(f"- {table}")
    else:
        st.warning(f"No futures data found for {underlying}")

def sql_query_interface(query_engine):
    st.header("🔍 SQL Query Interface")
    
    # Query input
    st.subheader("Query Editor")
    
    # Sample queries
    sample_queries = {
        "Show all tables": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'market_data' ORDER BY table_name",
        "Index data sample": "SELECT * FROM market_data.NSE_Index_NIFTY LIMIT 10",
        "Options data sample": "SELECT * FROM information_schema.tables WHERE table_name LIKE '%options%' LIMIT 5"
    }
    
    selected_sample = st.selectbox("Sample Queries (optional):", [""] + list(sample_queries.keys()))
    
    if selected_sample:
        default_query = sample_queries[selected_sample]
    else:
        default_query = ""
    
    sql_query = st.text_area(
        "Enter SQL Query:",
        value=default_query,
        height=150,
        help="Only SELECT queries are allowed for security."
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("Execute Query", type="primary")
    
    if execute_button and sql_query.strip():
        with st.spinner("Executing query..."):
            result, exec_time, error = query_engine.execute_query(sql_query)
            
            if error:
                st.error(f"Query Error: {error}")
            else:
                st.success(f"Query executed successfully in {exec_time:.2f} seconds")
                
                if len(result) > 0:
                    st.write(f"**Results: {len(result)} rows**")
                    st.dataframe(result)
                    
                    # Download options
                    col1, col2 = st.columns(2)
                    with col1:
                        csv = result.to_csv(index=False)
                        st.download_button("Download as CSV", csv, "query_results.csv")
                    with col2:
                        excel_buffer = io.BytesIO()
                        result.to_excel(excel_buffer, index=False)
                        st.download_button("Download as Excel", excel_buffer.getvalue(), "query_results.xlsx")
                else:
                    st.info("Query returned no results")
    
    # Query history (simplified)
    st.subheader("Recent Queries")
    if st.button("Clear History"):
        if 'query_history' in st.session_state:
            st.session_state.query_history = []
    
    # Store query in history
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    
    if execute_button and sql_query.strip():
        st.session_state.query_history.append({
            'query': sql_query,
            'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Keep only last 10 queries
        st.session_state.query_history = st.session_state.query_history[-10:]
    
    # Display history
    for i, hist_query in enumerate(reversed(st.session_state.query_history)):
        with st.expander(f"Query {len(st.session_state.query_history)-i} - {hist_query['timestamp']}"):
            st.code(hist_query['query'])

def data_explorer(query_engine):
    st.header("🗂️ Data Explorer")
    
    # Get available tables
    tables_df = query_engine.get_available_tables()
    
    if len(tables_df) > 0:
        st.subheader("Available Tables")
        
        # Group tables by instrument type
        index_tables = []
        options_tables = []
        futures_tables = []
        
        for table in tables_df['table_name']:
            if 'index' in table.lower():
                index_tables.append(table)
            elif 'options' in table.lower():
                options_tables.append(table)
            elif 'futures' in table.lower():
                futures_tables.append(table)
        
        # Display in tabs
        tab1, tab2, tab3 = st.tabs(["📊 Indices", "🎲 Options", "🚀 Futures"])
        
        with tab1:
            if index_tables:
                for table in index_tables[:20]:  # Limit display
                    with st.expander(f"📈 {table}"):
                        if st.button(f"Preview {table}", key=f"preview_idx_{table}"):
                            preview_table(query_engine, f"market_data.{table}")
            else:
                st.info("No index tables found")
        
        with tab2:
            if options_tables:
                options_tables = [tbl for tbl in options_tables if "MIDCPNIFTY" in tbl]
                for table in options_tables[:20]:  # Limit display
                    with st.expander(f"🎯 {table}"):
                        parsed = parse_table_name(table)
                        st.write(f"**Underlying:** {parsed.get('underlying', 'N/A')}")
                        st.write(f"**Strike:** {parsed.get('strike', 'N/A')}")
                        st.write(f"**Type:** {parsed.get('option_type', 'N/A')}")
                        if st.button(f"Preview {table}", key=f"preview_opt_{table}"):
                            preview_table(query_engine, f"market_data.{table}")
            else:
                st.info("No options tables found")
        
        with tab3:
            if futures_tables:
                for table in futures_tables[:20]:  # Limit display
                    with st.expander(f"🚀 {table}"):
                        parsed = parse_table_name(table)
                        st.write(f"**Underlying:** {parsed.get('underlying', 'N/A')}")
                        if st.button(f"Preview {table}", key=f"preview_fut_{table}"):
                            preview_table(query_engine, f"market_data.{table}")
            else:
                st.info("No futures tables found")
    else:
        st.warning("No tables found in the database")

def preview_table(query_engine, table_name):
    """Preview table data"""
    try:
        # Get basic info
        info_query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
        count_result, _, error = query_engine.execute_query(info_query)
        
        if not error:
            total_rows = count_result.iloc[0]['total_rows']
            st.write(f"**Total Rows:** {total_rows:,}")
        
        # Get sample data
        sample_query = f"SELECT * FROM {table_name} ORDER BY timestamp DESC LIMIT 100"
        result, exec_time, error = query_engine.execute_query(sample_query)
        
        if error:
            st.error(f"Error previewing table: {error}")
        else:
            st.write(f"**Sample Data (Latest 100 records):**")
            st.dataframe(result)
            
            # Basic statistics
            if 'c' in result.columns:  # Close price
                st.write("**Basic Statistics:**")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Latest Close", f"{result['c'].iloc[0]:.2f}")
                with col2:
                    st.metric("Min Close", f"{result['c'].min():.2f}")
                with col3:
                    st.metric("Max Close", f"{result['c'].max():.2f}")
                with col4:
                    st.metric("Avg Close", f"{result['c'].mean():.2f}")
            
    except Exception as e:
        st.error(f"Error previewing table: {e}")

if __name__ == "__main__":
    main()