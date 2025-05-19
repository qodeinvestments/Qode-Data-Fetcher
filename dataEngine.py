import duckdb
import pandas as pd
import plotly.graph_objects as go
import os
import time
import datetime
import traceback
import json
import psutil
import streamlit as st
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

st.set_page_config(
    page_title="Qode Data Fetcher",
    page_icon="Q",
    layout="wide"
)

st.image("./logo.jpg", width=100)

st.markdown("""
### Advanced Financial Data Fetcher
This platform enables quant traders to query financial market data using natural language.
The system automatically converts your queries into SQL, executes them against our high-performance 
DuckDB database, and provides visualization and export options.
""")

st.markdown("""
**Process:**
1. Enter your query in natural language (e.g., "Show NIFTY50 index data for the last 5 days")
2. The system converts it to SQL
3. Review the generated SQL (and edit if needed)
4. Execute the query and and view results
5. Export data or visualize and analyze as needed
""")

with st.expander("Getting Started Guide", expanded=False):
    st.markdown("""
    ### Welcome to Qode Data Fetcher!
    
    **For Quant Traders and Analysts:**
    
    This platform gives you direct access to financial market data using natural language queries. No need to write complex SQL - just describe what you want to see!
    
    **Example Queries:**
    
    * **Simple data retrieval:** "Show me NIFTY50 index data for yesterday"
    * **Time-based aggregation:** "Calculate 15-minute VWAP for Reliance futures from yesterday"
    * **Technical analysis:** "Find crossover points where 9-day EMA crossed above 21-day EMA for HDFC Bank in the last month"
    * **Options analysis:** "Compare put-call ratio for NIFTY options for last 5 expiries"
    
    **Tips for better queries:**
    
    * Specify the exact instrument and exchange when possible
    * Indicate time ranges clearly (e.g., "last 3 days", "between Jan 1 and Jan 15")
    * For aggregations, mention the interval (e.g., "15-minute intervals", "daily", "hourly")
    
    **Having issues?**
    
    * If the SQL generation seems incorrect, you can manually edit it before running
    * For complex analyses, consider breaking down into multiple queries
    * Use the "Advanced Options" in the sidebar to optimize performance for large datasets
    
    Ready to get started? Enter your first query above!
    """)

DB_PATH = "qode_engine_data.db"
DB_IN_MEMORY = ":memory:"
QUERY_HISTORY_DIR = "query_history"
DATA_DIR = "root/Database/cold_storage"

model_name = "defog/sqlcoder-7b-2"

tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
model = AutoModelForCausalLM.from_pretrained(model_name, device_map="auto", torch_dtype=torch.float16)

os.makedirs(QUERY_HISTORY_DIR, exist_ok=True)

@st.cache_resource
def get_duckdb_connection(in_memory=False):
    """Get a DuckDB connection (cached for performance)"""
    db_path = DB_IN_MEMORY if in_memory else DB_PATH
    conn = duckdb.connect(db_path)

    return conn

disk_conn = get_duckdb_connection(in_memory=False)
memory_conn = get_duckdb_connection(in_memory=True)

def nl_to_sql(prompt: str) -> str:
    system_prompt = """### Task
Generate a SQL query for DuckDB that answers the following question.

### Database Schema
The database has tables such as:
- options_nifty_20240101_18000_C
- index_nifty50
- futures_banknifty_20240101

Each has columns:
- timestamp, o, h, l, c, v, oi

### Question
""" + prompt + "\n\n### SQL Query\n"

    inputs = tokenizer(system_prompt, return_tensors="pt")
    
    outputs = model.generate(
        **inputs,
        max_new_tokens=256,
        temperature=0.2,
        top_p=0.95,
        do_sample=False,
        pad_token_id=tokenizer.eos_token_id,
    )
    
    decoded = tokenizer.decode(outputs[0], skip_special_tokens=True)

    if "```sql" in decoded:
        decoded = decoded.split("```sql")[1].split("```")[0].strip()
    elif "SELECT" in decoded:
        decoded = decoded.split("SELECT", 1)[1]
        decoded = "SELECT " + decoded.split("###")[0].strip()

    return decoded.strip()

def save_query_to_history(natural_query, sql_query, results_df=None):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    query_dir = f"{QUERY_HISTORY_DIR}/{timestamp}"
    os.makedirs(query_dir, exist_ok=True)
    
    with open(f"{query_dir}/natural_query.txt", "w") as f:
        f.write(natural_query)
    
    with open(f"{query_dir}/sql_query.sql", "w") as f:
        f.write(sql_query)
    
    if results_df is not None and not results_df.empty:
        results_df.to_excel(f"{query_dir}/results.xlsx", index=False)
        results_df.to_csv(f"{query_dir}/results.csv", index=False)

def execute_query(sql_query, in_memory=False):
    conn = memory_conn if in_memory else disk_conn
    start_time = time.time()
    
    try:
        conn.execute("PRAGMA enable_profiling")
        conn.execute("PRAGMA profiling_mode='detailed'")
        
        result = conn.execute(sql_query).fetchdf()
        
        profiling_info = conn.execute("SELECT * FROM pragma_last_profiling_output()").fetchdf()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        query_log = {
            "timestamp": datetime.datetime.now().isoformat(),
            "query": sql_query,
            "execution_time": execution_time,
            "in_memory": in_memory,
            "row_count": len(result) if result is not None else 0
        }
        
        log_dir = "query_logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = f"{log_dir}/query_log_{datetime.datetime.now().strftime('%Y%m%d')}.jsonl"
        
        with open(log_file, "a") as f:
            f.write(json.dumps(query_log) + "\n")
        
        return result, execution_time, profiling_info
    except Exception as e:
        error_traceback = traceback.format_exc()
        end_time = time.time()
        execution_time = end_time - start_time
        return None, execution_time, error_traceback

# Function to create candlestick chart
# def create_candlestick_chart(df):
#     # Check if dataframe contains necessary OHLC columns
#     # Handle the specific column names from the parquet files
    
#     # Map expected column names to actual columns in dataframe
#     column_mapping = {
#         'timestamp': ['timestamp', 'datetime', 'date', 'time'],
#         'open': ['o', 'open', 'Open'],
#         'high': ['h', 'high', 'High'],
#         'low': ['l', 'low', 'Low'],
#         'close': ['c', 'close', 'Close'],
#         'volume': ['v', 'volume', 'Volume'],
#         'open_interest': ['oi', 'open_interest', 'OpenInterest']
#     }
    
#     # Find actual column names in the dataframe
#     actual_columns = {}
#     for expected_col, possible_cols in column_mapping.items():
#         for col in possible_cols:
#             if col in df.columns:
#                 actual_columns[expected_col] = col
#                 break
    
#     # Check if we have the minimum required columns for a candlestick chart
#     required_cols = ['timestamp', 'open', 'high', 'low', 'close']
#     if not all(col in actual_columns for col in required_cols):
#         missing_cols = [col for col in required_cols if col not in actual_columns]
#         st.warning(f"Data doesn't contain required columns for candlestick chart: {', '.join(missing_cols)}")
#         return None
    
#     # Create a temporary dataframe with standardized column names
#     chart_df = df.copy()
    
#     # Sort data by timestamp
#     chart_df = chart_df.sort_values(actual_columns['timestamp'])
    
#     # Create candlestick chart
#     fig = go.Figure(data=[go.Candlestick(
#         x=chart_df[actual_columns['timestamp']],
#         open=chart_df[actual_columns['open']], 
#         high=chart_df[actual_columns['high']],
#         low=chart_df[actual_columns['low']], 
#         close=chart_df[actual_columns['close']],
#         name='OHLC'
#     )])
    
#     # Add volume if available
#     if 'volume' in actual_columns:
#         fig.add_trace(go.Bar(
#             x=chart_df[actual_columns['timestamp']],
#             y=chart_df[actual_columns['volume']],
#             name='Volume',
#             yaxis='y2',
#             marker_color='rgba(0, 0, 255, 0.3)'
#         ))
        
#         # Update layout for volume as secondary y-axis
#         fig.update_layout(
#             yaxis2=dict(
#                 title="Volume",
#                 overlaying="y",
#                 side="right",
#                 showgrid=False
#             )
#         )
    
#     # Add open interest if available (for futures and options)
#     if 'open_interest' in actual_columns:
#         fig.add_trace(go.Scatter(
#             x=chart_df[actual_columns['timestamp']],
#             y=chart_df[actual_columns['open_interest']],
#             name='Open Interest',
#             yaxis='y3',
#             line=dict(color='rgba(255, 165, 0, 0.7)', width=1.5)
#         ))
        
#         # Update layout for open interest as tertiary y-axis
#         fig.update_layout(
#             yaxis3=dict(
#                 title="Open Interest",
#                 overlaying="y",
#                 side="right",
#                 anchor="free",
#                 position=0.95,
#                 showgrid=False
#             )
#         )
    
#     # Update layout
#     fig.update_layout(
#         title='Market Data Visualization',
#         xaxis_title='Time',
#         yaxis_title='Price',
#         height=600,
#         margin=dict(l=50, r=50, t=100, b=50),
#         xaxis_rangeslider_visible=False,
#         template='plotly_white',
#         legend=dict(
#             orientation="h",
#             yanchor="bottom",
#             y=1.02,
#             xanchor="right",
#             x=1
#         )
#     )
    
#     return fig

# # Function to add technical indicators to chart if requested
# def add_technical_indicators(fig, df, indicators=None):
    if not indicators:
        return fig
    
    # Map column names
    column_mapping = {
        'timestamp': next((col for col in ['timestamp', 'datetime', 'date', 'time'] if col in df.columns), None),
        'close': next((col for col in ['c', 'close', 'Close'] if col in df.columns), None)
    }
    
    if not all(column_mapping.values()):
        return fig
    
    timestamp_col = column_mapping['timestamp']
    close_col = column_mapping['close']
    
    # Sort dataframe by timestamp
    df = df.sort_values(timestamp_col)
    
    # Add SMA indicator
    if 'sma' in indicators:
        window_size = indicators.get('sma_window', 20)
        df['sma'] = df[close_col].rolling(window=window_size).mean()
        fig.add_trace(go.Scatter(
            x=df[timestamp_col],
            y=df['sma'],
            name=f'SMA ({window_size})',
            line=dict(color='rgba(255, 0, 0, 0.7)', width=1.5)
        ))
    
    # Add EMA indicator
    if 'ema' in indicators:
        window_size = indicators.get('ema_window', 20)
        df['ema'] = df[close_col].ewm(span=window_size, adjust=False).mean()
        fig.add_trace(go.Scatter(
            x=df[timestamp_col],
            y=df['ema'],
            name=f'EMA ({window_size})',
            line=dict(color='rgba(0, 128, 0, 0.7)', width=1.5)
        ))
    
    # Add Bollinger Bands
    if 'bollinger' in indicators:
        window_size = indicators.get('bollinger_window', 20)
        std_dev = indicators.get('bollinger_std', 2)
        
        df['sma'] = df[close_col].rolling(window=window_size).mean()
        df['std'] = df[close_col].rolling(window=window_size).std()
        df['upper_band'] = df['sma'] + (df['std'] * std_dev)
        df['lower_band'] = df['sma'] - (df['std'] * std_dev)
        
        fig.add_trace(go.Scatter(
            x=df[timestamp_col],
            y=df['upper_band'],
            name=f'Upper BB ({window_size}, {std_dev})',
            line=dict(color='rgba(173, 216, 230, 0.7)', width=1)
        ))
        
        fig.add_trace(go.Scatter(
            x=df[timestamp_col],
            y=df['lower_band'],
            name=f'Lower BB ({window_size}, {std_dev})',
            line=dict(color='rgba(173, 216, 230, 0.7)', width=1),
            fill='tonexty',
            fillcolor='rgba(173, 216, 230, 0.1)'
        ))
    
    return fig

st.sidebar.header("Database Options")
use_in_memory = st.sidebar.checkbox("Use In-Memory Database", value=False, 
                                   help="In-memory database is faster but doesn't persist data")

# Technical indicators for chart visualization
# st.sidebar.header("Chart Options")
# chart_options = st.sidebar.expander("Technical Indicators", expanded=False)
# with chart_options:
#     show_sma = st.checkbox("Show Simple Moving Average (SMA)", value=False)
#     sma_window = st.slider("SMA Window", 5, 50, 20, 1) if show_sma else 20
    
#     show_ema = st.checkbox("Show Exponential Moving Average (EMA)", value=False)
#     ema_window = st.slider("EMA Window", 5, 50, 20, 1) if show_ema else 20
    
#     show_bollinger = st.checkbox("Show Bollinger Bands", value=False)
#     if show_bollinger:
#         bollinger_window = st.slider("Bollinger Window", 5, 50, 20, 1)
#         bollinger_std = st.slider("Standard Deviation", 1.0, 3.0, 2.0, 0.1)
#     else:
#         bollinger_window = 20
#         bollinger_std = 2.0

st.header("Query Input")
natural_query = st.text_area("Enter your query in natural language:", 
                           placeholder="Example: Show NIFTY50 index data for the last trading day with 15-minute intervals")

sql_query = ""
if natural_query:
    if st.button("Generate SQL"):
        with st.spinner("Converting to SQL..."):
            sql_query = nl_to_sql(natural_query)
            if sql_query:
                st.session_state['sql_query'] = sql_query
            else:
                st.error("Failed to generate SQL query")

st.header("SQL Query")
if 'sql_query' in st.session_state:
    sql_query = st.text_area("Generated SQL (you can edit if needed):", st.session_state['sql_query'], height=150)
else:
    sql_query = st.text_area("SQL query will appear here", height=150)

if sql_query:
    if st.button("Run Query"):
        with st.spinner("Executing query..."):
            results, execution_time, profiling_info = execute_query(sql_query, use_in_memory)
            
            if results is not None:
                st.success(f"Query executed successfully in {execution_time:.4f} seconds")
                
                # Show profiling info
                st.subheader("Query Profiling Information")
                st.dataframe(profiling_info, use_container_width=True)
                
                # Display results
                st.header("Query Results")
                st.dataframe(results, use_container_width=True)
                
                # Save results
                save_query_to_history(natural_query, sql_query, results)
                
                # Download options
                st.download_button(
                    label="Download as Excel",
                    data=results.to_excel(index=False).getvalue(),
                    file_name="query_results.xlsx",
                    mime="application/vnd.ms-excel"
                )
                
                # Create and display candlestick chart if applicable
                # if len(results) > 0:
                #     st.header("Data Visualization")
                #     chart = create_candlestick_chart(results)
                #     if chart:
                #         st.plotly_chart(chart, use_container_width=True)
            else:
                st.error(f"Query failed after {execution_time:.4f} seconds")
                st.code(profiling_info, language="python")  # In this case, profiling_info contains error traceback

with st.sidebar.expander("Database Schema", expanded=False):
    try:
        schema_info = disk_conn.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'financial_data'
        ORDER BY table_name, ordinal_position
        """).fetchdf()
        
        st.dataframe(schema_info, use_container_width=True)
    except:
        st.write("Schema information not available. Database may be empty.")

with st.sidebar.expander("Example Queries", expanded=True):
    st.markdown("""
    - Show NIFTY50 index data for the last trading day
    - Get highest and lowest prices for Reliance Futures in the past week
    - Compare daily closing prices of HDFC Bank options with strike price 1600 for both CE and PE
    - Find the average volume of Apple stock in NYSE by hour of day
    - Calculate 5-minute returns for TCS stock over the last 3 trading sessions
    - Show the top 5 highest volume intervals for NIFTY Bank options
    - Compare open interest trends for HDFC and ICICI futures
    - Display hourly OHLC data for Infosys with volume greater than 100000
    """)

# Show advanced options for database optimization
# with st.sidebar.expander("Advanced Options", expanded=False):
#     st.markdown("### Optimization Settings")
#     cache_size = st.slider("DuckDB Memory Limit (MB)", 100, 4000, 1000, 100)
#     threads = st.slider("Query Threads", 1, 16, 4, 1)
    
#     if st.button("Apply Settings"):
#         try:
#             disk_conn.execute(f"PRAGMA memory_limit='{cache_size}MB'")
#             disk_conn.execute(f"PRAGMA threads={threads}")
#             memory_conn.execute(f"PRAGMA memory_limit='{cache_size}MB'")
#             memory_conn.execute(f"PRAGMA threads={threads}")
#             st.success(f"Applied: {cache_size}MB memory limit with {threads} threads")
#         except Exception as e:
#             st.error(f"Failed to apply settings: {str(e)}")
    
    st.markdown("### Query Optimization")
    st.checkbox("Enable Parallel Execution", value=True, 
               help="Enable parallel query execution across multiple CPU cores")
    st.checkbox("Use Adaptive Filtering", value=True,
               help="Adaptively choose filtering strategy based on data statistics")
    st.checkbox("Cache Query Results", value=True,
               help="Cache frequently accessed query results in memory")

with st.sidebar.expander("Query History", expanded=False):
    history_folders = sorted([f for f in os.listdir(QUERY_HISTORY_DIR) if os.path.isdir(f"{QUERY_HISTORY_DIR}/{f}")], reverse=True)
    
    for folder in history_folders[:10]:  # Show most recent 10 queries
        timestamp = folder
        st.write(f"**{timestamp}**")
        
        try:
            with open(f"{QUERY_HISTORY_DIR}/{folder}/natural_query.txt", "r") as f:
                st.text(f.read())
        except:
            st.text("Query unavailable")
        
        cols = st.columns(3)
        with cols[0]:
            try:
                if os.path.exists(f"{QUERY_HISTORY_DIR}/{folder}/results.xlsx"):
                    with open(f"{QUERY_HISTORY_DIR}/{folder}/results.xlsx", "rb") as f:
                        st.download_button(
                            "Download Results", 
                            f, 
                            file_name=f"results_{timestamp}.xlsx",
                            mime="application/vnd.ms-excel"
                        )
            except:
                pass
        
        with cols[1]:
            if st.button("Load Query", key=f"load_{folder}"):
                try:
                    with open(f"{QUERY_HISTORY_DIR}/{folder}/sql_query.sql", "r") as f:
                        sql = f.read()
                    with open(f"{QUERY_HISTORY_DIR}/{folder}/natural_query.txt", "r") as f:
                        nl = f.read()
                    
                    st.session_state['sql_query'] = sql
                    st.experimental_rerun()
                except:
                    st.error("Failed to load query")
        
        with cols[2]:
            if st.button("Delete", key=f"delete_{folder}"):
                try:
                    import shutil
                    shutil.rmtree(f"{QUERY_HISTORY_DIR}/{folder}")
                    st.success("Query deleted")
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"Failed to delete: {str(e)}")

st.sidebar.markdown("---")
st.sidebar.markdown("### System Performance")

try:
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / 1024 / 1024  # in MB
    st.sidebar.text(f"Memory Usage: {memory_usage:.2f} MB")
except:
    st.sidebar.text("Memory Usage: Not available")

try:
    db_size = os.path.getsize(DB_PATH) / 1024 / 1024  # in MB
    st.sidebar.text(f"Database Size: {db_size:.2f} MB")
except:
    st.sidebar.text("Database Size: Not available")

st.sidebar.markdown("---")
st.sidebar.markdown("© 2025 Qode Data Engine")