import duckdb
import time
from datetime import datetime

DB_PATH = "qode_engine_data_fetcher.db"

try:
    conn = duckdb.connect(DB_PATH)
except Exception as e:
    print(f"Error connecting to database: {e}")

def get_tick(timestamp, symbol):
    """
    Get a specific tick for a symbol at a given timestamp
    
    Args:
        timestamp (str or datetime): The timestamp to search for
        symbol (str): The symbol/instrument to search for (e.g., 'BSE_Index_SENSEX', 'BSE_Options_SENSEX_20240816_83300_call')
        
    Returns:
        pandas.DataFrame: A dataframe with the tick data or empty if not found
    """
    if isinstance(timestamp, datetime):
        timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        result = conn.execute(f"""
            SELECT * FROM market_data.{symbol}
            WHERE timestamp = '{timestamp}'
            LIMIT 1
        """).fetchdf()
        
        if not result.empty:
            return result
    except Exception as e:
        print(f"Error querying table: {e}")
        
def get_all_ticks_by_symbol(symbol, start_date=None, end_date=None, limit=None):
    """
    Get all ticks for a specific symbol, optionally within a date range
    
    Args:
        symbol (str): The symbol/instrument to search for
        start_date (str or datetime, optional): Filter results to this start date or later
        end_date (str or datetime, optional): Filter results to this end date or earlier
        limit (int, optional): Maximum number of rows to return
        
    Returns:
        pandas.DataFrame: A dataframe with all tick data for the symbol
    """
    if isinstance(start_date, datetime):
        start_date = start_date.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(end_date, datetime):
        end_date = end_date.strftime('%Y-%m-%d %H:%M:%S')
    
    time_col = "timestamp"
    query = f"SELECT * FROM market_data.{symbol}"
    
    conditions = []
    if start_date:
        conditions.append(f"{time_col} >= '{start_date}'")
    if end_date:
        conditions.append(f"{time_col} <= '{end_date}'")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += f" ORDER BY {time_col}"
    
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        result = conn.execute(query).fetchdf()
        return result
    except Exception as e:
        print(f"Error querying for all ticks of {symbol}: {e}")

def profile_functions():
    """Profile the execution time of the functions with sample calls"""
    index_symbol = "BSE_Index_SENSEX"
    options_symbol = "BSE_Options_SENSEX_20240816_83300_call"
    
    sample_timestamp = "2024-05-15 10:30:00"
    
    print("Profiling get_tick function...")
    start_time = time.time()
    result = get_tick(sample_timestamp, index_symbol)
    elapsed = time.time() - start_time
    print(f"get_tick took {elapsed:.6f} seconds")
    print(f"Result shape: {result.shape}")
    
    print("\nProfiling get_all_ticks_by_symbol with no filters...")
    start_time = time.time()
    result = get_all_ticks_by_symbol(index_symbol, limit=100)
    elapsed = time.time() - start_time
    print(f"get_all_ticks_by_symbol took {elapsed:.6f} seconds")
    print(f"Result shape: {result.shape}")
    
    print("\nProfiling get_all_ticks_by_symbol with date filters...")
    start_time = time.time()
    result = get_all_ticks_by_symbol(
        options_symbol, 
        start_date="2024-05-01", 
        end_date="2024-05-20",
        limit=100
    )
    elapsed = time.time() - start_time
    print(f"get_all_ticks_by_symbol with filters took {elapsed:.6f} seconds")
    print(f"Result shape: {result.shape}")

def sample_function_calls():
    """Demonstrate how to call these functions"""
    print("=== Sample Function Calls ===")
    
    print("\nExample 1: Get a specific tick")
    tick_data = get_tick("2024-05-15 09:15:00", "BSE_Index_SENSEX")
    if not tick_data.empty:
        print("Result:")
        print(tick_data)
    else:
        print("No data found for this timestamp")
    
    print("\nExample 2: Get first 5 ticks for a symbol")
    all_ticks = get_all_ticks_by_symbol("BSE_Index_SENSEX", limit=5)
    if not all_ticks.empty:
        print("Result:")
        print(all_ticks)
    else:
        print("No data found for this symbol")
    
    print("\nExample 3: Get ticks within a date range")
    range_ticks = get_all_ticks_by_symbol(
        "BSE_Options_SENSEX_20240816_83300_call",
        start_date="2024-08-10",
        end_date="2024-08-15",
        limit=10
    )
    if not range_ticks.empty:
        print("Result:")
        print(range_ticks)
    else:
        print("No data found in this date range")

if __name__ == "__main__":
    profile_functions()
    
    sample_function_calls()