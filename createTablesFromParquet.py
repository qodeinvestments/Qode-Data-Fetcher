import duckdb
import os

DB_PATH = "qode_engine_data.db"
DB_IN_MEMORY = ":memory:"
QUERY_HISTORY_DIR = "query_history"
DATA_DIR = "cold_storage"

os.makedirs(QUERY_HISTORY_DIR, exist_ok=True)

def get_duckdb_connection(in_memory=False):
    """Get a DuckDB connection (cached for performance)"""
    db_path = DB_IN_MEMORY if in_memory else DB_PATH
    conn = duckdb.connect(db_path)
    print(f"Connected to DuckDB database at {db_path}")
    
    if os.path.exists(DATA_DIR):        
        conn.execute("""
        CREATE SCHEMA IF NOT EXISTS market_data;
        """)
        
        for exchange in os.listdir(f"{DATA_DIR}"):
            print(f"Processing exchange: {exchange}")
            exchange_path = f"{DATA_DIR}/{exchange}"
            if exchange != 'BSE':
                if os.path.isdir(exchange_path):
                    for instrument in os.listdir(exchange_path):
                        print(f"Processing instrument: {instrument} in exchange: {exchange}")
                        instrument_path = f"{exchange_path}/{instrument}"
                        if os.path.isdir(instrument_path):
                            if instrument == "Index":
                                for index_name in os.listdir(instrument_path):
                                    print(f"Processing index: {index_name} in instrument: {instrument}")
                                    index_dir = f"{instrument_path}/{index_name}"
                                    if os.path.isdir(index_dir):
                                        for file in os.listdir(index_dir):
                                            print(f"Processing file: {file} in index: {index_name}")
                                            if file.endswith(".parquet"):
                                                parquet_path = f"{index_dir}/{file}"
                                                
                                                correct_exchange = exchange
                                                print(f"Creating table for {correct_exchange} {instrument} {index_name}")
                                                
                                                table_name = f"market_data.{correct_exchange}_{instrument}_{index_name}"
                                                conn.execute(f"""
                                                DROP TABLE IF EXISTS {table_name};
                                                CREATE TABLE {table_name} AS 
                                                SELECT * FROM read_parquet('{parquet_path}')
                                                """)
                                                
                                                std_table_name = f"market_data.{correct_exchange}_{instrument}_{index_name}_std"
                                                conn.execute(f"""
                                                DROP TABLE IF EXISTS {std_table_name};
                                                CREATE TABLE {std_table_name} AS 
                                                SELECT 
                                                    timestamp as datetime,
                                                    o as open,
                                                    h as high,
                                                    l as low,
                                                    c as close
                                                FROM read_parquet('{parquet_path}')
                                                """)
                            
                            elif instrument == "Options":
                                for underlying in os.listdir(instrument_path):
                                    underlying_dir = f"{instrument_path}/{underlying}"
                                    if os.path.isdir(underlying_dir):
                                        for expiry in os.listdir(underlying_dir):
                                            print(f"Processing expiry: {expiry} for underlying: {underlying}")
                                            expiry_dir = f"{underlying_dir}/{expiry}"
                                            if os.path.isdir(expiry_dir):
                                                for strike in os.listdir(expiry_dir):
                                                    print(f"Processing strike: {strike} in expiry: {expiry} for underlying: {underlying}")
                                                    strike_dir = f"{expiry_dir}/{strike}"
                                                    if os.path.isdir(strike_dir):
                                                        for file in os.listdir(strike_dir):
                                                            if file.endswith(".parquet"):
                                                                print(f"Processing file: {file} in strike: {strike}")
                                                                parquet_path = f"{strike_dir}/{file}"
                                                                # Extract option type from filename (ensure CE/PE is handled correctly)
                                                                file_parts = file.split("_")
                                                                option_type = file_parts[-1].replace(".parquet", "")
                                                                
                                                                # Map CE/PE to call/put if needed for consistency
                                                                if option_type == "CE":
                                                                    option_type = "call"
                                                                elif option_type == "PE":
                                                                    option_type = "put"
                                                                
                                                                correct_exchange = exchange
                                                                
                                                                # Debug log to help track what's happening
                                                                print(f"Creating table for {correct_exchange} {instrument} {underlying} {expiry} {strike} {option_type}")
                                                                
                                                                table_name = f"market_data.{correct_exchange}_{instrument}_{underlying}_{expiry}_{strike}_{option_type}"
                                                                conn.execute(f"""
                                                                DROP TABLE IF EXISTS {table_name};
                                                                CREATE TABLE {table_name} AS 
                                                                SELECT * FROM read_parquet('{parquet_path}')
                                                                """)
                                                                
                                                                # Create standardized table with consistent column names
                                                                std_table_name = f"market_data.{correct_exchange}_{instrument}_{underlying}_{expiry}_{strike}_{option_type}_std"
                                                                conn.execute(f"""
                                                                DROP TABLE IF EXISTS {std_table_name};
                                                                CREATE TABLE {std_table_name} AS 
                                                                SELECT 
                                                                    timestamp as datetime,
                                                                    o as open,
                                                                    h as high,
                                                                    l as low,
                                                                    c as close,
                                                                    v as volume,
                                                                    oi as open_interest
                                                                FROM read_parquet('{parquet_path}')
                                                                """)
                            
                            elif instrument == "Futures":
                                for underlying in os.listdir(instrument_path):
                                    underlying_dir = f"{instrument_path}/{underlying}"
                                    if os.path.isdir(underlying_dir):
                                        for file in os.listdir(underlying_dir):
                                            if file.endswith(".parquet"):
                                                parquet_path = f"{underlying_dir}/{file}"
                                                
                                                # Ensure we use the correct exchange for each futures contract
                                                correct_exchange = exchange
                                                # The exchange should match the folder structure
                                                print(f"Creating table for {correct_exchange} {instrument} {underlying}")
                                                
                                                # Create tables instead of views
                                                table_name = f"market_data.{correct_exchange}_{instrument}_{underlying}"
                                                conn.execute(f"""
                                                DROP TABLE IF EXISTS {table_name};
                                                CREATE TABLE {table_name} AS 
                                                SELECT * FROM read_parquet('{parquet_path}')
                                                """)
                                                
                                                # Create standardized table with consistent column names
                                                std_table_name = f"market_data.{correct_exchange}_{instrument}_{underlying}_std"
                                                conn.execute(f"""
                                                DROP TABLE IF EXISTS {std_table_name};
                                                CREATE TABLE {std_table_name} AS 
                                                SELECT 
                                                    timestamp as datetime,
                                                    o as open,
                                                    h as high,
                                                    l as low,
                                                    c as close,
                                                    v as volume,
                                                    oi as open_interest
                                                FROM read_parquet('{parquet_path}')
                                                """)
        
    return conn

disk_conn = get_duckdb_connection(in_memory=False)