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
        # Create a schema with a different name to avoid ambiguity
        conn.execute("""
        CREATE SCHEMA IF NOT EXISTS market_data;
        """)
        
        for exchange in os.listdir(f"{DATA_DIR}"):
            print(f"Processing exchange: {exchange}")
            exchange_path = f"{DATA_DIR}/{exchange}"
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
                                            # Use the new schema name here
                                            view_name = f"market_data.{exchange}_{instrument}_{index_name}"
                                            conn.execute(f"""
                                            CREATE OR REPLACE VIEW {view_name} AS 
                                            SELECT * FROM read_parquet('{parquet_path}')
                                            """)
                                            
                                            # Create optimized view with standardized column names
                                            conn.execute(f"""
                                            CREATE OR REPLACE VIEW {view_name}_std AS 
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
                                                            option_type = file.split("_")[-1].replace(".parquet", "")
                                                            # Use the new schema name here
                                                            view_name = f"market_data.{exchange}_{instrument}_{underlying}_{expiry}_{strike}_{option_type}"
                                                            conn.execute(f"""
                                                            CREATE OR REPLACE VIEW {view_name} AS 
                                                            SELECT * FROM read_parquet('{parquet_path}')
                                                            """)
                                                            
                                                            # Create optimized view with standardized column names
                                                            conn.execute(f"""
                                                            CREATE OR REPLACE VIEW {view_name}_std AS 
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
                        
                        # Handle Futures data
                        elif instrument == "Futures":
                            for underlying in os.listdir(instrument_path):
                                underlying_dir = f"{instrument_path}/{underlying}"
                                if os.path.isdir(underlying_dir):
                                    for file in os.listdir(underlying_dir):
                                        if file.endswith(".parquet"):
                                            parquet_path = f"{underlying_dir}/{file}"
                                            # Use the new schema name here
                                            view_name = f"market_data.{exchange}_{instrument}_{underlying}"
                                            conn.execute(f"""
                                            CREATE OR REPLACE VIEW {view_name} AS 
                                            SELECT * FROM read_parquet('{parquet_path}')
                                            """)
                                            
                                            # Create optimized view with standardized column names
                                            conn.execute(f"""
                                            CREATE OR REPLACE VIEW {view_name}_std AS 
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
memory_conn = get_duckdb_connection(in_memory=True)