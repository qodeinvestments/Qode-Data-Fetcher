import requests
from datetime import datetime, timedelta
import time
import direct_redis
import pandas as pd
from io import StringIO
import concurrent.futures
import duckdb
import os
import logging
from dotenv import load_dotenv

load_dotenv()

DB_PATH = "/mnt/disk2/qode_edw_bp.db"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

r = direct_redis.DirectRedis(host='localhost', port=6379, db=0)
tdsymbolidTOsymbol = r.get('tdsymbolidTOsymbol')
# print(f"Loaded {len(tdsymbolidTOsymbol)} symbol mappings from Redis")

def get_connection():
    """Get database connection"""
    return duckdb.connect(DB_PATH)

def get_table_name(symbol, instrument_type, additional_params=None):
    """Generate table name based on symbol and instrument type"""
    parts = symbol.split('_')
    if len(parts) >= 2:
        exchange = parts[0]
        underlying = parts[1]
    else:
        exchange = "NSE"
        underlying = symbol
    
    if instrument_type == "Options" and additional_params:
        expiry_date = additional_params.get('expiry_date', '')
        strike_price = additional_params.get('strike_price', '')
        option_type = additional_params.get('option_type', '')
        table_name = f"{exchange}_{instrument_type}_{underlying}_{expiry_date}_{strike_price}_{option_type}"
    elif instrument_type == "Futures" and additional_params and additional_params.get('expiry_date'):
        expiry_date = additional_params.get('expiry_date')
        table_name = f"{exchange}_{instrument_type}_{underlying}_{expiry_date}"
    else:
        table_name = f"{exchange}_{instrument_type}_{underlying}"
    
    return table_name

def create_table_if_not_exists(conn, table_name, instrument_type):
    """Create table if it doesn't exist with the required schema based on instrument type"""
    if instrument_type == "Options":
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            oi BIGINT,
            PRIMARY KEY (timestamp)
        );
        """
    elif instrument_type == "Futures":
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            oi BIGINT,
            symbol VARCHAR,
            delivery_cycle VARCHAR,
            PRIMARY KEY (timestamp)
        );
        """
    elif instrument_type == "Index":
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            PRIMARY KEY (timestamp)
        );
        """
    else:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP,
            symbolid INTEGER,
            symbol VARCHAR,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            oi BIGINT,
            PRIMARY KEY (timestamp, symbolid)
        );
        """
    
    try:
        conn.execute(create_table_sql)
        logger.info(f"Table {table_name} created or verified")
        return True
    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {str(e)}")
        return False

def determine_instrument_type(symbol):
    """Determine instrument type based on symbol"""
    if '_OPT_' in symbol or '_CE_' in symbol or '_PE_' in symbol:
        return "Options"
    elif '_FUT_' in symbol or 'FUT' in symbol:
        return "Futures"
    elif symbol in ['NIFTY', 'BANKNIFTY', 'SENSEX', 'BANKEX'] or 'INDEX' in symbol:
        return "Index"
    else:
        return "Unknown"

def parse_option_symbol(symbol):
    """Parse option symbol to extract parameters"""
    additional_params = {}
    parts = symbol.split('_')
    
    if len(parts) >= 4:
        try:
            additional_params['expiry_date'] = parts[2] if len(parts[2]) == 8 else ''
            additional_params['strike_price'] = parts[3] if parts[3].isdigit() else ''
            additional_params['option_type'] = parts[4].lower() if len(parts) > 4 else ''
        except:
            pass
    
    return additional_params

def parse_futures_symbol(symbol):
    """Parse futures symbol to extract parameters"""
    additional_params = {}
    parts = symbol.split('_')
    
    if len(parts) >= 3 and len(parts[2]) == 8:
        additional_params['expiry_date'] = parts[2]
    
    return additional_params

def upsert_data_to_duckdb(conn, df, instrument_type, table_name):
    """Insert or update data in DuckDB table"""
    if df.empty:
        logger.warning("Empty dataframe received")
        return
    
    df = df.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'})
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    if not create_table_if_not_exists(conn, table_name, instrument_type):
        logger.error(f"Failed to create table {table_name}")
        return
    
    try:
        conn.execute("BEGIN TRANSACTION")
        
        timestamps_str = "'" + "','".join(df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()) + "'"
        
        if instrument_type == "Options":
            delete_sql = f"""
            DELETE FROM {table_name} 
            WHERE timestamp IN ({timestamps_str})
            """
            
            insert_sql = f"""
            INSERT INTO {table_name} (timestamp, o, h, l, c, v, oi)
            SELECT 
                timestamp,
                o,
                h,
                l,
                c,
                v,
                oi
            FROM df
            """
        elif instrument_type == "Futures":
            delete_sql = f"""
            DELETE FROM {table_name} 
            WHERE timestamp IN ({timestamps_str})
            """
            
            df['symbol'] = df['symbolid'].map(lambda x: tdsymbolidTOsymbol.get(str(x), f"UNKNOWN_{x}"))
            df['delivery_cycle'] = 'I'
            
            insert_sql = f"""
            INSERT INTO {table_name} (timestamp, o, h, l, c, v, oi, symbol, delivery_cycle)
            SELECT 
                timestamp,
                o,
                h,
                l,
                c,
                v,
                oi,
                symbol,
                delivery_cycle
            FROM df
            """
        elif instrument_type == "Index":
            delete_sql = f"""
            DELETE FROM {table_name} 
            WHERE timestamp IN ({timestamps_str})
            """
            
            insert_sql = f"""
            INSERT INTO {table_name} (timestamp, o, h, l, c)
            SELECT 
                timestamp,
                o,
                h,
                l,
                c
            FROM df
            """
        else:
            symbolids_str = ','.join(df['symbolid'].astype(str).tolist())
            delete_sql = f"""
            DELETE FROM {table_name} 
            WHERE timestamp IN ({timestamps_str}) 
            AND symbolid IN ({symbolids_str})
            """
            
            df['symbol'] = df['symbolid'].map(lambda x: tdsymbolidTOsymbol.get(str(x), f"UNKNOWN_{x}"))
            
            insert_sql = f"""
            INSERT INTO {table_name} (timestamp, symbolid, symbol, o, h, l, c, v, oi)
            SELECT 
                timestamp,
                symbolid,
                symbol,
                o,
                h,
                l,
                c,
                v,
                oi
            FROM df
            """
        
        conn.execute(delete_sql)
        logger.info(f"Deleted existing records for timestamps in {table_name}")
        
        conn.execute(insert_sql)
        
        conn.execute("COMMIT")
        
        logger.info(f"Successfully upserted {len(df)} records to {table_name}")
        
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Failed to upsert data to {table_name}: {str(e)}")
        raise

def store_in_duckdb(df, segment, max_workers=4):
    """Store dataframe in DuckDB with parallel processing for large datasets"""
    print(f'df length : {len(df)}, columns : {df.columns}')
    
    if df.empty:
        logger.warning("Empty dataframe received")
        return
    
    conn = get_connection()
    
    if 'symbolid' not in df.columns:
        logger.error("symbolid column not found in dataframe")
        return
    
    grouped = df.groupby('symbolid')
    
    for symbolid, group_df in grouped:
        symbol = tdsymbolidTOsymbol.get(str(symbolid), f"UNKNOWN_{symbolid}")
        instrument_type = determine_instrument_type(symbol)
        
        additional_params = None
        if instrument_type == "Options":
            additional_params = parse_option_symbol(symbol)
        elif instrument_type == "Futures":
            additional_params = parse_futures_symbol(symbol)
        
        table_name = get_table_name(symbol, instrument_type, additional_params)
        
        if len(group_df) <= 10000:
            upsert_data_to_duckdb(conn, group_df, instrument_type, table_name)
        else:
            chunk_size = max(10000, len(group_df) // max_workers)
            chunks = [group_df[i:i + chunk_size] for i in range(0, len(group_df), chunk_size)]
            
            def process_chunk(chunk_data):
                chunk_conn = get_connection()
                upsert_data_to_duckdb(chunk_conn, chunk_data, instrument_type, table_name)
                chunk_conn.close()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
                concurrent.futures.wait(futures)
            
            logger.info(f"Completed processing {len(chunks)} chunks for symbol {symbol}")
    
    conn.close()

def generate_timestamps(start_time_str, end_time_str, time_format="%y%m%dT%H:%M"):
    start_time = datetime.strptime(start_time_str, time_format)
    end_time = datetime.strptime(end_time_str, time_format)
    
    timestamps = []
    
    current_time = start_time
    while current_time <= end_time:
        timestamps.append(current_time.strftime(time_format))
        current_time += timedelta(minutes=1)
    
    return timestamps

def get_auth_token(username, password):
    url = "https://auth.truedata.in/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "username": username,
        "password": password,
        "grant_type": "password"
    }
    
    response = requests.post(url, headers=headers, data=data)
    
    if response.status_code == 200:
        token_data = response.json()
        return token_data.get("access_token")
    else:
        raise Exception("Failed to fetch the token. Status code: {}".format(response.status_code))

def fetch_data_for_segment(token, segment, timestamps):
    for timestamp in timestamps:
        url = f"https://history.truedata.in/getAllBars?segment={segment}&timestamp={timestamp}&response=csv"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                csv_content = StringIO(response.text)
                data = pd.read_csv(csv_content)
                print(f"First few rows:\n{data.head()}")
                
                if not data.empty:
                    store_in_duckdb(data, segment)
                    print(f"Data for segment {segment} and timestamp {timestamp} saved to DuckDB")
                else:
                    print(f"No data received for segment {segment} and timestamp {timestamp}")
            else:
                print(f"Failed to fetch data for segment {segment} and timestamp {timestamp}. Status code: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error processing segment {segment} timestamp {timestamp}: {str(e)}")
        
        time.sleep(2)

def fetch_data(token, segments, timestamps):
    for segment in segments:
        logger.info(f"Starting data fetch for segment: {segment}")
        fetch_data_for_segment(token, segment, timestamps)
        logger.info(f"Completed data fetch for segment: {segment}")

def initialize_database():
    """Initialize database and create necessary schemas"""
    logger.info(f"Initializing DuckDB database at: {DB_PATH}")
    
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = get_connection()
    logger.info("Database initialized successfully")
    conn.close()
    return True

def get_table_info(table_name):
    """Get information about existing tables"""
    conn = get_connection()
    
    try:
        result = conn.execute(f"""
        SELECT COUNT(*) as row_count, 
               MIN(timestamp) as min_timestamp,
               MAX(timestamp) as max_timestamp
        FROM {table_name}
        """).fetchone()
        
        if result:
            logger.info(f"Table {table_name} contains {result[0]} rows, "
                       f"from {result[1]} to {result[2]}")
        
    except Exception as e:
        logger.info(f"Table {table_name} does not exist or is empty")
    finally:
        conn.close()

if __name__ == "__main__":
    initialize_database()
    
    dt_start = datetime(year=2025, month=4, day=11, hour=9, minute=15, second=0)
    dt_end = datetime(year=2025, month=6, day=4, hour=15, minute=30, second=0)
    
    start_time = dt_start.strftime("%y%m%dT%H:%M")
    end_time = dt_end.strftime("%y%m%dT%H:%M")
    
    username = os.getenv("TRUEDATA_LOGIN_ID")
    password = os.getenv("TRUEDATA_LOGIN_PWD")

    try:
        token = get_auth_token(username, password)
        logger.info("Authentication successful")
        
        segments = ['fo', 'bsefo']
        
        timestamps = generate_timestamps(start_time, end_time)
        logger.info(f"Generated {len(timestamps)} timestamps from {start_time} to {end_time}")
        
        fetch_data(token, segments, timestamps)
        
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        raise