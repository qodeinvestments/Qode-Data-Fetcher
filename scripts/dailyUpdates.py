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
import threading

DB_PATH = "/mnt/disk2/qode_edw.db"

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

thread_local = threading.local()

def get_thread_connection():
    """Get or create a thread-local database connection"""
    if not hasattr(thread_local, 'conn'):
        thread_local.conn = duckdb.connect(DB_PATH)
        
    return thread_local.conn

def get_main_connection():
    """Get main database connection for schema operations"""
    conn = duckdb.connect(DB_PATH)

    return conn

def create_table_if_not_exists(conn, table_name):
    """Create table if it doesn't exist with the required schema"""
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

def upsert_data_to_duckdb(conn, df, segment):
    """Insert or update data in DuckDB table"""
    if df.empty:
        logger.warning("Empty dataframe received")
        return
    
    df = df.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'})
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    df['symbol'] = df['symbolid'].map(lambda x: tdsymbolidTOsymbol.get(str(x), f"UNKNOWN_{x}"))
    
    table_name = f"market_data.{segment}_data"
    
    # Create table if it doesn't exist
    if not create_table_if_not_exists(conn, table_name):
        logger.error(f"Failed to create table {table_name}")
        return
    
    try:
        # Begin transaction
        conn.execute("BEGIN TRANSACTION")
        
        # Delete existing records for the same timestamps and symbolids to handle overwrites
        timestamps_str = "'" + "','".join(df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()) + "'"
        symbolids_str = ','.join(df['symbolid'].astype(str).tolist())
        
        delete_sql = f"""
        DELETE FROM {table_name} 
        WHERE timestamp IN ({timestamps_str}) 
        AND symbolid IN ({symbolids_str})
        """
        
        conn.execute(delete_sql)
        logger.info(f"Deleted existing records for timestamps and symbols in {table_name}")
        
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
    
    
    if len(df) <= 10000:
        conn = get_thread_connection()
        upsert_data_to_duckdb(conn, df, segment)
        return
    
    chunk_size = max(1000, len(df) // max_workers)
    chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    
    def process_chunk(chunk_data):
        conn = get_thread_connection()
        upsert_data_to_duckdb(conn, chunk_data, segment)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        concurrent.futures.wait(futures)
    
    logger.info(f"Completed processing {len(chunks)} chunks for segment {segment}")

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
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = get_main_connection()
    logger.info("Database initialized successfully")
    return conn

def get_table_info(segment):
    """Get information about existing tables for a segment"""
    conn = get_thread_connection()
    table_name = f"market_data.{segment}_data"
    
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

if __name__ == "__main__":
    # Initialize database
    initialize_database()
    
    dt_start = datetime.now().replace(hour=9, minute=15)
    dt_end = datetime.now().replace(hour=15, minute=30)
    
    start_time = dt_start.strftime("%y%m%dT%H:%M")
    end_time = dt_end.strftime("%y%m%dT%H:%M")
    
    username = "tdwsf575"
    password = "vidhi@575"
    
    try:
        token = get_auth_token(username, password)
        logger.info("Authentication successful")
        
        segments = ['bsefo']  # Example segments
        # Available segments: 'eq', 'bseeq', 'bseind', 'bsefo', 'ind', 'fo'
        
        # Show current table information
        for segment in segments:
            get_table_info(segment)
        
        timestamps = generate_timestamps(start_time, end_time)
        logger.info(f"Generated {len(timestamps)} timestamps from {start_time} to {end_time}")
        
        fetch_data(token, segments, timestamps)
        
        # Show final table information
        logger.info("=== FINAL TABLE STATISTICS ===")
        for segment in segments:
            get_table_info(segment)
        
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        raise