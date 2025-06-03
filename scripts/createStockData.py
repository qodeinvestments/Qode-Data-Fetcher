import duckdb
import pandas as pd
import os
import logging
import time
import gc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = "/mnt/disk2/qode_edw.db"
STOCK_DATA_PARQUET = "/mnt/disk2/cold_storage/BSE_Stocks.parquet"
MAPPING_FILE = "/mnt/disk2/cold_storage/Accord Code Mapping.xlsx"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

thread_local = threading.local()

def get_thread_connection():
    if not hasattr(thread_local, 'conn'):
        thread_local.conn = duckdb.connect(DB_PATH)
        thread_local.conn.execute("SET memory_limit='64GB'")
        thread_local.conn.execute("SET threads=8")
        thread_local.conn.execute("SET max_memory='64GB'")
        thread_local.conn.execute("SET temp_directory='/tmp'")
    return thread_local.conn

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def execute_with_timing(conn, query, description):
    start_time = time.time()
    try:
        conn.execute(query)
        duration = time.time() - start_time
        logger.info(f"{description} - SUCCESS - Duration: {duration:.2f}s")
        return True
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"{description} - FAILED - Duration: {duration:.2f}s - Error: {str(e)}")
        return False

def sanitize_table_name(name):
    if not name or pd.isna(name):
        return "UNKNOWN"
    
    sanitized = str(name).replace('-', '_').replace('.', '_').replace(' ', '_')
    sanitized = ''.join(c for c in sanitized if c.isalnum() or c == '_')
    
    if sanitized and sanitized[0].isdigit():
        sanitized = f"S_{sanitized}"
    
    return sanitized or "UNKNOWN"

def load_mapping_data(mapping_file_path):
    try:
        logger.info(f"Loading mapping data from: {mapping_file_path}")
        mapping_df = pd.read_excel(mapping_file_path)
        
        logger.info(f"Mapping data loaded successfully. Shape: {mapping_df.shape}")
        logger.info(f"Mapping columns: {list(mapping_df.columns)}")
        
        mapping_dict = {}
        for _, row in mapping_df.iterrows():
            accord_code = row['Accord Code']
            mapping_dict[accord_code] = {
                'CD_Bse Scrip Name': sanitize_table_name(row['CD_Bse Scrip Name'])
            }
        
        logger.info(f"Created mapping for {len(mapping_dict)} accord codes")
        return mapping_dict
        
    except Exception as e:
        logger.error(f"Error loading mapping data: {str(e)}")
        return {}

def process_stock_batch(batch_data):
    conn = get_thread_connection()
    results = {'successful': 0, 'failed': 0, 'total_records': 0}
    
    try:
        conn.execute("BEGIN TRANSACTION")
        
        for accord_code, group_df, mapping_info in batch_data:
            try:
                bse_symbol = mapping_info['CD_Bse Scrip Name']
                
                if not bse_symbol or bse_symbol == 'UNKNOWN':
                    results['failed'] += 1
                    continue
                
                table_name = f"market_data.BSE_Stocks_{bse_symbol}"
                
                # Remove the columns we don't want and keep only the required columns
                columns_to_keep = [
                    'No of Trades', 'Market Cap', 'TTM PE(x)', 'Cons TTM PE(x)', 
                    'P/BV(x)', 'Cons P/BV(x)', 'EV/EBIDTA(x)', 'Cons EV/EBIDTA(x)', 
                    'MCAP/Sales(x)', 'Cons MCAP/Sales(x)', 'timestamp', 
                    'o', 'h', 'l', 'c', 'v'
                ]
                
                # Filter the dataframe to only include the columns we want
                available_columns = [col for col in columns_to_keep if col in group_df.columns]
                clean_df = group_df[available_columns].copy()
                
                conn.register(f'temp_stock_data_{accord_code}', clean_df)
                
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS 
                SELECT * FROM temp_stock_data_{accord_code}
                """
                
                if execute_with_timing(conn, create_table_query, f"Creating table {table_name}"):
                    # Create index only on timestamp since we removed Accord Code
                    index_query = f"CREATE INDEX IF NOT EXISTS idx_{bse_symbol}_timestamp ON {table_name}(timestamp)"
                    execute_with_timing(conn, index_query, f"Creating timestamp index for {table_name}")
                    
                    results['successful'] += 1
                    results['total_records'] += len(clean_df)
                    
                    logger.info(f"Successfully created {table_name} with {len(clean_df)} records")
                else:
                    results['failed'] += 1
                
                conn.unregister(f'temp_stock_data_{accord_code}')
                
            except Exception as e:
                results['failed'] += 1
                logger.error(f"Error processing Accord Code {accord_code}: {str(e)}")
        
        conn.execute("COMMIT")
        
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Batch processing failed: {str(e)}")
        results['failed'] += len(batch_data)
    
    return results

def create_stock_tables_from_parquet(stock_data_path, mapping_file_path):
    start_time = time.time()
    logger.info(f"Starting stock data migration from: {stock_data_path}")
    
    conn = duckdb.connect(DB_PATH)
    conn.execute("SET memory_limit='200GB'")
    conn.execute("SET threads=32")
    conn.execute("SET max_memory='200GB'")
    conn.execute("SET checkpoint_threshold='10GB'")
    conn.execute("SET temp_directory='/tmp'")
    conn.execute("SET force_compression='zstd'")
    
    execute_with_timing(conn, "CREATE SCHEMA IF NOT EXISTS market_data;", "Creating market_data schema")
    
    mapping_dict = load_mapping_data(mapping_file_path)
    if not mapping_dict:
        logger.error("No mapping data available. Exiting.")
        return
    
    try:
        logger.info(f"Loading stock data from: {stock_data_path}")
        stock_df = pd.read_parquet(stock_data_path)
        
        logger.info(f"Stock data loaded successfully. Shape: {stock_df.shape}")
        logger.info(f"Stock data columns: {list(stock_df.columns)}")
        
        required_columns = [
            'No of Trades', 'Market Cap', 'TTM PE(x)', 'Cons TTM PE(x)', 
            'P/BV(x)', 'Cons P/BV(x)', 'EV/EBIDTA(x)', 'Cons EV/EBIDTA(x)', 
            'MCAP/Sales(x)', 'Cons MCAP/Sales(x)', 'Accord Code', 'timestamp', 
            'o', 'h', 'l', 'c', 'v'
        ]
        
        missing_columns = [col for col in required_columns if col not in stock_df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return
        
        logger.info("Grouping data by Accord Code...")
        grouped_data = stock_df.groupby('Accord Code')
        
        successful_tables = 0
        failed_tables = 0
        total_records = 0
        
        logger.info(f"Found {len(grouped_data)} unique Accord Codes")
        
        batch_size = 10000
        batch_data = []
        
        for accord_code, group_df in grouped_data:
            if accord_code not in mapping_dict:
                logger.warning(f"Accord Code {accord_code} not found in mapping. Skipping.")
                failed_tables += 1
                continue
            
            mapping_info = mapping_dict[accord_code]
            batch_data.append((accord_code, group_df, mapping_info))
            
            if len(batch_data) >= batch_size:
                with ThreadPoolExecutor(max_workers=16) as executor:
                    futures = []
                    
                    chunk_size = 10
                    for i in range(0, len(batch_data), chunk_size):
                        chunk = batch_data[i:i + chunk_size]
                        future = executor.submit(process_stock_batch, chunk)
                        futures.append(future)
                    
                    for future in as_completed(futures):
                        try:
                            results = future.result()
                            successful_tables += results['successful']
                            failed_tables += results['failed']
                            total_records += results['total_records']
                        except Exception as e:
                            logger.error(f"Future execution error: {str(e)}")
                
                batch_data = []
                
                conn.execute("CHECKPOINT")
                logger.info(f"Checkpoint completed after processing {successful_tables + failed_tables} stocks")
                
                gc.collect()
        
        if batch_data:
            with ThreadPoolExecutor(max_workers=16) as executor:
                futures = []
                
                chunk_size = 10
                for i in range(0, len(batch_data), chunk_size):
                    chunk = batch_data[i:i + chunk_size]
                    future = executor.submit(process_stock_batch, chunk)
                    futures.append(future)
                
                for future in as_completed(futures):
                    try:
                        results = future.result()
                        successful_tables += results['successful']
                        failed_tables += results['failed']
                        total_records += results['total_records']
                    except Exception as e:
                        logger.error(f"Future execution error: {str(e)}")
        
        conn.execute("CHECKPOINT")
        
        migration_duration = time.time() - start_time
        data_size = os.path.getsize(stock_data_path)
        
        logger.info(f"=== STOCK MIGRATION SUMMARY ===")
        logger.info(f"Total stocks processed: {successful_tables + failed_tables}")
        logger.info(f"Successful table creations: {successful_tables}")
        logger.info(f"Failed table creations: {failed_tables}")
        logger.info(f"Success rate: {(successful_tables/(successful_tables + failed_tables)*100):.1f}%" if (successful_tables + failed_tables) > 0 else "No stocks processed")
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Data file size: {format_size(data_size)}")
        logger.info(f"Total migration time: {migration_duration:.2f}s")
        logger.info(f"Average throughput: {format_size(data_size/migration_duration)}/s" if migration_duration > 0 else "N/A")
        
        logger.info("=== SAMPLE OF CREATED TABLES ===")
        result = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'market_data' 
            AND table_name LIKE 'BSE_Stocks_%' 
            ORDER BY table_name 
            LIMIT 10
        """).fetchall()
        
        for table in result:
            logger.info(f"Created table: {table[0]}")
        
    except Exception as e:
        logger.error(f"Error during stock data migration: {str(e)}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    if not os.path.exists(STOCK_DATA_PARQUET):
        logger.error(f"Stock data file not found: {STOCK_DATA_PARQUET}")
        exit(1)
    
    if not os.path.exists(MAPPING_FILE):
        logger.error(f"Mapping file not found: {MAPPING_FILE}")
        exit(1)
    
    create_stock_tables_from_parquet(STOCK_DATA_PARQUET, MAPPING_FILE)
    
    logger.info("Stock data migration completed!")