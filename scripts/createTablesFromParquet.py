import duckdb
import os
import logging
import time
import gc
import threading

DB_PATH = "qode_edw.db"
DATA_DIR = "cold_storage"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

thread_local = threading.local()

def get_thread_connection():
    """Get or create a thread-local database connection"""
    if not hasattr(thread_local, 'conn'):
        thread_local.conn = duckdb.connect(DB_PATH)
        thread_local.conn.execute("SET memory_limit='8GB'")
        thread_local.conn.execute("SET threads=4")
        thread_local.conn.execute("SET max_memory='8GB'")
        thread_local.conn.execute("SET temp_directory='/tmp'")
    return thread_local.conn

def get_file_size(file_path):
    return os.path.getsize(file_path)

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

def process_parquet_file_batch(parquet_files_info):
    """Process multiple parquet files in a batch"""
    conn = get_thread_connection()
    results = {'successful': 0, 'failed': 0, 'total_size': 0}
    
    try:
        conn.execute("BEGIN TRANSACTION")
        
        for parquet_path, table_name, file_type, file_size in parquet_files_info:
            logger.info(f"Processing {file_type}: {parquet_path}")
            
            create_main = f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
            
            if execute_with_timing(conn, create_main, f"Creating main table {table_name}"):
                logger.info(f"Main table {table_name} created")
                results['successful'] += 1
                results['total_size'] += file_size
            else:
                logger.error(f"File migration failed: {parquet_path}")
                results['failed'] += 1
        
        conn.execute("COMMIT")
        
        gc.collect()
        
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Batch processing failed: {str(e)}")
        results['failed'] += len(parquet_files_info)
    
    return results

def process_parquet_file(conn, parquet_path, table_name, file_type):
    logger.info(f"Processing {file_type}: {parquet_path}")
 
    create_main = f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
    
    if execute_with_timing(conn, create_main, f"Creating main table {table_name}"):
        logger.info(f"Main table {table_name} created")
        return True

    logger.error(f"File migration failed: {parquet_path}")
    return False

def get_duckdb_connection():
    start_time = time.time()
    logger.info(f"Initializing DuckDB connection to: {DB_PATH}")
    
    conn = duckdb.connect(DB_PATH)
    
    conn.execute("SET memory_limit='20GB'")
    conn.execute("SET threads=16")
    conn.execute("SET max_memory='20GB'")
    conn.execute("SET checkpoint_threshold='2GB'")
    conn.execute("SET temp_directory='/tmp'")
    
    conn.execute("SET force_compression='zstd'")
    
    logger.info(f"DuckDB connection established - Duration: {time.time() - start_time:.2f}s")
    
    if not os.path.exists(DATA_DIR):
        logger.warning(f"Data directory not found: {DATA_DIR}")
        return conn
    
    logger.info(f"Starting data migration from: {DATA_DIR}")
    
    execute_with_timing(conn, "CREATE SCHEMA IF NOT EXISTS market_data;", "Creating market_data schema")
    
    total_files = 0
    successful_files = 0
    existing_files = 0
    failed_files = 0
    total_size = 0
    
    exchanges = [d for d in os.listdir(DATA_DIR) if os.path.isdir(f"{DATA_DIR}/{d}")]
    logger.info(f"Found {len(exchanges)} exchanges: {exchanges}")
    
    batch_size = 10000
    file_batch = []
    checkpoint_counter = 0
    
    for exchange in exchanges:
        if exchange == 'NSE':
            exchange_start = time.time()
            logger.info(f"=== Processing Exchange: {exchange} ===")
            exchange_path = f"{DATA_DIR}/{exchange}"
            
            instruments = [d for d in os.listdir(exchange_path) if os.path.isdir(f"{exchange_path}/{d}")]
            logger.info(f"Exchange {exchange} has {len(instruments)} instruments: {instruments}")
            
            for instrument in instruments:
                instrument_start = time.time()
                logger.info(f"--- Processing Instrument: {instrument} in {exchange} ---")
                instrument_path = f"{exchange_path}/{instrument}"
                
                # if instrument == "Index":
                #     indices = [d for d in os.listdir(instrument_path) if os.path.isdir(f"{instrument_path}/{d}")]
                #     logger.info(f"Found {len(indices)} indices in {exchange}/{instrument}: {indices}")
                    
                #     for index_name in indices:
                #         index_start = time.time()
                #         logger.info(f"Processing Index: {index_name}")
                #         index_dir = f"{instrument_path}/{index_name}"
                        
                #         parquet_files = [f for f in os.listdir(index_dir) if f.endswith(".parquet")]
                #         logger.info(f"Found {len(parquet_files)} parquet files in {exchange}/{instrument}/{index_name}")
                        
                #         for file in parquet_files:
                #             parquet_path = f"{index_dir}/{file}"
                #             total_files += 1
                #             file_size = get_file_size(parquet_path)
                #             total_size += file_size
                            
                #             table_name = f"market_data.{exchange}_{instrument}_{index_name}"
                            
                #             if process_parquet_file(conn, parquet_path, table_name, "Index"):
                #                 successful_files += 1
                #             else:
                #                 failed_files += 1
                        
                #         logger.info(f"Index {index_name} completed - Duration: {time.time() - index_start:.2f}s")
                
                if instrument == "Options":
                    underlyings = [d for d in os.listdir(instrument_path) if os.path.isdir(f"{instrument_path}/{d}")]
                    logger.info(f"Found {len(underlyings)} underlyings in {exchange}/{instrument}: {underlyings}")
                    
                    for underlying in underlyings:
                        if underlying == 'BANKNIFTY':
                            underlying_start = time.time()
                            logger.info(f"Processing Options Underlying: {underlying}")
                            underlying_dir = f"{instrument_path}/{underlying}"
                            
                            expiries = [d for d in os.listdir(underlying_dir) if os.path.isdir(f"{underlying_dir}/{d}")]
                            logger.info(f"Found {len(expiries)} expiries for {underlying}: {expiries}")
                            
                            for expiry in expiries:
                                expiry_start = time.time()
                                logger.info(f"Processing Expiry: {expiry} for {underlying}")
                                expiry_dir = f"{underlying_dir}/{expiry}"
                                
                                strikes = [d for d in os.listdir(expiry_dir) if os.path.isdir(f"{expiry_dir}/{d}")]
                                logger.info(f"Found {len(strikes)} strikes for {underlying}/{expiry}")
                                
                                for strike in strikes:
                                    strike_start = time.time()
                                    logger.info(f"Processing Strike: {strike}")
                                    strike_dir = f"{expiry_dir}/{strike}"
                                    
                                    parquet_files = [f for f in os.listdir(strike_dir) if f.endswith(".parquet")]
                                    logger.info(f"Found {len(parquet_files)} option files for strike {strike}")
                                    
                                    for file in parquet_files:
                                        parquet_path = f"{strike_dir}/{file}"
                                        total_files += 1
                                        file_size = get_file_size(parquet_path)
                                        total_size += file_size
                                        
                                        file_parts = file.split("_")
                                        option_type = file_parts[-1].replace(".parquet", "")
                                        option_type = "call" if option_type == "CE" else "put" if option_type == "PE" else option_type
                                        
                                        table_name = f"market_data.{exchange}_{instrument}_{underlying}_{expiry}_{strike}_{option_type}"
                                        
                                        file_batch.append((parquet_path, table_name, "Option", file_size))
                                        total_files += 1
                                        
                                        if len(file_batch) >= batch_size:
                                            results = process_parquet_file_batch(file_batch)
                                            successful_files += results['successful']
                                            failed_files += results['failed']
                                            file_batch = []
                                            checkpoint_counter += 1
                                            
                                            if checkpoint_counter % 5 == 0:
                                                conn.execute("CHECKPOINT")
                                                logger.info(f"Checkpoint completed after {checkpoint_counter} batches")
                                    
                                    logger.info(f"Strike {strike} completed - Duration: {time.time() - strike_start:.2f}s")
                                
                                logger.info(f"Expiry {expiry} completed - Duration: {time.time() - expiry_start:.2f}s")
                            
                            logger.info(f"Underlying {underlying} completed - Duration: {time.time() - underlying_start:.2f}s")
                
                # elif instrument == "Futures":
                #     underlyings = [d for d in os.listdir(instrument_path) if os.path.isdir(f"{instrument_path}/{d}")]
                #     logger.info(f"Found {len(underlyings)} futures underlyings in {exchange}/{instrument}: {underlyings}")
                    
                #     for underlying in underlyings:
                #         underlying_start = time.time()
                #         logger.info(f"Processing Futures Underlying: {underlying}")
                #         underlying_dir = f"{instrument_path}/{underlying}"
                        
                #         parquet_files = [f for f in os.listdir(underlying_dir) if f.endswith(".parquet")]
                #         logger.info(f"Found {len(parquet_files)} futures files for {underlying}")
                        
                #         for file in parquet_files:
                #             parquet_path = f"{underlying_dir}/{file}"
                #             total_files += 1
                #             file_size = get_file_size(parquet_path)
                #             total_size += file_size
                            
                #             table_name = f"market_data.{exchange}_{instrument}_{underlying}"
                            
                #             if process_parquet_file(conn, parquet_path, table_name, "Future"):
                #                 successful_files += 1
                #             else:
                #                 failed_files += 1
                        
                #         logger.info(f"Futures underlying {underlying} completed - Duration: {time.time() - underlying_start:.2f}s")
                
                logger.info(f"Instrument {instrument} completed - Duration: {time.time() - instrument_start:.2f}s")
            
            logger.info(f"Exchange {exchange} completed - Duration: {time.time() - exchange_start:.2f}s")
    
    if file_batch:
        results = process_parquet_file_batch(file_batch)
        successful_files += results['successful']
        failed_files += results['failed']
    
    conn.execute("CHECKPOINT")
    
    migration_duration = time.time() - start_time
    logger.info(f"=== MIGRATION SUMMARY ===")
    logger.info(f"Total files processed: {total_files}")
    logger.info(f"Successful migrations: {successful_files}")
    logger.info(f"Failed migrations: {failed_files}")
    logger.info(f"Success rate: {(successful_files/total_files*100):.1f}%" if total_files > 0 else "No files processed")
    logger.info(f"Total data size: {format_size(total_size)}")
    logger.info(f"Total migration time: {migration_duration:.2f}s")
    logger.info(f"Average throughput: {format_size(total_size/migration_duration)}/s" if migration_duration > 0 else "N/A")
    
    return conn

disk_conn = get_duckdb_connection()