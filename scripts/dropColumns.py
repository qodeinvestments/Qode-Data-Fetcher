import duckdb
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_PATH = "/mnt/disk2/qode_edw.db"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('column_removal.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

def get_stock_tables(conn):
    try:
        query = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%Stocks_%'
        ORDER BY table_schema, table_name
        """
        result = conn.execute(query).fetchall()
        
        tables = []
        for schema, table_name in result:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            tables.append(full_table_name)
        
        logger.info(f"Found {len(tables)} tables containing 'Stocks_'")
        return tables
    
    except Exception as e:
        logger.error(f"Error getting stock tables: {str(e)}")
        return []

def get_table_columns(conn, table_name):
    try:
        query = f"DESCRIBE {table_name}"
        result = conn.execute(query).fetchall()
        columns = [row[0] for row in result]
        return columns
    except Exception as e:
        logger.error(f"Error getting columns for table {table_name}: {str(e)}")
        return []

def get_table_indexes(conn, table_name):
    try:
        schema_name = table_name.split('.')[0] if '.' in table_name else 'main'
        table_only = table_name.split('.')[-1]
        
        query = f"""
        SELECT index_name 
        FROM duckdb_indexes() 
        WHERE table_name = '{table_only}' 
        AND schema_name = '{schema_name}'
        """
        
        result = conn.execute(query).fetchall()
        return [row[0] for row in result]
    except Exception as e:
        logger.warning(f"Could not get indexes for {table_name}: {str(e)}")
        return []

def drop_all_table_indexes(conn, table_name):
    try:
        indexes = get_table_indexes(conn, table_name)
        dropped_count = 0
        
        for index_name in indexes:
            try:
                drop_idx_query = f"DROP INDEX IF EXISTS {index_name}"
                conn.execute(drop_idx_query)
                dropped_count += 1
                logger.info(f"Dropped index {index_name} from {table_name}")
            except Exception as e:
                logger.warning(f"Failed to drop index {index_name}: {str(e)}")
        
        schema_name = table_name.split('.')[0] if '.' in table_name else 'main'
        table_only = table_name.split('.')[-1]
        bse_symbol = table_only.replace('BSE_Stocks_', '')
        
        additional_indexes = [
            f"{schema_name}.idx_{bse_symbol}_accord_code",
            f"idx_{bse_symbol}_accord_code", 
            f"{schema_name}.idx_{bse_symbol}_timestamp",
            f"idx_{bse_symbol}_timestamp",
            f"idx_{table_only}_accord_code",
            f"idx_{table_only}_timestamp"
        ]
        
        for idx_name in additional_indexes:
            try:
                drop_idx_query = f"DROP INDEX IF EXISTS {idx_name}"
                conn.execute(drop_idx_query)
                dropped_count += 1
            except:
                pass
        
        return dropped_count > 0
    except Exception as e:
        logger.warning(f"Error dropping indexes for {table_name}: {str(e)}")
        return False
    
def drop_column_indexes(conn, table_name, column_name):
    try:
        table_only = table_name.split('.')[-1]
        bse_symbol = table_only.replace('BSE_Stocks_', '')
        
        potential_indexes = [
            f"idx_{bse_symbol}_accord_code",
            f"idx_{table_only}_accord_code",
            f"idx_{bse_symbol}_{column_name.lower().replace(' ', '_')}",
            f"idx_{table_only}_{column_name.lower().replace(' ', '_')}"
        ]
        
        success_count = 0
        for idx_name in potential_indexes:
            try:
                drop_idx_query = f"DROP INDEX IF EXISTS {idx_name}"
                conn.execute(drop_idx_query)
                success_count += 1
            except:
                pass
        
        return True
    except Exception as e:
        logger.warning(f"Error dropping indexes for column {column_name} in {table_name}: {str(e)}")
        return False


def remove_columns_from_table(conn, table_name):
    columns_to_remove = [
        'Accord Code',
        'Company_Name', 
        'NSE_Symbol',
        'BSE_Scrip_Name',
        'BSE_Code',
        'ISIN_No'
    ]
    
    try:
        current_columns = get_table_columns(conn, table_name)
        if not current_columns:
            logger.warning(f"Could not get columns for table {table_name}")
            return False
        
        existing_columns_to_remove = []
        for col in columns_to_remove:
            if col in current_columns:
                existing_columns_to_remove.append(col)
        
        if not existing_columns_to_remove:
            logger.info(f"No target columns found in table {table_name}")
            return True
        
        logger.info(f"Removing columns {existing_columns_to_remove} from {table_name}")
        
        success_count = 0
        for column in existing_columns_to_remove:
            drop_column_indexes(conn, table_name, column)
            
            drop_query = f'ALTER TABLE {table_name} DROP COLUMN "{column}"'
            if execute_with_timing(conn, drop_query, f"Dropping column {column} from {table_name}"):
                success_count += 1
        
        if success_count == len(existing_columns_to_remove):
            logger.info(f"Successfully removed {success_count} columns from {table_name}")
            return True
        else:
            logger.warning(f"Only removed {success_count}/{len(existing_columns_to_remove)} columns from {table_name}")
            return False
        
    except Exception as e:
        logger.error(f"Error removing columns from table {table_name}: {str(e)}")
        return False

def process_table_batch(table_batch):
    conn = duckdb.connect(DB_PATH)
    conn.execute("SET memory_limit='200GB'")
    conn.execute("SET threads=32")
    conn.execute("SET max_memory='200GB'")
    conn.execute("SET temp_directory='/tmp'")
    
    results = {'successful': 0, 'failed': 0}
    
    try:
        conn.execute("BEGIN TRANSACTION")
        
        for table_name in table_batch:
            if remove_columns_from_table(conn, table_name):
                results['successful'] += 1
            else:
                results['failed'] += 1
        
        conn.execute("COMMIT")
        
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Batch processing failed: {str(e)}")
        results['failed'] += len(table_batch)
    
    finally:
        conn.close()
    
    return results

def remove_columns_from_stock_tables():
    start_time = time.time()
    logger.info("Starting column removal from stock tables...")
    
    conn = duckdb.connect(DB_PATH)
    conn.execute("SET memory_limit='250GB'")
    conn.execute("SET threads=32")
    conn.execute("SET max_memory='250GB'")
    
    try:
        stock_tables = get_stock_tables(conn)
        
        if not stock_tables:
            logger.warning("No stock tables found")
            return
        
        logger.info(f"Processing {len(stock_tables)} stock tables...")
        
        batch_size = 100
        max_workers = 32
        successful_tables = 0
        failed_tables = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            for i in range(0, len(stock_tables), batch_size):
                batch = stock_tables[i:i + batch_size]
                future = executor.submit(process_table_batch, batch)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    results = future.result()
                    successful_tables += results['successful']
                    failed_tables += results['failed']
                    
                    logger.info(f"Batch completed. Running totals - Success: {successful_tables}, Failed: {failed_tables}")
                    
                except Exception as e:
                    logger.error(f"Future execution error: {str(e)}")
                    failed_tables += batch_size
        
        conn.execute("CHECKPOINT")
        
        duration = time.time() - start_time
        
        logger.info("=== COLUMN REMOVAL SUMMARY ===")
        logger.info(f"Total tables processed: {successful_tables + failed_tables}")
        logger.info(f"Successful modifications: {successful_tables}")
        logger.info(f"Failed modifications: {failed_tables}")
        logger.info(f"Success rate: {(successful_tables/(successful_tables + failed_tables)*100):.1f}%" if (successful_tables + failed_tables) > 0 else "No tables processed")
        logger.info(f"Total processing time: {duration:.2f}s")
        logger.info(f"Average time per table: {duration/(successful_tables + failed_tables):.2f}s" if (successful_tables + failed_tables) > 0 else "N/A")
        
        logger.info("=== SAMPLE OF MODIFIED TABLES ===")
        sample_tables = stock_tables[:5]
        for table_name in sample_tables:
            try:
                columns = get_table_columns(conn, table_name)
                logger.info(f"Table {table_name} now has {len(columns)} columns")
            except Exception as e:
                logger.error(f"Could not verify table {table_name}: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error during column removal process: {str(e)}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    import os
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found: {DB_PATH}")
        exit(1)
    
    remove_columns_from_stock_tables()
    
    logger.info("Column removal process completed!")