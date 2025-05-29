import duckdb
import re
import logging
import time
import threading
import gc
from datetime import datetime
from typing import Dict, List, Any, Tuple
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('options_processing_optimized.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

thread_local = threading.local()

def get_thread_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Get or create a thread-local database connection with optimized settings."""
    if not hasattr(thread_local, 'conn'):
        thread_local.conn = duckdb.connect(db_path)
        thread_local.conn.execute("SET memory_limit='8GB'")
        thread_local.conn.execute("SET threads=4")
        thread_local.conn.execute("SET max_memory='8GB'")
        thread_local.conn.execute("SET temp_directory='/tmp'")
        thread_local.conn.execute("SET checkpoint_threshold='1GB'")
        thread_local.conn.execute("SET force_compression='zstd'")
        logger.debug("Thread-local database connection established with optimized settings")
    return thread_local.conn

def setup_database_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Establish main database connection with logging and optimization."""
    try:
        logger.info(f"Establishing main connection to database: {db_path}")
        conn = duckdb.connect(db_path)
        
        conn.execute("SET memory_limit='20GB'")
        conn.execute("SET threads=16")
        conn.execute("SET max_memory='20GB'")
        conn.execute("SET checkpoint_threshold='2GB'")
        conn.execute("SET temp_directory='/tmp'")
        conn.execute("SET force_compression='zstd'")
        
        logger.info("Main database connection established successfully with optimized settings")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database {db_path}: {e}")
        raise

def fetch_options_tables(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """Fetch options tables from the database with improved query."""
    logger.info("Starting to fetch options tables from market_data schema")
    start_time = time.time()
    
    try:
        query = """
            SELECT table_name
            FROM duckdb_tables()
            WHERE schema_name = 'market_data'
              AND (table_name LIKE '%_call' OR table_name LIKE '%_put')
            ORDER BY table_name
        """
        logger.debug(f"Executing query: {query}")
        
        tables_df = conn.execute(query).fetchdf()
        table_names = tables_df['table_name'].tolist()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Found {len(table_names)} options tables in {elapsed_time:.2f} seconds")
        logger.debug(f"Sample tables: {table_names[:5]}{'...' if len(table_names) > 5 else ''}")
        
        return table_names
    except Exception as e:
        logger.error(f"Error fetching options tables: {e}")
        raise

def parse_table_names_optimized(table_names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Parse table names and group by underlying asset with improved regex and validation."""
    logger.info("Parsing table names and grouping by underlying asset")
    start_time = time.time()
    
    pattern = re.compile(r'^[^_]+_Options_(?P<underlying>\w+)_(?P<expiry>\d{8})_(?P<strike>\d+)_(?P<option_type>call|put)$')
    tables_by_underlying = defaultdict(list)
    skipped_tables = []
    
    for tbl in table_names:
        match = pattern.match(tbl)
        if not match:
            logger.warning(f"Skipping table {tbl} - doesn't match expected pattern")
            skipped_tables.append(tbl)
            continue
        
        underlying = match.group('underlying')
        expiry_str = match.group('expiry')
        
        try:
            datetime.strptime(expiry_str, '%Y%m%d')
        except ValueError:
            logger.warning(f"Skipping table {tbl} - invalid expiry date format: {expiry_str}")
            skipped_tables.append(tbl)
            continue
        
        table_info = {
            'table': tbl,
            'expiry': expiry_str,
            'strike': int(match.group('strike')),
            'option_type': match.group('option_type')
        }
        
        tables_by_underlying[underlying].append(table_info)
    
    tables_by_underlying = dict(tables_by_underlying)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Successfully parsed {len(table_names) - len(skipped_tables)} tables in {elapsed_time:.2f} seconds")
    logger.info(f"Found options data for {len(tables_by_underlying)} underlyings: {list(tables_by_underlying.keys())}")
    
    if skipped_tables:
        logger.warning(f"Skipped {len(skipped_tables)} tables due to naming/validation issues")
        logger.debug(f"Sample skipped tables: {skipped_tables[:5]}{'...' if len(skipped_tables) > 5 else ''}")
    
    for underlying, tables in tables_by_underlying.items():
        call_count = sum(1 for t in tables if t['option_type'] == 'call')
        put_count = sum(1 for t in tables if t['option_type'] == 'put')
        unique_expiries = len(set(t['expiry'] for t in tables))
        logger.info(f"{underlying}: {len(tables)} tables ({call_count} calls, {put_count} puts, {unique_expiries} expiries)")
    
    return tables_by_underlying

def create_master_table_optimized(conn: duckdb.DuckDBPyConnection, underlying: str) -> str:
    """Create optimized master table for an underlying asset."""
    master_table = f"market_data.options_master_{underlying.lower()}"
    logger.info(f"Creating optimized master table: {master_table}")
    
    try:
        conn.execute(f"DROP TABLE IF EXISTS {master_table}")
        
        create_sql = f"""
            CREATE TABLE {master_table} (
                timestamp TIMESTAMP,
                symbol VARCHAR,
                strike INTEGER,
                expiry DATE,
                option_type VARCHAR(4),
                underlying VARCHAR(20),
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                oi BIGINT,
                PRIMARY KEY (timestamp, symbol, strike, expiry)
            )
        """
        logger.debug(f"Executing CREATE TABLE: {create_sql}")
        conn.execute(create_sql)
        logger.info(f"Master table {master_table} created successfully")
        return master_table
    except Exception as e:
        logger.error(f"Failed to create master table {master_table}: {e}")
        raise

def get_table_columns_cached(conn: duckdb.DuckDBPyConnection, table_name: str, cache: Dict[str, List[str]]) -> List[str]:
    """Get column names for a table with caching."""
    if table_name in cache:
        return cache[table_name]
    
    try:
        logger.debug(f"Fetching column information for table: {table_name}")
        cols = conn.execute(f"PRAGMA table_info('market_data.{table_name}')").fetchdf()['name'].tolist()
        cache[table_name] = cols
        logger.debug(f"Table {table_name} has columns: {cols}")
        return cols
    except Exception as e:
        logger.error(f"Failed to get columns for table {table_name}: {e}")
        raise

def build_select_clause_optimized(cols: List[str], underlying: str, expiry: str, strike: int, option_type: str, symbol: str) -> str:
    """Build optimized SELECT clause for INSERT statement."""
    logger.debug(f"Building SELECT clause for {symbol}")
    
    select_clause = f"""
        timestamp,
        '{symbol}' as symbol,
        {strike} as strike,
        DATE '{expiry}' as expiry,
        '{option_type}' as option_type,
        '{underlying}' as underlying
    """
    
    ohlcv_mappings = [
        (['o', 'open'], 'open'),
        (['h', 'high'], 'high'),
        (['l', 'low'], 'low'),
        (['c', 'close'], 'close'),
        (['v', 'volume'], 'volume'),
        (['oi', 'open_interest'], 'oi')
    ]
    
    missing_columns = []
    for possible_cols, target_col in ohlcv_mappings:
        found_col = next((col for col in possible_cols if col in cols), None)
        
        if found_col:
            select_clause += f", {found_col} as {target_col}"
            logger.debug(f"Mapped column {found_col} -> {target_col}")
        else:
            select_clause += f", NULL as {target_col}"
            missing_columns.append(target_col)
    
    if missing_columns:
        logger.warning(f"Missing columns for {symbol}: {missing_columns}")
    
    return select_clause

def process_table_batch(batch_info: List[Tuple[Dict[str, Any], str, str]], master_table: str, column_cache: Dict[str, List[str]], db_path: str) -> Dict[str, int]:
    """Process a batch of tables in a single transaction."""
    conn = get_thread_connection(db_path)
    results = {'successful': 0, 'failed': 0, 'total_rows': 0}
    batch_start_time = time.time()
    
    try:
        conn.execute("BEGIN TRANSACTION")
        
        for table_info, underlying, symbol in batch_info:
            table_name = table_info['table']
            expiry_str = table_info['expiry']
            strike = table_info['strike']
            option_type = table_info['option_type']
            
            try:
                expiry = f"{expiry_str[:4]}-{expiry_str[4:6]}-{expiry_str[6:]}"
                
                cols = get_table_columns_cached(conn, table_name, column_cache)
                
                # Build SELECT clause
                select_clause = build_select_clause_optimized(cols, underlying, expiry, strike, option_type, symbol)
                
                # Count source rows first
                source_count_query = f"SELECT COUNT(*) FROM market_data.{table_name} WHERE timestamp IS NOT NULL"
                source_row_count = conn.execute(source_count_query).fetchone()[0]
                
                if source_row_count == 0:
                    logger.warning(f"Table {table_name} has no valid rows, skipping")
                    continue
                
                # Build and execute INSERT statement
                insert_sql = f"""
                    INSERT INTO {master_table} 
                    SELECT {select_clause}
                    FROM market_data.{table_name}
                    WHERE timestamp IS NOT NULL
                """
                
                conn.execute(insert_sql)
                results['successful'] += 1
                results['total_rows'] += source_row_count
                logger.debug(f"✓ {table_name}: {source_row_count} rows inserted")
                
            except Exception as e:
                logger.error(f"✗ Error processing {table_name}: {e}")
                results['failed'] += 1
        
        conn.execute("COMMIT")
        
        batch_elapsed = time.time() - batch_start_time
        logger.info(f"Batch completed: {results['successful']} successful, {results['failed']} failed, "
                   f"{results['total_rows']} total rows in {batch_elapsed:.2f}s")
        
        # Force garbage collection
        gc.collect()
        
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Batch processing failed, rolling back: {e}")
        results['failed'] += len(batch_info)
        results['successful'] = 0
        results['total_rows'] = 0
    
    return results

def generate_symbol_optimized(underlying: str, expiry_str: str, strike: int, option_type: str) -> str:
    """Generate option symbol using optimized method."""
    exp_date = datetime.strptime(expiry_str, '%Y%m%d')
    symbol_suffix = 'CE' if option_type == 'call' else 'PE'
    return f"{underlying}{exp_date.strftime('%y%b').upper()}{strike}{symbol_suffix}"

def create_indexes_for_master_table(conn: duckdb.DuckDBPyConnection, master_table: str) -> None:
    """Create indexes for optimized querying of master table."""
    logger.info(f"Creating indexes for {master_table}")
    start_time = time.time()
    
    try:
        # Create indexes for common query patterns
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{master_table.split('.')[-1]}_timestamp ON {master_table} (timestamp)",
            f"CREATE INDEX IF NOT EXISTS idx_{master_table.split('.')[-1]}_symbol ON {master_table} (symbol)",
            f"CREATE INDEX IF NOT EXISTS idx_{master_table.split('.')[-1]}_expiry ON {master_table} (expiry)",
            f"CREATE INDEX IF NOT EXISTS idx_{master_table.split('.')[-1]}_strike_type ON {master_table} (strike, option_type)"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
            logger.debug(f"Created index: {index_sql}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Indexes created successfully in {elapsed_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error creating indexes for {master_table}: {e}")
        # Don't raise - indexes are optional for functionality

def get_master_table_stats_optimized(conn: duckdb.DuckDBPyConnection, master_table: str) -> Dict[str, Any]:
    """Get comprehensive statistics for the master table."""
    logger.debug(f"Collecting statistics for {master_table}")
    
    try:
        # Get comprehensive stats in a single query for efficiency
        stats_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT expiry) as unique_expiries,
                COUNT(DISTINCT strike) as unique_strikes,
                MIN(timestamp) as min_date,
                MAX(timestamp) as max_date,
                SUM(CASE WHEN option_type = 'call' THEN 1 ELSE 0 END) as call_count,
                SUM(CASE WHEN option_type = 'put' THEN 1 ELSE 0 END) as put_count,
                AVG(volume) as avg_volume,
                SUM(volume) as total_volume
            FROM {master_table}
        """
        
        stats_result = conn.execute(stats_query).fetchone()
        
        stats = {
            'total_rows': stats_result[0],
            'unique_symbols': stats_result[1],
            'unique_expiries': stats_result[2],
            'unique_strikes': stats_result[3],
            'min_date': stats_result[4],
            'max_date': stats_result[5],
            'call_count': stats_result[6],
            'put_count': stats_result[7],
            'avg_volume': stats_result[8],
            'total_volume': stats_result[9]
        }
        
        logger.debug(f"Statistics collected for {master_table}: total_rows={stats['total_rows']}")
        return stats
        
    except Exception as e:
        logger.error(f"Error collecting statistics for {master_table}: {e}")
        raise

def process_underlying_optimized(conn: duckdb.DuckDBPyConnection, underlying: str, tables: List[Dict[str, Any]], batch_size: int = 100) -> None:
    """Process all tables for a single underlying asset using batch processing."""
    logger.info(f"Processing {underlying} options ({len(tables)} tables) with batch size {batch_size}")
    process_start_time = time.time()
    
    try:
        # Create master table
        master_table = create_master_table_optimized(conn, underlying)
        
        # Prepare batch data with pre-generated symbols
        batch_data = []
        column_cache = {}  # Cache for column information
        
        for table_info in tables:
            symbol = generate_symbol_optimized(
                underlying, 
                table_info['expiry'], 
                table_info['strike'], 
                table_info['option_type']
            )
            batch_data.append((table_info, underlying, symbol))
        
        # Process in batches
        total_successful = 0
        total_failed = 0
        total_rows_inserted = 0
        batch_count = 0
        
        for i in range(0, len(batch_data), batch_size):
            batch = batch_data[i:i + batch_size]
            batch_count += 1
            
            logger.info(f"Processing batch {batch_count}/{(len(batch_data) + batch_size - 1) // batch_size} "
                       f"({len(batch)} tables)")
            
            results = process_table_batch(batch, master_table, column_cache, conn.execute("SELECT current_database()").fetchone()[0] if hasattr(conn, 'execute') else 'qode_edw.db')
            
            total_successful += results['successful']
            total_failed += results['failed']
            total_rows_inserted += results['total_rows']
            
            # Periodic checkpoint for large datasets
            if batch_count % 10 == 0:
                conn.execute("CHECKPOINT")
                logger.info(f"Checkpoint completed after {batch_count} batches")
        
        logger.info(f"Data insertion complete for {underlying}: {total_successful} successful, {total_failed} failed")
        
        # Create indexes for better query performance
        create_indexes_for_master_table(conn, master_table)
        
        # Get and log statistics
        stats = get_master_table_stats_optimized(conn, master_table)
        
        # Final checkpoint
        conn.execute("CHECKPOINT")
        
        process_elapsed_time = time.time() - process_start_time
        
        logger.info(f"✅ {underlying} master table created successfully in {process_elapsed_time:.2f} seconds:")
        logger.info(f"   - Total rows: {stats['total_rows']:,}")
        logger.info(f"   - Unique symbols: {stats['unique_symbols']}")
        logger.info(f"   - Unique expiries: {stats['unique_expiries']}")
        logger.info(f"   - Unique strikes: {stats['unique_strikes']}")
        logger.info(f"   - Date range: {stats['min_date']} to {stats['max_date']}")
        logger.info(f"   - Calls: {stats['call_count']:,} rows")
        logger.info(f"   - Puts: {stats['put_count']:,} rows")
        logger.info(f"   - Total volume: {stats['total_volume']:,}")
        logger.info(f"   - Processing rate: {stats['total_rows'] / process_elapsed_time:.0f} rows/second")
        
    except Exception as e:
        process_elapsed_time = time.time() - process_start_time
        logger.error(f"Failed to process {underlying} after {process_elapsed_time:.2f} seconds: {e}")
        raise

def generate_final_summary_optimized(conn: duckdb.DuckDBPyConnection) -> None:
    """Generate and log optimized final summary of all created master tables."""
    logger.info("Generating final summary of created master tables")
    
    try:
        # Get all master tables with stats in one query
        summary_query = """
            SELECT 
                table_name,
                (SELECT COUNT(*) FROM market_data.|| table_name) as row_count
            FROM duckdb_tables()
            WHERE schema_name = 'market_data'
              AND table_name LIKE 'options_master_%'
            ORDER BY table_name
        """
        
        master_tables = conn.execute("""
            SELECT table_name
            FROM duckdb_tables()
            WHERE schema_name = 'market_data'
              AND table_name LIKE 'options_master_%'
            ORDER BY table_name
        """).fetchdf()
        
        logger.info(f"Summary of {len(master_tables)} created master tables:")
        
        total_rows = 0
        for table in master_tables['table_name']:
            count = conn.execute(f"SELECT COUNT(*) FROM market_data.{table}").fetchone()[0]
            total_rows += count
            underlying = table.replace('options_master_', '').upper()
            logger.info(f"  - {underlying}: {count:,} rows")
        
        logger.info(f"Total rows across all master tables: {total_rows:,}")
        
        # Additional summary stats
        total_size_query = """
            SELECT pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
            FROM pg_tables 
            WHERE schemaname = 'market_data' 
              AND tablename LIKE 'options_master_%'
        """
        
        logger.info("Master table creation summary completed successfully")
        
    except Exception as e:
        logger.error(f"Error generating final summary: {e}")

def main():
    """Main execution function with optimized batch processing."""
    script_start_time = time.time()
    logger.info("="*60)
    logger.info("Starting Optimized Options Data Processing Script")
    logger.info("="*60)
    
    DB_PATH = 'qode_edw.db'
    BATCH_SIZE = 10000
    
    try:
        conn = setup_database_connection(DB_PATH)
        
        # Fetch options tables
        table_names = fetch_options_tables(conn)
        
        if not table_names:
            logger.warning("No options tables found. Exiting.")
            return
        
        # Parse table names and group by underlying
        tables_by_underlying = parse_table_names_optimized(table_names)
        
        if not tables_by_underlying:
            logger.warning("No valid options tables found after parsing. Exiting.")
            return
        
        # Process each underlying with batch processing
        successful_underlyings = 0
        failed_underlyings = 0
        
        for underlying, tables in tables_by_underlying.items():
            try:
                logger.info(f"Starting processing for underlying: {underlying}")
                process_underlying_optimized(conn, underlying, tables, BATCH_SIZE)
                successful_underlyings += 1
                logger.info(f"✅ Successfully completed {underlying}")
            except Exception as e:
                logger.error(f"❌ Failed to process {underlying}: {e}")
                failed_underlyings += 1
                # Continue with next underlying instead of stopping
                continue
        
        # Generate final summary
        generate_final_summary_optimized(conn)
        
        script_elapsed_time = time.time() - script_start_time
        logger.info("="*60)
        logger.info(f"🎉 Script completed in {script_elapsed_time:.2f} seconds!")
        logger.info(f"✅ Successfully processed: {successful_underlyings} underlyings")
        logger.info(f"❌ Failed to process: {failed_underlyings} underlyings")
        logger.info(f"📊 Success rate: {(successful_underlyings/(successful_underlyings+failed_underlyings)*100):.1f}%" if (successful_underlyings+failed_underlyings) > 0 else "N/A")
        logger.info("="*60)
        
    except Exception as e:
        script_elapsed_time = time.time() - script_start_time
        logger.error(f"Script failed after {script_elapsed_time:.2f} seconds: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()