import duckdb
import logging
import time
import os

DB_PATH = "/mnt/disk2/qode_edw.db"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bse_tables_cleanup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def execute_with_timing(conn, query, description):
    """Execute query with timing and logging - borrowed from reference script"""
    start_time = time.time()
    try:
        result = conn.execute(query)
        duration = time.time() - start_time
        logger.info(f"{description} - SUCCESS - Duration: {duration:.2f}s")
        return result
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"{description} - FAILED - Duration: {duration:.2f}s - Error: {str(e)}")
        return None

def cleanup_bse_stock_tables():
    """Clean up all BSE stock tables from the database"""
    start_time = time.time()
    logger.info("Starting BSE stock tables cleanup...")
    
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found: {DB_PATH}")
        return
    
    try:
        conn = duckdb.connect(DB_PATH)
        conn.execute("SET memory_limit='64GB'")
        conn.execute("SET threads=8")
        conn.execute("SET max_memory='64GB'")
        conn.execute("SET temp_directory='/tmp'")

        logger.info("Fetching list of BSE stock tables...")
        result = execute_with_timing(
            conn, 
            """
            SELECT table_name, table_schema
            FROM information_schema.tables 
            WHERE table_name LIKE 'BSE_Stocks_%'
            ORDER BY table_name;
            """,
            "Fetching BSE stock tables list"
        )
        
        if result is None:
            logger.error("Failed to fetch table list")
            return
        
        tables = result.fetchall()
        logger.info(f"Found {len(tables)} BSE stock tables to cleanup")
        
        if len(tables) == 0:
            logger.info("No BSE stock tables found to cleanup")
            return
        
        logger.info("=== TABLES TO BE DROPPED ===")
        for table_name, schema_name in tables:
            full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
            logger.info(f"Will drop: {full_table_name}")
        
        print(f"\nFound {len(tables)} BSE stock tables.")
        confirmation = input("Do you want to proceed with dropping all these tables? (yes/no): ").lower().strip()
        if confirmation not in ['yes', 'y']:
            logger.info("Cleanup cancelled by user")
            return
        
        successful_drops = 0
        failed_drops = 0
        
        conn.execute("BEGIN TRANSACTION")
        
        try:
            for table_name, schema_name in tables:
                try:
                    full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
                    
                    drop_query = f"DROP TABLE IF EXISTS {full_table_name};"
                    
                    if execute_with_timing(conn, drop_query, f"Dropping table {full_table_name}"):
                        successful_drops += 1
                        logger.info(f"Successfully dropped: {full_table_name}")
                    else:
                        failed_drops += 1
                        logger.error(f"Failed to drop: {full_table_name}")
                        
                except Exception as e:
                    failed_drops += 1
                    logger.error(f"Error dropping table {table_name}: {str(e)}")
            
            conn.execute("COMMIT")
            logger.info("Transaction committed successfully")
            
            execute_with_timing(conn, "CHECKPOINT;", "Running database checkpoint")
            
        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Transaction rolled back due to error: {str(e)}")
        
        cleanup_duration = time.time() - start_time
        logger.info("=== CLEANUP SUMMARY ===")
        logger.info(f"Total tables found: {len(tables)}")
        logger.info(f"Successfully dropped: {successful_drops}")
        logger.info(f"Failed to drop: {failed_drops}")
        logger.info(f"Success rate: {(successful_drops/len(tables)*100):.1f}%" if len(tables) > 0 else "N/A")
        logger.info(f"Total cleanup time: {cleanup_duration:.2f}s")
        
        verify_result = execute_with_timing(
            conn,
            """
            SELECT COUNT(*) as remaining_tables
            FROM information_schema.tables 
            WHERE table_name LIKE 'BSE_Stocks_%';
            """,
            "Verifying cleanup completion"
        )
        
        if verify_result:
            remaining_count = verify_result.fetchone()[0]
            logger.info(f"Remaining BSE stock tables after cleanup: {remaining_count}")
            
            if remaining_count == 0:
                logger.info("✅ All BSE stock tables successfully cleaned up!")
            else:
                logger.warning(f"⚠️  {remaining_count} BSE stock tables still remain")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
    
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

def list_bse_tables_only():
    """Just list the BSE tables without dropping them"""
    logger.info("Listing BSE stock tables...")
    
    try:
        conn = duckdb.connect(DB_PATH)
        
        result = conn.execute("""
            SELECT table_name, table_schema
            FROM information_schema.tables 
            WHERE table_name LIKE 'BSE_Stocks_%'
            ORDER BY table_name;
        """).fetchall()
        
        logger.info(f"Found {len(result)} BSE stock tables:")
        for table_name, schema_name in result:
            full_name = f"{schema_name}.{table_name}" if schema_name else table_name
            print(full_name)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")

if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found: {DB_PATH}")
        exit(1)
        
    cleanup_bse_stock_tables()
    
    logger.info("Script execution completed!")