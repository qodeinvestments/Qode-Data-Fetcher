from truedata_ws.websocket.TD import TD
import requests
import logging
import datetime
import os
import pandas as pd
import duckdb
import time
from pathlib import Path
from dotenv import load_dotenv
import gc
import threading
from typing import Dict, Optional

load_dotenv()

td_login_id = os.getenv("TRUEDATA_LOGIN_ID")
td_password = os.getenv("TRUEDATA_LOGIN_PWD")

if not td_login_id or not td_password:
    raise ValueError("TrueData credentials not found in environment variables")

COLD_STORAGE_PATH = "cold_storage"
DB_PATH = "qode_edw.db"
LOG_FILE = f"daily_update_{datetime.date.today().strftime('%Y%m%d')}.log"

UNDERLYINGS = ['NIFTY', 'FINNIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
EXCHANGES = {
    'NIFTY': 'NSE',
    'FINNIFTY': 'NSE', 
    'BANKNIFTY': 'NSE',
    'MIDCPNIFTY': 'NSE',
    'SENSEX': 'BSE',
    'BANKEX': 'BSE'
}

INDEX_SYMBOLS = {
    'NIFTY': 'NIFTYSPOT',
    'FINNIFTY': 'FINNIFTYSPOT',
    'BANKNIFTY': 'BANKNIFTYSPOT',
    'MIDCPNIFTY': 'MIDCPNIFTYSPOT',
    'SENSEX': 'SENSEXSPOT',
    'BANKEX': 'BANKEXSPOT'
}

FUTURES_SYMBOLS = {
    'NIFTY': ['NIFTY-I', 'NIFTY-II', 'NIFTY-III'],
    'FINNIFTY': ['FINNIFTY-I'],
    'BANKNIFTY': ['BANKNIFTY-I', 'BANKNIFTY-II', 'BANKNIFTY-III'],
    'MIDCPNIFTY': ['MIDCPNIFTY-I'],
    'SENSEX': ['SENSEX-I', 'SENSEX-II', 'SENSEX-III'],
    'BANKEX': ['BANKEX-I', 'BANKEX-II', 'BANKEX-III']
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

thread_local = threading.local()

class DailyDataUpdater:
    def __init__(self):
        self.td_obj = TD(td_login_id, td_password, live_port=None,
                        log_level=logging.WARNING, log_format="%(message)s")
        self.today = datetime.date.today()
        self.yesterday = self.today - datetime.timedelta(days=1)
        
        Path(COLD_STORAGE_PATH).mkdir(exist_ok=True)
        for exchange in set(EXCHANGES.values()):
            for instrument_type in ['Index', 'Futures', 'Options']:
                Path(f"{COLD_STORAGE_PATH}/{exchange}/{instrument_type}").mkdir(parents=True, exist_ok=True)
    
    def get_thread_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a thread-local database connection"""
        if not hasattr(thread_local, 'conn'):
            thread_local.conn = duckdb.connect(DB_PATH)
            thread_local.conn.execute("SET memory_limit='8GB'")
            thread_local.conn.execute("SET threads=4")
            thread_local.conn.execute("SET max_memory='8GB'")
            thread_local.conn.execute("SET temp_directory='/tmp'")
            thread_local.conn.execute("CREATE SCHEMA IF NOT EXISTS market_data")
        return thread_local.conn

    def get_option_chain_symbols(self, underlying: str, expiry: str = 'YYYYMMDD') -> pd.DataFrame:
        """Get all symbols in option chain for given underlying and expiry"""
        try:
            url = f"https://api.truedata.in/getOptionChain?user={td_login_id}&password={td_password}&symbol={underlying}&expiry={expiry}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame(data.get('Records', []))
        except Exception as e:
            logger.error(f"Error fetching option chain for {underlying}-{expiry}: {e}")
            return pd.DataFrame()

    def get_daily_data(self, symbol: str, end_date: Optional[datetime.date] = None) -> pd.DataFrame:
        """Get daily historical data for a symbol"""
        try:
            end_time = None
            if end_date:
                end_time = datetime.datetime.combine(end_date, datetime.time(15, 30))
            
            data = self.td_obj.get_historic_data(symbol, duration='1 D', end_time=end_time)
            if not data:
                return pd.DataFrame(columns=['timestamp', 'o', 'h', 'l', 'c', 'v', 'oi'])
            
            df = pd.DataFrame(data)
            df.columns = ['timestamp', 'o', 'h', 'l', 'c', 'v', 'oi']
            return df
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame(columns=['timestamp', 'o', 'h', 'l', 'c', 'v', 'oi'])

    def save_to_parquet(self, df: pd.DataFrame, file_path: str) -> bool:
        """Save dataframe to parquet file, appending if file exists"""
        try:
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            if os.path.exists(file_path):
                # Load existing data and append
                existing_df = pd.read_parquet(file_path)
                combined_df = pd.concat([existing_df, df]).drop_duplicates('timestamp').sort_values('timestamp')
                combined_df.to_parquet(file_path, index=False)
                logger.info(f"Appended {len(df)} rows to existing {file_path}")
            else:
                # Create new file
                df.to_parquet(file_path, index=False)
                logger.info(f"Created new parquet file {file_path} with {len(df)} rows")
            
            return True
        except Exception as e:
            logger.error(f"Error saving to parquet {file_path}: {e}")
            return False

    def update_duckdb_table(self, parquet_path: str, table_name: str) -> bool:
        """Update DuckDB table from parquet file"""
        try:
            conn = self.get_thread_connection()
            
            # Check if table exists
            table_exists = conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name.split('.')[-1]}' AND table_schema = 'market_data'"
            ).fetchone()[0] > 0
            
            if table_exists:
                # Insert new data, avoiding duplicates
                query = f"""
                INSERT INTO {table_name}
                SELECT * FROM read_parquet('{parquet_path}')
                WHERE timestamp NOT IN (SELECT timestamp FROM {table_name})
                """
            else:
                # Create new table
                query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
            
            conn.execute(query)
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Updated DuckDB table {table_name} - Total rows: {row_count}")
            return True
        
        except Exception as e:
            logger.error(f"Error updating DuckDB table {table_name}: {e}")
            return False

    def update_indices(self) -> Dict[str, int]:
        """Update index data"""
        logger.info("Starting index data update...")
        results = {'success': 0, 'failed': 0}
        
        for underlying, symbol in INDEX_SYMBOLS.items():
            try:
                logger.info(f"Processing index: {underlying} ({symbol})")
                
                # Get data
                df = self.get_daily_data(symbol)
                if df.empty:
                    logger.warning(f"No data received for index {symbol}")
                    results['failed'] += 1
                    continue
                
                # Save to parquet
                exchange = EXCHANGES[underlying]
                parquet_path = f"{COLD_STORAGE_PATH}/{exchange}/Index/{underlying}.parquet"
                
                if self.save_to_parquet(df, parquet_path):
                    # Update DuckDB
                    table_name = f"market_data.{exchange}_Index_{underlying}"
                    if self.update_duckdb_table(parquet_path, table_name):
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                else:
                    results['failed'] += 1
                    
            except Exception as e:
                logger.error(f"Error processing index {underlying}: {e}")
                results['failed'] += 1
        
        return results

    def update_futures(self) -> Dict[str, int]:
        """Update futures data"""
        logger.info("Starting futures data update...")
        results = {'success': 0, 'failed': 0}
        
        for underlying, symbols in FUTURES_SYMBOLS.items():
            exchange = EXCHANGES[underlying]
            
            for symbol in symbols:
                try:
                    logger.info(f"Processing future: {symbol}")
                    
                    # Get data
                    df = self.get_daily_data(symbol)
                    if df.empty:
                        logger.warning(f"No data received for future {symbol}")
                        results['failed'] += 1
                        continue
                    
                    # Save to parquet
                    parquet_path = f"{COLD_STORAGE_PATH}/{exchange}/Futures/{underlying}/{symbol}.parquet"
                    
                    if self.save_to_parquet(df, parquet_path):
                        # Update DuckDB
                        table_name = f"market_data.{exchange}_Futures_{underlying}_{symbol}"
                        if self.update_duckdb_table(parquet_path, table_name):
                            results['success'] += 1
                        else:
                            results['failed'] += 1
                    else:
                        results['failed'] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing future {symbol}: {e}")
                    results['failed'] += 1
        
        return results

    def update_options(self) -> Dict[str, int]:
        """Update options data"""
        logger.info("Starting options data update...")
        results = {'success': 0, 'failed': 0}
        
        # Get expiry dates for next 2 months
        start_date = self.today
        end_date = self.today + datetime.timedelta(days=60)
        
        for underlying in UNDERLYINGS:
            logger.info(f"Processing options for {underlying}")
            exchange = EXCHANGES[underlying]
            
            # Get option chain to find current expiries
            option_chain = self.get_option_chain_symbols(underlying)
            if option_chain.empty:
                logger.warning(f"No option chain data for {underlying}")
                continue
            
            # Process each unique expiry
            expiries = option_chain[1].apply(lambda x: x.split('_')[1] if '_' in str(x) else '').unique()
            
            for expiry_str in expiries:
                if not expiry_str or len(expiry_str) != 8:
                    continue
                
                try:
                    expiry_date = datetime.datetime.strptime(expiry_str, '%Y%m%d').date()
                    if expiry_date < start_date or expiry_date > end_date:
                        continue
                    
                    logger.info(f"Processing options expiry: {expiry_str} for {underlying}")
                    
                    # Get symbols for this expiry
                    expiry_symbols = self.get_option_chain_symbols(underlying, expiry_str)
                    if expiry_symbols.empty:
                        continue
                    
                    symbol_list = expiry_symbols[1].tolist() if 1 in expiry_symbols.columns else []
                    
                    for symbol in symbol_list[:50]:  # Limit to avoid overwhelming
                        try:
                            # Parse symbol to extract strike and option type
                            parts = symbol.split('_')
                            if len(parts) < 3:
                                continue
                            
                            strike = parts[2]
                            option_type = parts[3] if len(parts) > 3 else 'unknown'
                            option_type_clean = 'call' if option_type == 'CE' else 'put' if option_type == 'PE' else option_type
                            
                            # Get data
                            df = self.get_daily_data(symbol, expiry_date)
                            if df.empty:
                                continue
                            
                            # Save to parquet
                            parquet_path = f"{COLD_STORAGE_PATH}/{exchange}/Options/{underlying}/{expiry_str}/{strike}/{symbol}_{option_type_clean}.parquet"
                            
                            if self.save_to_parquet(df, parquet_path):
                                # Update DuckDB
                                table_name = f"market_data.{exchange}_Options_{underlying}_{expiry_str}_{strike}_{option_type_clean}"
                                if self.update_duckdb_table(parquet_path, table_name):
                                    results['success'] += 1
                                else:
                                    results['failed'] += 1
                            else:
                                results['failed'] += 1
                                
                        except Exception as e:
                            logger.error(f"Error processing option {symbol}: {e}")
                            results['failed'] += 1
                
                except Exception as e:
                    logger.error(f"Error processing expiry {expiry_str} for {underlying}: {e}")
                    continue
        
        return results

    def run_daily_update(self):
        """Run the complete daily update process"""
        start_time = time.time()
        logger.info(f"Starting daily market data update for {self.today}")
        
        total_results = {'success': 0, 'failed': 0}
        
        try:
            index_results = self.update_indices()
            total_results['success'] += index_results['success']
            total_results['failed'] += index_results['failed']
            logger.info(f"Index update completed - Success: {index_results['success']}, Failed: {index_results['failed']}")
            
            futures_results = self.update_futures()
            total_results['success'] += futures_results['success']
            total_results['failed'] += futures_results['failed']
            logger.info(f"Futures update completed - Success: {futures_results['success']}, Failed: {futures_results['failed']}")
            
            options_results = self.update_options()
            total_results['success'] += options_results['success']
            total_results['failed'] += options_results['failed']
            logger.info(f"Options update completed - Success: {options_results['success']}, Failed: {options_results['failed']}")
            
            conn = self.get_thread_connection()
            conn.execute("CHECKPOINT")
            gc.collect()
            
        except Exception as e:
            logger.error(f"Critical error during daily update: {e}")
        
        finally:
            duration = time.time() - start_time
            logger.info(f"=== DAILY UPDATE SUMMARY ===")
            logger.info(f"Date: {self.today}")
            logger.info(f"Total successful updates: {total_results['success']}")
            logger.info(f"Total failed updates: {total_results['failed']}")
            logger.info(f"Success rate: {(total_results['success']/(total_results['success']+total_results['failed'])*100):.1f}%" if (total_results['success']+total_results['failed']) > 0 else "No updates processed")
            logger.info(f"Total duration: {duration:.2f}s")


def main():
    """Main function to run daily update"""
    try:
        updater = DailyDataUpdater()
        updater.run_daily_update()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        raise


if __name__ == "__main__":
    main()