import duckdb
import numpy as np
import pandas as pd
import logging
import time
import threading
from scipy.stats import norm
from datetime import datetime
import gc

DB_PATH = "/mnt/disk2/qode_edw.db"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('greeks_calculation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

thread_local = threading.local()

def get_thread_connection():
    if not hasattr(thread_local, 'conn'):
        thread_local.conn = duckdb.connect(DB_PATH)
        
    return thread_local.conn

def black_scholes_call(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    call_price = (S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2))
    return call_price

def black_scholes_put(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    put_price = (K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1))
    return put_price

def calculate_delta(S, K, T, r, sigma, option_type):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    
    if option_type == 'call':
        delta = norm.cdf(d1)
    else:
        delta = -norm.cdf(-d1)
    
    return delta

def calculate_gamma(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    return gamma

def calculate_theta(S, K, T, r, sigma, option_type):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    if option_type == 'call':
        theta = (-(S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) 
                - r * K * np.exp(-r * T) * norm.cdf(d2)) / 365
    else:
        theta = (-(S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) 
                + r * K * np.exp(-r * T) * norm.cdf(-d2)) / 365
    
    return theta

def calculate_vega(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    vega = S * norm.pdf(d1) * np.sqrt(T) / 100
    return vega

def calculate_rho(S, K, T, r, sigma, option_type):
    d2 = (np.log(S / K) + (r - 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    
    if option_type == 'call':
        rho = K * T * np.exp(-r * T) * norm.cdf(d2) / 100
    else:
        rho = -K * T * np.exp(-r * T) * norm.cdf(-d2) / 100
    
    return rho

def newton_raphson_iv(market_price, S, K, T, r, option_type, max_iterations=100, tolerance=1e-6):
    sigma = 0.2
    
    for i in range(max_iterations):
        if option_type == 'call':
            price = black_scholes_call(S, K, T, r, sigma)
        else:
            price = black_scholes_put(S, K, T, r, sigma)
        
        vega = calculate_vega(S, K, T, r, sigma) * 100
        
        if abs(vega) < tolerance:
            break
            
        price_diff = price - market_price
        if abs(price_diff) < tolerance:
            break
            
        sigma = sigma - price_diff / vega
        
        if sigma <= 0:
            sigma = 0.01
    
    return max(sigma, 0.001)

def calculate_tte_minutes(expiry_date_str, timestamp):
    expiry_date = datetime.strptime(expiry_date_str, '%Y%m%d')
    expiry_datetime = expiry_date.replace(hour=15, minute=30)
    
    if isinstance(timestamp, str):
        current_time = pd.to_datetime(timestamp)
    else:
        current_time = timestamp
    
    if current_time.tzinfo is None:
        current_time = current_time.tz_localize('Asia/Kolkata')
    
    if expiry_datetime.tzinfo is None:
        expiry_datetime = expiry_datetime.replace(tzinfo=current_time.tzinfo)
    
    tte_minutes = (expiry_datetime - current_time).total_seconds() / 60
    print(f"TTE Minutes: {tte_minutes} for expiry {expiry_date_str} at {current_time}")
    return max(tte_minutes, 1)

def parse_table_name(table_name):
    parts = table_name.split('_')
    if len(parts) >= 6:
        exchange = parts[1].replace('data.', '')
        instrument = parts[2]
        underlying = parts[3]
        expiry = parts[4]
        strike = float(parts[5])
        option_type = parts[6]
        return exchange, instrument, underlying, expiry, strike, option_type
    return None

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

def get_options_tables(conn):
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'market_data' 
    AND table_name LIKE '%_Options_%'
    """
    return conn.execute(query).fetchall()

def get_spot_data(conn, exchange, underlying):
    spot_table = f"market_data.{exchange}_Index_{underlying}"
    query = f"""
    SELECT timestamp, c as spot_price 
    FROM {spot_table}
    WHERE c > 0
    ORDER BY timestamp
    """
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        logger.error(f"Failed to fetch spot data for {underlying}: {str(e)}")
        return pd.DataFrame()

def add_missing_greeks_columns(conn, table_name):
    try:
        check_columns_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name.split('.')[-1]}' 
        AND table_schema = 'market_data'
        """
        
        all_existing_columns = [row[0].lower() for row in conn.execute(check_columns_query).fetchall()]
        
        all_columns = ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'tte_minutes']
        missing_columns = [col for col in all_columns if col.lower() not in all_existing_columns]
        
        columns_to_add = []
        for column in missing_columns:
            columns_to_add.append(f"ADD COLUMN {column} DOUBLE DEFAULT NULL")
        
        return columns_to_add
    except Exception as e:
        logger.error(f"Error checking columns for {table_name}: {str(e)}")
        return []

def process_options_table(conn, table_name):
    start_time = time.time()
    logger.info(f"Processing table: {table_name}")
    
    parsed = parse_table_name(table_name)
    if not parsed:
        logger.error(f"Could not parse table name: {table_name}")
        return False
    
    exchange, instrument, underlying, expiry, strike, option_type = parsed
    
    spot_df = get_spot_data(conn, exchange, underlying)
    if spot_df.empty:
        logger.error(f"No spot data found for {underlying}")
        return False
    
    columns_to_add = add_missing_greeks_columns(conn, table_name)
    
    for column_def in columns_to_add:
        alter_query = f"ALTER TABLE {table_name} {column_def}"
        if not execute_with_timing(conn, alter_query, f"Adding column to {table_name}"):
            logger.warning(f"Failed to add column, may already exist: {column_def}")
    
    if columns_to_add:
        logger.info(f"Added {len(columns_to_add)} missing columns to {table_name}")
    else:
        logger.info(f"All required columns already exist in {table_name}")
    
    fetch_query = f"SELECT * FROM {table_name}"
    options_df = conn.execute(fetch_query).fetchdf()
    
    if options_df.empty:
        logger.warning(f"No data in table: {table_name}")
        return True
    
    options_df['timestamp'] = pd.to_datetime(options_df['timestamp'])
    spot_df['timestamp'] = pd.to_datetime(spot_df['timestamp'])
    
    merged_df = pd.merge_asof(
        options_df.sort_values('timestamp'),
        spot_df.sort_values('timestamp'),
        on='timestamp',
        direction='backward'
    )
    
    r = 0.06
    
    def calculate_row_greeks(row):
        try:
            tte_min = calculate_tte_minutes(expiry, row['timestamp'])
            tte_years = tte_min / (365 * 24 * 60)
            
            if tte_years <= 0:
                return pd.Series({
                    'delta': None, 'gamma': None, 'theta': None,
                    'vega': None, 'rho': None, 'iv': None, 'tte_minutes': tte_min
                })
            
            if (pd.isna(row['spot_price']) or pd.isna(row['c']) or 
                row['c'] <= 0 or row['spot_price'] <= 0):
                return pd.Series({
                    'delta': None, 'gamma': None, 'theta': None,
                    'vega': None, 'rho': None, 'iv': None, 'tte_minutes': tte_min
                })
            
            iv = newton_raphson_iv(row['c'], row['spot_price'], strike, tte_years, r, option_type)
            
            delta = calculate_delta(row['spot_price'], strike, tte_years, r, iv, option_type)
            gamma = calculate_gamma(row['spot_price'], strike, tte_years, r, iv)
            theta = calculate_theta(row['spot_price'], strike, tte_years, r, iv, option_type)
            vega = calculate_vega(row['spot_price'], strike, tte_years, r, iv)
            rho = calculate_rho(row['spot_price'], strike, tte_years, r, iv, option_type)
            
            print(f"Calculated greeks for row at {row['timestamp']}: "
                  f"Delta: {delta}, Gamma: {gamma}, Theta: {theta}, "
                  f"Vega: {vega}, Rho: {rho}, IV: {iv}, TTE Minutes: {tte_min}")
            
            return pd.Series({
                'delta': delta, 'gamma': gamma, 'theta': theta,
                'vega': vega, 'rho': rho, 'iv': iv, 'tte_minutes': tte_min
            })
        except Exception as e:
            logger.error(f"Error calculating greeks for row: {str(e)}")
            try:
                tte_min = calculate_tte_minutes(expiry, row['timestamp'])
            except:
                tte_min = None
            return pd.Series({
                'delta': None, 'gamma': None, 'theta': None,
                'vega': None, 'rho': None, 'iv': None, 'tte_minutes': tte_min
            })
    
    greeks_df = merged_df.apply(calculate_row_greeks, axis=1)
    merged_df = pd.concat([merged_df, greeks_df], axis=1)
    
    conn.execute("BEGIN TRANSACTION")
    
    try:
        temp_table = f"{table_name}_temp"
        conn.register(temp_table.split('.')[-1], merged_df)
        
        update_query = f"""
        UPDATE {table_name} 
        SET 
            delta = temp.delta,
            gamma = temp.gamma,
            theta = temp.theta,
            vega = temp.vega,
            rho = temp.rho,
            iv = temp.iv,
            tte_minutes = temp.tte_minutes
        FROM {temp_table.split('.')[-1]} temp
        WHERE {table_name}.timestamp = temp.timestamp
        AND temp.timestamp IS NOT NULL
        """
        
        if execute_with_timing(conn, update_query, f"Updating Greeks for {table_name}"):
            conn.execute("COMMIT")
            duration = time.time() - start_time
            logger.info(f"Table {table_name} completed - Duration: {duration:.2f}s")
            return True
        else:
            conn.execute("ROLLBACK")
            return False
            
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Failed to update table {table_name}: {str(e)}")
        return False
    finally:
        gc.collect()

def main():
    start_time = time.time()
    logger.info("Starting Greeks calculation process")
    
    conn = duckdb.connect(DB_PATH)
    conn.execute("SET memory_limit='20GB'")
    conn.execute("SET threads=16")
    conn.execute("SET max_memory='20GB'")
    conn.execute("SET temp_directory='/tmp'")
    
    options_tables = get_options_tables(conn)
    logger.info(f"Found {len(options_tables)} options tables")
    
    successful = 0
    failed = 0
    
    for table_tuple in options_tables:
        table_name = f"market_data.{table_tuple[0]}"
        
        if process_options_table(conn, table_name):
            successful += 1
        else:
            failed += 1
    
    duration = time.time() - start_time
    logger.info(f"=== GREEKS CALCULATION SUMMARY ===")
    logger.info(f"Total tables processed: {len(options_tables)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Success rate: {(successful/len(options_tables)*100):.1f}%" if options_tables else "No tables found")
    logger.info(f"Total processing time: {duration:.2f}s")
    
    conn.close()

if __name__ == "__main__":
    main()