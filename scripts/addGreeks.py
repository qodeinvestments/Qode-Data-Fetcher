import duckdb
import numpy as np
import pandas as pd
import logging
import time
from scipy.stats import norm
from datetime import datetime
from tqdm import tqdm
from collections import defaultdict

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

@np.vectorize
def black_scholes_call_vec(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)

@np.vectorize
def black_scholes_put_vec(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def calculate_greeks_vectorized(S, K, T, r, sigma, option_type):
    mask = (T > 0) & (S > 0) & (sigma > 0)
    
    results = {
        'delta': np.full_like(S, np.nan),
        'gamma': np.full_like(S, np.nan),
        'theta': np.full_like(S, np.nan),
        'vega': np.full_like(S, np.nan),
        'rho': np.full_like(S, np.nan)
    }
    
    if not np.any(mask):
        return results
    
    S_valid = S[mask]
    K_valid = K if np.isscalar(K) else K[mask]
    T_valid = T[mask]
    sigma_valid = sigma[mask]
    
    d1 = (np.log(S_valid / K_valid) + (r + 0.5 * sigma_valid ** 2) * T_valid) / (sigma_valid * np.sqrt(T_valid))
    d2 = d1 - sigma_valid * np.sqrt(T_valid)
    
    if option_type == 'call':
        results['delta'][mask] = norm.cdf(d1)
        results['theta'][mask] = (-(S_valid * norm.pdf(d1) * sigma_valid) / (2 * np.sqrt(T_valid)) 
                                 - r * K_valid * np.exp(-r * T_valid) * norm.cdf(d2)) / 365
        results['rho'][mask] = K_valid * T_valid * np.exp(-r * T_valid) * norm.cdf(d2) / 100
    else:
        results['delta'][mask] = -norm.cdf(-d1)
        results['theta'][mask] = (-(S_valid * norm.pdf(d1) * sigma_valid) / (2 * np.sqrt(T_valid)) 
                                 + r * K_valid * np.exp(-r * T_valid) * norm.cdf(-d2)) / 365
        results['rho'][mask] = -K_valid * T_valid * np.exp(-r * T_valid) * norm.cdf(-d2) / 100
    
    results['gamma'][mask] = norm.pdf(d1) / (S_valid * sigma_valid * np.sqrt(T_valid))
    results['vega'][mask] = S_valid * norm.pdf(d1) * np.sqrt(T_valid) / 100
    
    return results

def newton_raphson_iv_vectorized(market_price, S, K, T, r, option_type, max_iterations=50, tolerance=1e-5):
    mask = (T > 0) & (S > 0) & (market_price > 0)
    sigma = np.full_like(market_price, 0.2, dtype=float)
    
    if not np.any(mask):
        return sigma
    
    for _ in range(max_iterations):
        if option_type == 'call':
            price = black_scholes_call_vec(S, K, T, r, sigma)
        else:
            price = black_scholes_put_vec(S, K, T, r, sigma)
        
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        vega = S * norm.pdf(d1) * np.sqrt(T)
        
        price_diff = price - market_price
        converged = np.abs(price_diff) < tolerance
        
        if np.all(converged[mask]):
            break
        
        vega_safe = np.where(np.abs(vega) < tolerance, tolerance, vega)
        sigma_new = sigma - price_diff / vega_safe
        sigma = np.where(sigma_new <= 0, 0.01, sigma_new)
    
    return np.maximum(sigma, 0.001)

def calculate_tte_minutes_vectorized(expiry_date_str, timestamps):
    expiry_date = datetime.strptime(expiry_date_str, '%Y%m%d')
    expiry_datetime = expiry_date.replace(hour=15, minute=30)
    
    if isinstance(timestamps, pd.Series):
        timestamps = pd.to_datetime(timestamps)
        if timestamps.dt.tz is None:
            timestamps = timestamps.dt.tz_localize('Asia/Kolkata')
    
    if expiry_datetime.tzinfo is None:
        expiry_datetime = expiry_datetime.replace(tzinfo=timestamps.iloc[0].tzinfo)
    
    tte_minutes = (expiry_datetime - timestamps).dt.total_seconds() / 60
    return np.maximum(tte_minutes, 1)

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

def get_options_tables_by_underlying(conn):
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'market_data' 
    AND table_name LIKE '%_Options_%'
    """
    
    all_tables = conn.execute(query).fetchall()
    underlying_tables = defaultdict(list)
    
    for table_tuple in all_tables:
        table_name = table_tuple[0]
        parsed = parse_table_name(f"market_data.{table_name}")
        if parsed:
            exchange, instrument, underlying, expiry, strike, option_type = parsed
            underlying_tables[underlying].append(f"market_data.{table_name}")
    
    return dict(underlying_tables)

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

def add_missing_greeks_columns_batch(conn, table_names):
    all_columns = ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'tte_minutes']
    
    for table_name in table_names:
        try:
            check_columns_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name.split('.')[-1]}' 
            AND table_schema = 'market_data'
            """
            
            existing_columns = {row[0].lower() for row in conn.execute(check_columns_query).fetchall()}
            missing_columns = [col for col in all_columns if col.lower() not in existing_columns]
            
            if missing_columns:
                logger.info(f"Adding missing columns to {table_name}: {missing_columns}")
                columns_def = ', '.join([f"ADD COLUMN {col} DOUBLE DEFAULT NULL" for col in missing_columns])
                alter_query = f"ALTER TABLE {table_name} {columns_def}"
                conn.execute(alter_query)
                
        except Exception as e:
            logger.warning(f"Error adding columns to {table_name}: {str(e)}")

def process_options_table_optimized(conn, table_name, spot_df):
    parsed = parse_table_name(table_name)
    if not parsed:
        return False, f"Could not parse table name: {table_name}"
    
    exchange, instrument, underlying, expiry, strike, option_type = parsed
    
    try:
        fetch_query = f"SELECT timestamp, c FROM {table_name} WHERE c > 0"
        options_df = conn.execute(fetch_query).fetchdf()
        
        if options_df.empty:
            return True, f"No data in table: {table_name}"
        
        options_df['timestamp'] = pd.to_datetime(options_df['timestamp'])
        
        merged_df = pd.merge_asof(
            options_df.sort_values('timestamp'),
            spot_df.sort_values('timestamp'),
            on='timestamp',
            direction='backward'
        )
        
        valid_mask = (merged_df['c'] > 0) & (merged_df['spot_price'] > 0) & (~merged_df['spot_price'].isna())
        if not valid_mask.any():
            return True, f"No valid data after merge: {table_name}"
        
        merged_df = merged_df[valid_mask].copy()
        
        tte_minutes = calculate_tte_minutes_vectorized(expiry, merged_df['timestamp'])
        tte_years = tte_minutes / (365 * 24 * 60)
        
        valid_tte_mask = tte_years > 0
        if not valid_tte_mask.any():
            return True, f"No valid time to expiry: {table_name}"
        
        r = 0.06
        
        iv_values = newton_raphson_iv_vectorized(
            merged_df['c'].values, 
            merged_df['spot_price'].values, 
            strike, 
            tte_years.values, 
            r, 
            option_type
        )
        
        greeks = calculate_greeks_vectorized(
            merged_df['spot_price'].values,
            strike,
            tte_years.values,
            r,
            iv_values,
            option_type
        )
        
        merged_df['delta'] = greeks['delta']
        merged_df['gamma'] = greeks['gamma']
        merged_df['theta'] = greeks['theta']
        merged_df['vega'] = greeks['vega']
        merged_df['rho'] = greeks['rho']
        merged_df['iv'] = iv_values
        merged_df['tte_minutes'] = tte_minutes
        
        update_data = []
        for _, row in merged_df.iterrows():
            values = [
                row['delta'], row['gamma'], row['theta'], 
                row['vega'], row['rho'], row['iv'], row['tte_minutes']
            ]
            update_data.append((row['timestamp'],) + tuple(values))
        
        if update_data:
            update_query = f"""
            UPDATE {table_name} 
            SET delta = ?, gamma = ?, theta = ?, vega = ?, rho = ?, iv = ?, tte_minutes = ?
            WHERE timestamp = ?
            """
            
            conn.execute("BEGIN TRANSACTION")
            
            for data_row in update_data:
                timestamp = data_row[0]
                values = data_row[1:]
                conn.execute(update_query, values + (timestamp,))
            
            conn.execute("COMMIT")
        
        return True, f"Successfully processed {len(update_data)} rows"
        
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except:
            pass
        return False, f"Error processing {table_name}: {str(e)}"

def process_underlying_sequential(conn, underlying, tables):
    logger.info(f"Processing underlying: {underlying} with {len(tables)} tables")
    
    first_table = tables[0]
    parsed = parse_table_name(first_table)
    if not parsed:
        return 0, len(tables)
    
    exchange = parsed[0]
    spot_df = get_spot_data(conn, exchange, underlying)
    
    if spot_df.empty:
        logger.error(f"No spot data available for {underlying}")
        return 0, len(tables)
    
    spot_df['timestamp'] = pd.to_datetime(spot_df['timestamp'])
    
    add_missing_greeks_columns_batch(conn, tables)
    
    successful = 0
    failed = 0
    
    for table in tqdm(tables, desc=f"{underlying}"):
        try:
            logger.info(f"Processing table: {table}")
            success, message = process_options_table_optimized(conn, table, spot_df)
            if success:
                logger.info(f"Successfully processed {table}: {message}")
                successful += 1
            else:
                failed += 1
                logger.error(message)
        except Exception as e:
            failed += 1
            logger.error(f"Exception processing {table}: {str(e)}")
    
    logger.info(f"Underlying {underlying} completed - Success: {successful}, Failed: {failed}")
    return successful, failed

def main():
    start_time = time.time()
    logger.info("Starting sequential Greeks calculation process")
    
    conn = duckdb.connect(DB_PATH)
    conn.execute("SET threads=4")
    conn.execute("SET memory_limit='8GB'")
    
    underlying_tables = get_options_tables_by_underlying(conn)
    
    total_tables = sum(len(tables) for tables in underlying_tables.values())
    total_underlyings = len(underlying_tables)
    
    logger.info(f"Found {total_underlyings} underlyings with {total_tables} total options tables")
    
    overall_successful = 0
    overall_failed = 0
    
    for underlying, tables in tqdm(underlying_tables.items(), desc="Overall Progress"):
        try:
            successful, failed = process_underlying_sequential(conn, underlying, tables)
            overall_successful += successful
            overall_failed += failed
        except Exception as e:
            logger.error(f"Exception processing underlying {underlying}: {str(e)}")
            overall_failed += len(tables)
    
    duration = time.time() - start_time
    
    logger.info("="*80)
    logger.info("FINAL SUMMARY")
    logger.info("="*80)
    logger.info(f"Total underlyings processed: {total_underlyings}")
    logger.info(f"Total tables processed: {total_tables}")
    logger.info(f"Overall successful: {overall_successful}")
    logger.info(f"Overall failed: {overall_failed}")
    logger.info(f"Success rate: {(overall_successful/total_tables*100):.1f}%" if total_tables else "0%")
    logger.info(f"Total processing time: {duration:.2f}s ({duration/60:.1f} minutes)")
    logger.info(f"Average time per table: {duration/total_tables:.2f}s" if total_tables else "N/A")
    
    conn.close()

if __name__ == "__main__":
    main()