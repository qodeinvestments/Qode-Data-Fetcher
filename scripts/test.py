import pandas as pd
import numpy as np
from scipy.stats import norm
import py_vollib.black_scholes as bs
import py_vollib.black_scholes.greeks.analytical as greeks
import warnings
import logging
from pathlib import Path
import multiprocessing as mp
import gc
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

def black_scholes_price(S, K, T, r, sigma, option_type):
    """
    Calculate Black-Scholes option price
    S: Current stock price
    K: Strike price
    T: Time to expiration (in years)
    r: Risk-free rate
    sigma: Volatility
    option_type: 'call' or 'put'
    """
    if T <= 0:
        if option_type == 'call':
            return max(S - K, 0)
        else:
            return max(K - S, 0)
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    if option_type == 'call':
        price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    
    return price

def vega(S, K, T, r, sigma):
    """
    Calculate vega (sensitivity to volatility)
    """
    if T <= 0:
        return 0
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return S * norm.pdf(d1) * np.sqrt(T)

def newton_raphson_iv(market_price, S, K, T, r, option_type, max_iterations=100, tolerance=1e-6):
    """
    Calculate implied volatility using Newton-Raphson method
    """
    if T <= 0:
        return np.nan
    
    if option_type == 'call':
        intrinsic = max(S - K, 0)
    else:
        intrinsic = max(K - S, 0)
    
    if market_price <= intrinsic:
        return np.nan
    
    sigma = 0.2
    
    for i in range(max_iterations):
        price = black_scholes_price(S, K, T, r, sigma, option_type)
        vega_val = vega(S, K, T, r, sigma)
        
        if abs(vega_val) < 1e-10:
            break
            
        price_diff = price - market_price
        
        if abs(price_diff) < tolerance:
            return sigma
        
        sigma = sigma - price_diff / vega_val
        
        if sigma <= 0:
            sigma = 0.001
        elif sigma > 5:
            sigma = 5
    
    return sigma if sigma > 0 else np.nan

def calculate_greeks_custom(S, K, T, r, sigma, option_type):
    """
    Calculate Greeks using custom implementation
    """
    if T <= 0 or sigma <= 0:
        return {'delta': np.nan, 'gamma': np.nan, 'theta': np.nan, 'vega': np.nan, 'rho': np.nan}
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    if option_type == 'call':
        delta = norm.cdf(d1)
        rho = K * T * np.exp(-r * T) * norm.cdf(d2)
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T))
                - r * K * np.exp(-r * T) * norm.cdf(d2))
    else:
        delta = norm.cdf(d1) - 1
        rho = -K * T * np.exp(-r * T) * norm.cdf(-d2)
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T))
                + r * K * np.exp(-r * T) * norm.cdf(-d2))
    
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    vega_val = S * norm.pdf(d1) * np.sqrt(T)
    
    theta = theta / 365
    
    return {
        'delta': delta,
        'gamma': gamma,
        'theta': theta,
        'vega': vega_val / 100,
        'rho': rho / 100
    }

def test_black_scholes_functions():
    """
    Test Black-Scholes and Newton-Raphson implementation
    """
    S = 25108
    K = 25100
    T = 2/365
    r = 0.065
    sigma = 0.18
    option_type = 'put'
    market_price = 77.55
    
    logger.info("Testing Black-Scholes Implementation")
    logger.info(f"Spot: {S}, Strike: {K}, Time: {T:.4f} years, Rate: {r}, Vol: {sigma}")
    logger.info(f"Option Type: {option_type}")
    
    bs_price = black_scholes_price(S, K, T, r, sigma, option_type)
    logger.info(f"Black-Scholes Price: {bs_price:.4f}")
    
    pyvollib_price = bs.black_scholes(option_type[0], S, K, T, r, sigma)
    logger.info(f"PyVolLib Price: {pyvollib_price:.4f}")
    logger.info(f"Price Difference: {abs(bs_price - pyvollib_price):.6f}")
    
    implied_vol = newton_raphson_iv(market_price, S, K, T, r, option_type)
    logger.info(f"Implied Volatility (Newton-Raphson): {implied_vol:.6f}")
    logger.info(f"Original Volatility: {sigma:.6f}")
    logger.info(f"IV Difference: {abs(implied_vol - sigma):.8f}")
    
    custom_greeks = calculate_greeks_custom(S, K, T, r, sigma, option_type)
    logger.info(f"\nCustom Greeks:")
    for greek, value in custom_greeks.items():
        logger.info(f"{greek.capitalize()}: {value:.6f}")
    
    logger.info(f"\nPyVolLib Greeks:")
    logger.info(f"Delta: {greeks.delta(option_type[0], S, K, T, r, sigma):.6f}")
    logger.info(f"Gamma: {greeks.gamma(option_type[0], S, K, T, r, sigma):.6f}")
    logger.info(f"Theta: {greeks.theta(option_type[0], S, K, T, r, sigma):.6f}")
    logger.info(f"Vega: {greeks.vega(option_type[0], S, K, T, r, sigma):.6f}")
    logger.info(f"Rho: {greeks.rho(option_type[0], S, K, T, r, sigma):.6f}")

def calculate_time_to_expiry_minutes(timestamp, expiry_date):
    """
    Calculate time to expiry in minutes with expiry at 15:30
    """
    if pd.isna(timestamp) or pd.isna(expiry_date):
        return np.nan
    
    if isinstance(timestamp, str):
        timestamp = pd.to_datetime(timestamp)
    if isinstance(expiry_date, str):
        expiry_date = pd.to_datetime(expiry_date)
    
    expiry_time = expiry_date.replace(hour=15, minute=30, second=0, microsecond=0)
    
    time_diff = expiry_time - timestamp
    
    if time_diff.total_seconds() < 0:
        return 0
    
    return time_diff.total_seconds() / 60

def process_options_chunk(chunk_df, risk_free_rate=0.065):
    """
    Process a chunk of options data to calculate IV and Greeks
    chunk_df: DataFrame chunk with columns [timestamp, open, high, low, close, c, expiry, strike, symbol, option_type]
    risk_free_rate: Risk-free rate (default 0.065)
    """
    result_df = chunk_df.copy()
    
    mask = ~pd.isna(result_df['c'])
    
    if not mask.any():
        logger.debug("No valid underlying prices found in chunk")
        return result_df
    
    valid_data = result_df[mask].copy()
    
    valid_data['timestamp'] = pd.to_datetime(valid_data['timestamp'])
    valid_data['expiry'] = pd.to_datetime(valid_data['expiry'])
    
    valid_data['time_to_expiry_minutes'] = valid_data.apply(
        lambda row: calculate_time_to_expiry_minutes(row['timestamp'], row['expiry']), axis=1
    )
    
    valid_data['time_to_expiry_years'] = valid_data['time_to_expiry_minutes'] / (365 * 24 * 60)
    
    active_mask = valid_data['time_to_expiry_years'] > 0
    
    valid_data.loc[:, 'iv'] = np.nan
    valid_data.loc[:, 'delta'] = np.nan
    valid_data.loc[:, 'gamma'] = np.nan
    valid_data.loc[:, 'theta'] = np.nan
    valid_data.loc[:, 'vega'] = np.nan
    valid_data.loc[:, 'rho'] = np.nan
    
    if active_mask.any():
        active_data = valid_data[active_mask].copy()
        
        iv_results = []
        for idx, row in active_data.iterrows():
            try:
                iv = newton_raphson_iv(
                    row['close'], row['c'], row['strike'], 
                    row['time_to_expiry_years'], risk_free_rate, row['option_type']
                )
                iv_results.append(iv)
            except:
                iv_results.append(np.nan)
        
        valid_data.loc[active_mask, 'iv'] = iv_results
        
        iv_valid_mask = active_mask & ~pd.isna(valid_data['iv']) & (valid_data['iv'] > 0)
        
        if iv_valid_mask.any():
            iv_valid_data = valid_data[iv_valid_mask].copy()
            
            greeks_results = []
            for idx, row in iv_valid_data.iterrows():
                try:
                    greeks_dict = calculate_greeks_custom(
                        row['c'], row['strike'], row['time_to_expiry_years'],
                        risk_free_rate, row['iv'], row['option_type']
                    )
                    greeks_results.append(greeks_dict)
                except:
                    greeks_results.append({
                        'delta': np.nan, 'gamma': np.nan, 'theta': np.nan, 
                        'vega': np.nan, 'rho': np.nan
                    })
            
            for i, (idx, _) in enumerate(iv_valid_data.iterrows()):
                for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                    valid_data.loc[idx, greek] = greeks_results[i][greek]
    
    for col in ['iv', 'delta', 'gamma', 'theta', 'vega', 'rho']:
        result_df[col] = np.nan
    
    result_df.update(valid_data[['iv', 'delta', 'gamma', 'theta', 'vega', 'rho']])
    
    return result_df

def process_parquet_file_chunked(input_file_path, output_file_path, chunk_size=10000, risk_free_rate=0.065):
    """
    Process a parquet file in chunks to reduce memory consumption
    
    Args:
        input_file_path: Path to input parquet file
        output_file_path: Path to output parquet file
        chunk_size: Number of rows per chunk (default 10000)
        risk_free_rate: Risk-free rate (default 0.065)
    """
    logger.info(f"Processing {input_file_path} in chunks of {chunk_size} rows")
    
    try:
        parquet_file = pd.read_parquet(input_file_path)
        total_rows = len(parquet_file)
        logger.info(f"Total rows in file: {total_rows}")
        
        del parquet_file
        gc.collect()
        
        processed_chunks = []
        
        for chunk_start in range(0, total_rows, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_rows)
            logger.info(f"Processing rows {chunk_start} to {chunk_end-1}")
            
            chunk_df = pd.read_parquet(input_file_path, 
                                     engine='pyarrow',
                                     use_pandas_metadata=True)[chunk_start:chunk_end]
            
            processed_chunk = process_options_chunk(chunk_df, risk_free_rate)
            processed_chunks.append(processed_chunk)
            
            del chunk_df, processed_chunk
            gc.collect()
            
            logger.info(f"Completed chunk {chunk_start//chunk_size + 1}/{(total_rows + chunk_size - 1)//chunk_size}")
        
        logger.info("Combining processed chunks...")
        final_df = pd.concat(processed_chunks, ignore_index=True)
        
        logger.info(f"Saving processed data to {output_file_path}")
        final_df.to_parquet(output_file_path, index=False)
        
        del final_df, processed_chunks
        gc.collect()
        
        logger.info(f"Successfully processed {input_file_path}")
        
    except Exception as e:
        logger.error(f"Error processing {input_file_path}: {str(e)}")
        raise

def process_multiple_files_chunked(chunk_size=None, target_memory_mb=500, risk_free_rate=0.065):
    """
    Process multiple parquet files in chunks to reduce memory consumption
    
    Args:
        chunk_size: Fixed chunk size (if None, will be calculated automatically)
        target_memory_mb: Target memory usage per chunk in MB (used if chunk_size is None)
        risk_free_rate: Risk-free rate
    """
    input_path = Path("/mnt/disk2/cold_storage/processed_master_files/")
    output_path = Path("/mnt/disk2/cold_storage/greeks_master_files/")
    
    if not input_path.exists():
        logger.error(f"Input path does not exist: {input_path}")
        return
    
    output_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created output directory: {output_path}")
    
    parquet_files = list(input_path.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No parquet files found in {input_path}")
        return
    
    logger.info(f"Found {len(parquet_files)} parquet files to process")
    
    for i, file_path in enumerate(parquet_files[1:], 1):
        logger.info(f"Processing file {i}/{len(parquet_files)-1}: {file_path.name}")
        
        output_file_path = output_path / file_path.name
        
        if output_file_path.exists():
            logger.info(f"Output file already exists, skipping: {output_file_path}")
            continue
        
        try:
            process_parquet_file_chunked(
                file_path, 
                output_file_path, 
                chunk_size, 
                risk_free_rate
            )
            
            logger.info(f"Successfully completed: {file_path.name}")
            
        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {str(e)}")
            continue

if __name__ == "__main__":
    process_multiple_files_chunked(chunk_size=10000)