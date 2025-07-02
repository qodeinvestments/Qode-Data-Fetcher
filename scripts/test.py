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
from tqdm import tqdm
import time
import os
warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('options_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

def black_scholes_price(S, K, T, r, sigma, option_type):
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
    if T <= 0:
        return 0
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return S * norm.pdf(d1) * np.sqrt(T)

def newton_raphson_iv(market_price, S, K, T, r, option_type, max_iterations=100, tolerance=1e-6):
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

def process_options_chunk(chunk_data):
    chunk_df, chunk_index, total_chunks, risk_free_rate, process_id = chunk_data
    
    process_logger = logging.getLogger(f"Process-{process_id}")
    process_logger.info(f"Starting chunk {chunk_index+1}/{total_chunks} with {len(chunk_df)} rows")
    
    start_time = time.time()
    
    result_df = chunk_df.copy()
    
    mask = ~pd.isna(result_df['c'])
    
    if not mask.any():
        process_logger.debug(f"No valid underlying prices found in chunk {chunk_index+1}")
        return result_df
    
    valid_data = result_df[mask].copy()
    process_logger.debug(f"Found {len(valid_data)} valid rows in chunk {chunk_index+1}")
    
    valid_data['timestamp'] = pd.to_datetime(valid_data['timestamp'])
    valid_data['expiry'] = pd.to_datetime(valid_data['expiry'])
    
    valid_data['time_to_expiry_minutes'] = valid_data.apply(
        lambda row: calculate_time_to_expiry_minutes(row['timestamp'], row['expiry']), axis=1
    )
    
    valid_data['time_to_expiry_years'] = valid_data['time_to_expiry_minutes'] / (365 * 24 * 60)
    
    active_mask = valid_data['time_to_expiry_years'] > 0
    active_count = active_mask.sum()
    process_logger.debug(f"Found {active_count} active options in chunk {chunk_index+1}")
    
    valid_data.loc[:, 'iv'] = np.nan
    valid_data.loc[:, 'delta'] = np.nan
    valid_data.loc[:, 'gamma'] = np.nan
    valid_data.loc[:, 'theta'] = np.nan
    valid_data.loc[:, 'vega'] = np.nan
    valid_data.loc[:, 'rho'] = np.nan
    
    if active_mask.any():
        active_data = valid_data[active_mask].copy()
        
        iv_results = []
        iv_errors = 0
        for idx, row in active_data.iterrows():
            try:
                iv = newton_raphson_iv(
                    row['close'], row['c'], row['strike'], 
                    row['time_to_expiry_years'], risk_free_rate, row['option_type']
                )
                iv_results.append(iv)
            except Exception as e:
                process_logger.error(f"Error calculating IV for row {idx}: {str(e)}")
                iv_results.append(np.nan)
                iv_errors += 1
        
        if iv_errors > 0:
            process_logger.warning(f"IV calculation errors in chunk {chunk_index+1}: {iv_errors}")
        
        valid_data.loc[active_mask, 'iv'] = iv_results
        
        iv_valid_mask = active_mask & ~pd.isna(valid_data['iv']) & (valid_data['iv'] > 0)
        iv_valid_count = iv_valid_mask.sum()
        process_logger.debug(f"Successfully calculated IV for {iv_valid_count} options in chunk {chunk_index+1}")
        
        if iv_valid_mask.any():
            iv_valid_data = valid_data[iv_valid_mask].copy()
            
            greeks_results = []
            greeks_errors = 0
            for idx, row in iv_valid_data.iterrows():
                try:
                    greeks_dict = calculate_greeks_custom(
                        row['c'], row['strike'], row['time_to_expiry_years'],
                        risk_free_rate, row['iv'], row['option_type']
                    )
                    greeks_results.append(greeks_dict)
                except Exception as e:
                    greeks_results.append({
                        'delta': np.nan, 'gamma': np.nan, 'theta': np.nan, 
                        'vega': np.nan, 'rho': np.nan
                    })
                    greeks_errors += 1
            
            if greeks_errors > 0:
                process_logger.warning(f"Greeks calculation errors in chunk {chunk_index+1}: {greeks_errors}")
            
            for i, (idx, _) in enumerate(iv_valid_data.iterrows()):
                for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                    valid_data.loc[idx, greek] = greeks_results[i][greek]
    
    for col in ['iv', 'delta', 'gamma', 'theta', 'vega', 'rho']:
        result_df[col] = np.nan
    
    result_df.update(valid_data[['iv', 'delta', 'gamma', 'theta', 'vega', 'rho']])
    
    end_time = time.time()
    processing_time = end_time - start_time
    process_logger.info(f"Completed chunk {chunk_index+1}/{total_chunks} in {processing_time:.2f} seconds")
    
    return result_df

def process_parquet_file_multiprocess(input_file_path, output_file_path, chunk_size=10000, risk_free_rate=0.065, num_processes=24):
    logger.info(f"Processing {input_file_path} with {num_processes} processes and chunk size {chunk_size}")
    
    try:
        parquet_file = pd.read_parquet(input_file_path)
        total_rows = len(parquet_file)
        logger.info(f"Total rows in file: {total_rows}")
        
        chunks = []
        chunk_indices = []
        
        for chunk_start in range(0, total_rows, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_rows)
            chunk_df = parquet_file.iloc[chunk_start:chunk_end].copy()
            chunk_index = chunk_start // chunk_size
            chunks.append(chunk_df)
            chunk_indices.append(chunk_index)
        
        del parquet_file
        gc.collect()
        
        total_chunks = len(chunks)
        logger.info(f"Created {total_chunks} chunks for processing")
        
        chunk_data_list = []
        for i, chunk_df in enumerate(chunks):
            chunk_data_list.append((chunk_df, i, total_chunks, risk_free_rate, os.getpid()))
        
        logger.info(f"Starting multiprocessing with {num_processes} processes")
        
        with mp.Pool(processes=num_processes) as pool:
            with tqdm(total=total_chunks, desc="Processing chunks", unit="chunk") as pbar:
                results = []
                for result in pool.imap(process_options_chunk, chunk_data_list):
                    results.append(result)
                    pbar.update(1)
        
        logger.info("Combining processed chunks...")
        final_df = pd.concat(results, ignore_index=True)
        
        logger.info(f"Saving processed data to {output_file_path}")
        final_df.to_parquet(output_file_path, index=False)
        
        del final_df, results, chunks, chunk_data_list
        gc.collect()
        
        logger.info(f"Successfully processed {input_file_path} with multiprocessing")
        
    except Exception as e:
        logger.error(f"Error processing {input_file_path}: {str(e)}")
        raise

def process_multiple_files_multiprocess(chunk_size=10000, risk_free_rate=0.065, num_processes=24):
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
    logger.info(f"Using {num_processes} processes with chunk size {chunk_size}")
    print(parquet_files)
    
    with tqdm(total=len(parquet_files), desc="Processing files", unit="file") as file_pbar:
        for i, file_path in enumerate(parquet_files):
            print(file_path)
            logger.info(f"Processing file {i}: {file_path.name}")
            
            output_file_path = output_path / file_path.name
            print("output", output_file_path)
            
            if output_file_path.exists():
                logger.info(f"Output file already exists, skipping: {output_file_path}")
                file_pbar.update(1)
                continue
            
            try:
                file_start_time = time.time()
                
                process_parquet_file_multiprocess(
                    file_path, 
                    output_file_path, 
                    chunk_size, 
                    risk_free_rate,
                    num_processes
                )
                
                file_end_time = time.time()
                file_processing_time = file_end_time - file_start_time
                
                logger.info(f"Successfully completed: {file_path.name} in {file_processing_time:.2f} seconds")
                
            except Exception as e:
                logger.error(f"Failed to process {file_path.name}: {str(e)}")
                continue
            
            file_pbar.update(1)
    
    logger.info("All files processed successfully")

if __name__ == "__main__":
    logger.info("Starting multiprocessed options pricing calculation")
    logger.info(f"Available CPU cores: {mp.cpu_count()}")
    logger.info(f"Using 24 cores for processing")
    
    process_multiple_files_multiprocess(chunk_size=10000, num_processes=24)