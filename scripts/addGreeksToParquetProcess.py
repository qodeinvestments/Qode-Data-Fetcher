import pandas as pd
import numpy as np
from scipy.stats import norm
from pathlib import Path
import warnings
import logging
from typing import Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

class OptionsGreeksCalculator:
    def __init__(self, risk_free_rate: float = 0.06):
        self.risk_free_rate = risk_free_rate
        
    def _time_to_expiry_minutes(self, current_date: pd.Timestamp, expiry_date: pd.Timestamp) -> np.ndarray:
        time_diff_minutes = (expiry_date - current_date).dt.total_seconds() / 60
        time_diff_years = time_diff_minutes / (365.25 * 24 * 60)
        return np.maximum(time_diff_years, 1/(365.25 * 24 * 60))
    
    def _d1_d2(self, S: np.ndarray, K: np.ndarray, T: np.ndarray, 
               r: float, sigma: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        sigma = np.maximum(sigma, 1e-8)
        T = np.maximum(T, 1e-8)
        
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        return d1, d2
    
    def black_scholes_price(self, S: np.ndarray, K: np.ndarray, T: np.ndarray,
                           r: float, sigma: np.ndarray, option_type: np.ndarray) -> np.ndarray:
        d1, d2 = self._d1_d2(S, K, T, r, sigma)
        
        call_price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        put_price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
        
        is_call = (option_type == 'call') | (option_type == 'CE')
        price = np.where(is_call, call_price, put_price)
        
        return price
    
    def vega(self, S: np.ndarray, K: np.ndarray, T: np.ndarray,
             r: float, sigma: np.ndarray) -> np.ndarray:
        d1, _ = self._d1_d2(S, K, T, r, sigma)
        vega_val = S * norm.pdf(d1) * np.sqrt(T) / 100
        return vega_val
    
    def newton_raphson_iv(self, market_price: np.ndarray, S: np.ndarray, K: np.ndarray,
                         T: np.ndarray, r: float, option_type: np.ndarray,
                         max_iterations: int = 100, tolerance: float = 1e-6) -> np.ndarray:
        
        sigma = np.full_like(market_price, 0.2, dtype=np.float64)
        
        print("Starting Newton-Raphson IV calculation for ALL rows")
        
        # Only exclude rows with invalid basic parameters - NO time value mask
        valid_mask = (market_price > 0) & (S > 0) & (K > 0) & (T > 0)
        
        print(f"Valid mask: {np.sum(valid_mask)} valid entries out of {len(market_price)} total")
        
        # Initialize result array
        result_sigma = np.full_like(market_price, np.nan, dtype=np.float64)
        
        if not np.any(valid_mask):
            logger.warning("No valid market prices found. Returning NaN for all IVs.")
            return result_sigma
        
        # Work only with valid entries
        valid_indices = np.where(valid_mask)[0]
        market_price_valid = market_price[valid_mask]
        S_valid = S[valid_mask]
        K_valid = K[valid_mask]
        T_valid = T[valid_mask]
        option_type_valid = option_type[valid_mask]
        sigma_valid = sigma[valid_mask]
        
        # Track which entries are still being updated
        active_mask = np.ones(len(market_price_valid), dtype=bool)
        
        for iteration in range(max_iterations):
            if not np.any(active_mask):
                break
                
            bs_price = self.black_scholes_price(
                S_valid[active_mask], K_valid[active_mask], T_valid[active_mask], 
                r, sigma_valid[active_mask], option_type_valid[active_mask]
            )
            vega_val = self.vega(
                S_valid[active_mask], K_valid[active_mask], T_valid[active_mask], 
                r, sigma_valid[active_mask]
            )
            
            price_diff = bs_price - market_price_valid[active_mask]
            
            # Only skip if vega is essentially zero
            vega_nonzero = np.abs(vega_val) > 1e-12
            update_indices = np.where(active_mask)[0][vega_nonzero]
            
            if len(update_indices) == 0:
                break
            
            # Update sigma for valid entries
            sigma_update = price_diff[vega_nonzero] / vega_val[vega_nonzero]
            sigma_valid[update_indices] = sigma_valid[update_indices] - sigma_update
            
            # Keep sigma in reasonable bounds
            sigma_valid = np.clip(sigma_valid, 0.001, 10.0)
            
            # Check convergence
            converged = np.abs(sigma_update) < tolerance
            
            # Remove converged entries from active mask
            active_mask[update_indices[converged]] = False
            
            if iteration % 20 == 0:  # Reduced logging frequency
                print(f"Iteration {iteration + 1}: {np.sum(active_mask)} entries still active")
        
        # Store results back in the full array
        result_sigma[valid_indices] = sigma_valid
        
        print(f"Newton-Raphson IV calculation completed")
        print(f"Successfully calculated IV for {np.sum(~np.isnan(result_sigma))} out of {len(market_price)} rows")
        
        return result_sigma
    
    def calculate_greeks(self, S: np.ndarray, K: np.ndarray, T: np.ndarray,
                        r: float, sigma: np.ndarray, option_type: np.ndarray) -> dict:
        
        # More permissive mask - calculate greeks even for low IV values
        valid_mask = ~np.isnan(sigma) & (sigma > 0.001) & (T > 0) & (S > 0) & (K > 0)
        
        greeks = {
            'delta': np.full_like(sigma, np.nan),
            'gamma': np.full_like(sigma, np.nan),
            'theta': np.full_like(sigma, np.nan),
            'vega': np.full_like(sigma, np.nan),
            'rho': np.full_like(sigma, np.nan)
        }
        
        if not np.any(valid_mask):
            logger.warning("No valid entries for Greeks calculation")
            return greeks
        
        print(f"Calculating Greeks for {np.sum(valid_mask)} valid entries")
        
        S_valid = S[valid_mask]
        K_valid = K[valid_mask]
        T_valid = T[valid_mask]
        sigma_valid = sigma[valid_mask]
        option_type_valid = option_type[valid_mask]
        
        d1, d2 = self._d1_d2(S_valid, K_valid, T_valid, r, sigma_valid)
        
        is_call = (option_type_valid == 'call') | (option_type_valid == 'CE')
        
        # Delta
        delta_call = norm.cdf(d1)
        delta_put = norm.cdf(d1) - 1
        delta_val = np.where(is_call, delta_call, delta_put)
        
        # Gamma
        gamma_val = norm.pdf(d1) / (S_valid * sigma_valid * np.sqrt(T_valid))
        
        # Theta
        theta_common = (-S_valid * norm.pdf(d1) * sigma_valid / (2 * np.sqrt(T_valid)) 
                       - r * K_valid * np.exp(-r * T_valid))
        
        theta_call = (theta_common * norm.cdf(d2)) / 365.25
        theta_put = (theta_common * norm.cdf(-d2)) / 365.25
        theta_val = np.where(is_call, theta_call, theta_put)
        
        # Vega
        vega_val = S_valid * norm.pdf(d1) * np.sqrt(T_valid) / 100
        
        # Rho
        rho_call = K_valid * T_valid * np.exp(-r * T_valid) * norm.cdf(d2) / 100
        rho_put = -K_valid * T_valid * np.exp(-r * T_valid) * norm.cdf(-d2) / 100
        rho_val = np.where(is_call, rho_call, rho_put)
        
        # Assign results
        greeks['delta'][valid_mask] = delta_val
        greeks['gamma'][valid_mask] = gamma_val
        greeks['theta'][valid_mask] = theta_val
        greeks['vega'][valid_mask] = vega_val
        greeks['rho'][valid_mask] = rho_val
        
        return greeks
    
    def process_options_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Processing {len(df)} options records")
        
        df = df.copy()
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['expiry'] = pd.to_datetime(df['expiry'])
        
        df['time_to_expiry'] = self._time_to_expiry_minutes(df['timestamp'], df['expiry'])
        
        logger.info("Calculating implied volatility using Newton-Raphson method for ALL rows")
        df['implied_volatility'] = self.newton_raphson_iv(
            market_price=df['close'].values,
            S=df['c'].values,
            K=df['strike'].values,
            T=df['time_to_expiry'].values,
            r=self.risk_free_rate,
            option_type=df['option_type'].values
        )
        
        logger.info("Calculating Greeks for ALL rows")
        greeks = self.calculate_greeks(
            S=df['c'].values,
            K=df['strike'].values,
            T=df['time_to_expiry'].values,
            r=self.risk_free_rate,
            sigma=df['implied_volatility'].values,
            option_type=df['option_type'].values
        )
        
        for greek_name, greek_values in greeks.items():
            df[greek_name] = greek_values
        
        valid_iv_count = (~df['implied_volatility'].isna()).sum()
        valid_greeks_count = (~df['delta'].isna()).sum()
        logger.info(f"Successfully calculated IV for {valid_iv_count}/{len(df)} records")
        logger.info(f"Successfully calculated Greeks for {valid_greeks_count}/{len(df)} records")
        
        return df

def process_parquet_files():
    input_path = Path("/mnt/disk2/cold_storage/processed_master_files/")
    output_path = Path("/mnt/disk2/cold_storage/greeks_master_files/")
    
    if not input_path.exists():
        logger.error(f"Input path does not exist: {input_path}")
        return
    
    output_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created output directory: {output_path}")
    
    calculator = OptionsGreeksCalculator()
    
    parquet_files = list(input_path.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No parquet files found in {input_path}")
        return
    
    logger.info(f"Found {len(parquet_files)} parquet files to process")
    
    for file_path in parquet_files[:1]:
        try:
            logger.info(f"Processing file: {file_path.name}")
            
            df = pd.read_parquet(file_path)
            df = df.tail(100)
            logger.info(f"Loaded {len(df)} records from {file_path.name}")
            
            required_columns = ['timestamp', 'symbol', 'c', 'expiry', 'strike', 
                              'option_type', 'open', 'high', 'low', 'close', 'volume', 'open_interest']
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing columns in {file_path.name}: {missing_columns}")
                continue
            
            df_processed = calculator.process_options_data(df)
            
            output_file = output_path / f"greeks_{file_path.name}"
            df_processed.to_parquet(output_file, index=False)
            logger.info(f"Saved processed data to: {output_file}")
            
            memory_usage = df_processed.memory_usage(deep=True).sum() / 1024 / 1024
            logger.info(f"Memory usage: {memory_usage:.2f} MB")
            
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {str(e)}")
            continue
    
    logger.info("Processing completed successfully")

if __name__ == "__main__":
    logger.info("Starting Options IV and Greeks calculation for ALL rows")
    process_parquet_files()
    logger.info("Process completed")