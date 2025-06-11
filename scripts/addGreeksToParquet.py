import pandas as pd
import numpy as np
from scipy.optimize import minimize_scalar, brentq
from scipy.stats import norm
import logging
from tqdm import tqdm
import warnings
from datetime import datetime, time
import pytz
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IndianOptionsCalculator:
    def __init__(self):
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.market_open = time(9, 15)
        self.market_close = time(15, 30)
        self.expiry_time = time(15, 30)
        self.min_volatility = 0.001
        self.max_volatility = 10.0
        self.min_time_to_expiry = 1/(365.25 * 24 * 60)
        self.tolerance = 1e-8
        
    def is_market_hours(self, timestamp):
        try:
            if pd.isna(timestamp):
                return False
            
            if timestamp.tz is None:
                ist_time = timestamp.tz_localize('UTC').tz_convert(self.ist_tz).time()
            else:
                ist_time = timestamp.tz_convert(self.ist_tz).time()
            
            return self.market_open <= ist_time <= self.market_close
        except Exception:
            return True
    
    def calculate_time_to_expiry(self, current_time, expiry_date):
        try:
            if pd.isna(current_time) or pd.isna(expiry_date):
                return np.nan
            
            current_time = pd.to_datetime(current_time)
            expiry_date = pd.to_datetime(expiry_date)
            
            if current_time.tz is None:
                current_time = current_time.tz_localize('UTC').tz_convert(self.ist_tz)
            else:
                current_time = current_time.tz_convert(self.ist_tz)
            
            if expiry_date.tz is None:
                expiry_date = expiry_date.tz_localize('UTC').tz_convert(self.ist_tz)
            else:
                expiry_date = expiry_date.tz_convert(self.ist_tz)
            
            if current_time >= expiry_date:
                return self.min_time_to_expiry
            
            if expiry_date.time() == time(0, 0):
                expiry_datetime = expiry_date.replace(hour=15, minute=30)
            else:
                expiry_datetime = expiry_date
            
            time_diff = (expiry_datetime - current_time).total_seconds()
            time_in_years = time_diff / (365.25 * 24 * 3600)
            
            return max(self.min_time_to_expiry, time_in_years)
        except Exception:
            try:
                current_time = pd.to_datetime(current_time)
                expiry_date = pd.to_datetime(expiry_date)
                
                if current_time >= expiry_date:
                    return self.min_time_to_expiry
                
                time_diff = (expiry_date - current_time).total_seconds()
                time_in_years = time_diff / (365.25 * 24 * 3600)
                
                return max(self.min_time_to_expiry, time_in_years)
            except Exception:
                return np.nan

    def black_scholes_call(self, S, K, T, r, sigma):
        try:
            if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
                return np.nan
            
            sqrt_T = np.sqrt(T)
            sigma_sqrt_T = sigma * sqrt_T
            
            if sigma_sqrt_T < 1e-10:
                return max(0, S - K * np.exp(-r * T))
            
            d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / sigma_sqrt_T
            d2 = d1 - sigma_sqrt_T
            
            if abs(d1) > 10 or abs(d2) > 10:
                if S > K * np.exp(-r * T):
                    return S - K * np.exp(-r * T)
                else:
                    return 0.0
            
            call_price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
            return max(0, call_price)
            
        except (OverflowError, ZeroDivisionError, ValueError):
            return np.nan

    def black_scholes_put(self, S, K, T, r, sigma):
        try:
            if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
                return np.nan
            
            sqrt_T = np.sqrt(T)
            sigma_sqrt_T = sigma * sqrt_T
            
            if sigma_sqrt_T < 1e-10:
                return max(0, K * np.exp(-r * T) - S)
            
            d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / sigma_sqrt_T
            d2 = d1 - sigma_sqrt_T
            
            if abs(d1) > 10 or abs(d2) > 10:
                if K * np.exp(-r * T) > S:
                    return K * np.exp(-r * T) - S
                else:
                    return 0.0
            
            put_price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
            return max(0, put_price)
            
        except (OverflowError, ZeroDivisionError, ValueError):
            return np.nan

    def calculate_vega(self, S, K, T, r, sigma):
        try:
            if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
                return 0
            
            sqrt_T = np.sqrt(T)
            d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt_T)
            
            if abs(d1) > 10:
                return 0
            
            vega = S * norm.pdf(d1) * sqrt_T
            return vega if not np.isnan(vega) else 0
            
        except (OverflowError, ZeroDivisionError, ValueError):
            return 0

    def calculate_greeks(self, S, K, T, r, sigma, option_type):
        try:
            if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
                return [np.nan] * 5
            
            sqrt_T = np.sqrt(T)
            sigma_sqrt_T = sigma * sqrt_T
            d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / sigma_sqrt_T
            d2 = d1 - sigma_sqrt_T
            
            if abs(d1) > 10 or abs(d2) > 10:
                return [np.nan] * 5
            
            pdf_d1 = norm.pdf(d1)
            cdf_d1 = norm.cdf(d1)
            cdf_d2 = norm.cdf(d2)
            cdf_neg_d1 = norm.cdf(-d1)
            cdf_neg_d2 = norm.cdf(-d2)
            
            gamma = pdf_d1 / (S * sigma_sqrt_T) if sigma_sqrt_T > 0 else 0
            vega = S * pdf_d1 * sqrt_T / 100
            
            exp_neg_rT = np.exp(-r * T)
            theta_common = -(S * pdf_d1 * sigma) / (2 * sqrt_T)
            theta_call = theta_common - r * K * exp_neg_rT * cdf_d2
            theta_put = theta_common + r * K * exp_neg_rT * cdf_neg_d2
            
            if option_type.lower() == 'call':
                delta = cdf_d1
                theta = theta_call / 365
                rho = K * T * exp_neg_rT * cdf_d2 / 100
            else:
                delta = -cdf_neg_d1
                theta = theta_put / 365
                rho = -K * T * exp_neg_rT * cdf_neg_d2 / 100
            
            return delta, gamma, theta, vega, rho
            
        except (OverflowError, ZeroDivisionError, ValueError):
            return [np.nan] * 5

    def intrinsic_value(self, S, K, option_type):
        try:
            if S <= 0 or K <= 0:
                return 0
            if option_type.lower() == 'call':
                return max(0, S - K)
            else:
                return max(0, K - S)
        except Exception:
            return 0

    def get_underlying_price(self, df, symbol, timestamp):
        try:
            symbol_data = df[df['symbol'] == symbol]
            
            if len(symbol_data) == 0:
                return None
            
            closest_data = symbol_data.loc[(symbol_data['timestamp'] - timestamp).abs().idxmin()]
            return closest_data['close']
            
        except Exception:
            return None

    def implied_volatility_brent(self, market_price, S, K, T, r, option_type):
        try:
            if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
                return np.nan
            
            intrinsic = self.intrinsic_value(S, K, option_type)
            if market_price <= intrinsic * 1.001:
                return self.min_volatility
            
            pricing_func = self.black_scholes_call if option_type.lower() == 'call' else self.black_scholes_put
            
            def objective(sigma):
                theoretical_price = pricing_func(S, K, T, r, sigma)
                if np.isnan(theoretical_price):
                    return market_price
                return theoretical_price - market_price
            
            try:
                moneyness = S / K
                atm_vol = 0.2
                
                if option_type.lower() == 'call':
                    if moneyness > 1.1:
                        lower_bound = 0.05
                        upper_bound = 1.0
                    elif moneyness < 0.9:
                        lower_bound = 0.1
                        upper_bound = 2.0
                    else:
                        lower_bound = 0.1
                        upper_bound = 1.5
                else:
                    if moneyness < 0.9:
                        lower_bound = 0.05
                        upper_bound = 1.0
                    elif moneyness > 1.1:
                        lower_bound = 0.1
                        upper_bound = 2.0
                    else:
                        lower_bound = 0.1
                        upper_bound = 1.5
                
                if T < 0.05:
                    upper_bound = min(upper_bound, 3.0)
                
                obj_lower = objective(lower_bound)
                obj_upper = objective(upper_bound)
                
                if obj_lower * obj_upper > 0:
                    if abs(obj_lower) < abs(obj_upper):
                        return lower_bound
                    else:
                        return upper_bound
                
                iv = brentq(objective, lower_bound, upper_bound, xtol=1e-6, maxiter=100)
                return np.clip(iv, self.min_volatility, self.max_volatility)
                
            except (ValueError, RuntimeError):
                result = minimize_scalar(lambda x: abs(objective(x)), 
                                       bounds=(0.001, 5.0), method='bounded')
                if result.success and abs(result.fun) < market_price * 0.1:
                    return np.clip(result.x, self.min_volatility, self.max_volatility)
                return np.nan
                
        except Exception:
            return np.nan

    def implied_volatility_newton_raphson(self, market_price, S, K, T, r, option_type, max_iterations=50):
        try:
            if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
                return np.nan
            
            intrinsic = self.intrinsic_value(S, K, option_type)
            if market_price <= intrinsic * 1.001:
                return self.min_volatility
            
            pricing_func = self.black_scholes_call if option_type.lower() == 'call' else self.black_scholes_put
            
            moneyness = S / K
            time_factor = max(0.1, np.sqrt(T))
            
            if option_type.lower() == 'call':
                if moneyness > 1.2:
                    sigma = 0.15 * time_factor
                elif moneyness < 0.8:
                    sigma = 0.4 * time_factor
                else:
                    sigma = 0.25 * time_factor
            else:
                if moneyness < 0.8:
                    sigma = 0.15 * time_factor
                elif moneyness > 1.2:
                    sigma = 0.4 * time_factor
                else:
                    sigma = 0.25 * time_factor
            
            sigma = np.clip(sigma, 0.05, 2.0)
            
            for i in range(max_iterations):
                current_price = pricing_func(S, K, T, r, sigma)
                if np.isnan(current_price):
                    return self.implied_volatility_brent(market_price, S, K, T, r, option_type)
                
                vega = self.calculate_vega(S, K, T, r, sigma)
                
                if abs(vega) < 1e-10:
                    return self.implied_volatility_brent(market_price, S, K, T, r, option_type)
                
                price_diff = current_price - market_price
                
                if abs(price_diff) < market_price * 1e-6:
                    return np.clip(sigma, self.min_volatility, self.max_volatility)
                
                sigma_new = sigma - price_diff / vega
                sigma_new = np.clip(sigma_new, self.min_volatility, self.max_volatility)
                
                if abs(sigma_new - sigma) < self.tolerance:
                    return sigma_new
                
                sigma = sigma_new
            
            return self.implied_volatility_brent(market_price, S, K, T, r, option_type)
            
        except Exception:
            return np.nan

    def calculate_historical_volatility(self, prices, window=21):
        try:
            if len(prices) < 2:
                return np.nan
            
            prices = np.array(prices)
            valid_prices = prices[~np.isnan(prices)]
            valid_prices = valid_prices[valid_prices > 0]
            
            if len(valid_prices) < 2:
                return np.nan
            
            returns = np.log(valid_prices[1:] / valid_prices[:-1])
            returns = returns[~np.isnan(returns)]
            returns = returns[np.isfinite(returns)]
            
            if len(returns) == 0:
                return np.nan
            
            window_size = min(window, len(returns))
            if window_size < 2:
                return np.std(returns) * np.sqrt(252) if len(returns) > 0 else np.nan
            
            rolling_std = pd.Series(returns).rolling(window=window_size, min_periods=2).std()
            last_std = rolling_std.iloc[-1]
            
            if pd.isna(last_std):
                return np.std(returns) * np.sqrt(252)
            
            return last_std * np.sqrt(252)
            
        except Exception:
            return np.nan

    def get_indian_risk_free_rate(self, timestamp=None):
        try:
            if timestamp is None:
                return 0.06
            
            if hasattr(timestamp, 'year'):
                year = timestamp.year
            else:
                try:
                    ts = pd.to_datetime(timestamp)
                    year = ts.year
                except Exception:
                    year = datetime.now().year
            
            rate_mapping = {
                2020: 0.04, 2021: 0.035, 2022: 0.055, 2023: 0.065,
                2024: 0.070, 2025: 0.065
            }
            
            return rate_mapping.get(year, 0.06)
            
        except Exception:
            return 0.06

    def process_options_data(self, file_path, output_path=None, chunk_size=10000):
        logger.info(f"Loading data from {file_path}")
        
        try:
            df = pd.read_parquet(file_path)
            df = df.head(10000)
            logger.info(f"Original data shape: {df.shape}")
            
            df = self.clean_data(df)
            
            if df.empty:
                logger.error("No valid data remaining after cleaning")
                return pd.DataFrame()
            
            df = self.calculate_time_features(df)
            df = self.get_underlying_prices(df)
            df = self.calculate_implied_volatility_batch(df, chunk_size)
            df = self.calculate_greeks_batch(df)
            
            print(f"Processed data shape: {df.shape}")
            logger.info(f"Processed data head:\n{df.head()}")

            df.drop(columns=['time_to_expiry_years', 'time_to_expiry_days', 'is_market_hours'], inplace=True, errors='ignore')

            if output_path:
                df.to_parquet(output_path, index=False)
                logger.info(f"Results saved to {output_path}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            return pd.DataFrame()

    def clean_data(self, df):
        logger.info("Cleaning data")
        
        if 'underlying' in df.columns:
            df = df.drop('underlying', axis=1)
        
        required_cols = ['timestamp', 'expiry', 'close', 'strike', 'option_type', 'symbol']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return pd.DataFrame()
        
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df['expiry'] = pd.to_datetime(df['expiry'], errors='coerce')
        except Exception as e:
            logger.error(f"Error parsing dates: {str(e)}")
            return pd.DataFrame()
        
        df = df.dropna(subset=['timestamp', 'expiry', 'close', 'strike', 'option_type'])
        
        numeric_cols = ['close', 'strike']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df[(df['close'] > 0) & (df['strike'] > 0)]
        df = df[df['option_type'].str.lower().isin(['call', 'put'])]
        
        df = df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)
        
        logger.info(f"Cleaned data shape: {df.shape}")
        return df

    def calculate_time_features(self, df):
        logger.info("Calculating time features")
        
        df['time_to_expiry_years'] = df.apply(
            lambda row: self.calculate_time_to_expiry(row['timestamp'], row['expiry']), 
            axis=1
        )
        
        df['time_to_expiry_days'] = df['time_to_expiry_years'] * 365.25
        df['is_market_hours'] = df['timestamp'].apply(self.is_market_hours)
        
        df = df[df['time_to_expiry_years'] > self.min_time_to_expiry]
        
        logger.info(f"Data shape after time filtering: {df.shape}")
        return df

    def get_underlying_prices(self, df):
        logger.info("Getting underlying prices")
        
        df['underlying_symbol'] = df['symbol'].str.extract(r'^([A-Z]+)')
        
        underlying_prices = {}
        
        for symbol_group in df['underlying_symbol'].unique():
            if pd.isna(symbol_group):
                continue
                
            symbol_data = df[df['underlying_symbol'] == symbol_group].copy()
            
            median_price_by_time = symbol_data.groupby('timestamp')['close'].median()
            
            for timestamp, median_price in median_price_by_time.items():
                underlying_prices[(symbol_group, timestamp)] = median_price
        
        df['underlying_price'] = df.apply(
            lambda row: underlying_prices.get((row['underlying_symbol'], row['timestamp']), row['close']), 
            axis=1
        )
        
        return df

    def calculate_implied_volatility_batch(self, df, chunk_size):
        logger.info("Calculating implied volatility")
        
        iv_results = []
        total_chunks = (len(df) + chunk_size - 1) // chunk_size
        
        for i in tqdm(range(0, len(df), chunk_size), desc="Implied Volatility", total=total_chunks):
            end_idx = min(i + chunk_size, len(df))
            chunk = df.iloc[i:end_idx].copy()
            
            chunk_iv = []
            for _, row in chunk.iterrows():
                risk_free_rate = self.get_indian_risk_free_rate(row['timestamp'])
                
                underlying_price = row.get('underlying_price', row['close'])
                
                iv = self.implied_volatility_newton_raphson(
                    row['close'], underlying_price, row['strike'], 
                    row['time_to_expiry_years'], risk_free_rate, 
                    row['option_type']
                )
                chunk_iv.append(iv)
            
            iv_results.extend(chunk_iv)
        
        df['implied_volatility'] = iv_results
        
        valid_iv_count = pd.Series(iv_results).notna().sum()
        logger.info(f"Valid implied volatility calculations: {valid_iv_count}/{len(df)}")
        
        return df

    def calculate_greeks_batch(self, df):
        logger.info("Calculating Greeks")
        
        greeks_data = {'delta': [], 'gamma': [], 'theta': [], 'vega': [], 'rho': []}
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Greeks"):
            if pd.notna(row['implied_volatility']):
                risk_free_rate = self.get_indian_risk_free_rate(row['timestamp'])
                underlying_price = row.get('underlying_price', row['close'])
                
                greeks = self.calculate_greeks(
                    underlying_price, row['strike'], row['time_to_expiry_years'],
                    risk_free_rate, row['implied_volatility'], row['option_type']
                )
                
                for i, greek_name in enumerate(greeks_data.keys()):
                    greeks_data[greek_name].append(greeks[i])
            else:
                for greek_name in greeks_data.keys():
                    greeks_data[greek_name].append(np.nan)
        
        for greek_name, values in greeks_data.items():
            df[greek_name] = values
        
        return df

def main():
    calculator = IndianOptionsCalculator()
    
    input_file = "/mnt/disk2/cold_storage/master_parquet_files/finnifty_options_master.parquet"
    output_file = "/mnt/disk2/cold_storage/master_parquet_files/finnifty_options_processed.parquet"
    
    try:        
        result_df = calculator.process_options_data(input_file, output_file)
        
        if not result_df.empty:
            print(f"Processing completed successfully!")
            print(f"Processed {len(result_df)} records")
            print(f"Valid IV calculations: {result_df['implied_volatility'].notna().sum()}")
            print(f"Data shape: {result_df.shape}")
            print(f"Columns: {list(result_df.columns)}")
            
            if len(result_df) > 0:
                print("\nSample statistics:")
                numeric_cols = result_df.select_dtypes(include=[np.number]).columns
                print(result_df[numeric_cols].describe())
        else:
            print("No data was processed successfully")
            
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        logger.error(f"Main execution error: {str(e)}")

if __name__ == "__main__":
    main()