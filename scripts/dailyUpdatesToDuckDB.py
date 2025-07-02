import requests
import pandas as pd
import numpy as np
import duckdb
import os
import logging
import time
from datetime import datetime, timedelta
from io import StringIO
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

load_dotenv()
DB_PATH = os.getenv("DUCKDB_PATH", "../qode_edw.db")
RISK_FREE_RATE = float(os.getenv("RISK_FREE_RATE", 0.065))

# --- Greeks calculation logic (adapted from test.py) ---
from scipy.stats import norm

def black_scholes_price(S, K, T, r, sigma, option_type):
    if T <= 0:
        return max(S - K, 0) if option_type == 'call' else max(K - S, 0)
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == 'call':
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def vega(S, K, T, r, sigma):
    if T <= 0:
        return 0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return S * norm.pdf(d1) * np.sqrt(T)

def newton_raphson_iv(market_price, S, K, T, r, option_type, max_iterations=100, tolerance=1e-6):
    if T <= 0:
        return np.nan
    intrinsic = max(S - K, 0) if option_type == 'call' else max(K - S, 0)
    if market_price <= intrinsic:
        return np.nan
    sigma = 0.2
    for _ in range(max_iterations):
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
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * norm.cdf(d2))
    else:
        delta = norm.cdf(d1) - 1
        rho = -K * T * np.exp(-r * T) * norm.cdf(-d2)
        theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * norm.cdf(-d2))
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

def process_options_chunk(chunk_df, risk_free_rate=RISK_FREE_RATE):
    result_df = chunk_df.copy()
    mask = ~pd.isna(result_df['c'])
    if not mask.any():
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
        for _, row in active_data.iterrows():
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
            for _, row in iv_valid_data.iterrows():
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

# --- Symbol parsing and table name construction ---
def parse_symbol(symbol, segment):
    # Example symbol: NSE_NIFTY_20240125_21000_CE
    # Returns: exchange, instrument, underlying, expiry, strike, option_type
    parts = symbol.split('_')
    if segment in ['fo', 'bsefo'] or (len(parts) >= 5 and (parts[2].isdigit() or len(parts[2]) == 8)):
        # Options: NSE_NIFTY_20240125_21000_CE
        exchange = parts[0]
        underlying = parts[1]
        expiry = parts[2] if len(parts) > 2 else ''
        strike = parts[3] if len(parts) > 3 else ''
        opt_type = parts[4].lower() if len(parts) > 4 else ''
        instrument = 'Options'
        if opt_type in ['ce', 'call']:
            opt_type = 'call'
        elif opt_type in ['pe', 'put']:
            opt_type = 'put'
        else:
            opt_type = ''
        return exchange, instrument, underlying, expiry, strike, opt_type
    elif segment in ['eq', 'bseeq']:
        exchange = parts[0]
        underlying = parts[1]
        instrument = 'Stocks'
        return exchange, instrument, underlying, '', '', ''
    elif segment in ['ind', 'bseind']:
        exchange = parts[0]
        underlying = parts[1]
        instrument = 'Index'
        return exchange, instrument, underlying, '', '', ''
    else:
        # Fallback
        exchange = parts[0]
        underlying = parts[1] if len(parts) > 1 else ''
        instrument = 'Unknown'
        return exchange, instrument, underlying, '', '', ''

def build_table_name(exchange, instrument, underlying, expiry, strike, opt_type):
    if instrument == 'Options' and expiry and strike and opt_type:
        return f"{exchange}_Options_{underlying}_{expiry}_{strike}_{opt_type}"
    elif instrument == 'Futures':
        return f"{exchange}_Futures_{underlying}"
    elif instrument == 'Index':
        return f"{exchange}_Index_{underlying}"
    elif instrument == 'Stocks':
        return f"{exchange}_Stocks_{underlying}"
    else:
        return f"{exchange}_{instrument}_{underlying}"

# --- DuckDB helpers ---
def get_connection():
    return duckdb.connect(DB_PATH)

def create_table_if_not_exists(conn, table_name, df):
    # Create table with schema based on df columns
    cols = []
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            cols.append(f'"{col}" BIGINT')
        elif pd.api.types.is_float_dtype(dtype):
            cols.append(f'"{col}" DOUBLE')
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            cols.append(f'"{col}" TIMESTAMP')
        else:
            cols.append(f'"{col}" VARCHAR')
    col_str = ', '.join(cols)
    sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({col_str});'
    conn.execute(sql)

def append_data_to_table(conn, df, table_name):
    create_table_if_not_exists(conn, table_name, df)
    try:
        conn.execute("BEGIN TRANSACTION")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        conn.execute("COMMIT")
        logger.info(f"Appended {len(df)} rows to {table_name}")
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Failed to append to {table_name}: {e}")

# --- Data Fetching ---
def get_auth_token(username, password):
    url = "https://auth.truedata.in/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "username": username,
        "password": password,
        "grant_type": "password"
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        token_data = response.json()
        return token_data.get("access_token")
    else:
        raise Exception(f"Failed to fetch the token. Status code: {response.status_code}")

def generate_timestamps(start_time_str, end_time_str, time_format="%y%m%dT%H:%M"):
    start_time = datetime.strptime(start_time_str, time_format)
    end_time = datetime.strptime(end_time_str, time_format)
    timestamps = []
    current_time = start_time
    while current_time <= end_time:
        timestamps.append(current_time.strftime(time_format))
        current_time += timedelta(minutes=1)
    return timestamps

def fetch_data_for_segment(token, segment, timestamps):
    for timestamp in timestamps:
        url = f"https://history.truedata.in/getAllBars?segment={segment}&timestamp={timestamp}&response=csv"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            csv_content = StringIO(response.text)
            data = pd.read_csv(csv_content)
            if data.empty:
                logger.info(f"No data for segment {segment} at {timestamp}")
                continue
            
            data = data.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'})
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            
            group_col = 'symbolid' if 'symbolid' in data.columns else 'symbol'
            for _, group in data.groupby(group_col):
                symbol = group['symbol'].iloc[0]
                exchange, instrument, underlying, expiry, strike, opt_type = parse_symbol(symbol, segment)
                table_name = build_table_name(exchange, instrument, underlying, expiry, strike, opt_type)
                df_to_store = group.copy()
                
                if instrument == 'Options':
                    for col in ['expiry', 'strike', 'option_type']:
                        if col not in df_to_store.columns:
                            if col == 'expiry':
                                df_to_store['expiry'] = expiry
                            elif col == 'strike':
                                df_to_store['strike'] = float(strike) if strike else np.nan
                            elif col == 'option_type':
                                df_to_store['option_type'] = opt_type
                    df_to_store = process_options_chunk(df_to_store)
                    print(df_to_store.head())
                
                # with get_connection() as conn:
                #     append_data_to_table(conn, df_to_store, table_name)
            logger.info(f"Data for segment {segment} and timestamp {timestamp} saved")
        else:
            logger.error(f"Failed to fetch data for segment {segment} at {timestamp}. Status code: {response.status_code}")
        time.sleep(2)

def fetch_data(token, segments, timestamps):
    for segment in segments:
        logger.info(f"Fetching data for segment: {segment}")
        fetch_data_for_segment(token, segment, timestamps)
        logger.info(f"Completed data fetch for segment: {segment}")

if __name__ == "__main__":
    dt_start = (datetime.now() - pd.DateOffset(months=6)).replace(hour=9, minute=15)
    dt_end = datetime.now().replace(hour=15, minute=30)
    start_time = dt_start.strftime("%y%m%dT%H:%M")
    end_time = dt_end.strftime("%y%m%dT%H:%M")
    username = os.getenv("TRUEDATA_LOGIN_ID")
    password = os.getenv("TRUEDATA_LOGIN_PWD")
    token = get_auth_token(username, password)
    segments = os.getenv("TRUEDATA_SEGMENTS", "fo,bsefo,eq,ind").split(",")
    timestamps = generate_timestamps(start_time, end_time)
    fetch_data(token, segments, timestamps)
    logger.info("All data fetch and storage complete.")