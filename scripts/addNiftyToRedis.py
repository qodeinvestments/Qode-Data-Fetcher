import os
import pandas as pd
import direct_redis
from glob import glob
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

r = direct_redis.DirectRedis(host='localhost', port=6379, db=0)

PARQUET_DIR = "/mnt/disk2/cold_storage/NSE/Index/NIFTY"
INDEX_FILE = os.path.join(PARQUET_DIR, "NSE_Index_NIFTY.parquet")
OPTIONS_MASTER_FILE = os.path.join(PARQUET_DIR, "NSE_Options_NIFTY_Master.parquet")

def update_instruments(symbol, timestamp):
    current = r.hget('instruments', symbol)
    if current:
        timestamps = set(current.decode().split(','))
    else:
        timestamps = set()
    timestamps.add(str(timestamp))
    r.hset('instruments', symbol, ','.join(sorted(timestamps)))

def store_index_to_redis(df):
    logger.info(f"Storing {len(df)} NIFTY Index rows to Redis")
    for row in df.itertuples():
        timestamp = str(row.timestamp)
        values = {
            'o': row.o,
            'h': row.h,
            'l': row.l,
            'c': row.c,
        }
        r.hset(f"NIFTYSPOT", timestamp, values)
        r.hset(f"tick_{timestamp}", "NIFTY", values)
        r.sadd("instruments", "NIFTY")
        update_instruments("NIFTY", timestamp)

def format_symbol(row):
    expiry = pd.to_datetime(row.expiry)
    expiry_str = expiry.strftime('%y%m%d')
    strike = int(row.strike)
    option_type = 'CE' if row.option_type == 'call' else 'PE'
    return f"NIFTY{expiry_str}{strike}{option_type}"

def store_options_to_redis(df):
    logger.info(f"Storing {len(df)} NIFTY Options rows to Redis (processed_master_files)")
    for row in df.itertuples():
        symbol = format_symbol(row)
        timestamp = str(row.timestamp)
        values = {
            'o': row.open,
            'h': row.high,
            'l': row.low,
            'c': row.close,
            'v': row.volume,
            'oi': row.open_interest
        }
        r.hset(f"{symbol}", timestamp, values)
        r.hset(f"tick_{timestamp}", symbol, values)
        update_instruments(symbol, timestamp)

def store_greeks_options_to_redis(df):
    logger.info(f"Storing {len(df)} NIFTY Options rows to Redis (greeks_master_files)")
    for row in df.itertuples():
        symbol = format_symbol(row)
        timestamp = str(row.timestamp)
        values = {
            'o': row.o,
            'h': row.h,
            'l': row.l,
            'c': row.c,
            'v': row.v,
            'oi': row.oi,
            'iv': getattr(row, 'iv', None),
            'delta': getattr(row, 'delta', None),
            'gamma': getattr(row, 'gamma', None),
            'theta': getattr(row, 'theta', None),
            'vega': getattr(row, 'vega', None),
            'rho': getattr(row, 'rho', None)
        }
        # Remove None values
        values = {k: v for k, v in values.items() if v is not None}
        r.hset(f"{symbol}", timestamp, values)
        r.hset(f"tick_{timestamp}", symbol, values)
        update_instruments(symbol, timestamp)

def main():
    if os.path.exists(INDEX_FILE):
        df_index = pd.read_parquet(INDEX_FILE)
        one_year_ago = pd.Timestamp.now() - pd.Timedelta(days=365)
        df_index = df_index[df_index['timestamp'] >= one_year_ago]
        store_index_to_redis(df_index)
    else:
        logger.error(f"Index file not found: {INDEX_FILE}")

    # greeks_dir = "/mnt/disk2/cold_storage/greeks_master_files/"
    # if os.path.exists(greeks_dir):
    #     greeks_files = glob(os.path.join(greeks_dir, "*.parquet"))
    #     for file in greeks_files:
    #         logger.info(f"Processing (greeks) {file}")
    #         df_options = pd.read_parquet(file)
    #         one_year_ago = pd.Timestamp.now() - pd.Timedelta(days=365)
    #         df_options = df_options[df_options['timestamp'] >= one_year_ago]
    #         store_greeks_options_to_redis(df_options)
    # else:
    #     logger.warning(f"Greeks master dir not found: {greeks_dir}")

    # Process processed_master_files
    processed_dir = "/mnt/disk2/cold_storage/processed_master_files/"
    if os.path.exists(processed_dir):
        processed_files = glob(os.path.join(processed_dir, "*.parquet"))
        for file in processed_files:
            print(file)
            if 'nifty_options_master_processed.parquet' in file:
                logger.info(f"Processing (processed) {file}")
                df_options = pd.read_parquet(file)
                one_year_ago = pd.Timestamp.now() - pd.Timedelta(days=365)
                df_options = df_options[df_options['timestamp'] >= one_year_ago]
                store_options_to_redis(df_options)
    else:
        logger.warning(f"Processed master dir not found: {processed_dir}")

if __name__ == "__main__":
    main()