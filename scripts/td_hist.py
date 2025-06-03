import requests
from datetime import datetime, timedelta
import time
import direct_redis
import pandas as pd
from io import StringIO
import concurrent.futures
r = direct_redis.DirectRedis(host='localhost', port=6379, db=0)

# segments = eq, fo, ind
tdsymbolidTOsymbol = r.get('tdsymbolidTOsymbol')

def store_in_redis(df, max_workers=4):
    print(f'df length : {len(df)}, columns : {df.columns}')
    
    if df.empty:
        return
    
    df = df.rename(columns={'open': 'o', 'high': 'h', 'low': 'l', 'close': 'c', 'volume': 'v'})
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    def process_row(row):
        symbolid = row['symbolid']

        symbol = tdsymbolidTOsymbol[symbolid]
        timestamp = str(row['timestamp'])
        values = {
            'o': row['o'],
            'h': row['h'],
            'l': row['l'],
            'c': row['c'],
            'v': row['v'],
            'oi': row['oi']
        }
        r.hset(f"l.tick_{timestamp}", symbol, values)
        r.hset(f'l.{symbol}', timestamp, values)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, row) for _, row in df.iterrows()]
        concurrent.futures.wait(futures)  

def generate_timestamps(start_time_str, end_time_str, time_format="%y%m%dT%H:%M"):
    start_time = datetime.strptime(start_time_str, time_format)
    end_time = datetime.strptime(end_time_str, time_format)
    
    timestamps = []
    
    current_time = start_time
    while current_time <= end_time:
        timestamps.append(current_time.strftime(time_format))
        current_time += timedelta(minutes=1)
    
    return timestamps

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
        raise Exception("Failed to fetch the token. Status code: {}".format(response.status_code))

def fetch_data_for_segment(token, segment, timestamps):
    for timestamp in timestamps:
        url = f"https://history.truedata.in/getAllBars?segment={segment}&timestamp={timestamp}&response=csv"
        headers = {"Authorization": f"Bearer {token}"}
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            csv_content = StringIO(response.text)
            data = pd.read_csv(csv_content)
            store_in_redis(data)

            print(f"Data for segment {segment} and timestamp {timestamp} saved")
        else:
            print(f"Failed to fetch data for segment {segment}. Status code: {response.status_code}")

        time.sleep(2)

def fetch_data(token, segments, timestamps):
    
    for segment in segments:
        fetch_data_for_segment(token, segment, timestamps)

if __name__ == "__main__":
    dt_start = datetime.now().replace(hour=9, minute=15)
    dt_end = datetime.now().replace(hour=15, minute=30)

    start_time = dt_start.strftime("%y%m%dT%H:%M")
    end_time = dt_end.strftime("%y%m%dT%H:%M")

    username = "tdwsf575"
    password = "vidhi@575"

    token = get_auth_token(username, password)
    
    segments = ['bsefo']  # Example segments
    # 'eq', 'bseeq', , 'bseind', 'bsefo' 'ind', 'fo'
    # segments = ['ind', 'bseind']
    
    timestamps = generate_timestamps(start_time, end_time)
    
    fetch_data(token, segments, timestamps)
