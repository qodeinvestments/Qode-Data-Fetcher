
from truedata_ws.websocket.TD import TD
import requests, logging, datetime, os, pandas as pd
import direct_redis
import xxx.definitions as defs

r = direct_redis.DirectRedis()

td_login_id = "tdwsf575"
td_password = "vidhi@575"

start_date = datetime.datetime.now().date() - datetime.timedelta(days=31)
end_date = datetime.datetime.now().date() + datetime.timedelta(days=31)
folder_name = f'{str("2025-01-23").replace("-", "")}_{str("present").replace("-", "")}'

td_obj = TD(td_login_id, td_password, live_port=None,
            log_level=logging.WARNING, log_format="%(message)s")


def get_all_symbols_in_option_chain(underlying, expiry='YYYYMMDD'):
    url = f"https://api.truedata.in/getOptionChain?user={td_login_id}&password={td_password}&symbol={underlying}&expiry={expiry}"
    r = requests.get(url)
    return pd.DataFrame(r.json()['Records'])

def get_1month_data_of_given_symbol(symbol):
    hd = pd.DataFrame(td_obj.get_historic_data(symbol, duration='1 M'))
    return hd

def get_all_data_of_given_symbol(symbol):
  list_of_hds = []
  end_time = datetime.datetime.now()
  while True:
      print(end_time)
      hd = pd.DataFrame(td_obj.get_historic_data(symbol, duration='1 M', end_time=end_time))
      if len(list_of_hds) == 0:
          list_of_hds.append(hd)
          end_time = hd.iloc[0]['time']
      else:
          prev_hd = list_of_hds[-1]
          hd = hd[hd['time']<prev_hd.iloc[0]['time']].copy()
          if len(hd) == 0:
              print('done')
              break
          list_of_hds.append(hd)
          end_time = hd.iloc[0]['time']
  hd = pd.concat(list_of_hds).sort_values('time')
  return hd

indexes = [
    ('INDIA VIX', 'INDIAVIX'),
    ('NIFTY MID SELECT', 'MIDCPNIFTYSPOT'),
    ('NIFTY FIN SERVICE', 'FINNIFTYSPOT'),
    ('NIFTY 50', 'NIFTYSPOT'),
    ('NIFTY BANK', 'BANKNIFTYSPOT'),
    ('SENSEX','SENSEXSPOT'),
    ('BANKEX', 'BANKEXSPOT'),
    
    ('BANKNIFTY-I', 'BANKNIFTY-I'),
    ('BANKNIFTY-II', 'BANKNIFTY-II'),
    ('BANKNIFTY-III', 'BANKNIFTY-III'),
# 
    # ('SENSEX-I', 'SENSEX-I'),
    # ('SENSEX-II', 'SENSEX-II'),
    # ('SENSEX-III', 'SENSEX-III'),

    ('NIFTY-I', 'NIFTY-I'),
    ('NIFTY-II', 'NIFTY-II'),
    ('NIFTY-III', 'NIFTY-III'),

    ('FINNIFTY-I', 'FINNIFTY-I'),
    # ('FINNIFTY-II', 'FINNIFTY-II'),
    # ('FINNIFTY-III', 'FINNIFTY-III'),

    ('MIDCPNIFTY-I', 'MIDCPNIFTY-I'),
    # ('MIDCPNIFTY-II', 'MIDCPNIFTY-II'),
    # ('MIDCPNIFTY-III', 'MIDCPNIFTY-III'),

    # ('BANKEX-I', 'BANKEX-I'),
    # ('BANKEX-II', 'BANKEX-II'),
    # ('BANKEX-III', 'BANKEX-III'),
    
]


if os.path.exists(f'{defs.TRUEDATA_DAILY_DATA_PATH}{folder_name}'):
    pass
else:
    os.makedirs(f'{defs.TRUEDATA_DAILY_DATA_PATH}{folder_name}')


index_dict = {}
for i in indexes:
    print(i)
    symbol = i[0]
    my_symbol = i[1]
    df = get_all_data_of_given_symbol(symbol)
    # TAKING SMALL SUBSET OF RECENT PAST
    #df = df.tail(375*10).copy()
    df.columns = 'timestamp o h l c v oi'.split()
    #df['date'] = df['timestamp'].apply(lambda x: x.date())
    #df['weekday'] = df['date'].apply(lambda x: x.weekday())    
    # index_dict[my_symbol ] = df
    #df = df[df['date']>=datetime.date(2023,6,25)].copy()
    #asdfadsf
    
    old_data = r.hget('instrument', my_symbol)
    if old_data is not None and len(old_data) > 0 :
        df = pd.concat([old_data, df]).drop_duplicates('timestamp').sort_values('timestamp').reset_index(drop=True)
        df = df[df.timestamp.dt.time < datetime.time(15, 30)].reset_index(drop=True)
    r.hset('instrument', my_symbol, df)

    # with gzip.open(f'{defs.TRUEDATA_DAILY_DATA_PATH}{folder_name}/{i}.pkl.gz', 'wb') as file:
    #     df.to_pickle(file)

    # only keep data after last_saved_date !!!
    # print('here - ', df)
    # df = df[df['timestamp'].apply(lambda x: x.date() > datetime.date(2023, 1, 1))].copy()

    for x in df.to_dict('records'):
        if r.hexists(f"tick_{x['timestamp']}", my_symbol):
            continue
        print(f"{x['timestamp']} {my_symbol}", sep=' ', end='\r', flush=True)
        r.hset(f"tick_{x['timestamp']}", my_symbol, x)


    # index_dict[my_symbol] = df
    #r.hset('index_dict', my_symbol, df)
# for i in index_dict:
#     df = index_dict[i]
    # break

expiries_dict = r.hgetall('list_of_expiries')


symbols_already_done = []# list(index_dict.keys())#r.hkeys('index_dict')



for underlying, list_of_expiries in expiries_dict.items():
    if underlying not in ['BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTY', 'SENSEX', 'BANKEX']:
        continue
    list_of_expiries.sort()
    for expiry_date in list_of_expiries:
        # clear_output()
        # print(expiry_date)
        # time.sleep(3)
        # expiry_date = datetime.datetime.strptime(expiry_date, '%Y-%m-%d').date()
        
        if expiry_date < start_date: 
            # print('skipping')
            continue

        if expiry_date > end_date:
            continue
        
        symbols = get_all_symbols_in_option_chain(underlying, str(expiry_date).replace('-',''))
        list_of_symbols = list(symbols[1])
        for symbol in list_of_symbols[:]:
            
            if symbol in symbols_already_done:
                continue
            
            if os.path.exists(f'{defs.TRUEDATA_DAILY_DATA_PATH}{folder_name}/{symbol}.pkl.gz'):
                print('skipping')
                continue

            print(symbol, expiry_date)
            hd = pd.DataFrame(
                td_obj.get_historic_data(
                    symbol,
                    duration='1 M',
                    end_time=datetime.datetime.combine(expiry_date, datetime.time(15, 30))
                )
            )
            print(len(hd))
            if len(hd) == 0:
                hd = pd.DataFrame(columns='timestamp o h l c v oi'.split())
            else:
                hd.columns = 'timestamp o h l c v oi'.split()
            # index_dict[symbol] = hd

            old_data = r.hget('instrument', symbol)
            if old_data is not None and len(old_data) > 0:
                # old_data = pd.DataFrame(old_data)
                hd = pd.concat([old_data, hd]).drop_duplicates('timestamp').sort_values('timestamp').reset_index(drop=True)

            #     df = pd.read_pickle(f'{defs.TRUEDATA_DAILY_DATA_PATH}{folder_name}/{symbol}.pkl.gz')
            #     hd = pd.concat([df, hd]).drop_duplicates().sort_values('timestamp').reset_index(drop=True)

            
            if len(hd) > 0:

                # with gzip.open(f'{defs.TRUEDATA_DAILY_DATA_PATH}{folder_name}/{symbol}.pkl.gz', 'wb') as file:
                #     hd.to_pickle(file)

                r.hset('instrument', symbol, hd)

                for x in hd.to_dict('records'):
                    # if r.hexists(f"tick_{x['timestamp']}", symbol):
                    #     continue
                    r.hset(f"tick_{x['timestamp']}", symbol, x)
                
                symbols_already_done.append(symbol)
            
