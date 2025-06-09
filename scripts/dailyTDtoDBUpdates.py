from truedata_ws.websocket.TD import TD
from dotenv import load_dotenv
import os
import logging
import datetime
import pandas as pd

logging.basicConfig(level=logging.WARNING, format="%(message)s")

load_dotenv()

td_login_id = os.getenv('TRUEDATA_LOGIN_ID', 'tdwsf575')
td_password = os.getenv('TRUEDATA_LOGIN_PWD', 'vidhi@575')

td_obj = TD(td_login_id, td_password, live_port=None, log_level=logging.WARNING, log_format="%(message)s")

start_date = datetime.datetime.now().date() - datetime.timedelta(days=31)
end_date = datetime.datetime.now().date() + datetime.timedelta(days=31)

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


index_dict = {}
for i in indexes:
    print(i)
    symbol = i[0]
    my_symbol = i[1]
    
    df = get_all_data_of_given_symbol(symbol)
    df.columns = 'timestamp o h l c v oi'.split()
    
    old_data = r.hget('instrument', my_symbol)
    if old_data is not None and len(old_data) > 0 :
        if not isinstance(old_data, pd.DataFrame):
            old_data = pd.DataFrame(old_data)
            
        old_data['timestamp'] = pd.to_datetime(old_data['timestamp'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = pd.concat([old_data, df]).drop_duplicates('timestamp').sort_values('timestamp').reset_index(drop=True)
        df = df[df.timestamp.dt.time < datetime.time(15, 30)].reset_index(drop=True)
    r.hset('instrument', my_symbol, df)
