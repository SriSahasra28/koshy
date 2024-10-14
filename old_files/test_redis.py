import redis
import pandas as pd
import json
import numpy as np

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def get_token_by_symbol(symbol):
    return r.hget('symbol_to_token', symbol)

def get_symbol_by_token(instrument_token):
    return r.hget('token_to_symbol', instrument_token)

def get_ohlc_by_symbol(symbol):
    token = r.hget('symbol_to_token', symbol)
    if token == None:
        return None
    else:
        ohlc_sorted_data = r.zrange('ohlc_sorted:' + token, 0, -1)
        ohlc_list = [json.loads(data) for data in ohlc_sorted_data]
        ohlc_dtype = [('timestamp', 'U20'), ('open', 'f8'), ('high', 'f8'), ('low', 'f8'), ('close', 'f8')]
        ohlc_array = np.array([(item['timestamp'], 
                                round(item['open'], 2), 
                                round(item['high'], 2), 
                                round(item['low'], 2), 
                                round(item['close'], 2)) 
                                for item in ohlc_list], dtype=ohlc_dtype)
        df = pd.DataFrame(ohlc_array)
        return df
#token = get_token_by_symbol('MIDCPNIFTY24OCT14525PE')
#print(token)
# TCS24OCT4400CE -> 26445058
# MIDCPNIFTY24OCT14525PE -> 11422466
# symbol = get_symbol_by_token(2644505)
# print(symbol)

df = get_ohlc_by_symbol('TCS24OCT4400CE')
print(df.tail(1000))