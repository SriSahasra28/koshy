import redis
import pandas as pd
import json
import numpy as np

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

ohlc_sorted_data = r.zrange('ohlc_sorted:20203266', 0, -1)
#print(ohlc_sorted_data)

ohlc_list = [json.loads(data) for data in ohlc_sorted_data]



ohlc_dtype = [('timestamp', 'U20'), ('open', 'f8'), ('high', 'f8'), ('low', 'f8'), ('close', 'f8')]

ohlc_array = np.array([(item['timestamp'], 
                        round(item['open'], 2), 
                        round(item['high'], 2), 
                        round(item['low'], 2), 
                        round(item['close'], 2)) 
                        for item in ohlc_list], dtype=ohlc_dtype)

df = pd.DataFrame(ohlc_array)

print(df.tail(20))