
import redis
import pandas as pd
import json
import numpy as np
import gc


r = redis.Redis(host='localhost', port=6379, decode_responses=True)
instrument_code = '17411842'
ohlc_sorted_data = r.zrange('ohlc_sorted:' + instrument_code, 0, -1)

ohlc_list = [json.loads(data) for data in ohlc_sorted_data]
array_data = np.array([[entry['open'], entry['high'], entry['low'], entry['close']] for entry in ohlc_list])
timestamps = np.array([entry['timestamp'] for entry in ohlc_list])
                

df = pd.DataFrame(array_data, columns=['open', 'high', 'low', 'close'])
df['timestamp'] = pd.to_datetime(timestamps)  
df.set_index('timestamp', inplace=True)  

#print(df)

resampled_df = df.resample('2T').agg({
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last'
}).dropna()  # Drop any rows with NaN values

# # Reset index if needed
resampled_df.reset_index(inplace=True)

# Convert the OHLC data back to a NumPy array
array_data = resampled_df[['open', 'high', 'low', 'close']].to_numpy()

# Convert the timestamps back to a NumPy array
timestamps = resampled_df['timestamp'].to_numpy()

#del resampled_df

# Manually trigger garbage collection


print(array_data)
print(timestamps)

del resampled_df, array_data
del timestamps

gc.collect()