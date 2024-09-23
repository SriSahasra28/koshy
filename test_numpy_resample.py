import numpy as np
from numba import njit
import pandas as pd
import redis
import json

@njit
def resample_ohlc(timestamps, opens, highs, lows, closes, interval):
    # Calculate the number of intervals
    interval_duration = 60 * interval  # Convert to seconds
    start_time = timestamps[0]
    end_time = timestamps[-1]

    # Calculate the number of new intervals
    num_intervals = (end_time - start_time) // interval_duration + 1
    resampled_opens = np.empty(num_intervals)
    resampled_highs = np.empty(num_intervals)
    resampled_lows = np.empty(num_intervals)
    resampled_closes = np.empty(num_intervals)

    # Initialize with NaNs
    resampled_opens.fill(np.nan)
    resampled_highs.fill(np.nan)
    resampled_lows.fill(np.nan)
    resampled_closes.fill(np.nan)

    for i in range(num_intervals):
        interval_start = start_time + i * interval_duration
        interval_end = interval_start + interval_duration

        # Find indices for the current interval
        mask = (timestamps >= interval_start) & (timestamps < interval_end)
        if np.any(mask):
            resampled_opens[i] = opens[mask][0]  # First open
            resampled_highs[i] = highs[mask].max()  # Max high
            resampled_lows[i] = lows[mask].min()  # Min low
            resampled_closes[i] = closes[mask][-1]  # Last close

    return resampled_opens, resampled_highs, resampled_lows, resampled_closes

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
instrument_code = '17411842'
ohlc_sorted_data = r.zrange('ohlc_sorted:' + instrument_code, 0, -1)

ohlc_list = [json.loads(data) for data in ohlc_sorted_data]
array_data = np.array([[entry['open'], entry['high'], entry['low'], entry['close']] for entry in ohlc_list])
timestamps = np.array([entry['timestamp'] for entry in ohlc_list])

# Convert to NumPy arrays
timestamps = np.array(data['timestamps'])
opens = np.array(data['opens'])
highs = np.array(data['highs'])
lows = np.array(data['lows'])
closes = np.array(data['closes'])

# Resample with a 2-minute interval
interval = 2  # Interval in minutes
resampled_opens, resampled_highs, resampled_lows, resampled_closes = resample_ohlc(timestamps, opens, highs, lows, closes, interval)

# Display the results
print("Resampled Opens:", resampled_opens)
print("Resampled Highs:", resampled_highs)
print("Resampled Lows:", resampled_lows)
print("Resampled Closes:", resampled_closes)
