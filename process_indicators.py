import os
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
print(os.getcwd())
from background.instruments import instruments as instruments_class
from background.database import DBHelper
from background.zerodha import zeroda
from datetime import datetime
import pandas as pd
from background.async_db import dbconnection
global db
db = dbconnection()
from datetime import datetime, timedelta, date, time as tm
from background.instruments import instruments as instruments_class
instruments = instruments_class()
import asyncio
today = date.today()
from background.zerodha import zeroda
zerodha = zeroda('live', datetime.today())
global last_working_day
last_working_day = today
today_str = today.strftime('%Y-%m-%d')
yesterday = today - timedelta(days=1)
import numpy as np
import time
log = True
from numba import jit
interval_to_table = {
    'minute': 'one_min_ohlc','2minute': 'two_min_ohlc', '5minute': 'five_min_ohlc', '3minute': 'three_min_ohlc', '10minute': 'ten_min_ohlc',
    '15minute': 'fifteen_min_ohlc', '30minute': 'thirty_min_ohlc', '60minute': 'one_hour_ohlc'
}
global lrc_period, lrc_stdev, start_time, end_time

start_time = tm(9, 15)
end_time = tm(15, 30)
global data_collections, dates_collections

data_collections = {f"{key}": {} for key in interval_to_table}
dates_collections = {f"{key}": {} for key in interval_to_table}

global disabled_symbols, fail_count
disabled_symbols = []
fail_count = {}

async def get_data_zerodha_recursive_list(interval, from_date, edate, token, symbol):
    print(f" ---------- in get_data_zerodha_recursive_list {symbol} {interval} {from_date=} {edate=}")
    to_date = edate
    # List to accumulate dictionaries
    data_list = []  
    days = 95 
    if interval == 'minute':
        days = 50
    invalid_token = False
    error = ''
    
    while from_date < edate:
        if from_date >= (edate - timedelta(days)):
            status, data, Error = zerodha.gethistoricaldata_v3(token, from_date, edate, interval)
            if status == 0:
                error = "{}".format(Error)
                print(f"get_data_zerodha_recursive {error=}")
                if error == 'invalid token':
                    invalid_token = True
                    info = f"invalid token {symbol}"
                    await db.pre_process_logs(today_str, 'download_ohlc', 'invalid token', info, 5)
                    await db.run_query(f"update monitor_symbols set active = 0 where symbol = '{symbol}'")
                elif error == '':
                    # getting too many requests, save to log
                    pass
                break
            else:
                data_list.extend(data)  # Append the list of dictionaries
            break
        else:
            to_date = from_date + timedelta(days)
            status, data, Error = zerodha.gethistoricaldata_v3(token, from_date, to_date, interval)
            if status == 0:
                if Error == 'invalid token':
                    info = f"invalid token {symbol}"
                    await db.pre_process_logs(today_str, 'download_ohlc', 'invalid token', info, 5)
                    await db.run_query(f"update monitor_symbols set active = 0 where symbol = '{symbol}'")
                break
            else:
                data_list.extend(data)  # Append the list of dictionaries
            from_date = to_date
    
    if data_list:
        return 1, data_list, None  # Return the merged list of dictionaries
    else:
        print("No data to process")
        return 0, None, error

@jit(nopython=True)
def psar(high, low, close, af0=0.02, af=0.02, max_af=0.2):
    length = len(close)
    psar = np.zeros(length)
    psar[0] = close[0]
    trend = 1  # 1: uptrend, -1: downtrend
    ep = high[0]  # extreme point
    af = af0
    for i in range(1, length):
        psar[i] = psar[i-1] + af * (ep - psar[i-1])
        if trend == 1:
            if high[i] > ep:
                ep = high[i]
                af = min(af + af0, max_af)
            if low[i] < psar[i]:
                trend = -1
                psar[i] = ep
                ep = low[i]
                af = af0
        else:
            if low[i] < ep:
                ep = low[i]
                af = min(af + af0, max_af)
            if high[i] > psar[i]:
                trend = 1
                psar[i] = ep
                ep = high[i]
                af = af0

        if trend == 1:
            psar[i] = min(psar[i], low[i-1], low[i-2])
        else:
            psar[i] = max(psar[i], high[i-1], high[i-2])
    return psar

@jit(nopython=True)
def get_signals(close, psar_values):
    signals = np.zeros(len(close))
    
    for i in range(1, len(close)):
        if close[i] > psar_values[i] and close[i-1] <= psar_values[i-1]:
            signals[i] = 1  # Long signal
        elif close[i] < psar_values[i] and close[i-1] >= psar_values[i-1]:
            signals[i] = -1  # Short signal
    
    return signals

@jit(nopython=True)
def heikin_ashi_numpy(open_prices, high_prices, low_prices, close_prices):
    open_prices = np.asarray(open_prices)
    high_prices = np.asarray(high_prices)
    low_prices = np.asarray(low_prices)
    close_prices = np.asarray(close_prices)

    ha_open = np.zeros_like(open_prices)
    ha_high = np.zeros_like(high_prices)
    ha_low = np.zeros_like(low_prices)
    ha_close = np.zeros_like(close_prices)

    ha_close[0] = (open_prices[0] + high_prices[0] + low_prices[0] + close_prices[0]) / 4
    ha_open[0] = (open_prices[0] + close_prices[0]) / 2
    ha_high[0] = high_prices[0]
    ha_low[0] = low_prices[0]

    for i in range(1, len(open_prices)):
        ha_close[i] = (open_prices[i] + high_prices[i] + low_prices[i] + close_prices[i]) / 4
        ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2
        ha_high[i] = max(high_prices[i], ha_open[i], ha_close[i])
        ha_low[i] = min(low_prices[i], ha_open[i], ha_close[i])

    return ha_open, ha_high, ha_low, ha_close

@jit(nopython=True)
def linear_regression_channel_numba(close, period, std_multiplier):
    close = close[-period:]
    X = np.arange(len(close))
    N = len(X)
    sum_X = np.sum(X)
    sum_Y = np.sum(close)
    sum_XY = np.sum(X * close)
    sum_X2 = np.sum(X * X)
    slope = (N * sum_XY - sum_X * sum_Y) / (N * sum_X2 - sum_X * sum_X)
    intercept = (sum_Y - slope * sum_X) / N
    LRL = intercept + slope * X
    residuals = close - LRL
    std_dev = np.std(residuals)
    UCL = LRL + std_multiplier * std_dev
    LCL = LRL - std_multiplier * std_dev
    return LRL, UCL, LCL

async def process_PSAR(tpl_stocks, interval):
    global db, data_collections, dates_collections, lrc_period, lrc_stdev, start_time, end_time, lrc_settings
    table_name =  interval_to_table.get(interval, None)
    BATCH_SIZE = 1000 # for partial batch updates to database
    # parameters for indicator calculation
    (acceleration, max_acceleration) = psar_settings[0]

    for instrument_token, exchange_code in tpl_stocks:
        dates_list_old = []
        if exchange_code in dates_collections[interval]:
            print(exchange_code)
            dates_list_old = dates_collections[interval][exchange_code]
        else:
            #print(f"{exchange_code} not found in dates_collections[{interval}]")
            continue
        if exchange_code in data_collections[interval]:
            data_np_old = data_collections[interval][exchange_code]
        else:
            print("exchange_code not found in data_collection[interval]")
            continue

        # calculate PSAR
        batch_data = []
        if len(data_np_old[:,3]) >= 45: # minimum data required for correct calculation
            
            psar_values = psar(data_np_old[:,1], data_np_old[:,2], data_np_old[:,3], af0=float(acceleration), af=float(acceleration), max_af=float(max_acceleration))
            signals = get_signals(data_np_old[:,3], psar_values)
            batch_data = []

            for i in range(1, len(dates_list_old)):
                date_val = dates_list_old[i]
                python_time = date_val.time()
                is_within_range = start_time <= python_time <= end_time
                if is_within_range == False:
                    print(f'Time beyond range {date_val}')
                    continue
                PSAR = psar_values[i]
                signal = signals[i]
                PSAR_L = PSAR_S = None

                if signal == 1:
                    PSAR_L = 1
                elif signal == -1:
                    PSAR_S = 1

                batch_data.append((PSAR, PSAR_L, PSAR_L, PSAR_S, PSAR_S, exchange_code, date_val))

                if len(batch_data) >= BATCH_SIZE:
                    #print('-------------------------------update_PSAR_batch partial', exchange_code)
                    #asyncio.create_task(db.update_PSAR_batch(batch_data, table_name))  
                    await db.update_PSAR_batch(batch_data, table_name)
                    batch_data = []

            if len(batch_data) > 0:
                #print('---------------------Final PSAR--------', batch_data, exchange_code)
                #asyncio.create_task(db.update_PSAR_batch(batch_data, table_name)) 
                await db.update_PSAR_batch(batch_data, table_name) 
        else:
            pass
            #print('--------------------PSAR skipped not enough data ', exchange_code)

async def process_PSAR_new(tpl_stocks, interval):
    print('in process_PSAR')
    global db, data_collections, dates_collections, lrc_period, lrc_stdev, start_time, end_time, lrc_settings
    table_name = interval_to_table.get(interval, None)
    BATCH_SIZE = 1000  # for partial batch updates to database
    (acceleration, max_acceleration) = psar_settings[0]

    for instrument_token, exchange_code in tpl_stocks:
        update_tasks = []  # Collect all tasks here
        print(f"{exchange_code=}")
        dates_list_old = []
        if exchange_code in dates_collections[interval]:
            dates_list_old = dates_collections[interval][exchange_code]
        else:
            continue
        if exchange_code in data_collections[interval]:
            data_np_old = data_collections[interval][exchange_code]
        else:
            print("exchange_code not found in data_collection[interval]")
            continue

        if len(data_np_old[:,3]) >= 45:  # minimum data required for correct calculation
            print('before calc psar')
            psar_values = psar(data_np_old[:,1], data_np_old[:,2], data_np_old[:,3], af0=float(acceleration), af=float(acceleration), max_af=float(max_acceleration))
            signals = get_signals(data_np_old[:,3], psar_values)
            batch_data = []
            print('after calc psar')
            for i in range(1, len(dates_list_old)):
                date_val = dates_list_old[i]
                PSAR = psar_values[i]
                signal = signals[i]
                PSAR_L = PSAR_S = None

                if signal == 1:
                    PSAR_L = 1
                elif signal == -1:
                    PSAR_S = 1

                batch_data.append((PSAR, PSAR_L, PSAR_L, PSAR_S, PSAR_S, exchange_code, date_val))

                if len(batch_data) >= BATCH_SIZE:
                    print('create task partial')
                    update_task = asyncio.create_task(db.update_PSAR_batch(batch_data, table_name))
                    update_tasks.append(update_task)
                    batch_data = []

            if len(batch_data) > 0:
                print('create task final')
                update_task = asyncio.create_task(db.update_PSAR_batch(batch_data, table_name))
                update_tasks.append(update_task)
        else:
            pass

        # Await all update tasks
        if update_tasks:
            print('waiting to gather')
            await asyncio.gather(*update_tasks)

async def main():
    global db, lrc_settings, psar_settings
    loop = asyncio.get_event_loop()
    await db.create_pool(loop)
    # Get indicator settings
    lrc_settings = await db.get_lrc_settings()
    psar_settings = await db.get_psar_settings()

    # Prepare List of symbols to process
    priority_stocks_tpl = await db.get_priority_instruments_to_trade()
    non_priority_stocks_tpl = await db.get_non_priority_instruments_to_trade()

    # Cache Data of all interval tables for fast processing
    global data_collections, dates_collections
    for interval, table_name in interval_to_table.items():
        # Other tables disabled temporarily
        if interval != 'minute':
            continue
        #print(f"{interval=}, {table_name=}")
        #prvdata = await db.get_old_data(table_name) # already in ascending order
        print('downloading data ', table_name)
        prvdata = await db.get_old_data_all(table_name) # already in ascending order   
        print(f"total rows: {len(prvdata)}") 
        for s_value, group_df in prvdata.groupby('symbol'):
            #group_df = group_df.tail(50)
            ohlc_np = group_df[['open', 'high', 'low', 'close']].values.astype(float)
            data_collections[interval][s_value] = ohlc_np
            group_df['datetime'] = pd.to_datetime(group_df['datetime'])
            datetime_list = group_df['datetime'].tolist()
            dates_collections[interval][s_value] = datetime_list

    # Run 3 times to test time taken
    print('starting for loop')
    for _ in range(3):
        start_time = time.time()
        for interval, table_name in interval_to_table.items():
            if interval == 'minute':
                print('start process:', start_time)
                await process_PSAR(priority_stocks_tpl, interval)
                await process_PSAR(non_priority_stocks_tpl, interval)
                break
        end_time = time.time()
        total_time = end_time - start_time
        print(f"{end_time=} - {start_time=}")
        print(f"------------------------Total time taken to execute the code: {total_time:.2f} seconds")

    await asyncio.sleep(5)
    await db.close_pool()   
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())