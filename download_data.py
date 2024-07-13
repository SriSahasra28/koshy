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

async def download(tpl_stocks, interval):
    global db, data_collections, dates_collections, fail_count, disabled_symbols, lrc_period, lrc_stdev, start_time, end_time, lrc_settings
    table_name =  interval_to_table.get(interval, None)
    end_date_today = datetime.today().replace(hour=15, minute=30, second=0, microsecond=0)
    end_date_now = datetime.now().replace(second=0, microsecond=0)
    if end_date_now > end_date_today:
        end_date_now = end_date_today;

    for instrument_token, exchange_code in tpl_stocks:
        if exchange_code in disabled_symbols:
            print('Skipping as not enabled', exchange_code)
            await db.pre_process_logs(today_str, 'checking disabled_symbols', 'skip not enabled', exchange_code, 1)
            continue
        else:
            print(exchange_code)
            dates_list_old = []
            # check for interval
            #data_collections['minute']['ULTRACEMCO24JUN9800CE']
            if exchange_code in dates_collections[interval]:
                dates_list_old = dates_collections[interval][exchange_code]
            
            if len(dates_list_old) > 0:
                last_datetime = dates_list_old[-1]
                last_datetime = last_datetime.replace(second=0, microsecond=0)
                cutoff_datetime = last_datetime
                #print('cache datetime available', cutoff_datetime)  
            else:
                #print('no cache')
                last_datetime = datetime.today() - timedelta(days=90)
                last_datetime = last_datetime.replace(hour=9, minute=15, second=0, microsecond=0)
                cutoff_datetime = last_datetime
            
            if isinstance(last_datetime, pd.Timestamp):
                last_datetime = last_datetime.to_pydatetime()
            
            if isinstance(cutoff_datetime, pd.Timestamp):
                cutoff_datetime = cutoff_datetime.to_pydatetime()
            #print(f"{last_datetime=} {cutoff_datetime=}")
        result = status = 0
        try:
            #print('start date', last_datetime)
            #print(f"{end_date_now=}, {end_date_today=}")
            if cutoff_datetime >= end_date_now:
                print(f"skipping cutoff_datetime:{cutoff_datetime} >= end_date_now:{end_date_now}", instrument_token)
                continue
            elif interval == '60minute':
                exptime = cutoff_datetime + timedelta(hours=1)
                print(f"{exptime=}")
                if exptime > end_date_today:
                    print(f'skipping {exptime=}', instrument_token)
                    continue
            status, data, Error = await get_data_zerodha_recursive_list(interval, last_datetime, end_date_today, instrument_token, exchange_code)
            print(f"{status=}")
        except Exception as e:
            if log == True:
                print('Error in downloading', exchange_code, e)
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'Error downloading', '', 3)
            result = -1

        if status == 1 and len(data) > 0:
            result = 1
        else:
            if exchange_code in fail_count:
                fail_count[exchange_code] += 1
            else:
                fail_count[exchange_code] = 1
            if log == True:
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'download using history api', 'len data = 0', 1)
            print('no data skipping processing')
            continue
        if result == -1:
            if log == True:
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'download using history api', 'Error', 4)
            if exchange_code not in disabled_symbols:
                disabled_symbols.append(exchange_code)
            print('Error getting data skipping processing')
            continue
        if status == 0:
            if Error == 'invalid token':
                if exchange_code not in disabled_symbols:
                    disabled_symbols.append(exchange_code)
                await db.run_query(f"update monitor_symbols set active = 0 where symbol = '{exchange_code}'")
            if log == True:
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'invalid token', '', 4)
            print('invalid token skipping processing')
            continue        
        #print('data', data[0:50])
        dates_new = []
        dates_new = [entry['date'].replace(tzinfo=None) for entry in data]  # Convert datetime to timezone-naive
        opens = [entry['open'] for entry in data]
        highs = [entry['high'] for entry in data]
        lows = [entry['low'] for entry in data]
        closes = [entry['close'] for entry in data]
        volumes = [entry['volume'] for entry in data]

        if cutoff_datetime in dates_new:
            index = dates_new.index(cutoff_datetime)
            print(f"Index of {cutoff_datetime}: {index}")
            dates_new = dates_new[index+1:]
            opens = opens[index+1:]
            highs = highs[index+1:]
            lows = lows[index+1:]
            closes = closes[index+1:]
            volumes = volumes[index+1:]
        else:
            print(f"{cutoff_datetime} not found in the list.")

        dates_combined = dates_new
        if len(dates_list_old) > 0:
            dates_combined = dates_list_old + dates_new
            dates_collections[interval][exchange_code] = dates_combined

        data_np_new = np.zeros((len(dates_new), 5), dtype='float64')
        data_np_new[:,0] = np.array(opens)
        data_np_new[:,1] = np.array(highs)
        data_np_new[:,2] = np.array(lows)
        data_np_new[:,3] = np.array(closes)
        data_np_new[:,4] = np.array(volumes)
        if len(data_np_new) == 0:
            print('No Data to process skipping')
            continue
        data_combined = data_np_new

        if exchange_code in data_collections[interval]:
            print("exchange_code found in data_collection[interval]")
            data_np_old = data_collections[interval][exchange_code]
            if len(data_np_new) > 0 and len(data_np_old) > 0:
                print('in if len(data_np_new) > 0 and len(data_np_old) > 0')
                data_combined = np.vstack((data_np_old, data_np_new))
                data_collections[interval][exchange_code] = data_combined
            elif len(data_np_new) == 0 and len(data_np_old) > 0:
                print('len(data_np_new) == 0')
                data_combined = data_np_old
            elif len(data_np_new) > 0 and len(data_np_old) == 0:
                print('len(data_np_old) == 0')
            else:
                print('len(data_np_new)', len(data_np_new))
                print('len(data_np_old)', len(data_np_old))
        else:
            print('exchange_code not in data_collections[interval] add')
            data_collections[interval][exchange_code] = data_combined

        print('len data_combined:', len(data_combined), 'len dates_combined:', len(dates_combined))
        # calculate indicators
        ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(data_combined[:,0], data_combined[:,1], data_combined[:,2], data_combined[:,3])

        index_start = 0
        if cutoff_datetime in dates_combined:
            index_start = dates_combined.index(cutoff_datetime)
        
        BATCH_SIZE = 1000
        batch_data = []

        for i in range(index_start + 1, len(dates_combined)):
            date_val = dates_combined[i]
            python_time = date_val.time()
            is_within_range = start_time <= python_time <= end_time
            if is_within_range == False:
                print(f'Time beyond range {date_val}')
                continue
            open_val = data_combined[i,0]
            high_val = data_combined[i,1]
            low_val = data_combined[i,2]
            close_val = data_combined[i,3]
            volume_val = data_combined[i,4]
            ha_open_val = ha_open[i]
            ha_high_val = ha_high[i]
            ha_low_val = ha_low[i]
            ha_close_val = ha_close[i]
            batch_data.append((exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val, ha_open_val, ha_high_val, ha_low_val, ha_close_val))
        
            if len(batch_data) >= BATCH_SIZE:
                print('insert_batch_data partial', exchange_code)
                #await db.insert_batch_data(table_name, batch_data)
                asyncio.create_task(db.insert_batch_data(table_name, batch_data))  # Run insert in background
                batch_data = []

        if batch_data:
            print('insert_batch_data final', exchange_code)
            #print(batch_data)
            asyncio.create_task(db.insert_batch_data(table_name, batch_data))  # Run insert in background
            #await db.insert_batch_data(table_name, batch_data)
        else:
            print('no batch_data', exchange_code)
        (lrc_period, lrc_stdev) = lrc_settings[0]

        if len(data_combined[:,3]) >= lrc_period:
            await db.run_query(f"update {table_name} set LRL = NULL, UCL = NULL, LCL = NULL where symbol = '{exchange_code}' and LRL is not NULL;")
            LRL, UCL, LCL = linear_regression_channel_numba(data_combined[:,3], lrc_period, lrc_stdev)
            signal_len = len(LRL)
            last_n_dates = dates_combined[-signal_len:]
            batch_data = []
            for i in range(len(last_n_dates)):
                date_val = last_n_dates[i]
                python_time = date_val.time()
                is_within_range = start_time <= python_time <= end_time
                if is_within_range == False:
                    print(f'Time beyond range {date_val}')
                    continue
                LRL_val = round(LRL[i], 4)
                UCL_val = round(UCL[i], 4)
                LCL_val = round(LCL[i], 4)
                batch_data.append((LRL_val, UCL_val, LCL_val, exchange_code, date_val))
            if len(batch_data) > 0:
                asyncio.create_task(db.update_LRC_batch(batch_data, table_name))  # Run insert in background

        # PSAR Calculations
        if len(data_combined[:,3]) >= 45:
            (acceleration, max_acceleration) = psar_settings[0]
            psar_values = psar(data_combined[:,1], data_combined[:,2], data_combined[:,3], af0=float(acceleration), af=float(acceleration), max_af=float(max_acceleration))
            signals = get_signals(data_combined[:,3], psar_values)
            batch_data = []
            for i in range(index_start + 1, len(dates_combined)):
                date_val = dates_combined[i]
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

            if len(batch_data) > 0:
                #print('---------------------PSAR--------', batch_data)
                asyncio.create_task(db.update_PSAR_batch(batch_data, table_name))  
        else:
            print('--------------------PSAR skipped len close 45 ')
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
        #print(f"{interval=}, {table_name=}")
        prvdata = await db.get_old_data(table_name) # already in ascending order    
        for s_value, group_df in prvdata.groupby('symbol'):
            group_df = group_df.tail(50)
            ohlc_np = group_df[['open', 'high', 'low', 'close', 'volume']].values.astype(float)
            data_collections[interval][s_value] = ohlc_np
            group_df['datetime'] = pd.to_datetime(group_df['datetime'])
            datetime_list = group_df['datetime'].tolist()
            dates_collections[interval][s_value] = datetime_list

    start_time = time.time()
    for interval, table_name in interval_to_table.items():
        print(f"{interval=}, {table_name=}")
        if interval != 'minute':
            await download(priority_stocks_tpl, interval)
            await download(non_priority_stocks_tpl, interval)
            break
    end_time = time.time()  
    total_time = end_time - start_time
    print(f"------------------------Total time taken to execute the code: {total_time:.2f} seconds")

    await asyncio.sleep(20)
    await db.close_pool()   
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())