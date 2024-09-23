import os
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
print(os.getcwd())
import pandas as pd
import asyncio
import requests
import codecs
import json
from datetime import datetime, timedelta, date, time as tm
from pandas._libs.tslibs.timestamps import Timestamp
from background.async_db import dbconnection
from background.zerodha import zeroda
from background.instruments import instruments as instruments_class
db = dbconnection()
instruments = instruments_class()
import numpy as np
import pandas_ta as ta
from numba import jit
from celery import Celery
from myapp import batch_insert_trade_logs, insert_one_min_ohlc_proc_batch , Insert_three_min_ohlc_proc_batch, Insert_two_min_ohlc_proc_batch, Insert_five_min_ohlc_proc_batch, Insert_ten_min_ohlc_proc_batch, Insert_fifteen_min_ohlc_proc_batch, Insert_thirty_min_ohlc_proc_batch, Insert_hour_ohlc_proc_batch
interval_to_table = {
    'minute': 'one_min_ohlc','2minute': 'two_min_ohlc', '5minute': 'five_min_ohlc', '3minute': 'three_min_ohlc', '10minute': 'ten_min_ohlc',
    '15minute': 'fifteen_min_ohlc', '30minute': 'thirty_min_ohlc', '60minute': 'one_hour_ohlc'
}

global last_working_day, today, yesterday, holidays, holidays_datetime, lrc_period, lrc_stdev
global data_collections, dates_collections, instr_tpl

data_collections = {f"{key}": {} for key in interval_to_table}
dates_collections = {f"{key}": {} for key in interval_to_table}

lrc_period = 100
lrc_stdev = 2

today = date.today()
zerodha = zeroda('live', datetime.today())
today_str = today.strftime('%Y-%m-%d')
yesterday = today - timedelta(days=1)
Is_weekend = False
last_working_day = today

if datetime.now().hour < 16:
    last_working_day = yesterday
    if last_working_day.weekday() == 5:
        last_working_day = last_working_day - timedelta(days=1)
    elif last_working_day.weekday() == 6:
        last_working_day = last_working_day - timedelta(days=2)
else:
    last_working_day = today
    if last_working_day.weekday() == 5:
        last_working_day = last_working_day - timedelta(days=1)
    elif last_working_day.weekday() == 6:
        last_working_day = last_working_day - timedelta(days=2)

global df_instruments, df_dates, start_time, end_time, step, zerodha_last_trans
start_time = datetime.strptime('09:15:00', '%H:%M:%S')
end_time = datetime.strptime('15:29:00', '%H:%M:%S')
zerodha_last_trans = None
start_time_trans = tm(9, 15)
end_time_trans = tm(15, 30)

step = timedelta(minutes=1)

global data_collection, log, sdate_iso, interval
sdate_iso = today.isoformat()[:10] + 'T09:15:00.000Z'
data_collection = {}
log = True
interval = '1minute'

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

async def get_data_zerodha_recursive(interval, from_date, edate, symbol):
    if not isinstance(from_date, datetime):
        from_date = datetime.combine(from_date, tm(9, 15, 0))

    if not isinstance(edate, datetime):
        edate = datetime.combine(edate, tm(15, 30, 0))
    df_instrument = await db.get_instrument_token(symbol)
    if len(df_instrument) == 0:
        info = f"instrument token not found {symbol}"
        print(info)
        return pd.DataFrame()
    token = int(df_instrument.instrument_token.iloc[0])
    to_date = edate
    data_frames = []  # List to store DataFrames
    days = 5
    print(f"{from_date=} {edate=}")
    while from_date < edate:
        if from_date >= (edate - timedelta(days)):
            print('in if');
            df = zerodha.gethistoricaldata(token, from_date.date(), edate.date(), interval)
            if len(df) == 0:
                print('if len df 0 break')
                break
            else:
                print('append if')
                data_frames.append(df)
            break
        else:
            print('in else');
            to_date = from_date + timedelta(days)
            print(f"{from_date=}, {to_date=}")
            df = zerodha.gethistoricaldata(token, from_date.date(), to_date.date(), interval)
            if len(df) == 0:
                print('else len df 0 break')
                #break
            else:
                print('append else')
                data_frames.append(df)
            from_date = to_date
    if data_frames:
        data = pd.concat(data_frames, ignore_index=True)
        return data
    else:
        print("No data frames to concatenate")
        data = pd.DataFrame() 
        return data 

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
                elif Error == 'Too many requests':
                    await asyncio.sleep(1)
                    info = f"{Error}"
                    await db.pre_process_logs(today_str, 'Error download_ohlc', symbol, info, 5)
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
                elif Error == 'Too many requests':
                    await asyncio.sleep(1)
                    info = f"{Error}"
                    await db.pre_process_logs(today_str, 'Error download_ohlc', symbol, info, 5)
                break
            else:
                data_list.extend(data)  # Append the list of dictionaries
            from_date = to_date
    
    if data_list:
        return 1, data_list, None  # Return the merged list of dictionaries
    else:
        print("No data to process")
        return 0, None, error
    
async def initialize():
    global df_instruments, df_dates, df_last_five_dates
    await db.truncate_pre_process_logs()
    df_instruments = await db.get_symbols_instruments_to_trade()
    df_dates = await db.get_unprocessed_dates()
    df_dates = df_dates.iloc[::-1]
    await get_holidays()

async def get_holidays():
    global holidays, last_working_day, holidays_datetime
    year = datetime.now().year
    url = f"http://api.kimbly.in/holidays.php?year={year}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            response_data = codecs.decode(response.content, 'utf-8-sig')
            holidays = json.loads(response_data)
            holidays_datetime = [datetime.strptime(date, "%Y-%m-%d").date() for date in holidays]
            while last_working_day in holidays_datetime:
                last_working_day = last_working_day - timedelta(days=1)               
        else:
            if log == True:
                print("Request failed with status code:", response.status_code)
                print(url)
    except requests.exceptions.RequestException as e:
        if log == True:
            print("Request error:", e)

async def process_heikinashi(unproc_datetime, df_new, df_old, table, exchange_code):
    df_old.sort_values(by='datetime', inplace=True)
    df_concatenated = pd.concat([df_old, df_new], ignore_index=True)
    data = ta.candles.ha(df_concatenated['open'], df_concatenated['high'], df_concatenated['low'], df_concatenated['close'])

    df_concatenated['ha_open'] = data['HA_open'].astype(float).round(2)
    df_concatenated['ha_high'] = data['HA_high'].astype(float).round(2)
    df_concatenated['ha_low'] = data['HA_low'].astype(float).round(2)
    df_concatenated['ha_close'] = data['HA_close'].astype(float).round(2)
    df_filtered = df_concatenated.loc[df_concatenated['datetime'] >= unproc_datetime]
    
    print('len df_filtered', len(df_filtered))
    for index, row in df_filtered.iterrows():
        datetime_val = row['datetime']  
        ha_open = row['ha_open']
        ha_high = row['ha_high']
        ha_low = row['ha_low']
        ha_close = row['ha_close']
        await db.update_heikin_ashi(ha_open, ha_high, ha_low, ha_close, exchange_code, datetime_val, table)

async def process_min_heikin(df_all_stocks, interval):
    print('in process_min_heikin')
    count = 0
    table_name = 'one_min_ohlc'
    if interval == '5minute':
        table_name = 'five_min_ohlc'
    elif interval == '3minute':
        table_name = 'three_min_ohlc'
    elif interval == '10minute':
        table_name = 'ten_min_ohlc'
    elif interval == '15minute':
        table_name = 'fifteen_min_ohlc'
    elif interval == '30minute':
        table_name = 'thirty_min_ohlc'
    elif interval == '60minute':
        table_name = 'one_hour_ohlc'
    for index, row in df_all_stocks.iterrows():
        count += 1
        exchange_code = row['symbol']
        df_new = await db.get_null_ohlc(exchange_code, table_name)
        if len(df_new) == 0:
            if log == True:
                print('NULL ohlc not found No need to process', exchange_code, table_name)
            continue
        else:
            df_new['open'] = df_new['open'].astype(float)
            df_new['high'] = df_new['high'].astype(float)
            df_new['low'] = df_new['low'].astype(float)
            df_new['close'] = df_new['close'].astype(float)
        unproc_datetime = df_new.datetime.iloc[0]
        df_old = await db.get_prior_rows(exchange_code, unproc_datetime, table_name)
        if len(df_old) == 0:
            if log == True:
                print('data not found - get_prior_thirty_rows', table_name)
            process_fresh = True
        else:
            df_old['open'] = df_old['open'].astype(float)
            df_old['high'] = df_old['high'].astype(float)
            df_old['low'] = df_old['low'].astype(float)
            df_old['close'] = df_old['close'].astype(float)
            
        await process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)

    return 1, None, count

async def download_ohlc_2min(df_all_stocks):
    table_name = 'two_min_ohlc'
    interval = '2minute'
    global last_working_day
    last_working_day_str = last_working_day.strftime('%d-%m-%Y')
    count = 0
    end_date_now = datetime.now().replace(second=0, microsecond=0)
    for index, row in df_all_stocks.iterrows():
        exchange_code = row['symbol']
        last_datetime = None
        # Get Last datetime for the symbol in from cache
        if exchange_code in dates_collections[interval]:
            last_datetime = cutoff_datetime = dates_collections[interval][exchange_code][-1].replace(second=0, microsecond=0)
            print(interval, 'cache datetime available', cutoff_datetime)  
        else:
            info = f"No cache {exchange_code}{interval}"
            await db.insert_trade_log(date_log=today, module='download_ohlc_2min', activity='check cache', important_data=info, priority=2, strategy_trade_id = '', timestamp=datetime.now()) 
            group_df = await db.get_old_data_by_symbol(table_name, exchange_code)
            if len(group_df) > 0:
                info = f"Data found in db {exchange_code}{interval} {len(group_df)} rows"
                await db.insert_trade_log(date_log=today, module='download_ohlc_2min', activity='get_old_data_by_symbol', important_data=info, priority=2, strategy_trade_id = '', timestamp=datetime.now()) 
                ohlc_np = group_df[['open', 'high', 'low', 'close']].values.astype(float)
                data_collections[interval][exchange_code] = ohlc_np
                group_df['datetime'] = pd.to_datetime(group_df['datetime'])
                datetime_list = group_df['datetime'].tolist()
                dates_collections[interval][exchange_code] = datetime_list
                last_datetime = cutoff_datetime = dates_collections[interval][exchange_code][-1].replace(second=0, microsecond=0)
            else:
                info = f"Data not found in db {exchange_code}{interval}"
                await db.insert_trade_log(date_log=today, module='download_ohlc_v2', activity='get_old_data_by_symbol', important_data=info, priority=2, strategy_trade_id = '', timestamp=datetime.now()) 
                last_datetime = datetime.today() - timedelta(days=90)
                last_datetime = last_datetime.replace(hour=9, minute=15, second=0, microsecond=0)
                cutoff_datetime = last_datetime

        print(f"{exchange_code} {last_datetime=}")

        if isinstance(last_datetime, pd.Timestamp):
            last_datetime = last_datetime.to_pydatetime()
        
        if isinstance(cutoff_datetime, pd.Timestamp):
            cutoff_datetime = cutoff_datetime.to_pydatetime()
        
        #print(f"{cutoff_datetime=} {last_datetime=} {last_working_day=}")

        if cutoff_datetime >= end_date_now:
            print(f"skipping cutoff_datetime:{cutoff_datetime} >= end_date_now:{end_date_now}", exchange_code)
            continue
      
        result = 0
        df = ""
        try:
            if log == True:
                print('get_one_min_datetime', exchange_code)
            # dates_list_one_min = []
            # data_one_min = []
            # if exchange_code in dates_collections['minute']:
            #     dates_list_one_min = dates_collections['minute'][exchange_code]
            #     data_one_min = data_collections['minute'][exchange_code]
            # df = pd.DataFrame(data_one_min, columns=['open', 'high', 'low', 'close'], index=dates_list_one_min)

           
            df = await db.get_one_min_datetime_after(exchange_code, cutoff_datetime)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True, drop=True)
            result = 1
        except Exception as e:
            if log == True:
                print('Error in getting 1 min data from cache', exchange_code, e)

        if len(df) == 0:
            if log == True:
                await db.pre_process_logs(today_str, 'download_ohlc_2min', '1 min Data not available', exchange_code, 4)
            continue
        
        df = df.resample('2T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })
        df.dropna(inplace=True)
        if len(df) == 0:
            continue
        df.reset_index(inplace=True, names="datetime")

        # ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(df['open'].to_list(), df['high'].to_list(), df['low'].to_list(), df['close'].to_list())

        # df['ha_open'] = ha_open
        # df['ha_high'] = ha_high
        # df['ha_low'] = ha_low
        # df['ha_close'] = ha_close

        df = df[df['datetime'] > cutoff_datetime]
        BATCH_SIZE = 1000
        batch_data = []
        tasks = []

        for index, row in df.iterrows():
            date_val = row['datetime']
            open_val = row['open']
            high_val = row['high']
            low_val = row['low']
            close_val = row['close']
            # ha_open = row['ha_open']
            # ha_high = row['ha_high']
            # ha_low = row['ha_low']
            # ha_close = row['ha_close']
            
            #batch_data.append((exchange_code, date_val, open_val, high_val, low_val, close_val, ha_open, ha_high, ha_low, ha_close))
            batch_data.append((exchange_code, date_val, open_val, high_val, low_val, close_val))
            if len(batch_data) >= BATCH_SIZE:
                Insert_two_min_ohlc_proc_batch.delay(batch_data)
                # task = asyncio.create_task(db.Insert_two_min_ohlc_proc_batch(batch_data))
                # tasks.append(task)
                batch_data = []

        if batch_data:
            Insert_two_min_ohlc_proc_batch.delay(batch_data)
            #print(exchange_code, batch_data)
            # task = asyncio.create_task(db.Insert_two_min_ohlc_proc_batch(batch_data))
            # tasks.append(task)
            if log == True:
                print('insert_' + table_name, exchange_code, date_val)
            count += 1
        if len(tasks) > 0:
            await asyncio.gather(*tasks)

    if count > 0:
        return 1, 'None', count
    else:
        return 0, 'Unknown Error', count   

async def download_ohlc_v2(df_all_stocks, interval):
    global db, data_collections, dates_collections, instr_tpl, zerodha_last_trans
    table_name =  interval_to_table.get(interval, None)
    end_date_today = datetime.today().replace(hour=15, minute=30, second=0, microsecond=0)
    end_date_now = datetime.now().replace(second=0, microsecond=0)
    error =''
    if end_date_now > end_date_today:
        end_date_now = end_date_today;
    count_iter = 0
    for index, row in df_all_stocks.iterrows():
        exchange_code = row['symbol']
        instrument_token = row['instrument_token'] # new added
        current_time = datetime.now().strftime("%H:%M:%S")
        print(current_time, exchange_code)
        dates_list_old = []
        
        # Get Last datetime for the symbol in database already downloaded
        if exchange_code in dates_collections[interval]:
            dates_list_old = dates_collections[interval][exchange_code]
        
        if len(dates_list_old) > 0:
            last_datetime = dates_list_old[-1]
            last_datetime = last_datetime.replace(second=0, microsecond=0)
            cutoff_datetime = last_datetime
            info = f"cache datetime available {exchange_code} {interval}"
            if log == True:
                await db.pre_process_logs(today_str, 'cache', 'check date cache', info, 1) 
        else:
            info = f"no cache {exchange_code} {interval}"
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
            print(f"{end_date_now=}, {end_date_today=}")
            if cutoff_datetime >= end_date_now:
                info = f"skipping cutoff_datetime:{cutoff_datetime} >= end_date_now:{end_date_now} {exchange_code} {interval}"
                await db.pre_process_logs(today_str, 'cache', 'check date cache', info, 1) 
                continue
            elif interval == '60minute':
                exptime = cutoff_datetime + timedelta(hours=1)
                print(f"{exptime=}")
                if exptime > end_date_today:
                    info = "skipping {exptime=} {exchange_code} {interval}"
                    await db.pre_process_logs(today_str, 'cache', 'check date cache', info, 1) 
                    continue
            print(f"{exchange_code} {last_datetime=}, {end_date_today=}")
            
            if zerodha_last_trans != None and last_datetime >= zerodha_last_trans:
                info = "skipping last_datetime >= zerodha_last_trans {exchange_code} {interval}"
                await db.pre_process_logs(today_str, 'cache', 'check date cache', info, 1) 
                continue

            status, data, Error = await get_data_zerodha_recursive_list(interval, last_datetime, end_date_today, instrument_token, exchange_code)
            #print(f"{status=}")
        except Exception as e:
            if log == True:
                info = "Error in downloading {exchange_code} {interval} {e}"
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'Error downloading', info, 3)
            result = -1

        if status == 1 and len(data) > 0:
            result = 1
        else:
            if log == True:
                info = f"len data = 0 {exchange_code} {interval}"
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'download using history api', info, 1)
            print('no data skipping processing')
            continue
        if result == -1:
            if log == True:
                info = f"Error downloading {exchange_code} {interval}"
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'download using history api', info, 4)
            print('Error getting data skipping processing')
            continue
        if status == 0:
            if Error == 'invalid token':
                await db.run_query(f"update monitor_symbols set active = 0 where symbol = '{exchange_code}'")
            if log == True:
                info = f"invalid token {exchange_code} {interval}"
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'invalid token', info, 4)
            print('invalid token skipping processing')
            continue        

        dates_new = []
        dates_new = [entry['date'].replace(tzinfo=None) for entry in data]  # Convert datetime to timezone-naive
        opens = [entry['open'] for entry in data]
        highs = [entry['high'] for entry in data]
        lows = [entry['low'] for entry in data]
        closes = [entry['close'] for entry in data]

        if cutoff_datetime in dates_new:
            index = dates_new.index(cutoff_datetime)
            #print(f"Index of {cutoff_datetime}: {index}")
            dates_new = dates_new[index+1:]
            opens = opens[index+1:]
            highs = highs[index+1:]
            lows = lows[index+1:]
            closes = closes[index+1:]
        else:
            print(f"cutoff_datetime: {cutoff_datetime} not found in the list.")

        dates_combined = dates_new

        data_np_new = np.zeros((len(dates_new), 5), dtype='float64')
        data_np_new[:,0] = np.array(opens)
        data_np_new[:,1] = np.array(highs)
        data_np_new[:,2] = np.array(lows)
        data_np_new[:,3] = np.array(closes)
        #data_np_new[:,4] = np.array(volumes)
        if len(data_np_new) == 0:
            info = f"No Data to process skipping {exchange_code} {interval}"
            await db.pre_process_logs(today_str, 'gethistorical_cash', 'no data', info, 4)
            continue
        data_combined = data_np_new
        print('len data_combined:', len(data_combined), 'len dates_combined:', len(dates_combined))
        
        # calculate indicators
        #ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(data_combined[:,0], data_combined[:,1], data_combined[:,2], data_combined[:,3])

        index_start = 0
        if cutoff_datetime in dates_combined:
            index_start = dates_combined.index(cutoff_datetime)
        tasks = []
        count_iter = count_iter + 1
        BATCH_SIZE = 1000
        batch_data = []
        total_count = len(dates_combined)
        for i in range(index_start + 1, total_count):
            date_val = dates_combined[i]
            python_time = date_val.time()
            is_within_range = start_time_trans <= python_time <= end_time_trans
            if is_within_range == False:
                print(f'Time beyond range {date_val}')
                continue
            open_val = data_combined[i,0]
            high_val = data_combined[i,1]
            low_val = data_combined[i,2]
            close_val = data_combined[i,3]
            batch_data.append((exchange_code, date_val, open_val, high_val, low_val, close_val))
            if len(batch_data) >= BATCH_SIZE:
                if table_name == 'one_min_ohlc':
                    insert_one_min_ohlc_proc_batch.delay(batch_data)
                elif table_name == 'three_min_ohlc':
                    Insert_three_min_ohlc_proc_batch.delay(batch_data)
                elif table_name == 'five_min_ohlc':
                    Insert_five_min_ohlc_proc_batch.delay(batch_data)
                elif table_name == 'ten_min_ohlc':
                    Insert_ten_min_ohlc_proc_batch.delay(batch_data)
                elif table_name == 'fifteen_min_ohlc':
                    Insert_fifteen_min_ohlc_proc_batch.delay(batch_data)
                elif table_name == 'thirty_min_ohlc':
                    Insert_thirty_min_ohlc_proc_batch.delay(batch_data)
                elif table_name == 'one_hour_ohlc':
                    Insert_hour_ohlc_proc_batch.delay(batch_data)
                batch_data = []
        if batch_data:
            if table_name == 'one_min_ohlc':
                insert_one_min_ohlc_proc_batch.delay(batch_data)
            elif table_name == 'three_min_ohlc':
                Insert_three_min_ohlc_proc_batch.delay(batch_data)
            elif table_name == 'five_min_ohlc':
                Insert_five_min_ohlc_proc_batch.delay(batch_data)
            elif table_name == 'ten_min_ohlc':
                Insert_ten_min_ohlc_proc_batch.delay(batch_data)
            elif table_name == 'fifteen_min_ohlc':
                Insert_fifteen_min_ohlc_proc_batch.delay(batch_data)
            elif table_name == 'thirty_min_ohlc':
                Insert_thirty_min_ohlc_proc_batch.delay(batch_data)
            elif table_name == 'one_hour_ohlc':
                Insert_hour_ohlc_proc_batch.delay(batch_data) 
            batch_data = []
        #     if len(batch_data) >= BATCH_SIZE:
        #         current_time = datetime.now().strftime("%H:%M:%S")
        #         print('insert_batch_data partial',current_time, ':', exchange_code)
        #         if table_name == 'one_min_ohlc':
        #             task = asyncio.create_task(db.Insert_one_min_ohlc_proc_batch(batch_data))
        #             tasks.append(task)
        #         elif table_name == 'three_min_ohlc':
        #             task = asyncio.create_task(db.Insert_three_min_ohlc_proc_batch(batch_data))
        #             tasks.append(task)
        #         elif table_name == 'five_min_ohlc':
        #             task = asyncio.create_task(db.Insert_five_min_ohlc_proc_batch(batch_data))
        #             tasks.append(task)
        #         elif table_name == 'ten_min_ohlc':
        #             task = asyncio.create_task(db.Insert_ten_min_ohlc_proc_batch(batch_data))
        #             tasks.append(task)
        #         elif table_name == 'fifteen_min_ohlc':
        #             task = asyncio.create_task(db.Insert_fifteen_min_ohlc_proc_batch(batch_data))
        #             tasks.append(task)
        #         elif table_name == 'thirty_min_ohlc':
        #             task = asyncio.create_task(db.Insert_thirty_min_ohlc_proc_batch(batch_data))
        #             tasks.append(task)
        #         elif table_name == 'one_hour_ohlc':
        #             task = asyncio.create_task(db.Insert_hour_ohlc_proc_batch(batch_data))
        #             tasks.append(task)
        #         batch_data = []

        # if batch_data:
        #     current_time = datetime.now().strftime("%H:%M:%S")
        #     print('insert_batch_data partial',current_time, ':', exchange_code)
        #     if table_name == 'one_min_ohlc':
        #         task = asyncio.create_task(db.Insert_one_min_ohlc_proc_batch(batch_data))
        #         tasks.append(task)
        #         #await db.Insert_one_min_ohlc_proc_batch(batch_data)
        #     elif table_name == 'three_min_ohlc':
        #         task = asyncio.create_task(db.Insert_three_min_ohlc_proc_batch(batch_data))
        #         tasks.append(task)
        #     elif table_name == 'five_min_ohlc':
        #         task = asyncio.create_task(db.Insert_five_min_ohlc_proc_batch(batch_data))
        #         tasks.append(task)
        #     elif table_name == 'ten_min_ohlc':
        #         task = asyncio.create_task(db.Insert_ten_min_ohlc_proc_batch(batch_data))
        #         tasks.append(task)
        #     elif table_name == 'fifteen_min_ohlc':
        #         task = asyncio.create_task(db.Insert_fifteen_min_ohlc_proc_batch(batch_data))
        #         tasks.append(task)
        #     elif table_name == 'thirty_min_ohlc':
        #         task = asyncio.create_task(db.Insert_thirty_min_ohlc_proc_batch(batch_data))
        #         tasks.append(task)
        #     elif table_name == 'one_hour_ohlc':
        #         task = asyncio.create_task(db.Insert_hour_ohlc_proc_batch(batch_data))
        #         tasks.append(task)
            
        else:
            info = f"no batch_data {exchange_code} {interval}"
            await db.pre_process_logs(today_str, 'insert in db', 'batch data', info, 1)
            
        info = f"processed rows {total_count} {exchange_code} {interval}"
        await db.pre_process_logs(today_str, 'data insertted', 'finishing', info, 1)
       #await asyncio.gather(*tasks)
    return 1, error, count_iter

async def update_symbols_to_monitor():
    df_basket_stocks = await db.get_active_basket_symbols()
    #df_basket_stocks = await db.get_all_stocks_token()
    symbols_list = df_basket_stocks['tradingsymbol'].unique()
    prefixed_symbols_list = ['NSE:' + symbol for symbol in symbols_list]
    df_ltp = zerodha.getLTPMulti(prefixed_symbols_list)

    def get_last_price(symbol):
        if symbol in df_ltp:
            return df_ltp[symbol]['last_price']
        else:
            return None  
    await db.run_query('Delete FROM monitor_symbols where active = 1;')
    current_date_string = datetime.now().strftime("%Y-%m-%d")
    await db.pre_process_logs(current_date_string, 'update_symbols_to_monitor', 'symbols deleted', 'truncate table monitor_symbols', 1)
    print('df_basket_stocks', df_basket_stocks)
    print('df_ltp', df_ltp)
    for index_baket, row_basket in df_basket_stocks.iterrows():
        option_type = row_basket['option_type']
        instrument_token = row_basket['instrument_token']
        main_symbol = row_basket['tradingsymbol']
        ltp = get_last_price('NSE:' + main_symbol)
        symbol = main_symbol
        if symbol == 'NIFTY 50':
            symbol = 'NIFTY'
        elif symbol == 'NIFTY BANK':
            symbol = 'BANKNIFTY'
        elif symbol == 'NIFTY FIN SERVICE':
            symbol = 'FINNIFTY'
        elif symbol == 'NIFTY MIDCAP 50':
            symbol = 'MIDCPNIFTY'
        print(f"{symbol} {ltp=}")
        if ltp is not None:
            df_strikes = instruments.get_nearest_ten_strikes(symbol, ltp, option_type)
            df_strikes.expiry = pd.to_datetime(df_strikes.expiry)
            for index, row in df_strikes.iterrows():
                instrument_token = row['instrument_token']
                tradingsymbol = row['tradingsymbol']
                expiry = row['expiry'].strftime('%Y-%m-%d')
                strike = row['strike']
                instrument_type = row['instrument_type']
                print(f"{instrument_token}, {tradingsymbol}, {expiry=}, {strike=}, {instrument_type=}")
                await db.insert_into_monitor_symbols(instrument_token, tradingsymbol, expiry, strike, instrument_type, ltp, main_symbol)
    return 1, 'None', 1

async def process_option(symbol, ltp, option_type, main_symbol, next_month=False):
    status, df_strikes = instruments.get_nearest_ten_strikes(symbol, ltp, option_type, next_month)
    if status == -1:
        return
    df_strikes.expiry = pd.to_datetime(df_strikes.expiry)
    for index, row in df_strikes.iterrows():
        instrument_token = row['instrument_token']
        tradingsymbol = row['tradingsymbol']
        expiry = row['expiry'].strftime('%Y-%m-%d')
        strike = row['strike']
        instrument_type = row['instrument_type']
        print(f"{instrument_token}, {tradingsymbol}, {expiry=}, {strike=}, {instrument_type=}")
        await db.insert_into_monitor_symbols(instrument_token, tradingsymbol, expiry, strike, instrument_type, ltp, main_symbol)

async def update_symbols_to_download(next_month=False):
    #df_basket_stocks = await db.get_active_basket_symbols()
    df_all_stocks = await db.get_all_stocks_token()
    symbols_list = df_all_stocks['tradingsymbol'].unique()
    prefixed_symbols_list = ['NSE:' + symbol for symbol in symbols_list]
    df_ltp = zerodha.getLTPMulti(prefixed_symbols_list)

    def get_last_price(symbol):
        if symbol in df_ltp:
            return df_ltp[symbol]['last_price']
        else:
            return None  
    #await db.run_query('truncate table download_symbols;')
    current_date_string = datetime.now().strftime("%Y-%m-%d")
    await db.pre_process_logs(current_date_string, 'update_symbols_to_download', 'symbols deleted', 'truncate table download_symbols', 1)
    print('df_basket_stocks', df_all_stocks)
    print('df_ltp', df_ltp)
    for index_baket, row_basket in df_all_stocks.iterrows():
        #instrument_token = row_basket['instrument_token']
        main_symbol = row_basket['tradingsymbol']
        ltp = get_last_price('NSE:' + main_symbol)
        symbol = main_symbol
        if symbol == 'NIFTY 50':
            symbol = 'NIFTY'
        elif symbol == 'NIFTY BANK':
            symbol = 'BANKNIFTY'
        elif symbol == 'NIFTY FIN SERVICE':
            symbol = 'FINNIFTY'
        elif symbol == 'NIFTY MIDCAP 50':
            symbol = 'MIDCPNIFTY'
        print(f"{symbol} {ltp=}")
        if ltp is not None:
            await process_option(symbol, ltp, 'CE', main_symbol, next_month)
            await process_option(symbol, ltp, 'PE', main_symbol, next_month)
    return 1, 'None', 1

async def rollover():
    global today
    df_cred = await db.get_data("SELECT option_rollover_date FROM credentials;")
    option_rollover_date = df_cred['option_rollover_date'].iloc[0]
    print(f"{option_rollover_date=}")
    if option_rollover_date.month < today.month:
        #df_expiry = await db.get_data("SELECT expiry from instruments where exchange = 'NFO' and month(expiry) = month(curdate()) and year(expiry) = year(curdate()) and name in ('NIFTY', 'BANKNIFTY','FINNIFTY') order by expiry desc limit 1;")
        df_expiry = await db.get_data("SELECT expiry from instruments where exchange = 'NFO' and month(expiry) = month(curdate()) and year(expiry) = year(curdate()) and name in ('NIFTY') order by expiry desc limit 1;")
        last_expiry = df_expiry.expiry.iloc[0]
        if (datetime.now().hour >= 16 and last_expiry <= today) or (last_expiry < today):
            print('Truncate tables & turnover option_date')
            # download instruments
            await db.run_query('Truncate table one_min_ohlc;') 
            await db.run_query('Truncate table fifteen_min_ohlc;')
            await db.run_query('Truncate table five_min_ohlc;')
            await db.run_query('Truncate table one_hour_ohlc;')
            await db.run_query('Truncate table ten_min_ohlc;')
            await db.run_query('Truncate table thirty_min_ohlc;')
            await db.run_query('Truncate table three_min_ohlc;')
            await db.run_query('Truncate table two_min_ohlc;')

            #Recreate     
            await db.run_query('truncate table monitor_symbols;')
            await update_symbols_to_download(next_month=True)
            # Update option_rollover_date
            await db.run_query(f"Update credentials set option_rollover_date = '{today}'")
            print('option rollover done')
            return 1
        else:
            print(f'No rollover of option inner {last_expiry=} {today=}')
            return 0
    else:
        print('No rollover of option- option_rollover_date same month')
        return 0

async def main():
    loop = asyncio.get_event_loop()
    await db.create_pool(loop)
    global df_dates, df_last_five_dates
    df= pd.DataFrame()
    global last_working_day, today, instr_tpl
    # check monthly turnover
    rollover_status = 0
    await db.run_query('truncate table pre_process_logs;')

    if today.day > 20:
        rollover_status = await rollover()
    df_all_stocks = await db.get_monitor_symbols_to_trade()
    all_symbols = df_all_stocks['symbol'].to_list()
    all_symbols_set = set(all_symbols)
    global dates_collections, zerodha_last_trans

    for interval, table_name in interval_to_table.items():
        prvdata = await db.get_lastdate_symbols_all(table_name)
        for s_value, group_df in prvdata.groupby('symbol'):
            group_df['datetime'] = pd.to_datetime(group_df['datetime'])
            datetime_list = group_df['datetime'].tolist()
            dates_collections[interval][s_value] = datetime_list


    # result, error, count_symbol = await download_ohlc_2min(df_all_stocks)
    # return

    status, data, Error = await get_data_zerodha_recursive_list('minute',  datetime.now() - timedelta(minutes=5), datetime.now(), 256265, 'NIFTY 50')
    if status == 1:
        zerodha_last_trans = data[-1]['date'].replace(tzinfo=None)
    else:
        print('Error getting NIFTY data from Zerodha')
        await db.pre_process_logs(datetime.now().strftime("%Y-%m-%d"), 'test zerodha', 'zerodha_last_trans', Error, 4)
    
    df = await db.get_pre_market_steps()
    print('now hour: ', datetime.now().hour)
    if datetime.now().hour >= 16:
        print('get_pre_market_steps_ignore_date after 4 PM')
        df = await db.get_pre_market_steps_ignore_date()
    else:
        print('get_pre_market_steps before 5 PM')

    print('Pre Market Steps', df)
    for index, row in df.iterrows():
        id = row['id']
        action = row['action']
        priority = row['priority']
        last_status = row['last_status']
        last_record_date = row['last_record_date']
        time_planned = row['time_planned']
        last_execution = row['last_execution']
        print(f"{action=}")
    
        if action == 'update_symbols_to_monitor' and rollover_status == 0 and datetime.now().hour < 10:
            result, error, count_symbol = await update_symbols_to_download()
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'update_symbols_to_monitor', 'function result', important_data, 1)
            if result == 1:
                await db.update_pre_market_steps(id, last_status=1, last_record_date=current_date_string)
        elif action == 'download onemin ohlc':
            result, error, count_symbol = await download_ohlc_v2(df_all_stocks, 'minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download onemin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('one_min_ohlc')
                if len(last_record_date) > 0:
                    last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                    await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download twomin ohlc':
            result, error, count_symbol = await download_ohlc_2min(df_all_stocks)
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download twomin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('two_min_ohlc')
                if len(last_record_date) > 0:
                    last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                    await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download threemin ohlc':
            result, error, count_symbol = await download_ohlc_v2(df_all_stocks, '3minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download threemin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('three_min_ohlc')
                if len(last_record_date) > 0:
                    last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                    await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download fivemin ohlc':
            result, error, count_symbol = await download_ohlc_v2(df_all_stocks, '5minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download fivemin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('five_min_ohlc')
                if len(last_record_date) > 0:
                    last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                    await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download tenmin ohlc':
            result, error, count_symbol = await download_ohlc_v2(df_all_stocks, '10minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download tenmin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('ten_min_ohlc')
                if len(last_record_date) > 0:
                    last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                    await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download fiften_min ohlc':
            result, error, count_symbol = await download_ohlc_v2(df_all_stocks, '15minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download fiften_min ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('fifteen_min_ohlc')
                if len(last_record_date) > 0:
                    last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                    await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download thirty_min ohlc':
            result, error, count_symbol = await download_ohlc_v2(df_all_stocks, '30minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download thirty_min ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('thirty_min_ohlc')
                if len(last_record_date) > 0:
                    last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                    await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download one_hour ohlc':
            result, error, count_symbol = await download_ohlc_v2(df_all_stocks, '60minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download one_hour ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('one_hour_ohlc')
                if len(last_record_date) > 0:
                    last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                    await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)

    await db.close_pool()
    print('Done')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


