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

import math
global last_working_day, today, yesterday, holidays, holidays_datetime
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

global df_instruments, df_dates, start_time, end_time, step
start_time = datetime.strptime('09:15:00', '%H:%M:%S')
end_time = datetime.strptime('15:29:00', '%H:%M:%S')
step = timedelta(minutes=1)

global data_collection, log, sdate_iso, interval
sdate_iso = today.isoformat()[:10] + 'T09:15:00.000Z'
data_collection = {}
log = True
interval = '1minute'

async def get_data_zerodha_recursive(interval, from_date, edate, symbol):
    df_instrument = await db.get_instrument_token(symbol)
    if len(df_instrument) == 0:
        info = f"instrument token not found {symbol}"
        print(info)
        return pd.DataFrame()
    token = int(df_instrument.instrument_token.iloc[0])
    to_date = edate
    data_frames = []  # List to store DataFrames
    days = 5
    while from_date < edate:
        if from_date >= (edate - timedelta(days)):
            data_frames.append(zerodha.gethistoricaldata(token, from_date, edate, interval))
            break
        else:
            to_date = from_date + timedelta(days)
            data_frames.append(zerodha.gethistoricaldata(token, from_date, to_date, interval))
            from_date = to_date

    data = pd.concat(data_frames, ignore_index=True)
    return data

async def get_data_zerodha(interval, sdate, edate, symbol):
    df_instrument = await db.get_instrument_token(symbol)
    if len(df_instrument) == 0:
        info = f"instrument token not found {symbol}"
        print(info)
        return pd.DataFrame()
    token = int(df_instrument.instrument_token.iloc[0])
    global log
    for retry in range(10):
        data = zerodha.gethistoricaldata(token, sdate, edate, interval)
        if len(data) > 0:
            if log == True:
                print(f"Data Received in {retry} try")
            break  
        else:
            print(f'unable to get data {retry} of 10')
            asyncio.sleep(1) 
    if len(data) < 2:
        if log == True:
            print("Unable to fetch data after multiple retries.")
        return data
    else:
        if log == True:
            print('return new data')
        return data
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

async def download_daily_ohlc_of_stocks(df_all_stocks):
    global last_working_day
    last_working_day_str = last_working_day.strftime('%d-%m-%Y')
    count = 0
    for index, row in df_all_stocks.iterrows():
        exchange_code = row['symbol']
        df_last_date = await db.get_last_ohlc_date_symbol(exchange_code)
        len_df_last_date = len(df_last_date)
        if log == True:
            print(f"{exchange_code=} {len_df_last_date=}")
        if len(df_last_date) == 0:
            if log == True:
                print('lastdate not found for ', exchange_code)
            # download 7 days data
            days_prior = yesterday - timedelta(days=7)
            startdate = days_prior
            
            startdate_str = days_prior.strftime('%d-%m-%Y')
        else:
            last_date = df_last_date.datetime.iloc[0]
            if log == True:
                print(f"{exchange_code} {last_date=}")
            startdate = last_date
            sdate_iso = last_date.isoformat()[:10] + 'T07:00:00.000Z'
            startdate_str = last_date.strftime('%d-%m-%Y')
        if not isinstance(startdate, date):
            startdate = startdate.date() 
        if startdate >= last_working_day:
            if log == True:
                important_data = f"{exchange_code} startdate:{startdate_str} > last_working_day:{last_working_day_str}"
                print(important_data)
                await db.pre_process_logs(today_str, 'download_daily_ohlc_of_stocks', 'startdate >= last_working_dayignore', important_data, 0)
            continue

        if log == True:
            important_data = f"{exchange_code=} {startdate_str=} {last_working_day=}"
            await db.pre_process_logs(today_str, 'download_daily_ohlc_of_stocks', 'download using zerodha', important_data, 0)

        result = 0
        df = ""
        try:
            if log == True:
                print('gethistorical_daily', exchange_code)
            df = await get_data_zerodha('day', startdate, last_working_day, exchange_code)
            print(df.head())
            result = 1
        except Exception as e:
            if log == True:
                print('Error in downloading', exchange_code, e)
            result = -1

        if result == -1:
            await db.pre_process_logs(today_str, 'download_daily_ohlc_of_stocks', 'download using history api', 'Timeout Error', 4)
            continue
        if type(df) is str:
            if log == True:
                print(df) 
                await db.pre_process_logs(today_str, 'download_daily_ohlc_of_stocks', 'download using history api', 'df str', 4)
            continue

        for index, row in df.iterrows():
            date_val = row['date']
            open_val = row['open']
            high_val = row['high']
            low_val = row['low']
            close_val = row['close']
            volume_val = row['volume']
            await db.insert_daily_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)
            if log == True:
                print('insert_daily_ohlc', exchange_code, date_val)
        count += 1
    if count > 0:
        return 1, 'None', count
    else:
        return 0, 'Unknown Error', count

async def pine_ema(src, length):
    alpha = 2 / (length + 1)
    ema_values = []
    sum_ema = None
    for value in src:
        if sum_ema is None:
            sum_ema = np.mean(src[:length]) 
        else:
            sum_ema = alpha * value + (1 - alpha) * sum_ema            
        ema_values.append(sum_ema)
    return ema_values
def calculate_new_ema(latest_close, previous_ema, length):
    alpha = 2 / (length + 1)
    new_ema = (latest_close - previous_ema) * alpha + previous_ema
    return new_ema
async def MACD(data, fast_ma_period = 12, slow_ma_period = 26, signal_period = 9):
    data["FMA"] = data['close'].ewm(span=fast_ma_period).mean()
    data["SMA"] = data['close'].ewm(span=slow_ma_period).mean()
    data["MACD"] = data["FMA"] - data["SMA"]
    data["Signal"] = data['MACD'].ewm(span=signal_period).mean()
    data["histogram"] = data["MACD"] - data["Signal"]
    data = data[["close", "MACD", "Signal", "histogram"]]
    return data
async def process_hist(unproc_datetime, df_new, df_old, table, exchange_code):
    df_new['histogram'] = 0
    df_new['hist_color'] = None
    df_new['sell_counter'] = 0
    df_new['buy_counter'] = 0
    df_new['macd_buy'] = ''
    df_new['macd_sell'] = ''
    df_old.sort_values(by='datetime', inplace=True)
    df_concatenated = pd.concat([df_old, df_new], ignore_index=True)
    df_MACD = await MACD(df_concatenated[['datetime', 'close']])
    df_concatenated['histogram'] = df_MACD['histogram']
    index_with_none = df_concatenated[df_concatenated['hist_color'].isna()].index
    first_index_with_none = index_with_none[0] if len(index_with_none) > 0 else 1
    print("First index with None value in 'hist_color' column:", first_index_with_none)

    for i in range(first_index_with_none, len(df_concatenated)):
        if df_concatenated['histogram'].iloc[i] < df_concatenated['histogram'].iloc[i-1]:
            df_concatenated.at[i, 'hist_color'] = 'r'  # If current value is lower than previous, assign red color
        elif df_concatenated['histogram'].iloc[i] == df_concatenated['histogram'].iloc[i-1]:
            df_concatenated.at[i, 'hist_color'] = df_concatenated['hist_color'].iloc[i-1] 
        else:
            df_concatenated.at[i, 'hist_color'] = 'g'  # Otherwise, assign green color

    sell_exit = buy_exit = True
    filtered_df = df_concatenated[df_concatenated['macd_buy'] != 'EASE']
    if not filtered_df.empty:
        last_value = filtered_df['macd_buy'].iloc[-1]
        if last_value != 'EXIT':
            buy_exit = False

    filtered_df = df_concatenated[df_concatenated['macd_sell'] != 'EASE']
    if not filtered_df.empty:
        last_value = filtered_df['macd_sell'].iloc[-1]
        if last_value != 'EXIT':
            sell_exit = False
    print(f"{buy_exit=} {sell_exit=}")
    for i in range(first_index_with_none, len(df_concatenated)):
        row_0 = df_concatenated.iloc[i]
        row_minus_1 = df_concatenated.iloc[i - 1]
        row_minus_2 = df_concatenated.iloc[i - 2]
        hist_0 = row_0['histogram']
        hist_minus_1 = row_minus_1['histogram']
        
        color_0 = row_0['hist_color']
        color_minus_1 = row_minus_1['hist_color']
        
        buy_minus_1 = row_minus_1['macd_buy']
        sell_minus_1 = row_minus_1['macd_sell']
        
        counter_buy_minus_1 = row_minus_1['buy_counter']
        counter_sell_minus_1 = row_minus_1['sell_counter']
        #print(f"{color_minus_1=} {color_0=} {buy_minus_1=}")
        if color_minus_1 == 'g' and color_0 == 'g' and (buy_minus_1 == 'EASE' or buy_minus_1 == 'BUY' or buy_minus_1 == 'Strong BUY'):
            sell_minus_2 = row_minus_2['macd_sell']
            df_concatenated.at[i, 'macd_buy'] = 'BUY'
            df_concatenated.at[i,'sell_counter'] = 0
            buy_exit = False
            if hist_minus_1 < 0 and hist_0 > 0:
                df_concatenated.at[i, 'macd_buy'] = 'Strong BUY'            
            if counter_buy_minus_1 == 0:
                df_concatenated.at[i,'buy_counter'] = 1
            elif buy_minus_1 == 'BUY' or buy_minus_1 == 'Strong BUY':
                df_concatenated.at[i,'buy_counter'] = counter_buy_minus_1 + 1
            if sell_minus_1 == 'EASE' and sell_exit == False:
                df_concatenated.at[i, 'macd_sell'] = 'EXIT'
                sell_exit = True
            else:
                df_concatenated.at[i, 'macd_sell'] = 'EASE'
        elif color_minus_1 == 'r' and color_0 == 'r' and (sell_minus_1 == 'EASE' or sell_minus_1 == 'SELL' or sell_minus_1 == 'Strong SELL'):
            buy_minus_2 = row_minus_2['macd_buy']
            df_concatenated.at[i, 'macd_sell'] = 'SELL'
            df_concatenated.at[i,'sell_counter'] = 0
            sell_exit = False
            if hist_minus_1 > 0 and hist_0 < 0:
                df_concatenated.at[i, 'macd_sell'] = 'Strong SELL'
            if counter_sell_minus_1 == 0:
                df_concatenated.at[i,'sell_counter'] = 1
            elif sell_minus_1 == 'SELL' or sell_minus_1 == 'Strong SELL':
                df_concatenated.at[i,'sell_counter'] = counter_sell_minus_1 + 1
            if buy_minus_1 == 'EASE' and buy_exit == False:
                df_concatenated.at[i, 'macd_buy'] = 'EXIT'
                buy_exit = True
            else:
                df_concatenated.at[i, 'macd_buy'] = 'EASE'
        elif color_minus_1 == 'g' and color_0 == 'r':
            df_concatenated.at[i, 'macd_buy'] = 'EASE'
            df_concatenated.at[i,'buy_counter'] = 0
            df_concatenated.at[i, 'macd_sell'] = 'EASE'
            df_concatenated.at[i,'sell_counter'] = 0
            if buy_minus_1 == 'EASE' and buy_exit == False:
                df_concatenated.at[i, 'macd_buy'] = 'EXIT'
                buy_exit = True

        elif color_minus_1 == 'r' and color_0 == 'g':
            df_concatenated.at[i, 'macd_sell'] = 'EASE'
            df_concatenated.at[i,'sell_counter'] = 0
            df_concatenated.at[i,'buy_counter'] = 0
            df_concatenated.at[i, 'macd_buy'] = 'EASE'
            if sell_minus_1 == 'EASE' and sell_exit == False:
                df_concatenated.at[i, 'macd_sell'] = 'EXIT'
                sell_exit = True

    df_filtered = df_concatenated.loc[df_concatenated['datetime'] >= unproc_datetime]
    df_filtered['histogram'] = df_filtered['histogram'].astype(float).round(2)
    print('len df_filtered', len(df_filtered))
    for index, row in df_filtered.iterrows():
        datetime_val = row['datetime']  
        histogram = row['histogram']
        hist_color = row['hist_color'] 
        sell_counter = row['sell_counter'] 
        buy_counter = row['buy_counter']
        macd_buy= row['macd_buy'] 
        macd_sell= row['macd_sell']
        if table == '5min':
            await db.update_five_min_histogram(histogram, hist_color, sell_counter, buy_counter, macd_buy, macd_sell,  exchange_code, datetime_val)
        elif table == '15min':
            await db.update_fifteen_min_histogram(histogram, hist_color, sell_counter, buy_counter, macd_buy, macd_sell,  exchange_code, datetime_val)
        elif table == '30min':
            await db.update_thirty_min_histogram(histogram, hist_color, sell_counter, buy_counter, macd_buy, macd_sell,  exchange_code, datetime_val)
        elif table == '1hour':
            await db.update_hour_histogram(histogram, hist_color, sell_counter, buy_counter, macd_buy, macd_sell,  exchange_code, datetime_val)
        elif table == 'daily':
            await db.update_day_histogram(histogram, hist_color, sell_counter, buy_counter, macd_buy, macd_sell,  exchange_code, datetime_val)

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
            df_new['open'] = df_new['open'].astype(float)
            df_new['high'] = df_new['high'].astype(float)
            df_new['low'] = df_new['low'].astype(float)
            df_new['close'] = df_new['close'].astype(float)
        await process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)
        
    return 1, None, count

async def process_fivemin_heikin(df_all_stocks):
    print('in process_fivemin_heikin')
    count = 0
    table_name = 'five_min_ohlc'
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
            df_new['open'] = df_new['open'].astype(float)
            df_new['high'] = df_new['high'].astype(float)
            df_new['low'] = df_new['low'].astype(float)
            df_new['close'] = df_new['close'].astype(float)
        await process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)
        
    return 1, None, count


async def download_ohlc_2min(df_all_stocks):
    table_name = 'two_min_ohlc'
    global last_working_day
    last_working_day_str = last_working_day.strftime('%d-%m-%Y')
    count = 0
    for index, row in df_all_stocks.iterrows():
        exchange_code = row['symbol']
        df_last_datetime = await db.get_ohlc_last_datetime(exchange_code, table_name)
        len_df_last_datetime = len(df_last_datetime)
        if log == True:
            print(f"{exchange_code=} {len_df_last_datetime=}")
        if len(df_last_datetime) == 0:
            if log == True:
                print('lastdate not found for ', exchange_code)
            days_prior = yesterday - timedelta(days=90)
            startdate = days_prior
            startdate_str = days_prior.strftime('%d-%m-%Y HH:MM:00')
        else:
            last_date = df_last_datetime.datetime.iloc[0]
            if log == True:
                print(f"{exchange_code} {last_date=}")
            startdate = last_date
            startdate = startdate.to_pydatetime().date()
            startdate_str = last_date.strftime('%d-%m-%Y HH:MM:00')

        if startdate >= last_working_day:
            last_working_day_str = last_working_day.strftime('%d-%m-%Y')
            if log == True:
                important_data = f"{interval} {exchange_code} startdate:{startdate_str} > last_working_day:{last_working_day_str}"
                print(important_data)
            await db.pre_process_logs(today_str, 'download_ohlc', 'startdate >= last_working_dayignore', important_data, 0)
            continue
        if log == True:
            important_data = f"{interval} {exchange_code=} {startdate_str=} {last_working_day_str=}"
            await db.pre_process_logs(today_str, 'download_ohlc', 'download using zerodha', important_data, 0)

        result = 0
        df = ""
        try:
            if log == True:
                print('get_one_min_datetime', exchange_code)
            df = await db.get_one_min_datetime(exchange_code, startdate, last_working_day)
            result = 1
        except Exception as e:
            if log == True:
                print('Error in getting from db', exchange_code, e)

        if len(df) == 0:
            if log == True:
                await db.pre_process_logs(today_str, 'download_ohlc_2min', 'Data not available', df, 4)
            continue
        
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)

        # Resample to 2-minute OHLC DataFrame
        df = df.resample('2T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })
        df.dropna(inplace=True)
        if len(df) == 0:
            continue
        data = ta.candles.ha(df['open'], df['high'], df['low'], df['close'])
        if len(data) > 0:
            df['ha_open'] = data['HA_open'].astype(float).round(2)
            df['ha_high'] = data['HA_high'].astype(float).round(2)
            df['ha_low'] = data['HA_low'].astype(float).round(2)
            df['ha_close'] = data['HA_close'].astype(float).round(2)
            df.dropna(inplace=True)
            df.reset_index(inplace=True)

            for index, row in df.iterrows():
                date_val = row['datetime']
                open_val = row['open']
                high_val = row['high']
                low_val = row['low']
                close_val = row['close']
                volume_val = 0
                ha_open = row['ha_open']
                ha_high = row['ha_high']
                ha_low = row['ha_low']
                ha_close = row['ha_close']
                print(f"{date_val=} {ha_open=} {ha_close=}")
                await db.insert_ohlc_data(table_name, exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val, ha_open, ha_high, ha_low, ha_close)
                if log == True:
                    print('insert_' + table_name, exchange_code, date_val)
            count += 1
    if count > 0:
        return 1, 'None', count
    else:
        return 0, 'Unknown Error', count                
async def download_ohlc(df_all_stocks, interval):
    global last_working_day
    last_working_day_str = last_working_day.strftime('%d-%m-%Y')
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
        exchange_code = row['symbol']
        df_last_datetime = await db.get_ohlc_last_datetime(exchange_code, table_name)
        len_df_last_datetime = len(df_last_datetime)
        if log == True:
            print(f"{exchange_code=} {len_df_last_datetime=}")
        if len(df_last_datetime) == 0:
            if log == True:
                print('lastdate not found for ', exchange_code)
            # download 200 days data
            days_prior = yesterday - timedelta(days=90)
            startdate = days_prior
            #sdate_iso = days_prior.isoformat()[:10] + 'T09:15:00.000Z'
            startdate_str = days_prior.strftime('%d-%m-%Y HH:MM:00')
        else:
            last_date = df_last_datetime.datetime.iloc[0]
            if log == True:
                print(f"{exchange_code} {last_date=}")
            startdate = last_date
            startdate = startdate.to_pydatetime().date()
            startdate_str = last_date.strftime('%d-%m-%Y HH:MM:00')

        if startdate >= last_working_day:
            last_working_day_str = last_working_day.strftime('%d-%m-%Y')
            if log == True:
                important_data = f"{interval} {exchange_code} startdate:{startdate_str} > last_working_day:{last_working_day_str}"
                print(important_data)
            await db.pre_process_logs(today_str, 'download_ohlc', 'startdate >= last_working_dayignore', important_data, 0)
            continue
        if log == True:
            important_data = f"{interval} {exchange_code=} {startdate_str=} {last_working_day_str=}"
            await db.pre_process_logs(today_str, 'download_ohlc', 'download using zerodha', important_data, 0)

        result = 0
        df = ""
        try:
            if log == True:
                print('gethistorical_daily', exchange_code)
            #df = await get_data_zerodha(interval, startdate, last_working_day, exchange_code)
            df = await get_data_zerodha_recursive(interval, startdate, last_working_day, exchange_code)
            result = 1
        except Exception as e:
            if log == True:
                print('Error in downloading', exchange_code, e)
            result = -1

        if result == -1:
            if log == True:
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'download using history api', 'Timeout Error', 4)
            continue
        if type(df) is str:
            if log == True:
                print(df) 
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'download using history api', 'df str', 4)
            continue
        if len(df) == 0:
            if log == True:
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'Data not available', df, 4)
            continue
        data = ta.candles.ha(df['open'], df['high'], df['low'], df['close'])

        df['ha_open'] = data['HA_open'].astype(float).round(2)
        df['ha_high'] = data['HA_high'].astype(float).round(2)
        df['ha_low'] = data['HA_low'].astype(float).round(2)
        df['ha_close'] = data['HA_close'].astype(float).round(2)
        df.dropna(inplace=True)
        print(df.tail())
        for index, row in df.iterrows():
            date_val = row['date']
            open_val = row['open']
            high_val = row['high']
            low_val = row['low']
            close_val = row['close']
            volume_val = row['volume']  
            ha_open = row['ha_open']
            ha_high = row['ha_high']
            ha_low = row['ha_low']
            ha_close = row['ha_close']
            await db.insert_ohlc_data(table_name, exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val, ha_open, ha_high, ha_low, ha_close)
            if log == True:
                print('insert_' + table_name, exchange_code, date_val)
        count += 1
    if count > 0:
        return 1, 'None', count
    else:
        return 0, 'Unknown Error', count

async def update_symbols_to_monitor():
    df_basket_stocks = await db.get_active_basket_symbols()
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
        symbol = row_basket['tradingsymbol']
        ltp = get_last_price('NSE:' + symbol)
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
                await db.insert_into_monitor_symbols(instrument_token, tradingsymbol, expiry, strike, instrument_type, ltp, symbol)
    return 1, 'None', 1

async def main():
    loop = asyncio.get_event_loop()
    await db.create_pool(loop)
    global df_dates, df_last_five_dates
    df= pd.DataFrame()
    global last_working_day
    #await process_min_heikin(df_all_stocks, '15minute')
    #return
    df_all_stocks = await db.get_monitor_symbols_to_trade()
    df = await db.get_pre_market_steps()
    if datetime.now().hour > 16:
        df = await db.get_pre_market_steps_ignore_date()
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

        if action == 'update_symbols_to_monitor':
            result, error, count_symbol = await update_symbols_to_monitor()
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'update_symbols_to_monitor', 'function result', important_data, 1)
            if result == 1:
                await db.update_pre_market_steps(id, last_status=1, last_record_date=current_date_string)
        elif action == 'download onemin ohlc':
            result, error, count_symbol = await download_ohlc(df_all_stocks, 'minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download onemin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('one_min_ohlc')
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download twomin ohlc':
            result, error, count_symbol = await download_ohlc_2min(df_all_stocks)
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download twomin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('two_min_ohlc')
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download threemin ohlc':
            result, error, count_symbol = await download_ohlc(df_all_stocks, '3minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download threemin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('three_min_ohlc')
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download fivemin ohlc':
            result, error, count_symbol = await download_ohlc(df_all_stocks, '5minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download fivemin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('five_min_ohlc')
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download tenmin ohlc':
            result, error, count_symbol = await download_ohlc(df_all_stocks, '10minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download tenmin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('ten_min_ohlc')
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download fiften_min ohlc':
            result, error, count_symbol = await download_ohlc(df_all_stocks, '15minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download fiften_min ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('fifteen_min_ohlc')
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download thirty_min ohlc':
            result, error, count_symbol = await download_ohlc(df_all_stocks, '30minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download thirty_min ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('thirty_min_ohlc')
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'download one_hour ohlc':
            result, error, count_symbol = await download_ohlc(df_all_stocks, '60minute')
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download one_hour ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_min_ohlc_date('one_hour_ohlc')
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)


  
    await db.close_pool()
    print('Done')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


