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

async def process_fifteen_min_ema(df_all_stocks):
    count = 0
    process_fresh = False
    for index, row in df_all_stocks.iterrows():
        process_fresh = False
        count += 1
        exchange_code = row['symbol']
        df2 = await db.get_fifteen_min_null_ema200(exchange_code)
        if len(df2) == 0:
            if log == True:
                print('No need to process ', exchange_code)
            continue
        else:
            df2['close'] = df2['close'].astype(float)
        df = await db.get_fifteen_min_last_non_null_ema200(exchange_code)
        if len(df) == 0:
            if log == True:
                print('data not found - get_fifteen_min_last_non_null_by_symbol')
            process_fresh = True
        else:
            df['ema_200'] = df['ema_200'].astype(float)
            df['ema_100'] = df['ema_100'].astype(float)
            df['ema_50'] = df['ema_50'].astype(float)
            df['ema_20'] = df['ema_20'].astype(float)
        if process_fresh == True:
            print('Processing Fresh ..........................')
            close = df2['close'].to_numpy()
            EMA_20 = await pine_ema(close, 20)
            EMA_50 = await pine_ema(close, 50)
            EMA_100 = await pine_ema(close, 100)
            EMA_200 = await pine_ema(close, 200)
            df2['EMA_20'] = EMA_20
            df2['EMA_50'] = EMA_50
            df2['EMA_100'] = EMA_100
            df2['EMA_200'] = EMA_200
            print(df2)
            df2.replace({np.nan: None}, inplace=True)
            ema_200_color = ema_100_color = ema_50_color = ema_20_color = ''
            df2.replace({np.nan: None}, inplace=True)
            for index, row in df2.iterrows():
                datetime_val = row['datetime']  
                ema_20_val = row['EMA_20'] 
                ema_50_val = row['EMA_50']
                EMA_100_val = row['EMA_100']
                EMA_200_val = row['EMA_200']
                ema_200_color = ema_100_color = ema_50_color = ema_20_color = 'SELL'
                close_value = row['close']
                if close_value > EMA_200_val:
                    ema_200_color = 'BUY'
                if close_value > EMA_100_val:
                    ema_100_color = 'BUY'
                if close_value > ema_50_val:
                    ema_50_color = 'BUY'
                if close_value > ema_20_val:
                    ema_20_color = 'BUY'
                await db.update_fifteen_min_ema(EMA_200_val, ema_200_color,EMA_100_val, ema_100_color, ema_50_val, ema_50_color, ema_20_val, ema_20_color, exchange_code, datetime_val)

        elif process_fresh == False:
            previous_ema_200 = df.iloc[0]['ema_200']
            previous_ema_100 = df.iloc[0]['ema_100']
            previous_ema_50 = df.iloc[0]['ema_50']
            previous_ema_20 = df.iloc[0]['ema_20']
            close_values = df2.close
            i = 0
            ema_200_color = ema_100_color = ema_50_color = ema_20_color = ''
            for close_value in close_values:
                ema_200_color = ema_100_color = ema_50_color = ema_20_color = 'SELL'
                datetime_val = df2.iloc[i]['datetime']
                
                ema_200 = calculate_new_ema(close_value, previous_ema_200, 200)
                if close_value > ema_200:
                    ema_200_color = 'BUY'
                
                ema_100 = calculate_new_ema(close_value, previous_ema_100, 100)
                if close_value > ema_100:
                    ema_100_color = 'BUY'

                ema_50 = calculate_new_ema(close_value, previous_ema_50, 50)
                if close_value > ema_50:
                    ema_50_color = 'BUY'

                ema_20 = calculate_new_ema(close_value, previous_ema_20, 20)
                if close_value > ema_20:
                    ema_20_color = 'BUY'                 
                previous_ema_200 = ema_200
                previous_ema_100 = ema_100
                previous_ema_50 = ema_50
                previous_ema_20 = ema_20
                print('Updating ', exchange_code)
                await db.update_fifteen_min_ema(ema_200, ema_200_color,ema_100, ema_100_color, ema_50, ema_50_color, ema_20, ema_20_color, exchange_code, datetime_val)
                i = i + 1
    return 1, None, count

async def process_thirty_min_ema(df_all_stocks):
    count = 0
    process_fresh = False
    for index, row in df_all_stocks.iterrows():
        count += 1
        process_fresh = False
        exchange_code = row['symbol']
        df2 = await db.get_thirty_min_null_ema200(exchange_code)
        if len(df2) == 0:
            if log == True:
                print('No need to Process 30 min ', exchange_code)
            continue
        else:
            df2['close'] = df2['close'].astype(float)
        df = await db.get_thirty_min_last_non_null_ema200(exchange_code)
        if len(df) == 0:
            if log == True:
                print('data not found - get_thirty_min_last_non_null_ema200')
            process_fresh = True
        else:
            df['ema_200'] = df['ema_200'].astype(float)
            df['ema_100'] = df['ema_100'].astype(float)
            df['ema_50'] = df['ema_50'].astype(float)
            df['ema_20'] = df['ema_20'].astype(float)

        if process_fresh == True:
            print('Processing Fresh ..........................')
            close = df2['close'].to_numpy()
            EMA_20 = await pine_ema(close, 20)
            EMA_50 = await pine_ema(close, 50)
            EMA_100 = await pine_ema(close, 100)
            EMA_200 = await pine_ema(close, 200)
            df2['EMA_20'] = EMA_20
            df2['EMA_50'] = EMA_50
            df2['EMA_100'] = EMA_100
            df2['EMA_200'] = EMA_200
            df2.replace({np.nan: None}, inplace=True)
            ema_200_color = ema_100_color = ema_50_color = ema_20_color = ''
            df2.replace({np.nan: None}, inplace=True)
            for index, row in df2.iterrows():
                datetime_val = row['datetime']  
                ema_20_val = row['EMA_20'] 
                ema_50_val = row['EMA_50']
                EMA_100_val = row['EMA_100']
                EMA_200_val = row['EMA_200']
                ema_200_color = ema_100_color = ema_50_color = ema_20_color = 'SELL'
                close_value = row['close']
                if close_value > EMA_200_val:
                    ema_200_color = 'BUY'
                if close_value > EMA_100_val:
                    ema_100_color = 'BUY'
                if close_value > ema_50_val:
                    ema_50_color = 'BUY'
                if close_value > ema_20_val:
                    ema_20_color = 'BUY'
                await db.update_thirty_min_ema(EMA_200_val, ema_200_color,EMA_100_val, ema_100_color, ema_50_val, ema_50_color, ema_20_val, ema_20_color, exchange_code, datetime_val)
        elif process_fresh == False:
            previous_ema_200 = df.iloc[0]['ema_200']
            previous_ema_100 = df.iloc[0]['ema_100']
            previous_ema_50 = df.iloc[0]['ema_50']
            previous_ema_20 = df.iloc[0]['ema_20']
            close_values = df2.close
            i = 0
            ema_200_color = ema_100_color = ema_50_color = ema_20_color = ''
            for close_value in close_values:
                ema_200_color = ema_100_color = ema_50_color = ema_20_color = 'SELL'
                datetime_val = df2.iloc[i]['datetime']
                
                ema_200 = calculate_new_ema(close_value, previous_ema_200, 200)
                if close_value > ema_200:
                    ema_200_color = 'BUY'
                
                ema_100 = calculate_new_ema(close_value, previous_ema_100, 100)
                if close_value > ema_100:
                    ema_100_color = 'BUY'

                ema_50 = calculate_new_ema(close_value, previous_ema_50, 50)
                if close_value > ema_50:
                    ema_50_color = 'BUY'

                ema_20 = calculate_new_ema(close_value, previous_ema_20, 20)
                if close_value > ema_20:
                    ema_20_color = 'BUY'                 

                previous_ema_200 = ema_200
                previous_ema_100 = ema_100
                previous_ema_50 = ema_50
                previous_ema_20 = ema_20
                print('Updating ', exchange_code)
                await db.update_thirty_min_ema(ema_200, ema_200_color,ema_100, ema_100_color, ema_50, ema_50_color, ema_20, ema_20_color, exchange_code, datetime_val)
                i = i + 1
    return 1, None, count
async def process_hour_ema(df_all_stocks):
    count = 0
    process_fresh = False
    for index, row in df_all_stocks.iterrows():
        count += 1
        process_fresh = False
        exchange_code = row['symbol']
        df2 = await db.get_hour_null_ema200(exchange_code)
        if len(df2) == 0:
            if log == True:
                print('1 hour No need to process ', exchange_code)
            continue
        else:
            df2['close'] = df2['close'].astype(float)
        df = await db.get_hour_last_non_null_ema200(exchange_code)
        if len(df) == 0:
            if log == True:
                print('data not found - get_hour_last_non_null_ema200')
            process_fresh = True
        else:
            df['ema_200'] = df['ema_200'].astype(float)
            df['ema_100'] = df['ema_100'].astype(float)
            df['ema_50'] = df['ema_50'].astype(float)
            df['ema_20'] = df['ema_20'].astype(float)

        if process_fresh == True:
            print('Processing Fresh ..........................')
            close = df2['close'].to_numpy()
            EMA_20 = await pine_ema(close, 20)
            EMA_50 = await pine_ema(close, 50)
            EMA_100 = await pine_ema(close, 100)
            EMA_200 = await pine_ema(close, 200)
            df2['EMA_20'] = EMA_20
            df2['EMA_50'] = EMA_50
            df2['EMA_100'] = EMA_100
            df2['EMA_200'] = EMA_200
            df2.replace({np.nan: None}, inplace=True)
            ema_200_color = ema_100_color = ema_50_color = ema_20_color = ''
            df2.replace({np.nan: None}, inplace=True)
            for index, row in df2.iterrows():
                datetime_val = row['datetime']  
                ema_20_val = row['EMA_20'] 
                ema_50_val = row['EMA_50']
                EMA_100_val = row['EMA_100']
                EMA_200_val = row['EMA_200']
                ema_200_color = ema_100_color = ema_50_color = ema_20_color = 'SELL'
                close_value = row['close']
                if close_value > EMA_200_val:
                    ema_200_color = 'BUY'
                if close_value > EMA_100_val:
                    ema_100_color = 'BUY'
                if close_value > ema_50_val:
                    ema_50_color = 'BUY'
                if close_value > ema_20_val:
                    ema_20_color = 'BUY'
                await db.update_hourly_ema(EMA_200_val, ema_200_color,EMA_100_val, ema_100_color, ema_50_val, ema_50_color, ema_20_val, ema_20_color, exchange_code, datetime_val)
        elif process_fresh == False:
            previous_ema_200 = df.iloc[0]['ema_200']
            previous_ema_100 = df.iloc[0]['ema_100']
            previous_ema_50 = df.iloc[0]['ema_50']
            previous_ema_20 = df.iloc[0]['ema_20']
            close_values = df2.close
            i = 0
            ema_200_color = ema_100_color = ema_50_color = ema_20_color = ''
            for close_value in close_values:
                ema_200_color = ema_100_color = ema_50_color = ema_20_color = 'SELL'
                datetime_val = df2.iloc[i]['datetime']
                
                ema_200 = calculate_new_ema(close_value, previous_ema_200, 200)
                if close_value > ema_200:
                    ema_200_color = 'BUY'
                
                ema_100 = calculate_new_ema(close_value, previous_ema_100, 100)
                if close_value > ema_100:
                    ema_100_color = 'BUY'

                ema_50 = calculate_new_ema(close_value, previous_ema_50, 50)
                if close_value > ema_50:
                    ema_50_color = 'BUY'

                ema_20 = calculate_new_ema(close_value, previous_ema_20, 20)
                if close_value > ema_20:
                    ema_20_color = 'BUY'                 

                previous_ema_200 = ema_200
                previous_ema_100 = ema_100
                previous_ema_50 = ema_50
                previous_ema_20 = ema_20
                print('Updating ', exchange_code)
                await db.update_hourly_ema(ema_200, ema_200_color,ema_100, ema_100_color, ema_50, ema_50_color, ema_20, ema_20_color, exchange_code, datetime_val)
                i = i + 1
    return 1, None, count

async def process_daily_ema(df_all_stocks):
    # get_basket_symbols_to_trade for main file
    df_all_stocks = await db.get_symbols_instruments_to_trade()
    count = 0
    process_fresh = False
    for index, row in df_all_stocks.iterrows():
        count += 1
        process_fresh = False
        exchange_code = row['symbol']
        df2 = await db.get_daily_null_ema200(exchange_code)
        if len(df2) == 0:
            if log == True:
                print('Daily ema No need to process ', exchange_code)
            continue
        else:
            df2['close'] = df2['close'].astype(float)
        df = await db.get_day_last_non_null_ema200(exchange_code)
        if len(df) == 0:
            if log == True:
                print('data not found - get_day_last_non_null_ema200')
            process_fresh = True
        else:
            df['ema_200'] = df['ema_200'].astype(float)
            df['ema_100'] = df['ema_100'].astype(float)
            df['ema_50'] = df['ema_50'].astype(float)
            df['ema_20'] = df['ema_20'].astype(float)

        if process_fresh == True:
            print('Processing Fresh ..........................')
            close = df2['close'].to_numpy()
            EMA_20 = await pine_ema(close, 20)
            EMA_50 = await pine_ema(close, 50)
            EMA_100 = await pine_ema(close, 100)
            EMA_200 = await pine_ema(close, 200)
            df2['EMA_20'] = EMA_20
            df2['EMA_50'] = EMA_50
            df2['EMA_100'] = EMA_100
            df2['EMA_200'] = EMA_200
            df2.replace({np.nan: None}, inplace=True)
            ema_200_color = ema_100_color = ema_50_color = ema_20_color = ''
            df2.replace({np.nan: None}, inplace=True)
            for index, row in df2.iterrows():
                datetime_val = row['datetime']  
                ema_20_val = row['EMA_20'] 
                ema_50_val = row['EMA_50']
                EMA_100_val = row['EMA_100']
                EMA_200_val = row['EMA_200']
                ema_200_color = ema_100_color = ema_50_color = ema_20_color = 'SELL'
                close_value = row['close']
                if close_value > EMA_200_val:
                    ema_200_color = 'BUY'
                if close_value > EMA_100_val:
                    ema_100_color = 'BUY'
                if close_value > ema_50_val:
                    ema_50_color = 'BUY'
                if close_value > ema_20_val:
                    ema_20_color = 'BUY'
                await db.update_daily_ema(EMA_200_val, ema_200_color,EMA_100_val, ema_100_color, ema_50_val, ema_50_color, ema_20_val, ema_20_color, exchange_code, datetime_val)
        elif process_fresh == False:
            previous_ema_200 = df.iloc[0]['ema_200']
            previous_ema_100 = df.iloc[0]['ema_100']
            previous_ema_50 = df.iloc[0]['ema_50']
            previous_ema_20 = df.iloc[0]['ema_20']
            close_values = df2.close
            i = 0
            ema_200_color = ema_100_color = ema_50_color = ema_20_color = ''
            for close_value in close_values:
                ema_200_color = ema_100_color = ema_50_color = ema_20_color = 'SELL'
                datetime_val = df2.iloc[i]['datetime']
                
                ema_200 = calculate_new_ema(close_value, previous_ema_200, 200)
                if close_value > ema_200:
                    ema_200_color = 'BUY'
                
                ema_100 = calculate_new_ema(close_value, previous_ema_100, 100)
                if close_value > ema_100:
                    ema_100_color = 'BUY'

                ema_50 = calculate_new_ema(close_value, previous_ema_50, 50)
                if close_value > ema_50:
                    ema_50_color = 'BUY'

                ema_20 = calculate_new_ema(close_value, previous_ema_20, 20)
                if close_value > ema_20:
                    ema_20_color = 'BUY'                 

                previous_ema_200 = ema_200
                previous_ema_100 = ema_100
                previous_ema_50 = ema_50
                previous_ema_20 = ema_20
                print('Updating ', exchange_code)
                await db.update_daily_ema(ema_200, ema_200_color,ema_100, ema_100_color, ema_50, ema_50_color, ema_20, ema_20_color, exchange_code, datetime_val)
                i = i + 1
    return 1, None, count
async def process_indicators_daily(df):
    # await process_daily_ema(df)
    # await process_histogram_daily(df)
    # await process_supertrend_day(df)
    # await process_volume_daily(df)
    # await update_daily_dashboard(df)

    return 1, None, 1

async def download_fivemin_ohlc(df_all_stocks):
    global last_working_day
    last_working_day_str = last_working_day.strftime('%d-%m-%Y')
    count = 0
    for index, row in df_all_stocks.iterrows():
        exchange_code = row['symbol']
        df_last_datetime = await db.get_last_datetime_five_min(exchange_code)
        len_df_last_datetime = len(df_last_datetime)
        if log == True:
            print(f"{exchange_code=} {len_df_last_datetime=}")
        if len(df_last_datetime) == 0:
            if log == True:
                print('lastdate not found for ', exchange_code)
            # download 200 days data
            days_prior = yesterday - timedelta(days=8)
            startdate = days_prior
            #sdate_iso = days_prior.isoformat()[:10] + 'T09:15:00.000Z'
            startdate_str = days_prior.strftime('%d-%m-%Y HH:MM:00')
        else:
            last_date = df_last_datetime.datetime.iloc[0]
            if log == True:
                print(f"{exchange_code} {last_date=}")
            startdate = last_date
            #sdate_iso = last_date.isoformat()[:10] + 'T09:15:00.000Z'
            startdate_str = last_date.strftime('%d-%m-%Y HH:MM:00')
        print('Timestamp class instance ', type(Timestamp))
        if not isinstance(startdate, (date, Timestamp)):
            print('in if not isinstance')
            if isinstance(startdate, Timestamp):
                startdate = startdate.to_pydatetime().date()
            else:
                print('in else')
                startdate = startdate.date()
        else:
            print('in outer else')   
        print(type(startdate), type(last_working_day))   
        print(f"{startdate=} {last_working_day=}")
        if startdate >= last_working_day:
            last_working_day_str = last_working_day.strftime('%d-%m-%Y')
            if log == True:
                important_data = f"{exchange_code} startdate:{startdate_str} > last_working_day:{last_working_day_str}"
                print(important_data)
            await db.pre_process_logs(today_str, 'download_fivemin_ohlc', 'startdate >= last_working_dayignore', important_data, 0)
            continue
        if log == True:
            important_data = f"{exchange_code=} {startdate_str=} {last_working_day_str=}"
            await db.pre_process_logs(today_str, 'download_fivemin_ohlc', 'download using zerodha', important_data, 0)

        result = 0
        df = ""
        try:
            if log == True:
                print('gethistorical_daily', exchange_code)
            df = await get_data_zerodha('5minute', startdate, last_working_day, exchange_code)
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

        for index, row in df.iterrows():
            date_val = row['date']
            open_val = row['open']
            high_val = row['high']
            low_val = row['low']
            close_val = row['close']
            volume_val = row['volume']
            await db.insert_five_min_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)
            if log == True:
                print('insert_five_min', exchange_code, date_val)
        count += 1
    if count > 0:
        return 1, 'None', count
    else:
        return 0, 'Unknown Error', count

async def download_thirtymin_ohlc(df_all_stocks):
    print('download_thirtymin_ohlc')
    global last_working_day
    last_working_day_str = last_working_day.strftime('%d-%m-%Y')
    count = 0
    for index, row in df_all_stocks.iterrows():
        exchange_code = row['symbol']
        df_last_datetime = await db.get_last_datetime_thirty_min(exchange_code)
        len_df_last_datetime = len(df_last_datetime)
        if log == True:
            print(f"{exchange_code=} {len_df_last_datetime=}")
        if len(df_last_datetime) == 0:
            if log == True:
                print('lastdate not found for ', exchange_code)
            # download 200 days data
            days_prior = yesterday - timedelta(days=8)
            startdate = days_prior
            startdate_str = days_prior.strftime('%d-%m-%Y HH:MM:00')
        else:
            last_date = df_last_datetime.datetime.iloc[0]
            if log == True:
                print(f"{exchange_code} {last_date=}")
            startdate = last_date
            startdate_str = last_date.strftime('%d-%m-%Y HH:MM:00')
        if not isinstance(startdate, date):
            startdate = startdate.date()            
        if startdate >= last_working_day:
            last_working_day_str = last_working_day.strftime('%d-%m-%Y')
            if log == True:
                important_data = f"{exchange_code} startdate:{startdate_str} > last_working_day:{last_working_day_str}"
                print(important_data)
                await db.pre_process_logs(today_str, 'download_thirtymin_ohlc', 'startdate >= last_working_dayignore', important_data, 0)
            continue

        if log == True:
            important_data = f"{exchange_code=} {startdate_str=} {last_working_day=}"
            await db.pre_process_logs(today_str, 'download_thirtymin_ohlc', 'download using zerodha', important_data, 0)

        result = 0
        df = ""
        try:
            if log == True:
                print('gethistorical_daily', exchange_code)
            df = await get_data_zerodha('30minute', startdate, last_working_day, exchange_code)
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
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'Data not available', 'len df 0', 4)
            continue
        # df = df[(df['datetime'].dt.time >= pd.to_datetime('09:15:00').time()) & 
        #          (df['datetime'].dt.time <= pd.to_datetime('15:30:00').time())]
        for index, row in df.iterrows():
            date_val = row['date']
            open_val = row['open']
            high_val = row['high']
            low_val = row['low']
            close_val = row['close']
            volume_val = row['volume']
            await db.insert_thirty_min_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)
            if log == True:
                print('insert_thirty_min', exchange_code, date_val)
        count += 1
    if count > 0:
        return 1, 'None', count
    else:
        return 0, 'Unknown Error', count

async def download_fifteen_min_ohlc(df_all_stocks):
    print('download_fifteen_min_ohlc')
    global last_working_day
    last_working_day_str = last_working_day.strftime('%d-%m-%Y')
    count = 0
    for index, row in df_all_stocks.iterrows():
        exchange_code = row['symbol']
        df_last_datetime = await db.get_last_datetime_fifteen_min(exchange_code)
        #df_last_datetime['datetime'] = pd.to_datetime(df_last_datetime['datetime'])
        print(df_last_datetime)
        len_df_last_datetime = len(df_last_datetime)
        if log == True:
            print(f"{exchange_code=} {len_df_last_datetime=}")
        if len(df_last_datetime) == 0:
            if log == True:
                print('lastdate not found for ', exchange_code)
            # download 200 days data
            days_prior = yesterday - timedelta(days=15)
            startdate = days_prior
            #sdate_iso = days_prior.isoformat()[:10] + 'T09:15:00.000Z'
            startdate_str = days_prior.strftime('%d-%m-%Y HH:MM:00')
        else:
            last_date = df_last_datetime['datetime'].iloc[0]
            if log == True:
                print(f"{exchange_code} {last_date=}")
            startdate = last_date
            #sdate_iso = last_date.isoformat()[:10] + 'T09:15:00.000Z'
            startdate_str = last_date.strftime('%d-%m-%Y HH:MM:00')
        print(startdate, type(startdate))
        print(last_working_day, type(last_working_day))    
        if not isinstance(startdate, date):
            startdate = startdate.date()
        if startdate >= last_working_day:
            last_working_day_str = last_working_day.strftime('%d-%m-%Y')
            if log == True:
                important_data = f"{exchange_code} startdate:{startdate_str} > last_working_day:{last_working_day_str}"
                print(important_data)
                await db.pre_process_logs(today_str, 'download_fifteen_min_ohlc', 'startdate >= last_working_dayignore', important_data, 0)
            continue
        #enddate_iso = last_working_day.isoformat()[:10] + 'T15:30:00.000Z'
        if log == True:
            important_data = f"{exchange_code=} {startdate_str=} {last_working_day=}"
            await db.pre_process_logs(today_str, 'download_fifteen_min_ohlc', 'download using zerodha', important_data, 0)

        result = 0
        df = ""
        try:
            df = await get_data_zerodha('15minute', startdate, last_working_day, exchange_code)
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
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'download_fifteen_min_ohlc', 'df str', 4)
            continue
        if len(df) == 0:
            if log == True:
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'download_fifteen_min_ohlc', 'df len 0', 4)
            continue
       
        for index, row in df.iterrows():
            date_val = row['date']
            open_val = row['open']
            high_val = row['high']
            low_val = row['low']
            close_val = row['close']
            volume_val = row['volume']
            await db.insert_fifteen_min_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)
        count += 1
    if count > 0:
        return 1, 'None', count
    else:
        return 0, 'Unknown Error', count

async def download_1hour_ohlc(df_all_stocks):
    print('download_1hour_ohlc')
    global last_working_day
    last_working_day_str = last_working_day.strftime('%d-%m-%Y')
    count = 0
    for index, row in df_all_stocks.iterrows():
        exchange_code = row['symbol']
        icici_code = row['symbol']
        df_last_datetime = await db.get_last_datetime_one_hour_ohlc(exchange_code)
        len_df_last_datetime = len(df_last_datetime)
        if log == True:
            print(f"{exchange_code=} {len_df_last_datetime=}")
        if len(df_last_datetime) == 0:
            if log == True:
                print('lastdate not found for ', exchange_code)
            # download 200 days data
            days_prior = yesterday - timedelta(days=50)
            startdate = days_prior
            #sdate_iso = days_prior.isoformat()[:10] + 'T09:15:00.000Z'
            startdate_str = days_prior.strftime('%d-%m-%Y HH:MM:00')
        else:
            last_date = df_last_datetime.datetime.iloc[0]
            if log == True:
                print(f"{exchange_code} {last_date=}")
            startdate = last_date
            #sdate_iso = last_date.isoformat()[:10] + 'T09:15:00.000Z'
            startdate_str = last_date.strftime('%d-%m-%Y HH:MM:00')
        if not isinstance(startdate, date):
            startdate = startdate.date()
        if startdate >= last_working_day:
            last_working_day_str = last_working_day.strftime('%d-%m-%Y')
            if log == True:
                important_data = f"{exchange_code} startdate:{startdate_str} > last_working_day:{last_working_day_str}"
                print(important_data)
                await db.pre_process_logs(today_str, 'download_1hour_ohlc', 'startdate >= last_working_dayignore', important_data, 0)
            continue
        #enddate_iso = last_working_day.isoformat()[:10] + 'T15:30:00.000Z'
        if log == True:
            important_data = f"{exchange_code=} {startdate_str=} {last_working_day=}"
            await db.pre_process_logs(today_str, 'download_1hour_ohlc', 'download using zerodha', important_data, 0)

        result = 0
        df = ""
        try:
            df = await get_data_zerodha('60minute', startdate, last_working_day, exchange_code)
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
                await db.pre_process_logs(today_str, 'gethistorical_cash', 'Data not available', 'len(df) is 0', 4)
            continue
        
        for index, row in df.iterrows():
            date_val = row['date']
            open_val = row['open']
            high_val = row['high']
            low_val = row['low']
            close_val = row['close']
            volume_val = row['volume']
            await db.insert_one_hour_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)

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
    await db.run_query('truncate table monitor_symbols;')
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
    # Recreate a list of symbols for which data downloading is required
    #await update_symbols_to_monitor()
    
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
        if action == 'Download Daily OHLC of Stocks':
            result, error, count_symbol = await download_daily_ohlc_of_stocks(df_all_stocks)
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download_daily_ohlc_of_stocks', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_ohlc_date()
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'process_indicators_daily':
            result, error, count_symbol = await process_indicators_daily(df_all_stocks)
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'process_indicators_daily', 'function result', important_data, 1)
            if result == 1:
                await db.update_pre_market_steps(id, last_status=1, last_record_date=current_date_string)
        elif action == 'download fivemin ohlc':
            result, error, count_symbol = await download_fivemin_ohlc(df_all_stocks)
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'download fivemin ohlc', 'function result', important_data, 1)
            if result == 1:
                last_record_date = await db.get_last_fivemin_ohlc_date()
                last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
                await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        elif action == 'process_fivemin_heikin':
            result, error, count_symbol = await process_fivemin_heikin(df_all_stocks)
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'process_fivemin_heikin', 'function result', important_data, 1)
            if result == 1:
                await db.update_pre_market_steps(id, last_status=1, last_record_date=current_date_string)
        elif action == 'update_symbols_to_monitor':
            result, error, count_symbol = await update_symbols_to_monitor()
            current_date_string = datetime.now().strftime("%Y-%m-%d")
            important_data = f"{result=} {error=} {count_symbol=} {id=}"
            await db.pre_process_logs(current_date_string, 'update_symbols_to_monitor', 'function result', important_data, 1)
            if result == 1:
                await db.update_pre_market_steps(id, last_status=1, last_record_date=current_date_string)

        # elif action == 'process_indicators_five_min':
        #     result, error, count_symbol = await process_indicators_five_min(df_all_stocks)
        #     current_date_string = datetime.now().strftime("%Y-%m-%d")
        #     important_data = f"{result=} {error=} {count_symbol=} {id=}"
        #     await db.pre_process_logs(current_date_string, 'process_indicators_five_min', 'function result', important_data, 1)
        #     if result == 1:
        #         await db.update_pre_market_steps(id, last_status=1, last_record_date=current_date_string)
        # elif action == 'download thirty_min ohlc':
        #     result, error, count_symbol = await download_thirtymin_ohlc(df_all_stocks)
        #     current_date_string = datetime.now().strftime("%Y-%m-%d")
        #     important_data = f"{result=} {error=} {count_symbol=} {id=}"
        #     await db.pre_process_logs(current_date_string, 'download thirtymin ohlc', 'function result', important_data, 1)
        #     if result == 1:
        #         last_record_date = await db.get_last_thirty_min_ohlc_date()
        #         last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
        #         await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        # elif action == 'process_indicators_thirty_min':
        #     result, error, count_symbol = await process_indicators_thirty_min(df_all_stocks)
        #     current_date_string = datetime.now().strftime("%Y-%m-%d")
        #     important_data = f"{result=} {error=} {count_symbol=} {id=}"
        #     await db.pre_process_logs(current_date_string, 'process_indicators_five_min', 'function result', important_data, 1)
        #     if result == 1:
        #         await db.update_pre_market_steps(id, last_status=1, last_record_date=current_date_string)
        # elif action == 'download fiften_min ohlc':
        #     result, error, count_symbol = await download_fifteen_min_ohlc(df_all_stocks)
        #     current_date_string = datetime.now().strftime("%Y-%m-%d")
        #     important_data = f"{result=} {error=} {count_symbol=} {id=}"
        #     await db.pre_process_logs(current_date_string, 'download thirtymin ohlc', 'function result', important_data, 1)
        #     if result == 1:
        #         last_record_date = await db.get_last_fifteen_min_ohlc_date()
        #         last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
        #         await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        # elif action == 'process_indicators_fifteen_min':
        #     result, error, count_symbol = await process_indicators_fifteen_min(df_all_stocks)
        #     current_date_string = datetime.now().strftime("%Y-%m-%d")
        #     important_data = f"{result=} {error=} {count_symbol=} {id=}"
        #     await db.pre_process_logs(current_date_string, 'process_indicators_fifteen_min', 'function result', important_data, 1)
        #     if result == 1:
        #         await db.update_pre_market_steps(id, last_status=1, last_record_date=current_date_string)
        # elif action == 'download one_hour ohlc':
        #     result, error, count_symbol = await download_1hour_ohlc(df_all_stocks)
        #     current_date_string = datetime.now().strftime("%Y-%m-%d")
        #     important_data = f"{result=} {error=} {count_symbol=} {id=}"
        #     await db.pre_process_logs(current_date_string, 'download_1hour_ohlc', 'function result', important_data, 1)
        #     if result == 1:
        #         last_record_date = await db.get_last_fifteen_min_ohlc_date()
        #         last_record_date_str = last_record_date.datetime.iloc[0].strftime('%Y-%m-%d')
        #         await db.update_pre_market_steps(id, last_status=1, last_record_date=last_record_date_str)
        # elif action == 'process_indicators_one_hour':
        #     result, error, count_symbol = await process_indicators_one_hour(df_all_stocks)
        #     current_date_string = datetime.now().strftime("%Y-%m-%d")
        #     important_data = f"{result=} {error=} {count_symbol=} {id=}"
        #     await db.pre_process_logs(current_date_string, 'process_indicators_one_hour', 'function result', important_data, 1)
        #     if result == 1:
        #         await db.update_pre_market_steps(id, last_status=1, last_record_date=current_date_string)
  
    await db.close_pool()
    print('Done')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


