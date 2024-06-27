# Download Live market Data
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
print(os.getcwd())
from background.zerodha import zeroda
from datetime import datetime, date, timedelta, time as tm
from background.instruments import instruments
import pandas as pd
import pandas_ta as ta
import numpy as np
import math
import warnings
import os
import asyncio
import time
from background.async_db import dbconnection
warnings.filterwarnings('ignore')

class Start(object):
    def __init__(self):
        self.zerodha = zeroda('live', datetime.today())
        self.status = self.zerodha.status
        print('login status', self.status)
        if self.status == False:
            print('Login Failed')
            exit()
        self.db = dbconnection()
        self.instruments = instruments()
        self.exchange = 'NFO'
        self.userid = 'koshy'
        self.sdate = datetime.now()
        self.log = True
        self.run_job = True
        self.initiate_time = tm(9,15,1)
        self.exit_time = tm(15, 30)
        self.today = date.today()  
        self.today_str = self.today.strftime('%Y-%m-%d')
        self.yesterday = self.today - timedelta(days=1)
        self.last_working_day = self.yesterday
        if self.last_working_day.weekday() == 5:
            self.last_working_day = self.last_working_day - timedelta(days=1)
        elif self.last_working_day.weekday() == 6:
            self.last_working_day = self.last_working_day - timedelta(days=2)
        self.last_working_day_str = self.last_working_day.strftime('%d-%m-%Y')
        self.interval_to_table = {
            'minute': 'one_min_ohlc', '5minute': 'five_min_ohlc', '3minute': 'three_min_ohlc', '10minute': 'ten_min_ohlc',
            '15minute': 'fifteen_min_ohlc', '30minute': 'thirty_min_ohlc', '60minute': 'one_hour_ohlc'
        }
        self.one_min_table_summary = pd.DataFrame()
        self.one_hour_table_summary = pd.DataFrame()
    async def start_pool(self):
        #print('start pool')
        loop = asyncio.get_event_loop()
        await self.db.create_pool(loop=loop)
    
    async def close_pool(self):
        await self.db.close_pool()

    async def get_data_zerodha_recursive(self, interval, from_date, edate, token, symbol):
        print('---------- in get_data_zerodha_recursive --------')
        #print('from_date', type(from_date), 'edate:', type(edate))
        # Ensure that we send datetime only, so there is no need to check
        # if not isinstance(from_date, datetime):
        #     from_date = datetime.combine(from_date, tm(9, 15, 0))

        # if not isinstance(edate, datetime):
        #     edate = datetime.combine(edate, tm(15, 30, 0))

        to_date = edate

        # Can we use numpy here ?
        data_frames = []  # List to store DataFrames
        days = 95 # Check Max possible
        if interval == 'minute':
            days = 50;
        invalid_token = False
        error = ''
        print(f"{from_date=} {edate=}")
        while from_date < edate:
            if from_date >= (edate - timedelta(days)):
                # ---------- Check if we can send time along with date
                #df = self.zerodha.gethistoricaldata(token, from_date.date(), edate.date(), interval)
                status, df, Error = self.zerodha.gethistoricaldata_v2(token, from_date, edate, interval)
                if status == 0:
                    error = "{}".format(Error)
                    print(f"get_data_zerodha_recursive {error=}")
                    if error == 'invalid token':
                        invalid_token = True
                        info = f"invalid token {symbol}"
                        await self.db.pre_process_logs(self.today_str, 'download_ohlc', 'invalid token', info, 5)
                        await self.db.run_query(f"update monitor_symbols set active = 0 where symbol = '{symbol}'")
                    break
                else:
                    data_frames.append(df)
                break
            else:
                to_date = from_date + timedelta(days)
                status, df, Error = self.zerodha.gethistoricaldata_v2(token, from_date, edate, interval)
                if status == 0:
                    if Error == 'invalid token':
                        info = f"invalid token {symbol}"
                        await self.db.pre_process_logs(self.today_str, 'download_ohlc', 'invalid token', info, 5)
                        await self.db.run_query(f"update monitor_symbols set active = 0 where symbol = '{symbol}'")
                else:
                    data_frames.append(df)
                from_date = to_date
        if data_frames:
            data = pd.concat(data_frames, ignore_index=True)
            return 1, data, None
        else:
            print("No data frames to concatenate")
            return 0, None, error

    async def download_ohlc(self, tpl_stocks, interval):
        table_name =  self.interval_to_table.get(interval, None)
        end_date_today = datetime.today().replace(hour=15, minute=30, second=0, microsecond=0)
        end_date_now = datetime.now().replace(second=0, microsecond=0)

        for instrument_token, exchange_code in tpl_stocks:
            disable = False
            # Process if enabled
            filtered_df = None
            if interval == 'minute' and len(self.one_min_table_summary) > 0:
                filtered_df = self.one_min_table_summary[self.one_min_table_summary.symbol == exchange_code]
            elif interval == '60minute' and len(self.one_hour_table_summary) > 0:
                filtered_df = self.one_hour_table_summary[self.one_hour_table_summary.symbol == exchange_code]
            
            is_enabled = filtered_df['enabled'].any()
            if is_enabled == False:
                print('Skipping as not enabled', exchange_code)
                continue
            else:
                print(exchange_code)

            # Get last updated date
            if filtered_df is not None and len(filtered_df) > 0:
                last_datetime = filtered_df.datetime.iloc[0]
                last_datetime = last_datetime.replace(second=0, microsecond=0)
                print('cache datetime available', last_datetime)                    
            else:
                print('no cache')
                last_datetime = datetime.today() - timedelta(days=90)
                last_datetime = last_datetime.replace(hour=9, minute=15, second=0, microsecond=0)

            print(f"{last_datetime=}")
            result = status = 0
            df = pd.DataFrame()
            try:
                print('start date', last_datetime)
                print(f"{end_date_now=}, {end_date_today=}")
                if last_datetime >= end_date_now:
                    print(f"skipping last_datetime:{last_datetime} >= end_date_now:{end_date_now}", instrument_token)
                    continue
                elif interval == '60minute':
                    exptime = last_datetime + timedelta(hours=1)
                    if exptime > datetime.now():
                        print(f'skipping {exptime=}', instrument_token)
                        continue
                status, df, Error = await self.get_data_zerodha_recursive(interval, last_datetime, end_date_today, instrument_token, exchange_code)
            except Exception as e:
                if self.log == True:
                    print('Error in downloading', exchange_code, e)
                    await self.db.pre_process_logs(self.today_str, 'gethistorical_cash', 'Error downloading', '', 3)
                result = -1
            if status == 1 and len(df) > 0:
                # filter data for market timing
                df = df[(df['date'].dt.time >= pd.to_datetime('09:15:00').time()) & 
                    (df['date'].dt.time <= pd.to_datetime('15:30:00').time())]
                result = 1
                print('data filtered')
            else:
                if interval == 'minute':
                    self.one_min_table_summary.loc[self.one_min_table_summary.symbol == exchange_code, 'fail_count'] += 1
                    print('minute failcount incremented')
                elif interval == '60minute':
                    self.one_hour_table_summary.loc[self.one_hour_table_summary.symbol == exchange_code, 'fail_count'] += 1
                    print('hour failcount incremented')
                
                if self.log == True:
                    await self.db.pre_process_logs(self.today_str, 'gethistorical_cash', 'download using history api', 'len data = 0', 1)
                print('skipping indicator calculation')
                continue
            if result == -1:
                if self.log == True:
                    await self.db.pre_process_logs(self.today_str, 'gethistorical_cash', 'download using history api', 'Error', 4)
                if interval == 'minute':
                    self.one_min_table_summary.loc[self.one_min_table_summary.symbol == exchange_code, 'enabled'] = False
                    print('disabled locally')
                elif interval == '60minute':
                    self.one_hour_table_summary.loc[self.one_hour_table_summary.symbol == exchange_code, 'enabled'] = False
                    print('disabled locally')
                print('skipping indicator calculation')
                continue
            if status == 1 and type(df) is str:
                if self.log == True:
                    print(df) 
                    await self.db.pre_process_logs(self.today_str, 'gethistorical_cash', 'download using history api', 'df str', 4)
                if interval == 'minute':
                    self.one_min_table_summary.loc[self.one_min_table_summary.symbol == exchange_code, 'fail_count'] += 1
                    print('fail count increased')
                elif interval == '60minute':
                    self.one_hour_table_summary.loc[self.one_hour_table_summary.symbol == exchange_code, 'fail_count'] += 1
                    print('fail count increased')
                print('skipping indicator calculation')
                continue
            if status == 0:
                if Error == 'invalid token':
                    if interval == 'minute':
                        self.one_min_table_summary.loc[self.one_min_table_summary.symbol == exchange_code, 'enabled'] = False
                        print('disabled locally')
                    elif interval == '60minute':
                        self.one_hour_table_summary.loc[self.one_hour_table_summary.symbol == exchange_code, 'enabled'] = False
                        print('disabled locally')
                if self.log == True:
                    await self.db.pre_process_logs(self.today_str, 'gethistorical_cash', 'Data not available', '', 4)
                print('skipping indicator calculation')
                continue
            # --------------- See if we can use numpy here
            data = ta.candles.ha(df['open'], df['high'], df['low'], df['close'])
            df['ha_open'] = data['HA_open'].astype(float).round(2)
            df['ha_high'] = data['HA_high'].astype(float).round(2)
            df['ha_low'] = data['HA_low'].astype(float).round(2)
            df['ha_close'] = data['HA_close'].astype(float).round(2)
            df.dropna(inplace=True)

            # Remove Duplicate Prior data
            df['date'] = df['date'].dt.tz_localize(None)
            df = df.loc[df['date'] > last_datetime]
            # if self.log == True:
            #     print(df.tail())

            BATCH_SIZE = 1000
            batch_data = []
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
                batch_data.append((exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val, ha_open, ha_high, ha_low, ha_close))

                if len(batch_data) >= BATCH_SIZE:
                    await self.db.insert_batch_data(table_name, batch_data)
                    batch_data = []

            if batch_data:
                print('insert_batch_data', exchange_code)
                print(batch_data)
                await self.db.insert_batch_data(table_name, batch_data)
            
            if len(df) > 0:
                end_date = df['date'].iloc[-1]
            else:
                end_date = last_datetime

            if isinstance(end_date, datetime):
                end_date_date = end_date.date()

            now = datetime.now()
            target_time = now.replace(hour=9, minute=20, second=0, microsecond=0)
            
            if now > target_time and end_date_date < datetime.today().date():
                end_date = target_time
            if end_date_date <  datetime.today().date() - timedelta(days=5):
                info = f"No data 5 days disable {exchange_code}"
                await self.db.pre_process_logs(self.today_str, 'download_ohlc', 'Data not available', info, 5)
                await self.db.run_query(f"update monitor_symbols set active = 0 where symbol = '{exchange_code}'")
                end_date = now.replace(hour=15, minute=30, second=0, microsecond=0)

            end_date = end_date.replace(second=0, microsecond=0) 

            if interval == 'minute' and len(self.one_min_table_summary) > 0:
                self.one_min_table_summary.loc[self.one_min_table_summary.symbol == exchange_code, 'datetime'] = end_date
            elif interval == '60minute' and len(self.one_hour_table_summary) > 0:
                self.one_hour_table_summary.loc[self.one_hour_table_summary.symbol == exchange_code, 'datetime'] = end_date
            # Just for verification
            filtered_summary = self.one_hour_table_summary[self.one_hour_table_summary.symbol == exchange_code]
            if len(filtered_summary) > 0:
                last_datetime = filtered_summary.datetime.iloc[0]
                last_datetime = last_datetime.replace(second=0, microsecond=0)
                print(f"new {last_datetime=}")
            #break
        return 1, 'None', 1

async def main():
    #--------------------- running on test database -------------
    start = Start()
    current_datetime = datetime.now()
    await start.start_pool()
    start_time = time.time()
    #start.one_min_table_summary = await start.db.get_table_datetime_groupby('one_min_ohlc')
    start.one_hour_table_summary = await start.db.get_table_datetime_groupby('one_hour_ohlc')
    start.one_hour_table_summary['enabled'] = True
    start.one_hour_table_summary['fail_count'] = 0
    #print(start.one_hour_table_summary)
    print('table summary downloaded')

    priority_stocks_tpl = await start.db.get_priority_instruments_to_trade()
    non_priority_stocks_tpl = await start.db.get_non_priority_instruments_to_trade()
    interval = '60minute'
   
    await start.download_ohlc(priority_stocks_tpl, interval)
    await start.download_ohlc(non_priority_stocks_tpl, interval)

    end_time = time.time()  # Record the end time
    total_time = end_time - start_time
    print(f"------------------------Total time taken to execute the code: {total_time:.2f} seconds")

    await asyncio.sleep(10)
    # Repeat same
    start_time = time.time()
    await start.download_ohlc(priority_stocks_tpl, interval)
    await start.download_ohlc(non_priority_stocks_tpl, interval)
    
    end_time = time.time()  # Record the end time
    total_time = end_time - start_time
    print(f"Total time taken to execute the code: {total_time:.2f} seconds")

    

    await asyncio.sleep(1)
    await start.close_pool()
    return

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.run(main())