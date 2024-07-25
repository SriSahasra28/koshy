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
from background.async_db import dbconnection
warnings.filterwarnings('ignore')
from numba import jit
import time

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

class Start(object):
    def __init__(self):
        self.backtest = 0
        test_date = date(2024, 1, 29)
        self.zerodha = zeroda('live', datetime.today())
        self.status = self.zerodha.status
        print('login status', self.status)
        if self.status == False:
            print('Login Failed')
            exit()
        self.db = dbconnection()
        self.instruments = instruments()
        self.ordertype = 'market'
        self.exchange = 'NFO'
        self.userid = 'koshy'
        self.sdate = datetime.now()
        #self.sdate_iso = self.sdate.isoformat()[:10] + 'T09:15:00.000Z'
        self.log = True
        self.trade =True
        self.run_job = True
        self.initiate_time = tm(9,15,1)
        self.exit_time = tm(15, 30)
        self.today = date.today()  
        self.today_str = self.today.strftime('%Y-%m-%d')
        self.yesterday = self.today - timedelta(days=1)
        self.last_working_day = self.yesterday
        self.fast = 12
        self.slow = 26
        self.signal = 9
        if self.last_working_day.weekday() == 5:
            self.last_working_day = self.last_working_day - timedelta(days=1)
        elif self.last_working_day.weekday() == 6:
            self.last_working_day = self.last_working_day - timedelta(days=2)
        self.interval_to_table = {
                    'minute': 'one_min_ohlc','2minute': 'two_min_ohlc', '5minute': 'five_min_ohlc', '3minute': 'three_min_ohlc', '10minute': 'ten_min_ohlc',
                    '15minute': 'fifteen_min_ohlc', '30minute': 'thirty_min_ohlc', '60minute': 'one_hour_ohlc'
                }
        self.data_collections = {f"{key}": {} for key in self.interval_to_table}
        self.dates_collections = {f"{key}": {} for key in self.interval_to_table}
        self.end_date_today = datetime.today().replace(hour=15, minute=30, second=0, microsecond=0)
        self.start_time_trans = tm(9, 15)
        self.end_time_trans = tm(15, 30)
        self.df_priority_stocks = None
        self.zerodha_last_trans = None
    async def start_pool(self):
        print('start pool')
        loop = asyncio.get_event_loop()
        await self.db.create_pool(loop=loop)
    
    async def close_pool(self):
        await self.db.close_pool()
    
    async def get_data_zerodha_recursive_list(self, interval, from_date, edate, token, symbol):
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
                status, data, Error = self.zerodha.gethistoricaldata_v3(token, from_date, edate, interval)
                if status == 0:
                    error = "{}".format(Error)
                    print(f"get_data_zerodha_recursive {error=}")
                    if error == 'invalid token':
                        invalid_token = True
                        info = f"invalid token {symbol}"
                        await self.db.pre_process_logs(self.today_str, 'download_ohlc', 'invalid token', info, 5)
                        await self.db.run_query(f"update monitor_symbols set active = 0 where symbol = '{symbol}'")
                    elif Error == 'Too many requests':
                        await asyncio.sleep(1)
                        info = f"{Error}"
                        await self.db.pre_process_logs(self.today_str, 'Error download_ohlc', symbol, info, 5)
                    break
                else:
                    data_list.extend(data)  # Append the list of dictionaries
                break
            else:
                to_date = from_date + timedelta(days)
                status, data, Error = self.zerodha.gethistoricaldata_v3(token, from_date, to_date, interval)
                if status == 0:
                    if Error == 'invalid token':
                        info = f"invalid token {symbol}"
                        await self.db.pre_process_logs(self.today_str, 'download_ohlc', 'invalid token', info, 5)
                        await self.db.run_query(f"update monitor_symbols set active = 0 where symbol = '{symbol}'")
                    elif Error == 'Too many requests':
                        await asyncio.sleep(1)
                        info = f"{Error}"
                        await self.db.pre_process_logs(self.today_str, 'Error download_ohlc', symbol, info, 5)
                    break
                else:
                    data_list.extend(data)  # Append the list of dictionaries
                from_date = to_date
        
        if data_list:
            return 1, data_list, None  # Return the merged list of dictionaries
        else:
            print("No data to process")
            return 0, None, error

    async def download_ohlc_v2(self, df_all_stocks, interval):
        # data_collections, dates_collections, zerodha_last_trans
        table_name =  self.interval_to_table.get(interval, None)
        end_date_now = datetime.now().replace(second=0, microsecond=0)
        error =''
        if end_date_now > self.end_date_today:
            end_date_now = self.end_date_today;
        count_iter = 0
        for index, row in df_all_stocks.iterrows():
            exchange_code = row['symbol']
            instrument_token = row['instrument_token'] # new added
            current_time = datetime.now().strftime("%H:%M:%S")
            print(current_time, exchange_code)
            #dates_list_old = []
            last_datetime = None
            # Get Last datetime for the symbol in database already downloaded
            if exchange_code in self.dates_collections[interval]:
                last_datetime = cutoff_datetime = self.dates_collections[interval][exchange_code][-1].replace(second=0, microsecond=0)
                print('cache datetime available', cutoff_datetime)  
            else:
                print('no cache')
                last_datetime = datetime.today() - timedelta(days=90)
                last_datetime = last_datetime.replace(hour=9, minute=15, second=0, microsecond=0)
                cutoff_datetime = last_datetime

            print(f"{exchange_code} {last_datetime=}")

            if isinstance(last_datetime, pd.Timestamp):
                last_datetime = last_datetime.to_pydatetime()
            
            if isinstance(cutoff_datetime, pd.Timestamp):
                cutoff_datetime = cutoff_datetime.to_pydatetime()
                #print(f"{last_datetime=} {cutoff_datetime=}")
            result = status = 0
            try:
                print(f"{end_date_now=}, {self.end_date_today=}")
                if cutoff_datetime >= end_date_now:
                    print(f"skipping cutoff_datetime:{cutoff_datetime} >= end_date_now:{end_date_now}", instrument_token)
                    continue
                elif interval == '60minute':
                    exptime = cutoff_datetime + timedelta(hours=1)
                    print(f"{exptime=}")
                    if exptime > self.end_date_today:
                        print(f'skipping {exptime=}', instrument_token)
                        continue
                print(f"{exchange_code} {last_datetime=}, {self.end_date_today=}")
                
                if self.zerodha_last_trans != None and last_datetime >= self.zerodha_last_trans:
                    print('skipping last_datetime >= zerodha_last_trans ', exchange_code)
                    continue
                else:
                    print(f"process as NOT last_datetime:{last_datetime} >= zerodha_last_trans: {self.zerodha_last_trans}")

                status, data, Error = await self.get_data_zerodha_recursive_list(interval, last_datetime, self.end_date_today, instrument_token, exchange_code)
            except Exception as e:
                print('Error in downloading', exchange_code, e)
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_v2', activity='get_data_zerodha', important_data=exchange_code, priority=2, strategy_trade_id = '', timestamp=end_date_now)
                result = -1

            if status == 1 and len(data) > 0:
                result = 1
            else:
                info = 'len data = 0'
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_v2', activity='get_data_zerodha', important_data=info, priority=2, strategy_trade_id = '', timestamp=end_date_now)
                print('no data skipping processing')
                await asyncio.sleep(0.25)
                continue
            if result == -1:
                # Another table
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_v2', activity='get_data_zerodha', important_data='Error', priority=2, strategy_trade_id = '', timestamp=end_date_now)
                print('Error getting data skipping processing')
                await asyncio.sleep(0.25)
                continue
            if status == 0:
                if Error == 'invalid token':
                    await self.db.run_query(f"update monitor_symbols set active = 0 where symbol = '{exchange_code}'")
                info = f"invalid token {exchange_code} {instrument_token}"
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_v2', activity='get_data_zerodha', important_data=info, priority=2, strategy_trade_id = '', timestamp=end_date_now)
                print('invalid token skipping processing')
                await asyncio.sleep(0.25)
                continue        

            dates_new = []
            dates_new = [entry['date'].replace(tzinfo=None) for entry in data]  # Convert datetime to timezone-naive
            opens = [entry['open'] for entry in data]
            highs = [entry['high'] for entry in data]
            lows = [entry['low'] for entry in data]
            closes = [entry['close'] for entry in data]

            if cutoff_datetime in dates_new:
                index = dates_new.index(cutoff_datetime)
                dates_new = dates_new[index+1:]
                opens = opens[index+1:]
                highs = highs[index+1:]
                lows = lows[index+1:]
                closes = closes[index+1:]
            else:
                print(f"cutoff_datetime: {cutoff_datetime} not found in the list.")

            dates_list_old = []
            if exchange_code in self.dates_collections[interval]:
                dates_list_old = self.dates_collections[interval][exchange_code]

            dates_combined = dates_new
            if len(dates_list_old) > 0:
                dates_combined = dates_list_old + dates_new
                self.dates_collections[interval][exchange_code] = dates_combined

            data_np_new = np.zeros((len(dates_new), 4), dtype='float64')
            data_np_new[:,0] = np.array(opens)
            data_np_new[:,1] = np.array(highs)
            data_np_new[:,2] = np.array(lows)
            data_np_new[:,3] = np.array(closes)
            if len(data_np_new) == 0:
                print('No Data to process skipping')
                continue
            data_combined = data_np_new

            if exchange_code in self.data_collections[interval]:
                print("exchange_code found in data_collection[interval]")
                data_np_old = self.data_collections[interval][exchange_code]
                if len(data_np_new) > 0 and len(data_np_old) > 0:
                    print('in if len(data_np_new) > 0 and len(data_np_old) > 0')
                    data_combined = np.vstack((data_np_old, data_np_new))
                    self.data_collections[interval][exchange_code] = data_combined
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
                self.data_collections[interval][exchange_code] = data_combined

            print('len data_combined:', len(data_combined), 'len dates_combined:', len(dates_combined))
            
            # calculate indicators
            ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(data_combined[:,0], data_combined[:,1], data_combined[:,2], data_combined[:,3])
            index_start = 0
            if cutoff_datetime in dates_combined:
                index_start = dates_combined.index(cutoff_datetime)

            count_iter = count_iter + 1
            BATCH_SIZE = 1000
            batch_data = []
            tasks = []
            total_count = len(dates_combined)
            for i in range(index_start + 1, total_count):
                date_val = dates_combined[i]
                python_time = date_val.time()
                is_within_range = self.start_time_trans <= python_time <= self.end_time_trans
                if is_within_range == False:
                    print(f'Time beyond range {date_val}')
                    continue
                open_val = data_combined[i,0]
                high_val = data_combined[i,1]
                low_val = data_combined[i,2]
                close_val = data_combined[i,3]
                ha_open_val = ha_open[i]
                ha_high_val = ha_high[i]
                ha_low_val = ha_low[i]
                ha_close_val = ha_close[i]
                batch_data.append((exchange_code, date_val, open_val, high_val, low_val, close_val, ha_open_val, ha_high_val, ha_low_val, ha_close_val))
                print('batch_data', len(batch_data))
                
                if len(batch_data) >= BATCH_SIZE:
                    if table_name == 'one_min_ohlc':
                        task = asyncio.create_task(self.db.Insert_one_min_ohlc_proc_batch(batch_data))
                        tasks.append(task)
                    elif table_name == 'three_min_ohlc':
                        task = asyncio.create_task(self.db.Insert_three_min_ohlc_proc_batch(batch_data))
                        tasks.append(task)
                    elif table_name == 'five_min_ohlc':
                        task = asyncio.create_task(self.db.Insert_five_min_ohlc_proc_batch(batch_data))
                        tasks.append(task)
                    elif table_name == 'ten_min_ohlc':
                        task = asyncio.create_task(self.db.Insert_ten_min_ohlc_proc_batch(batch_data))
                        tasks.append(task)
                    elif table_name == 'fifteen_min_ohlc':
                        task = asyncio.create_task(self.db.Insert_fifteen_min_ohlc_proc_batch(batch_data))
                        tasks.append(task)
                    elif table_name == 'thirty_min_ohlc':
                        task = asyncio.create_task(self.db.Insert_thirty_min_ohlc_proc_batch(batch_data))
                        tasks.append(task)
                    elif table_name == 'one_hour_ohlc':
                        task = asyncio.create_task(self.db.Insert_hour_ohlc_proc_batch(batch_data))
                        tasks.append(task)
                    else:
                        print('No appropraite function found to insert data ', table_name)
                    batch_data = []

            if batch_data:
                if table_name == 'one_min_ohlc':
                    task = asyncio.create_task(self.db.Insert_one_min_ohlc_proc_batch(batch_data))
                    tasks.append(task)
                elif table_name == 'three_min_ohlc':
                    task = asyncio.create_task(self.db.Insert_three_min_ohlc_proc_batch(batch_data))
                    tasks.append(task)
                elif table_name == 'five_min_ohlc':
                    task = asyncio.create_task(self.db.Insert_five_min_ohlc_proc_batch(batch_data))
                    tasks.append(task)
                elif table_name == 'ten_min_ohlc':
                    task = asyncio.create_task(self.db.Insert_ten_min_ohlc_proc_batch(batch_data))
                    tasks.append(task)
                elif table_name == 'fifteen_min_ohlc':
                    task = asyncio.create_task(self.db.Insert_fifteen_min_ohlc_proc_batch(batch_data))
                    tasks.append(task)
                elif table_name == 'thirty_min_ohlc':
                    task = asyncio.create_task(self.db.Insert_thirty_min_ohlc_proc_batch(batch_data))
                    tasks.append(task)
                elif table_name == 'one_hour_ohlc':
                    task = asyncio.create_task(self.db.Insert_hour_ohlc_proc_batch(batch_data))
                    tasks.append(task)
                else:
                    print('No appropraite function found to insert data ', table_name)

            print('done ', table_name,' ', exchange_code)
            await asyncio.gather(*tasks)

        return 1, error, count_iter

    async def pine_ema(self, src, length):
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
    def calculate_new_ema(self, latest_close, previous_ema, length):
        alpha = 2 / (length + 1)
        new_ema = (latest_close - previous_ema) * alpha + previous_ema
        return new_ema
    async def MACD(self, data, fast_ma_period = 12, slow_ma_period = 26, signal_period = 9):
        data["FMA"] = data['close'].ewm(span=self.fast).mean()
        data["SMA"] = data['close'].ewm(span=self.slow).mean()
        data["MACD"] = data["FMA"] - data["SMA"]
        data["Signal"] = data['MACD'].ewm(span=self.signal).mean()
        data["histogram"] = data["MACD"] - data["Signal"]
        data = data[["close", "MACD", "Signal", "histogram"]]
        return data

    async def process_indicators_one_min(self, df):
        print('-------------------- process_indicators_1 min -----------------')
        await self.process_one_min_heikin(df)
        await self.process_min_PSAR(df, 'minute')
        return 1
    async def process_indicators_three_min(self, df):
        print('-------------------- process_indicators_3 min -----------------')
        await self.process_three_min_heikin(df)
        await self.process_min_PSAR(df, '3minute')
        return 1    
    async def process_indicators_two_min(self, df):
        print('-------------------- process_indicators_2 min -----------------')
        await self.process_min_PSAR(df, '2minute')
        return 1  
    async def process_indicators_five_min(self, df):
        print('-------------------- process_indicators_5 min -----------------')
        await self.process_fivemin_heikin(df)
        await self.process_min_PSAR(df, '5minute')
        return 1
    async def process_indicators_fifteen_min(self, df):
        print('-------------------- process_indicators_15 min -----------------')
        await self.process_fifteenmin_heikin(df)
        await self.process_min_PSAR(df, '15minute')
        return 1
    async def process_indicators_ten_min(self, df):
        print('-------------------- process_indicators_10 min -----------------')
        await self.process_tenmin_heikin(df)
        await self.process_min_PSAR(df, '10minute')
        return 1
    async def process_indicators_thirty_min(self, df):
        print('-------------------- process_indicators 30 minutes -----------------')
        await self.process_thirtymin_heikin(df)
        await self.process_min_PSAR(df, '30minute')
        return 1
    async def process_indicators_hour(self, df):
        print('-------------------- process_indicators 1 hour -----------------')
        await self.process_hour_heikin(df)
        await self.process_min_PSAR(df, '60minute')
        return 1

    async def download_ohlc_2min(self, df_all_stocks):
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
            if exchange_code in self.dates_collections[interval]:
                last_datetime = cutoff_datetime = self.dates_collections[interval][exchange_code][-1].replace(second=0, microsecond=0)
                print('cache datetime available', cutoff_datetime)  
            else:
                print('no cache')
                last_datetime = datetime.today() - timedelta(days=90)
                last_datetime = last_datetime.replace(hour=9, minute=15, second=0, microsecond=0)
                cutoff_datetime = last_datetime

            if isinstance(last_datetime, pd.Timestamp):
                last_datetime = last_datetime.to_pydatetime()
            
            if isinstance(cutoff_datetime, pd.Timestamp):
                cutoff_datetime = cutoff_datetime.to_pydatetime()
            
            if cutoff_datetime >= end_date_now:
                info = f"skipping cutoff_datetime:{cutoff_datetime} >= end_date_now:{end_date_now} {exchange_code}"
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_2min', activity='skip process', important_data=info, priority=1, strategy_trade_id = '', timestamp=end_date_now)
                continue
        
            result = 0
            df = ""
            try:
                if self.log == True:
                    print('get_one_min_datetime', exchange_code)
                dates_list_one_min = []
                data_one_min = []
                if exchange_code in self.dates_collections['minute']:
                    dates_list_one_min = self.dates_collections['minute'][exchange_code]
                    data_one_min = self.data_collections['minute'][exchange_code]
                df = pd.DataFrame(data_one_min, columns=['open', 'high', 'low', 'close'], index=dates_list_one_min)
                #df = await db.get_one_min_datetime(exchange_code, startdate, last_working_day)
                result = 1
            except Exception as e:
                if self.log == True:
                    info = f"Error in getting 1 min data from cache {exchange_code} {e}"
                    await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_2min', activity='error get 1min', important_data=info, priority=4, strategy_trade_id = '', timestamp=end_date_now)
                continue
            if len(df) == 0:
                if self.log == True:
                    info = 'skip len df == 0'
                    await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_2min', activity='len 1mindata =0', important_data=info, priority=4, strategy_trade_id = '', timestamp=end_date_now)
                continue
            
            df = df.resample('2T').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last'
            })
            df.dropna(inplace=True)
            if len(df) == 0:
                info = 'skip len df after resample == 0'
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_2min', activity='len df =0', important_data=info, priority=4, strategy_trade_id = '', timestamp=end_date_now)
                continue
            df.reset_index(inplace=True, names="datetime")

            ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(df['open'].to_list(), df['high'].to_list(), df['low'].to_list(), df['close'].to_list())

            df['ha_open'] = ha_open
            df['ha_high'] = ha_high
            df['ha_low'] = ha_low
            df['ha_close'] = ha_close

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
                ha_open = row['ha_open']
                ha_high = row['ha_high']
                ha_low = row['ha_low']
                ha_close = row['ha_close']
                batch_data.append((exchange_code, date_val, open_val, high_val, low_val, close_val, ha_open, ha_high, ha_low, ha_close))
                
                if len(batch_data) >= BATCH_SIZE:
                    task = asyncio.create_task(self.db.Insert_two_min_ohlc_proc_batch(batch_data))
                    tasks.append(task)
                    batch_data = []

            if batch_data:
                task = asyncio.create_task(self.db.Insert_two_min_ohlc_proc_batch(batch_data))
                tasks.append(task)
                if self.log == True:
                    print('insert_' + table_name, exchange_code, date_val)
                count += 1
            num_tasks = len(tasks)
            info = f"{exchange_code} tasks gathered {num_tasks}"
            if num_tasks > 0:
                await asyncio.gather(*tasks)
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_2min', activity='tasks gathered', important_data=info, priority=1, strategy_trade_id = '', timestamp=end_date_now)
            else:
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_2min', activity='len tasks =0', important_data=info, priority=3, strategy_trade_id = '', timestamp=end_date_now)

        if count > 0:
            return 1, 'None', count
        else:
            return 0, 'Unknown Error', count   

    async def process_heikinashi(self, unproc_datetime, df_new, df_old, table, exchange_code):
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
            await self.db.update_heikin_ashi(ha_open, ha_high, ha_low, ha_close, exchange_code, datetime_val, table)
    async def process_fivemin_heikin(self, df_all_stocks):
        print('in process_fivemin_heikin')
        count = 0
        table_name = 'five_min_ohlc'
        for index, row in df_all_stocks.iterrows():
            count += 1
            exchange_code = row['symbol']
            df_new = await self.db.get_null_ohlc(exchange_code, table_name)
            if len(df_new) == 0:
                if self.log == True:
                    print('NULL ohlc not found No need to process', exchange_code, table_name)
                continue
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            unproc_datetime = df_new.datetime.iloc[0]
            df_old = await self.db.get_prior_rows(exchange_code, unproc_datetime, table_name)
            if len(df_old) == 0:
                if self.log == True:
                    print('data not found - get_prior_thirty_rows', table_name)
                process_fresh = True
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            await self.process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)
        
        return 1, None, count
    async def process_tenmin_heikin(self, df_all_stocks):
        print('in process_tenmin_heikin')
        count = 0
        table_name = 'ten_min_ohlc'
        for index, row in df_all_stocks.iterrows():
            count += 1
            exchange_code = row['symbol']
            df_new = await self.db.get_null_ohlc(exchange_code, table_name)
            if len(df_new) == 0:
                if self.log == True:
                    print('NULL ohlc not found No need to process', exchange_code, table_name)
                continue
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            unproc_datetime = df_new.datetime.iloc[0]
            df_old = await self.db.get_prior_rows(exchange_code, unproc_datetime, table_name)
            if len(df_old) == 0:
                if self.log == True:
                    print('data not found - get_prior_thirty_rows', table_name)
                process_fresh = True
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            await self.process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)
        
        return 1, None, count
    async def process_one_min_heikin(self, df_all_stocks):
        print('in process_onemin_heikin')
        count = 0
        table_name = 'one_min_ohlc'
        for index, row in df_all_stocks.iterrows():
            count += 1
            exchange_code = row['symbol']
            df_new = await self.db.get_null_ohlc(exchange_code, table_name)
            if len(df_new) == 0:
                if self.log == True:
                    print('NULL ohlc not found No need to process', exchange_code, table_name)
                continue
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            unproc_datetime = df_new.datetime.iloc[0]
            df_old = await self.db.get_prior_rows(exchange_code, unproc_datetime, table_name)
            if len(df_old) == 0:
                if self.log == True:
                    print('data not found - get_prior_thirty_rows', table_name)
                process_fresh = True
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            await self.process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)
        
        return 1, None, count
    async def process_three_min_heikin(self, df_all_stocks):
        print('in process_threemin_heikin')
        count = 0
        table_name = 'three_min_ohlc'
        for index, row in df_all_stocks.iterrows():
            count += 1
            exchange_code = row['symbol']
            df_new = await self.db.get_null_ohlc(exchange_code, table_name)
            if len(df_new) == 0:
                if self.log == True:
                    print('NULL ohlc not found No need to process', exchange_code, table_name)
                continue
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            unproc_datetime = df_new.datetime.iloc[0]
            df_old = await self.db.get_prior_rows(exchange_code, unproc_datetime, table_name)
            if len(df_old) == 0:
                if self.log == True:
                    print('data not found - get_prior_thirty_rows', table_name)
                process_fresh = True
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            await self.process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)
        
        return 1, None, count    
    async def process_fifteenmin_heikin(self, df_all_stocks):
        print('in process_fifteenmin_heikin')
        count = 0
        table_name = 'fifteen_min_ohlc'
        for index, row in df_all_stocks.iterrows():
            count += 1
            exchange_code = row['symbol']
            df_new = await self.db.get_null_ohlc(exchange_code, table_name)
            if len(df_new) == 0:
                if self.log == True:
                    print('NULL ohlc not found No need to process', exchange_code, table_name)
                continue
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            unproc_datetime = df_new.datetime.iloc[0]
            df_old = await self.db.get_prior_rows(exchange_code, unproc_datetime, table_name)
            if len(df_old) == 0:
                if self.log == True:
                    print('data not found - get_prior_thirty_rows', table_name)
                process_fresh = True
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            await self.process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)
        
        return 1, None, count
    async def process_thirtymin_heikin(self, df_all_stocks):
        print('in process_thirtymin_heikin')
        count = 0
        table_name = 'thirty_min_ohlc'
        for index, row in df_all_stocks.iterrows():
            count += 1
            exchange_code = row['symbol']
            df_new = await self.db.get_null_ohlc(exchange_code, table_name)
            if len(df_new) == 0:
                if self.log == True:
                    print('NULL ohlc not found No need to process', exchange_code, table_name)
                continue
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            unproc_datetime = df_new.datetime.iloc[0]
            df_old = await self.db.get_prior_rows(exchange_code, unproc_datetime, table_name)
            if len(df_old) == 0:
                if self.log == True:
                    print('data not found - get_prior_thirty_rows', table_name)
                process_fresh = True
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            await self.process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)
        
        return 1, None, count
    async def process_hour_heikin(self, df_all_stocks):
        print('in process_hour_heikin')
        count = 0
        table_name = 'one_hour_ohlc'
        for index, row in df_all_stocks.iterrows():
            count += 1
            exchange_code = row['symbol']
            df_new = await self.db.get_null_ohlc(exchange_code, table_name)
            if len(df_new) == 0:
                if self.log == True:
                    print('NULL ohlc not found No need to process', exchange_code, table_name)
                continue
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            unproc_datetime = df_new.datetime.iloc[0]
            df_old = await self.db.get_prior_rows(exchange_code, unproc_datetime, table_name)
            if len(df_old) == 0:
                if self.log == True:
                    print('data not found - get_prior_thirty_rows', table_name)
                process_fresh = True
            else:
                df_new['open'] = df_new['open'].astype(float)
                df_new['high'] = df_new['high'].astype(float)
                df_new['low'] = df_new['low'].astype(float)
                df_new['close'] = df_new['close'].astype(float)
            await self.process_heikinashi(unproc_datetime, df_new, df_old, table_name, exchange_code)
        
        return 1, None, count
    async def process_PSAR(self, unproc_datetime, df_new, df_old, table, exchange_code):
        print('in process_PSAR')
        df_old.sort_values(by='datetime', inplace=True)
        df_concatenated = pd.concat([df_old, df_new], ignore_index=True)
        print('df_concatenated', df_concatenated)
        # get from db af=0.02, max_af=0.2
        ta_psar = ta.psar(high=df_concatenated['high'], low=df_concatenated['low'], close=df_concatenated['close'], af0=0.02, af=0.02, max_af=0.2)

        df_concatenated['PSAR_D'] = ta_psar['PSARr_0.02_0.2']
        df_concatenated['PSAR_L'] = ta_psar['PSARl_0.02_0.2']
        df_concatenated['PSAR_S'] = ta_psar['PSARs_0.02_0.2']
        df_concatenated['L'] = np.where(pd.isna(df_concatenated['PSAR_S']), 1, None)
        df_concatenated['S'] = np.where(pd.isna(df_concatenated['PSAR_L']), 1, None)
        df_concatenated['L'] = np.where(df_concatenated['PSAR_D'] == 1, df_concatenated.L, None)
        df_concatenated['S'] = np.where(df_concatenated['PSAR_D'] == 1, df_concatenated.S, None)
        df_concatenated['PSAR'] = df_concatenated['PSAR_L'].combine_first(df_concatenated['PSAR_S'])
        
        df_filtered = df_concatenated.loc[df_concatenated['datetime'] >= unproc_datetime]
        df_filtered.dropna(subset=['PSAR'], inplace=True)

        print('len df_filtered', len(df_filtered))
        if len(df_filtered) == 0:
            return
        
        updates = []
        for index, row in df_filtered.iterrows():
            datetime_val = row['datetime']
            PSAR = row['PSAR']
            PSAR_L = row['L']
            PSAR_S = row['S']
            if (PSAR_L != None) or (PSAR_S != None):
                print(PSAR, PSAR_L, PSAR_S, datetime_val)
            updates.append((PSAR, PSAR_L, PSAR_L, PSAR_S, PSAR_S, exchange_code, datetime_val))
        if len(updates) > 0:
            print(updates[0])
            await self.db.update_PSAR_batch(updates, table)
            print('after update_PSAR_batch')    
    async def process_min_PSAR(self, df_all_stocks, interval):
        print('in process_min_PSAR')
        count = 0
        table_name = 'one_min_ohlc'
        if interval == '5minute':
            table_name = 'five_min_ohlc'
        if interval == '2minute':
            table_name = 'two_min_ohlc'
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
            print(exchange_code);
            df_new = await self.db.get_psar_null_ohlc(exchange_code, table_name)

            print('len(dfnew)', len(df_new))
            if len(df_new) == 0:
                if self.log == True:
                    print('PSAR NULL ohlc not found skipping', exchange_code, table_name)
                continue
            elif len(df_new) == 1:
                df_new[['open', 'high', 'low', 'close']] = df_new[['open', 'high', 'low', 'close']].astype(float)                
            else:
                df_new = df_new[1:]             # Remove Outliars
                df_new[['open', 'high', 'low', 'close']] = df_new[['open', 'high', 'low', 'close']].astype(float)
            #print(df_new)
            unproc_datetime = df_new.datetime.iloc[0]
            print(f"{unproc_datetime=}")
            df_old = await self.db.get_prior_rows_fifty(exchange_code, unproc_datetime, table_name)
            if len(df_old) == 0:
                if self.log == True:
                    print('data not found - get_prior_rows_fifty', table_name, unproc_datetime)
                process_fresh = True
            else:
                df_old[['open', 'high', 'low', 'close']] = df_old[['open', 'high', 'low', 'close']].astype(float)
                print('len(df_old):', len(df_old))
            await self.process_PSAR(unproc_datetime, df_new, df_old, table_name, exchange_code)
        return 1, None, count    
    async def download_current_data(self):
        current_datetime = datetime.now()
        print(f"{current_datetime=}")
        status, data, Error = await self.get_data_zerodha_recursive_list('minute',  datetime.now() - timedelta(hours=5), datetime.now(), 256265, 'NIFTY 50')
        if status == 1:
            self.zerodha_last_trans = data[-1]['date'].replace(tzinfo=None).replace(second=0, microsecond=0)
        interval = 'minute'
        await self.download_ohlc_v2(self.df_priority_stocks, interval)
        #await self.process_indicators_one_min(self.df_priority_stocks)
        
        if current_datetime.minute % 2 == 0:
            await self.download_ohlc_2min(self.df_priority_stocks, current_datetime)
            #await self.process_indicators_two_min(self.df_priority_stocks)

        if current_datetime.minute % 3 == 0:
            await self.download_ohlc_v2(self.df_priority_stocks, '3minute')
            #await self.process_indicators_three_min(self.df_priority_stocks)
        if current_datetime.minute % 5 == 0:
            await self.download_ohlc_v2(self.df_priority_stocks, '5minute')
            #await self.process_indicators_five_min(self.df_priority_stocks)

        if current_datetime.minute % 10 == 0:
            await self.download_ohlc_v2(self.df_priority_stocks, '10minute')
            #await self.process_indicators_ten_min(self.df_priority_stocks)
        if current_datetime.minute % 15 == 0:
            await self.download_ohlc_v2(self.df_priority_stocks, '15minute')
            #await self.process_indicators_fifteen_min(self.df_priority_stocks)
        if current_datetime.minute % 30 == 0:
            await self.download_ohlc_v2(self.df_priority_stocks, '30minute')
            #await self.process_indicators_thirty_min(self.df_priority_stocks)
        if current_datetime.minute == 15:
            await self.download_ohlc_v2(self.df_priority_stocks, '60minute')
            #await self.process_indicators_hour(self.df_priority_stocks)
    
async def main():
    start = Start()
    await start.start_pool()
    priority_stocks_tpl = await start.db.get_priority_instruments_to_trade()
    start.df_priority_stocks = pd.DataFrame(priority_stocks_tpl, columns=['instrument_token', 'symbol'])
    all_symbols = start.df_priority_stocks['symbol'].to_list()

    for interval, table_name in start.interval_to_table.items():
        prvdata = await start.db.get_old_data_limit(table_name)
        for s_value, group_df in prvdata.groupby('symbol'):
            if s_value in all_symbols:
                group_df = group_df.tail(500)
                ohlc_np = group_df[['open', 'high', 'low', 'close']].values.astype(float)
                start.data_collections[interval][s_value] = ohlc_np
                group_df['datetime'] = pd.to_datetime(group_df['datetime'])
                datetime_list = group_df['datetime'].tolist()
                start.dates_collections[interval][s_value] = datetime_list
    
    # return
    while True:
        CurrentDateTime = datetime.now()
        current_time = CurrentDateTime.time()
        print(current_time)
        if current_time > start.initiate_time and current_time < start.exit_time and current_time.second == 2:
            await start.start_pool()
            await start.download_current_data()
            await asyncio.sleep(1)
            await start.close_pool()
        elif current_time <= start.initiate_time:
            print('Waiting for the market to open')
            await asyncio.sleep(1)
        elif current_time >= start.exit_time:
            print('Exitting Market time Over')
            break
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.run(main())