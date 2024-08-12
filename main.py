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
def calc_fastStochastics(low, high, close, lookback_period, d_period, k_smoothing_period=1):
    n = len(close)
    lowest_low = np.full(n, np.nan)
    highest_high = np.full(n, np.nan)
    raw_K = np.full(n, np.nan)
    
    # Calculate lowest low and highest high for the lookback period
    for i in range(lookback_period - 1, n):
        ll = np.min(low[i - lookback_period + 1:i + 1])
        hh = np.max(high[i - lookback_period + 1:i + 1])
        lowest_low[i] = ll
        highest_high[i] = hh
        
        # Check for division by zero
        if hh != ll:
            raw_K[i] = 100 * (close[i] - ll) / (hh - ll)
        else:
            raw_K[i] = 0  # or np.nan
    
    # Smooth the K values
    K = np.full(n, np.nan)
    if k_smoothing_period > 1:
        for i in range(k_smoothing_period - 1, n):
            K[i] = np.mean(raw_K[i - k_smoothing_period + 1:i + 1])
    else:
        K = raw_K
    
    # Calculate the D values
    D = np.full(n, np.nan)
    for i in range(d_period - 1, n):
        D[i] = np.mean(K[i - d_period + 1:i + 1])
    
    return K, D

@jit(nopython=True)
def linear_regression_channel_numba(close, period, std_multiplier):
    close = close[-period:]
    X = np.arange(len(close))
    N = len(X)
    sum_X = np.sum(X)
    sum_Y = np.sum(close)
    sum_XY = np.sum(X * close)
    sum_X2 = np.sum(X * X)
    
    # Initialize slope and intercept with default values
    slope = 0.0
    intercept = np.mean(close)
    
    # Avoid division by zero in slope and intercept calculations
    denominator_slope = (N * sum_X2 - sum_X * sum_X)
    if denominator_slope != 0:
        slope = (N * sum_XY - sum_X * sum_Y) / denominator_slope

    denominator_intercept = N
    if denominator_intercept != 0:
        intercept = (sum_Y - slope * sum_X) / denominator_intercept
    
    LRL = intercept + slope * X
    residuals = close - LRL
    std_dev = np.std(residuals)
    UCL = LRL + std_multiplier * std_dev
    LCL = LRL - std_multiplier * std_dev

    angle_radians = np.arctan(slope)
    angle_degrees = np.degrees(angle_radians)

    return LRL, UCL, LCL, angle_degrees

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
def get_psar_signals(close, psar_values):
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
        self.alertLog = False
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
        self.ha_collection = {f"{key}": {} for key in self.interval_to_table}

        self.end_date_today = datetime.today().replace(hour=15, minute=30, second=0, microsecond=0)
        self.start_time_trans = tm(9, 15)
        self.end_time_trans = tm(15, 30)
        self.df_priority_stocks = None
        self.zerodha_last_trans = None
        self.interval_to_digit = {
                'minute': '1 min','2minute': '2 min', '5minute': '5 min', '3minute': '3 min', '10minute': '10 min',
                '15minute': '15 min', '30minute': '30 min', '60minute': '1 hour'
            }
        self.df_scan_items = None
        self.df_custom_indicators = None
        self.df_conditions = None
        self.df_HLFP = None
        self.priority_stocks_tpl = None

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
                    info = f"skipping cutoff_datetime:{cutoff_datetime} >= end_date_now:{end_date_now} {instrument_token}"
                    await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_v2', activity='skip', important_data=info, priority=2, strategy_trade_id = '', timestamp=end_date_now)
                    continue
                elif interval == '60minute':
                    exptime = cutoff_datetime + timedelta(hours=1)
                    print(f"{exptime=}")
                    if exptime > self.end_date_today:
                        info = f"skipping exptime: {exptime} > end_date_today: {self.end_date_today} {instrument_token=}"
                        await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_v2', activity='skip', important_data=info, priority=2, strategy_trade_id = '', timestamp=end_date_now)
                        continue
                print(f"{exchange_code} {last_datetime=}, {self.end_date_today=}")
                
                if self.zerodha_last_trans != None and last_datetime >= self.zerodha_last_trans:
                    info = f"skipping last_datetime:{last_datetime} >= zerodha_last_trans:{self.zerodha_last_trans} {exchange_code}"
                    await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_v2', activity='skip', important_data=info, priority=2, strategy_trade_id = '', timestamp=end_date_now)
                    continue
                else:
                    print(f"process as NOT last_datetime:{last_datetime} >= zerodha_last_trans: {self.zerodha_last_trans}")

                status, data, Error = await self.get_data_zerodha_recursive_list(interval, last_datetime, self.end_date_today, instrument_token, exchange_code)
            except Exception as e:
                info = f"Error in downloading {exchange_code} {e}"
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_v2', activity='get_data_zerodha', important_data=info, priority=2, strategy_trade_id = '', timestamp=end_date_now)
                result = -1

            if status == 1 and len(data) > 0:
                info = f"{len(data)} rows downloaded {exchange_code} {interval}"
                await self.db.insert_trade_log(date_log=self.today, module='download_ohlc_v2', activity='data downloaded', important_data=info, priority=2, strategy_trade_id = '', timestamp=end_date_now)
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
            ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
            self.ha_collection[interval][exchange_code] = ha_combined

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
                #print('batch_data', len(batch_data))
                
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

    async def download_ohlc_2min(self, df_all_stocks):
        table_name = 'two_min_ohlc'
        interval = '2minute'
        
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

    async def download_current_data(self):
        current_datetime = datetime.now()
        print(f"{current_datetime=}")
        status, data, Error = await self.get_data_zerodha_recursive_list('minute',  datetime.now() - timedelta(hours=5), datetime.now(), 256265, 'NIFTY 50')
        if status == 1:
            self.zerodha_last_trans = data[-1]['date'].replace(tzinfo=None).replace(second=0, microsecond=0)
        interval = 'minute'
        await self.download_ohlc_v2(self.df_priority_stocks, interval)
        await self.run_alerts_check(interval)
        
        if current_datetime.minute % 2 == 0:
            interval = '2minute'
            await self.download_ohlc_2min(self.df_priority_stocks)
            await self.run_alerts_check(interval)
        if current_datetime.minute % 3 == 0:
            interval = '3minute'
            await self.download_ohlc_v2(self.df_priority_stocks, interval)
            await self.run_alerts_check(interval)
        if current_datetime.minute % 5 == 0:
            interval = '5minute'
            await self.download_ohlc_v2(self.df_priority_stocks, interval)
            await self.run_alerts_check(interval)

        if current_datetime.minute % 10 == 0:
            interval = '10minute'
            await self.download_ohlc_v2(self.df_priority_stocks, interval)
            await self.run_alerts_check(interval)
        if current_datetime.minute % 15 == 0:
            interval = '15minute'
            await self.download_ohlc_v2(self.df_priority_stocks, interval)
            await self.run_alerts_check(interval)
        if current_datetime.minute % 30 == 0:
            interval = '30minute'
            await self.download_ohlc_v2(self.df_priority_stocks, interval)
            await self.run_alerts_check(interval)
        if current_datetime.minute == 15:
            interval = '60minute'
            info = 'begin to download 1 hour data'
            await self.db.insert_trade_log(date_log=self.today, module='download_current_data', activity='begin', important_data=info, priority=2, strategy_trade_id = '', timestamp=current_datetime)
            await self.download_ohlc_v2(self.df_priority_stocks, interval)
            await self.run_alerts_check(interval)
    
    async def checkAlerts_interval(self, interval, priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev):
        #print('in CheckAlerts_interval:', interval)
        info = ''
        if self.alertLog:
            info = f"{PSAR_acceleration=} {PSAR_max_acceleration=} {stoch_period=} {k_avg=} {d_avg=} {psarCandles=} {LineThreshold=}"
            await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='start', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
            info = f"{signaldirection=} {lrcangletype=} {lrcanglestart=} {lrcangleend=} {scanID=} {lrc_period=} {lrc_stdev=}"
            await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='start', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
        for instrument_token, exchange_code in priority_stocks_tpl:
            if exchange_code in self.data_collections[interval]:
                data = self.data_collections[interval][exchange_code]
                date_vals = None
                if exchange_code in self.dates_collections[interval]:
                    date_vals = self.dates_collections[interval][exchange_code]
                else:
                    if self.alertLog:
                        info = f"{exchange_code=}"
                        await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='dates not found', important_data=info, priority=4, strategy_trade_id = '', timestamp=datetime.now())
                    continue
                #print(exchange_code, date_vals[-1])
                low = data[:,2]
                high = data[:,1]
                close = data[:,3]
                LRL, UCL, LCL, angle_degrees = linear_regression_channel_numba(close, lrc_period, lrc_stdev)
                if self.alertLog:
                    info = f"{interval} {exchange_code} {LRL[-1]} {angle_degrees=}"
                    await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='linear_reg_channel', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
                psar_data = psar(high, low, close, af0=float(PSAR_acceleration), af=float(PSAR_acceleration), max_af=float(PSAR_max_acceleration))
                signals = get_psar_signals(close, psar_data)
                psar_signal = signals[-1]
                K, D = calc_fastStochastics(low, high, close, stoch_period, k_avg, d_avg)

                # K line crosses below ____ level and within ____ candles PSAR is positive 
                # on a green HA candle 
                # not crossing or touching middle LRC
                last_n_elements = K[-psarCandles:]
                crossover_index = -1  
                for i in range(len(last_n_elements) - 1):
                    if last_n_elements[i] > LineThreshold and last_n_elements[i + 1] <= LineThreshold:
                        crossover_index = i + 1
                    elif last_n_elements[i + 1] > LineThreshold:
                        crossover_index = -1
                if crossover_index > -1:
                    crossover_index = psarCandles - crossover_index
                if crossover_index == -1 or crossover_index == psarCandles:
                    if self.alertLog:
                        info = f'K crossover didnt occur, ignore {crossover_index=} {psarCandles=}'
                        await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='no crossover', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
                else:
                    if self.alertLog:
                        info = f"{crossover_index=} {psar_signal=} {signaldirection=}"
                        await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='crossover', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
                    # put log
                    if psar_signal == signaldirection: # signaldirection = 1 PSAR Signal is Long
                        info = f"psar_signal: {psar_signal} == signaldirection: {signaldirection}"
                        # Get last HA candle and cal color
                        if exchange_code not in self.ha_collection[interval]:
                            if self.alertLog:
                                info = "{exchange_code} not in ha_collection {interval}"
                                await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='get ha values', important_data=info, priority=4, strategy_trade_id = '', timestamp=datetime.now())
                            continue
                        data_ha = self.ha_collection[interval][exchange_code]
                        open_ha = data_ha[-1,0]
                        high_ha = data_ha[-1,1]
                        low_ha = data_ha[-1,2]
                        close_ha = data_ha[-1,3]
                        candle_color = 'g'
                        if close_ha < open_ha:
                            candle_color = 'r'
                        
                        LRL_value = LRL[-1]
                        if self.alertLog:
                            info = f"{open_ha=} {high_ha=} {low_ha=} {close_ha=} {LRL_value=} {candle_color=} {hlfpid=}"
                            await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='ret HA data', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())                    
                        digit_name =  self.interval_to_digit.get(interval, None)
                        if hlfpid == 1:
                            if candle_color == 'g' and high_ha < LRL_value:
                                alert_timestamp = date_vals[-1]
                                if self.alertLog:
                                    info = f"{hlfpid=} LRC angle_type: {lrcangletype} angle: {angle_degrees} > angle_start: {lrcanglestart} and < angle_end: {lrcangleend}"
                                    await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='if hlfpid=1', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())                    
                                if lrcangletype == 'custom' and angle_degrees > lrcanglestart and angle_degrees < lrcangleend: 
                                    info = f"Alert {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
                                    await self.db.insert_trade_log(date_log=self.today, module='alert custom angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id = '', timestamp=datetime.now())                                             
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name)
                                elif lrcangletype != 'custom':
                                    info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
                                    await self.db.insert_trade_log(date_log=self.today, module='alert normal angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id = '', timestamp=datetime.now())
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name)
                        elif hlfpid == 2:
                            # green pin bar 
                            no_lower_wick = low_ha == open_ha
                            upper_wick = high_ha > close_ha
                            if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and upper_wick:
                                if self.alertLog:
                                    info = f"Green pinbar color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {upper_wick=}"
                                    await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='if hlfpid=2', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
                                #digit_name =  interval_to_digit.get(interval, None)
                                alert_timestamp = date_vals[-1]
                                if self.alertLog:
                                    info = f"LRC angle_type: {lrcangletype} angle: {angle_degrees} > angle_start: {lrcanglestart} and angle: {angle_degrees} < angle_end: {lrcangleend}"
                                    await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='angle data', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
                                if lrcangletype == 'custom' and angle_degrees > lrcanglestart and angle_degrees < lrcangleend: 
                                    info = f"Alert {exchange_code} {alert_timestamp} K crossover candle {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
                                    print(info)                              
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name)
                                    await self.db.insert_trade_log(date_log=self.today, module='alert custom angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id = '', timestamp=datetime.now())
                                elif lrcangletype != 'custom':
                                    info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
                                    print(info)                                  
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name)
                                    await self.db.insert_trade_log(date_log=self.today, module='alert normal angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id = '', timestamp=datetime.now())
                            else:
                                if self.alertLog:
                                    info = f"in else color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {upper_wick=}"
                                    await self.db.insert_trade_log(date_log=self.today, module='alert normal angle', activity='cond not met', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
                        elif hlfpid == 3:
                            # Third condition - wickless
                            no_upper_wick = high_ha == close_ha
                            no_lower_wick = low_ha == open_ha
                            if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and no_upper_wick:
                                info = f"Wickless color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {no_upper_wick=}"
                                print(info)
                                # insert into db
                                #digit_name =  interval_to_digit.get(interval, None)
                                alert_timestamp = date_vals[-1]
                                info = f"LRC angle_type: {lrcangletype} angle: {angle_degrees} > angle_start: {lrcanglestart} and angle: {angle_degrees} < angle_end: {lrcangleend}"
                                #print(info)
                                if lrcangletype == 'custom' and angle_degrees > lrcanglestart and angle_degrees < lrcangleend: 
                                    info = f"Alert {exchange_code} {alert_timestamp} K crossover candle {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
                                    print(info)                              
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name)
                                elif lrcangletype != 'custom':
                                    info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
                                    print(info)                                  
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name)
                            else:
                                if self.alertLog:
                                    info = f"Not Wickless color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {no_upper_wick=}"
                                    await self.db.insert_trade_log(date_log=self.today, module='alert wickless', activity='cond not met', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
                    else:
                        if self.alertLog:
                            info = f"NOT psarsignal: {psar_signal} == signaldirection: {signaldirection}"
                            await self.db.insert_trade_log(date_log=self.today, module='alert normal angle', activity='no psar', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())

    async def run_alerts_check(self, interval):
        for index, row in self.df_scan_items.iterrows():
            scanID = row['scanID']
            conditionID = row['conditionID'] 
            one_min = bool(row['1min'])   
            two_min = bool(row['2min']) 
            three_min = bool(row['3min'])  
            five_min = bool(row['5min'])  
            ten_min = bool(row['10min'])  
            fifteen_min = bool(row['15min'])  
            thirty_min = bool(row['30min'])  
            sixty_min = bool(row['60min'])
            condition_filtered = self.df_conditions[self.df_conditions['id'] == conditionID]
            if len(condition_filtered) == 0:
                continue
            
            lrcid = condition_filtered['lrcid'].iloc[0]
            lrc_filtered = self.df_custom_indicators[self.df_custom_indicators.id == lrcid]
            lrc_values = lrc_filtered['value'].iloc[0]
            period_str, standard_deviation_str = lrc_values.split(',')
            lrc_period = int(period_str.strip())
            lrc_stdev = float(standard_deviation_str.strip())
            
            psarid = condition_filtered['psarid'].iloc[0] 
            psar_filtered = self.df_custom_indicators[self.df_custom_indicators.id == psarid]
            psar_values = psar_filtered['value'].iloc[0]
            acceleration_str, max_acceleration_str = psar_values.split(',')
            PSAR_acceleration = float(acceleration_str.strip())
            PSAR_max_acceleration = float(max_acceleration_str) 

            stochid = condition_filtered['stochid'].iloc[0] 
            stoch_filtered = self.df_custom_indicators[self.df_custom_indicators.id == stochid]
            stoch_values = stoch_filtered['value'].iloc[0]
            
            period_str, k_avg_str, d_avg_str = stoch_values.split(',')
            stoch_period = float(period_str)
            k_avg = float(k_avg_str)
            d_avg = float(d_avg_str)
            
            #print(f"{stoch_period=} {k_avg=} {d_avg=}")
            lrcangletype = condition_filtered['lrcangletype'].iloc[0] 
            lrcanglestart = condition_filtered['lrcanglestart'].iloc[0] 
            lrcangleend = condition_filtered['lrcangleend'].iloc[0] 
            signaldirection = condition_filtered['signaldirection'].iloc[0] 
            #signalColor = condition_filtered['signalColor'].iloc[0] 
            
            hlfpid = condition_filtered['hlfpid'].iloc[0]
            LineThreshold = self.df_HLFP['kLineThresholdOne'].iloc[0]
            psarCandles =  self.df_HLFP['psarCandlesOne'].iloc[0]
            
            if hlfpid == 2:
                LineThreshold = self.df_HLFP['kLineThresholdTwo'].iloc[0]
                psarCandles =  self.df_HLFP['psarCandlesTwo'].iloc[0]
            elif hlfpid == 3:
                LineThreshold = self.df_HLFP['kLineThresholdThree'].iloc[0]
                psarCandles =  self.df_HLFP['psarCandlesThree'].iloc[0]

            #print(f"{LineThreshold=} {psarCandles=}")
            if one_min and interval == 'minute':
                #print(f"{interval} {scanID=}")
                await self.checkAlerts_interval(interval, self.priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev)
            if two_min and interval == '2minute':
                #print(f"{interval} {scanID=}")
                await self.checkAlerts_interval(interval, self.priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev)
            if three_min and interval == '3minute':
                #print(f"{interval} {scanID=}")
                await self.checkAlerts_interval(interval, self.priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev)
            if five_min and interval == '5minute':
                #print(f"{interval} {scanID=}")
                await self.checkAlerts_interval(interval, self.priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev)
            if ten_min and interval == '10minute':
                #print(f"{interval} {scanID=}")
                await self.checkAlerts_interval(interval, self.priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev)
            if fifteen_min and interval == '15minute':
                #print(f"{interval} {scanID=}")
                await self.checkAlerts_interval(interval, self.priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev)
            if thirty_min and interval == '30minute':
                #print(f"{interval} {scanID=}")
                await self.checkAlerts_interval(interval, self.priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev)
            if sixty_min and interval == '60minute':
                #print(f"{interval} {scanID=}")
                await self.checkAlerts_interval(interval, self.priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev)
async def main():
    start = Start()
    await start.start_pool()
    start.priority_stocks_tpl = await start.db.get_priority_instruments_to_trade()
    start.df_priority_stocks = pd.DataFrame(start.priority_stocks_tpl, columns=['instrument_token', 'symbol'])
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
    
    start.df_scan_items = await start.db.get_scan_items()
    start.df_custom_indicators = await start.db.get_custom_indicators()
    start.df_conditions = await start.db.get_conditions()
    start.df_HLFP = await start.db.get_hlfp()
    #await start.run_alerts_check('minute')
    #return
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