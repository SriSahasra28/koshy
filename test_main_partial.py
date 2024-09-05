# Download Live market Data
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
print(os.getcwd())
from background.zerodha import zeroda
from datetime import datetime, date, timedelta, time as tm
from background.instruments import instruments
import pandas as pd
#import pandas_ta as ta
import numpy as np
import warnings
import os
import asyncio
from background.async_db import dbconnection
warnings.filterwarnings('ignore')
from numba import jit
import time
#from celery import Celery
#from myapp import batch_insert_trade_logs, insert_one_min_ohlc_proc_batch , Insert_three_min_ohlc_proc_batch, Insert_two_min_ohlc_proc_batch, Insert_five_min_ohlc_proc_batch, Insert_ten_min_ohlc_proc_batch, Insert_fifteen_min_ohlc_proc_batch, Insert_thirty_min_ohlc_proc_batch, Insert_hour_ohlc_proc_batch

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
        self.loglevel = 2 # Low 0, Medium 1, High 2
        self.trade =True
        self.run_job = True
        self.initiate_time = tm(9, 15, 59)
        self.exit_time = tm(15, 40)
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
                'minute': '1','2minute': '2', '5minute': '5', '3minute': '3', '10minute': '10',
                '15minute': '15', '30minute': '30', '60minute': '60'
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
        table_name =  self.interval_to_table.get(interval, None)
        end_date_now = datetime.now().replace(second=0, microsecond=0)
        error =''
        if end_date_now > self.end_date_today:
            end_date_now = self.end_date_today;
        count_iter = 0
        log_batch = []
        BATCH_SIZE = 500
        total_symbol = len(df_all_stocks)
        skipped = 0
        processed = 0
        alerts_skip = 0
        alerts_process = 0
        alerts_gen = 0
        alerts_fail = 0
        cache_available = 0
        cache_unavailable = 0
        data_unavailable_db = 0
        data_unavailable_zerodha = 0
        invalid_token = 0
        datetime_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        for index, row in df_all_stocks.iterrows():
            exchange_code = row['symbol']
            instrument_token = row['instrument_token'] # new added
            basket_id = None
            if 'basket_id' in row:
                basket_id = row['basket_id'] 

            if self.loglevel >= 2:
                print(exchange_code)
            last_datetime = None
            if exchange_code in self.dates_collections[interval]:
                last_datetime = cutoff_datetime = self.dates_collections[interval][exchange_code][-1].replace(second=0, microsecond=0)
                cache_available += 1
                if self.loglevel >= 2:
                    info = f"cache datetime available {cutoff_datetime=}{exchange_code} {interval}"
                    log_batch.append((self.today, 'download_ohlc_v2', 'check cache', info, 2, datetime.now()))
            else:
                cache_unavailable += 1
                if self.loglevel >= 1:
                    info = f"No cache {exchange_code} {interval}"
                    log_batch.append((self.today, 'download_ohlc_v2', 'check cache', info, 2, datetime.now()))

                group_df = await self.db.get_old_data_by_symbol(table_name, exchange_code)
                if len(group_df) > 0:
                    if self.loglevel >= 1:
                        info = f"Data found in db {exchange_code} {interval} {len(group_df)} rows"
                        log_batch.append((self.today, 'download_ohlc_v2', 'get_old_data_by_symbol', info, 2, datetime.now()))

                    ohlc_np = group_df[['open', 'high', 'low', 'close']].values.astype(float)
                    self.data_collections[interval][exchange_code] = ohlc_np
                    group_df['datetime'] = pd.to_datetime(group_df['datetime'])
                    datetime_list = group_df['datetime'].tolist()
                    self.dates_collections[interval][exchange_code] = datetime_list
                    last_datetime = cutoff_datetime = self.dates_collections[interval][exchange_code][-1].replace(second=0, microsecond=0)
                else:
                    data_unavailable_db += 1
                    if self.loglevel >= 1:
                        info = f"Data not found in db {exchange_code} {interval}"
                        log_batch.append((self.today, 'download_ohlc_v2', 'get_old_data_by_symbol', info, 2, datetime.now()))
                    
                    last_datetime = datetime.today() - timedelta(days=90)
                    last_datetime = last_datetime.replace(hour=9, minute=15, second=0, microsecond=0)
                    cutoff_datetime = last_datetime

            if isinstance(last_datetime, pd.Timestamp):
                last_datetime = last_datetime.to_pydatetime()
            
            if isinstance(cutoff_datetime, pd.Timestamp):
                cutoff_datetime = cutoff_datetime.to_pydatetime()

            result = status = 0
            try:
                if cutoff_datetime >= end_date_now:
                    if self.loglevel >= 2:
                        info = f"skipping cutoff_datetime:{cutoff_datetime} >= end_date_now:{end_date_now} {instrument_token} {interval}"
                        log_batch.append((self.today, 'download_ohlc_v2', 'skip', info, 2, datetime.now()))

                    skipped += 1
                    continue
                elif interval == '60minute':
                    exptime = cutoff_datetime + timedelta(hours=1)

                    if exptime > self.end_date_today:
                        if self.loglevel >= 2:
                            info = f"skipping exptime: {exptime} > end_date_today: {self.end_date_today} {instrument_token} {interval}"
                            log_batch.append((self.today, 'download_ohlc_v2', 'skip', info, 2, datetime.now()))

                        skipped += 1
                        continue
                
                if self.zerodha_last_trans != None and last_datetime >= self.zerodha_last_trans:
                    if self.loglevel >= 2:
                        info = f"last_dtime:{last_datetime} >= zeroda_l_tran:{self.zerodha_last_trans} {exchange_code} {interval}"
                        log_batch.append((self.today, 'download_ohlc_v2', 'skip', info, 2, datetime.now()))

                    skipped += 1
                    continue
                else:
                    if self.loglevel >= 2:
                        print(f"process as NOT last_datetime:{last_datetime} >= zerodha_last_trans: {self.zerodha_last_trans}")
                status, data, Error = await self.get_data_zerodha_recursive_list(interval, last_datetime, self.end_date_today, instrument_token, exchange_code)
            except Exception as e:
                data_unavailable_zerodha += 1
                if self.loglevel >= 1:
                    info = f"Error in downloading {exchange_code} {interval} {e}"
                    log_batch.append((self.today, 'download_ohlc_v2', 'get_data_zerodha', info, 2, datetime.now()))

                result = -1

            if status == 1 and len(data) > 0:
                if self.loglevel >= 2:
                    info = f"{len(data)} rows downloaded {exchange_code} {interval}"
                    print(info)
                    log_batch.append((self.today, 'download_ohlc_v2', 'get_data_zerodha', info, 2, datetime.now()))
                    df_data = pd.DataFrame(data)
                    filename = exchange_code + '_data_' + datetime_str + '.csv'
                    df_data.tail().to_csv(f'data/{filename}')
                result = 1
            else:
                data_unavailable_zerodha += 1
                if self.loglevel >= 2:
                    info = f'skip len data = 0  {exchange_code} {interval}'
                    log_batch.append((self.today, 'download_ohlc_v2', 'get_data_zerodha', info, 2, datetime.now()))

                skipped += 1
                continue
            if result == -1:
                if self.loglevel >= 2:
                    info = f"Error download skip {exchange_code} {interval} "
                    log_batch.append((self.today, 'download_ohlc_v2', 'get_data_zerodha', info, 2, datetime.now()))
                    
                skipped += 1
                continue
            if status == 0:
                data_unavailable_zerodha += 1
                if Error == 'invalid token':
                    await self.db.run_query(f"update monitor_symbols set active = 0 where symbol = '{exchange_code}'")
                info = f"skip {exchange_code} {instrument_token} {interval}"
                if self.loglevel >= 0:
                    log_batch.append((self.today, 'download_ohlc_v2', 'invalid token', info, 2, datetime.now()))

                skipped += 1
                invalid_token += 1
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
                if self.loglevel >= 2:
                    info = f"skip {exchange_code} {interval}"
                    log_batch.append((self.today, 'download_ohlc_v2', 'data_np_new = 0', info, 2, datetime.now()))
                skipped += 1
                continue

            data_combined = data_np_new

            if exchange_code in self.data_collections[interval]:
                data_np_old = self.data_collections[interval][exchange_code]
                if len(data_np_new) > 0 and len(data_np_old) > 0:
                    data_combined = np.vstack((data_np_old, data_np_new))
                    self.data_collections[interval][exchange_code] = data_combined
                elif len(data_np_new) == 0 and len(data_np_old) > 0:
                    data_combined = data_np_old
            else:
                self.data_collections[interval][exchange_code] = data_combined
            
            # New Log
            df_data_combined = pd.DataFrame(data_combined)
            df_data_combined['datetime'] = dates_combined
            filename = exchange_code + '_data_combined_' + datetime_str + '.csv'
            df_data_combined.to_csv(f'data/{filename}')
            #------------- new log end
            if self.loglevel >= 2:
                info = f"calc heikin_ashi  {exchange_code} {interval}"
                log_batch.append((self.today, 'download_ohlc_v2', 'calc heikin_ashi', info, 2, datetime.now()))

            ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(data_combined[:,0], data_combined[:,1], data_combined[:,2], data_combined[:,3])
            ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))

            # New log
            df_ha_combined = pd.DataFrame(ha_combined)
            df_ha_combined['datetime'] = dates_combined
            filename = exchange_code + '_ha_combined_' + datetime_str + '.csv'
            df_ha_combined.to_csv(f'data/{filename}')

            # -------------------  Alert Code here --------------------------------
            if self.loglevel >= 2:
                info = f'Alert code begin {exchange_code} {interval}'
                log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'start alert code', info, 2, datetime.now()))

            digit_name =  self.interval_to_digit.get(interval, None)
            column_name = str(digit_name) + 'min'

            df_items = self.df_scan_items[self.df_scan_items[column_name] == 1]
            alert_check = True
            if basket_id == None:
                info = f'basket_id None skip {exchange_code} {interval}'
                log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'basket_id None', info, 2, datetime.now()))
                alert_check = False
            else:
                df_items = df_items[df_items['basket_id'] == basket_id]

            if df_items.empty:
                if self.loglevel >= 1:
                    info = f'df_items empty skip {exchange_code} {interval}'
                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no alert items', info, 2, datetime.now()))
                alert_check = False
                alerts_skip += 1

            conditions = df_items.conditionID.unique()
            if len(conditions) == 0:
                if self.loglevel >= 1:
                    info = f'No condition {exchange_code} {interval}'
                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no conditions', info, 2, datetime.now()))

                alerts_skip += 1
                alert_check = False
            if alert_check:
                alerts_process += 1
                if self.loglevel >= 2:
                    info = f'Alert check true {exchange_code} {interval}'
                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'alert check', info, 2, datetime.now()))

                for conditionID in conditions:
                    if self.loglevel >= 2:
                        info = f"{conditionID=} {exchange_code} {interval}"
                        log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'process cond', info, 2, datetime.now()))

                    condition_filtered = self.df_conditions[self.df_conditions['id'] == conditionID]
                    scanID = df_items.loc[(df_items[column_name] == 1) & (df_items['conditionID'] == conditionID), 'scanID'].iloc[0]     
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
                    
                    lrcangletype = condition_filtered['lrcangletype'].iloc[0] 
                    lrcanglestart = condition_filtered['lrcanglestart'].iloc[0] 
                    lrcangleend = condition_filtered['lrcangleend'].iloc[0] 
                    signaldirection = condition_filtered['signaldirection'].iloc[0] 
                    hlfpid = condition_filtered['hlfpid'].iloc[0]

                    LineThreshold, psarCandles = await self.get_hlfp_values(hlfpid)
                    
                    low = data_combined[:,2]
                    high = data_combined[:,1]
                    close = data_combined[:,3]
                   
                    psar_data = psar(high, low, close, af0=float(PSAR_acceleration), af=float(PSAR_acceleration), max_af=float(PSAR_max_acceleration))
                    signals = get_psar_signals(close, psar_data)
                    #psar_signal = signals[-1]
                    K, D = calc_fastStochastics(low, high, close, stoch_period, k_avg, d_avg)
                    crossover_index = await self.get_crossover_index(K, LineThreshold, psarCandles)
                    if crossover_index == -1 or crossover_index == psarCandles:
                        if self.loglevel >= 2:
                            info = f'{crossover_index=} {psarCandles=} {exchange_code} {interval}'
                            log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no crossover', info, 2, datetime.now()))
                    else:
                        candles_to_check = 3
                        if crossover_index < candles_to_check:
                            candles_to_check = crossover_index
                        for i in range(-candles_to_check, 0):
                            psar_signal = signals[i] # psar_signal = signals[-1]
                            if self.loglevel >= 2:
                                info = f"index:{crossover_index} psig:{psar_signal} direction:{signaldirection} {exchange_code} {interval}"
                                log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'crossover', info, 2, datetime.now()))

                            if psar_signal == signaldirection: # signaldirection = 1 PSAR Signal is Long
                                if self.loglevel >= 1:
                                    info = f"psar_signal: {psar_signal} == signaldirection: {signaldirection}"
                                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'signal', info, 2, datetime.now()))

                                open_ha = ha_combined[i,0]
                                high_ha = ha_combined[i,1]
                                low_ha = ha_combined[i,2]
                                close_ha = ha_combined[i,3]
                                candle_color = 'g'
                                if close_ha < open_ha:
                                    candle_color = 'r'
                                
                                if i == -1:
                                    sliced_close = close
                                else:
                                    sliced_close = close[:i + 1]
                                LRL, UCL, LCL, angle_degrees = linear_regression_channel_numba(sliced_close, lrc_period, lrc_stdev)        
                                LRL_value = LRL[-1]

                                if self.loglevel >= 2:
                                    info = f"{open_ha=} {high_ha=} {low_ha=} {close_ha=} {LRL_value=} {candle_color=} {hlfpid=}"
                                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'imp data', info, 2, datetime.now()))

                                alert_timestamp = dates_combined[i]
                                if hlfpid == 1:
                                    if candle_color == 'g' and high_ha < LRL_value:
                                        result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name)
                                        if result == 1:
                                            alerts_gen += 1
                                        else:
                                            alerts_fail += 1
                                    else:
                                        alerts_fail += 1
                                        if self.loglevel >= 1:
                                            info = f"NOT color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value}"
                                            log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

                                elif hlfpid == 2:
                                    no_lower_wick = low_ha == open_ha
                                    upper_wick = high_ha > close_ha
                                    if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and upper_wick:
                                        result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name)
                                        if result == 1:
                                            alerts_gen += 1
                                        else:
                                            alerts_fail += 1
                                    else:
                                        alerts_fail += 1
                                        if self.loglevel >= 1:
                                            info = f"Not color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {upper_wick=}"
                                            log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

                                elif hlfpid == 3:
                                    no_upper_wick = high_ha == close_ha
                                    no_lower_wick = low_ha == open_ha
                                    if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and no_upper_wick:
                                        result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name)
                                        if result == 1:
                                            alerts_gen += 1
                                        else:
                                            alerts_fail += 1
                                    else:
                                        alerts_fail += 1
                                        if self.loglevel >= 1:
                                            info = f"Not Wickless color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {no_upper_wick=}"
                                            log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

                            else:
                                alerts_fail += 1
                                if self.loglevel >= 1:
                                    info = f"NOT psarsignal: {psar_signal} == signaldirection: {signaldirection}"
                                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no signal', info, 2, datetime.now()))

            # Continue with downloading code
            index_start = 0
            if cutoff_datetime in dates_combined:
                index_start = dates_combined.index(cutoff_datetime)
            count_iter = count_iter + 1
            processed += 1
            batch_data = []
            tasks = []
            total_count = len(dates_combined)
            for i in range(index_start + 1, total_count):
                date_val = dates_combined[i]
                python_time = date_val.time()
                is_within_range = self.start_time_trans <= python_time <= self.end_time_trans
                if is_within_range == False:
                    continue
                open_val = data_combined[i,0]
                high_val = data_combined[i,1]
                low_val = data_combined[i,2]
                close_val = data_combined[i,3]
                
                batch_data.append((exchange_code, date_val, open_val, high_val, low_val, close_val))
                if len(batch_data) >= BATCH_SIZE:
                    task = asyncio.create_task(self.db.Insert_one_min_ohlc_proc_batch(batch_data))
                    tasks.append(task)
                    batch_data = []
            if batch_data:
                task = asyncio.create_task(self.db.Insert_one_min_ohlc_proc_batch(batch_data))
                tasks.append(task)
                
            if log_batch:
                task = asyncio.create_task(self.db.insert_trade_log_v2(log_batch))
                tasks.append(task)
                log_batch= []
            print('done ', table_name,' ', exchange_code)
            await asyncio.gather(*tasks)

        return total_symbol, skipped, processed, alerts_skip, alerts_process, alerts_gen, alerts_fail, cache_available, cache_unavailable, data_unavailable_db, data_unavailable_zerodha, invalid_token

    async def download_current_data(self):
        log_batch_main = []
        current_datetime = datetime.now()
        #print(f"{current_datetime=}")
        # To avoid unnecessary trips to Zerodha if data not available
        status, data, Error = await self.get_data_zerodha_recursive_list('minute',  datetime.now() - timedelta(minutes=2), datetime.now(), 256265, 'NIFTY 50')
        if status == 1:
            self.zerodha_last_trans = data[-1]['date'].replace(tzinfo=None).replace(second=0, microsecond=0)
        interval = 'minute'
        start_time = time.time()
        total_symbol, skipped, processed, alerts_skip, alerts_process, alerts_gen, alerts_fail, cache_available, cache_unavailable, data_unavailable_db, data_unavailable_zerodha, invalid_token = await self.download_ohlc_v2(self.df_priority_stocks, interval)
        end_time = time.time()  
        total_time = end_time - start_time
        digit_name =  self.interval_to_digit.get(interval, None)
        await self.db.insert_into_dashboard(total_symbol, skipped, processed, alerts_skip, alerts_process, alerts_gen, alerts_fail, cache_available, cache_unavailable, data_unavailable_db, data_unavailable_zerodha, invalid_token, current_datetime, digit_name, total_time)
        info = f'{total_time=} to download {interval} data'
        await self.db.insert_trade_log(date_log=self.today, module='download_current_data', activity='time_taken', important_data=info, priority=5, strategy_trade_id = '', timestamp=datetime.now())
        
        if log_batch_main:
            #batch_insert_trade_logs.delay(log_batch_main)
            batch_insert_trade_logs.apply_async(args=[log_batch_main], queue='low_priority')

            log_batch_main = []
    
    async def checkAlerts_interval(self, interval, priority_stocks_tpl, hlfpid, PSAR_acceleration, PSAR_max_acceleration, stoch_period, k_avg, d_avg, psarCandles, LineThreshold, signaldirection, lrcangletype, lrcanglestart, lrcangleend, scanID, lrc_period, lrc_stdev):
        #print('in CheckAlerts_interval:', interval)
        info = ''
        if self.loglevel >= 2:
            info = f"{PSAR_acceleration=} {PSAR_max_acceleration=} {stoch_period=} {k_avg=} {d_avg=} {psarCandles=} {LineThreshold=}"
            await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='start', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
            info = f"{signaldirection=} {lrcangletype=} {lrcanglestart=} {lrcangleend=} {scanID=} {lrc_period=} {lrc_stdev=}"
            await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='start', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())

        for instrument_token, exchange_code, basket_id in priority_stocks_tpl:
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
                if self.loglevel >= 2:
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
                    if self.loglevel >= 2:
                        info = f'K crossover didnt occur, ignore {crossover_index=} {psarCandles=}'
                        await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='no crossover', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
                else:
                    if self.loglevel >= 2:
                        info = f"{crossover_index=} {psar_signal=} {signaldirection=}"
                        await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='crossover', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())
                    # put log
                    if psar_signal == signaldirection: # signaldirection = 1 PSAR Signal is Long
                        info = f"psar_signal: {psar_signal} == signaldirection: {signaldirection}"
                        # Get last HA candle and cal color
                        if exchange_code not in self.ha_collection[interval]:
                            if self.loglevel >= 1:
                                info = f"{exchange_code} not in ha_collection {interval}"
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
                        if self.loglevel >= 2:
                            info = f"{open_ha=} {high_ha=} {low_ha=} {close_ha=} {LRL_value=} {candle_color=} {hlfpid=}"
                            await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='ret HA data', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())                    
                        digit_name =  self.interval_to_digit.get(interval, None)
                        if hlfpid == 1:
                            if candle_color == 'g' and high_ha < LRL_value:
                                alert_timestamp = date_vals[-1]
                                if self.loglevel >= 1:
                                    info = f"{hlfpid=} LRC angle_type: {lrcangletype} angle: {angle_degrees} > angle_start: {lrcanglestart} and < angle_end: {lrcangleend}"
                                    await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='if hlfpid=1', important_data=info, priority=1, strategy_trade_id = '', timestamp=datetime.now())                    
                                if lrcangletype == 'custom' and angle_degrees > lrcanglestart and angle_degrees < lrcangleend: 
                                    info = f"Alert {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
                                    await self.db.insert_trade_log(date_log=self.today, module='alert custom angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id = '', timestamp=datetime.now())                                             
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
                                elif lrcangletype != 'custom':
                                    info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
                                    await self.db.insert_trade_log(date_log=self.today, module='alert normal angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id = '', timestamp=datetime.now())
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
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
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
                                    await self.db.insert_trade_log(date_log=self.today, module='alert custom angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id = '', timestamp=datetime.now())
                                elif lrcangletype != 'custom':
                                    info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
                                    print(info)                                  
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
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
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
                                elif lrcangletype != 'custom':
                                    info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
                                    print(info)                                  
                                    await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
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

    async def get_hlfp_values(self, hlfpid):
        if hlfpid == 1:
            return self.df_HLFP['kLineThresholdOne'].iloc[0], self.df_HLFP['psarCandlesOne'].iloc[0]
        elif hlfpid == 2:
            return self.df_HLFP['kLineThresholdTwo'].iloc[0], self.df_HLFP['psarCandlesTwo'].iloc[0]
        elif hlfpid == 3:
            return self.df_HLFP['kLineThresholdThree'].iloc[0], self.df_HLFP['psarCandlesThree'].iloc[0]

    async def get_crossover_index(self, K, LineThreshold, psarCandles):
        last_n_elements = K[-psarCandles:]
        crossover_index = -1
        for i in range(len(last_n_elements) - 1):
            if last_n_elements[i] > LineThreshold and last_n_elements[i + 1] <= LineThreshold:
                crossover_index = i + 1
            elif last_n_elements[i + 1] > LineThreshold:
                crossover_index = -1
        return psarCandles - crossover_index if crossover_index > -1 else crossover_index

    async def process_alert(self, exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name):
        if lrcangletype == 'custom' and lrcanglestart < angle_degrees < lrcangleend:
            info = f"Alert {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
            print(info)
            await self.db.insert_trade_log(date_log=self.today, module='alert custom angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id='', timestamp=datetime.now())
            await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
            return 1
        elif lrcangletype != 'custom':
            info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
            print(info)
            await self.db.insert_trade_log(date_log=self.today, module='alert normal angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id='', timestamp=datetime.now())
            await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
            return 1
        else:
            if self.loglevel >= 1:
                info = f"NOT {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
                await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='no angle', important_data=info, priority=2, strategy_trade_id = '', timestamp=datetime.now())
            return 0



async def main():
    start = Start()
    await start.start_pool()
    start.priority_stocks_tpl = await start.db.get_priority_instruments_to_trade()
    start.df_priority_stocks = pd.DataFrame(start.priority_stocks_tpl, columns=['instrument_token', 'symbol', 'basket_id'])

    all_symbols = start.df_priority_stocks['symbol'].to_list()
    log_batch = []
    start_time = time.time()
    info = ''
    for interval, table_name in start.interval_to_table.items():
        for symbol in all_symbols:
            group_df = await start.db.get_old_data_by_symbol(table_name, symbol)
            if len(group_df) > 0:
                ohlc_np = group_df[['open', 'high', 'low', 'close']].values.astype(float)
                start.data_collections[interval][symbol] = ohlc_np
                group_df['datetime'] = pd.to_datetime(group_df['datetime'])
                datetime_list = group_df['datetime'].tolist()
                start.dates_collections[interval][symbol] = datetime_list
            else:
                info = f'Data not found for {symbol} {table_name}'
                log_batch.append((start.today, 'main', 'cache creation', info, 2, datetime.now())) 
   
    start.df_scan_items = await start.db.get_scan_items()
    start.df_custom_indicators = await start.db.get_custom_indicators()
    start.df_conditions = await start.db.get_conditions()
    start.df_HLFP = await start.db.get_hlfp()

    if log_batch:
        #batch_insert_trade_logs.delay(log_batch)
        batch_insert_trade_logs.apply_async(args=[log_batch], queue='low_priority')

        log_batch= []

    last_run_minute = None  

    while True:
        CurrentDateTime = datetime.now()
        current_time = CurrentDateTime.time()
        current_minute = CurrentDateTime.minute
        print(current_time)
        
        if current_time > start.initiate_time and current_time < start.exit_time and current_time.second < 50:
            # Ensure the code runs only if the minute has changed
            if last_run_minute is None or current_minute != last_run_minute:
                last_run_minute = current_minute
                await start.start_pool()
                await start.download_current_data()
                await start.close_pool()
            await asyncio.sleep(1)
        elif current_time <= start.initiate_time:
            print('Waiting for the market to open')
            await asyncio.sleep(1)
        elif current_time >= start.exit_time:
            print('Exitting Market time Over')
            break
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.run(main())