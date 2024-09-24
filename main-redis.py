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
#from numba import jit
import time
from celery import Celery
from myapp import batch_insert_trade_logs
from background.indicators import *
import json
import redis.asyncio as redis
import gc
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

    async def process_alert(self, exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r):
        alert_timestamp_str = str(alert_timestamp)
        alert_timestamp_str = alert_timestamp_str[:26]  
        if 'T' in alert_timestamp_str:
            alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%d %H:%M:%S")

        print('alert_timestamp type:', type(alert_timestamp))
        print('alert_timestamp_dt', alert_timestamp_dt)
        if lrcangletype == 'custom' and lrcanglestart < angle_degrees < lrcangleend:
            info = f"Alert {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
            print(info)
            await self.db.insert_trade_log(date_log=self.today, module='alert custom angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id='', timestamp=datetime.now())
            await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
            
            alert_data = {
                "symbol": exchange_code,
                "datetime": alert_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "scanid": str(scanID),  # Convert Int64 to string
                "timeframe": str(digit_name),  # Convert to string if needed
                "bottime": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                "conditionID": str(conditionID)  # Convert Int64 to string
            }
            print(alert_data)
            sorted_set_key = "Alerts"
            timestamp_score = datetime.now().timestamp() 
            await r.zadd(sorted_set_key, {json.dumps(alert_data): timestamp_score})
            return 1
        elif lrcangletype != 'custom':
            info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
            print(info)
            await self.db.insert_trade_log(date_log=self.today, module='alert normal angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id='', timestamp=datetime.now())
            await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now())
            
            alert_data = {
                "symbol": exchange_code,
                "datetime": alert_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "scanid": str(scanID),  # Convert Int64 to string
                "timeframe": str(digit_name),  # Convert to string if needed
                "bottime": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                "conditionID": str(conditionID)  # Convert Int64 to string
            }
            print(alert_data)
            sorted_set_key = "Alerts"
            timestamp_score = datetime.now().timestamp() 
            await r.zadd(sorted_set_key, {json.dumps(alert_data): timestamp_score})
            return 1
        else:
            if self.loglevel >= 1:
                info = f"NOT {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
                await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='no angle', important_data=info, priority=2, strategy_trade_id = '', timestamp=datetime.now())
            return 0

    async def process_symbol(self, exchange_code, instrument_token, interval, data_combined, dates_combined, basket_id, r):
        #table_name =  self.interval_to_table.get(interval, None)
        print(exchange_code, interval)
        #count_iter = 0
        log_batch = []
        #BATCH_SIZE = 500   
        ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(data_combined[:,0], data_combined[:,1], data_combined[:,2], data_combined[:,3])
        ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
        # if exchange_code in ['MARUTI24SEP11800PE', 'TRENT24SEP7200CE']:
        #     df = pd.DataFrame(data_combined, columns=['open', 'high', 'low', 'close'])
        #     df['timestamp'] = dates_combined
        #     df = df[::-1]
        #     datetime_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        #     filename = f"data/Hekin-{exchange_code}-{datetime_str}.csv"
        #     df.to_csv(filename, index=False)
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

        conditions = df_items.conditionID.unique()
        if len(conditions) == 0:
            if self.loglevel >= 1:
                info = f'No condition {exchange_code} {interval}'
                log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no conditions', info, 2, datetime.now()))

            alert_check = False
        if alert_check:
            if self.loglevel >= 2:
                info = f'Alert check true {exchange_code} {interval}'
                log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'alert check', info, 2, datetime.now()))

            for conditionID in conditions:
                if self.loglevel >= 2:
                    info = f"{conditionID=} {exchange_code} {interval}"
                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'process cond', info, 2, datetime.now()))

                condition_filtered = self.df_conditions[self.df_conditions['id'] == conditionID]
                if len(condition_filtered) == 0:
                    info = f'len(condition_filtered) == 0 {conditionID=} {exchange_code} {interval}'
                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'conditionFilter', info, 2, datetime.now()))
                    continue

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
                                    result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r)
                                else:
                                    if self.loglevel >= 1:
                                        info = f"NOT color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value}"
                                        log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

                            elif hlfpid == 2:
                                no_lower_wick = low_ha == open_ha
                                upper_wick = high_ha > close_ha
                                if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and upper_wick:
                                    result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r)
                                else:
                                    if self.loglevel >= 1:
                                        info = f"Not color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {upper_wick=}"
                                        log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

                            elif hlfpid == 3:
                                no_upper_wick = high_ha == close_ha
                                no_lower_wick = low_ha == open_ha
                                if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and no_upper_wick:
                                    result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r)
                                else:
                                    if self.loglevel >= 1:
                                        info = f"Not Wickless color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {no_upper_wick=}"
                                        log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

                        else:
                            if self.loglevel >= 1:
                                info = f"NOT psarsignal: {psar_signal} == signaldirection: {signaldirection}"
                                log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no signal', info, 2, datetime.now()))

            if log_batch:
                #batch_insert_trade_logs.delay(log_batch)
                batch_insert_trade_logs.apply_async(args=[log_batch], queue='low_priority')
                log_batch= []
            print('done ', exchange_code)

        return 1

    async def handle_resampling(self, df, interval, symbol, instrument_code, basket_id, r):
        print('in handle_resampling')
        resampled_df = df.resample(f'{interval}T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }).dropna()
        resampled_df.reset_index(inplace=True)
        array_data = resampled_df[['open', 'high', 'low', 'close']].to_numpy()
        timestamps = resampled_df['timestamp'].to_numpy()
        await self.process_symbol(symbol, instrument_code, f"{interval}minute", array_data, timestamps, basket_id, r)

    async def process_current_minute(self):
        print('in process_current_minute')
        log_batch_main = []
        current_datetime = datetime.now()
        r = redis.from_url('redis://localhost', decode_responses=True)
        # ohlc_dtype = [('timestamp', 'U20'), ('open', 'f8'), ('high', 'f8'), ('low', 'f8'), ('close', 'f8')]
        last_processed = time.time()
        while datetime.now().second < 50:
            instrument_code = await r.rpop('ohlc_ready')
            if instrument_code:
                instrument_code = instrument_code.decode() if isinstance(instrument_code, bytes) else instrument_code
                filtered_df = self.df_priority_stocks[self.df_priority_stocks['instrument_token'] == int(instrument_code)]
                symbol = None
                basket_id = None
                if not filtered_df.empty:
                    symbol = filtered_df['symbol'].values[0] 
                    basket_id = filtered_df['basket_id'].values[0]
                else:
                    print(f"Skipping No symbol found for {instrument_code} type: {type(instrument_code)}")
                    continue
                # process minute
                ohlc_sorted_data = await r.zrange('ohlc_sorted:' + instrument_code, 0, -1)
                ohlc_list = [json.loads(data) for data in ohlc_sorted_data]
                array_data = np.array([[entry['open'], entry['high'], entry['low'], entry['close']] for entry in ohlc_list])
                timestamps = np.array([entry['timestamp'] for entry in ohlc_list])
                # if symbol in ['MARUTI24SEP11800PE', 'TRENT24SEP7200CE']:
                #     df = pd.DataFrame(array_data, columns=['open', 'high', 'low', 'close'])
                #     df['timestamp'] = timestamps
                #     df = df[::-1]
                #     datetime_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                #     filename = f"data/{symbol}-{datetime_str}.csv"
                #     df.to_csv(filename, index=False)
                interval = 'minute'
                await self.process_symbol(symbol, instrument_code, interval, array_data, timestamps, basket_id, r)
                df = pd.DataFrame(array_data, columns=['open', 'high', 'low', 'close'])
                df['timestamp'] = pd.to_datetime(timestamps)  
                datetime_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                filename = f"data/{symbol}-{datetime_str}.csv"
                df.to_csv(filename, index=False)

                df.set_index('timestamp', inplace=True)  

                intervals = [2, 3, 5, 10, 15, 30]
                for interval in intervals:
                    if current_datetime.minute % interval == 0:
                        print(f"criteria match {interval}")
                        await self.handle_resampling(df, interval, symbol, instrument_code, basket_id, r)

                if current_datetime.hour > 9 and current_datetime.minute == 16:
                    interval = 60
                    await self.handle_resampling(df, interval, symbol, instrument_code, basket_id, r)

                last_processed = time.time()
            else:    
                await asyncio.sleep(0.1)
        #gc.collect()
        return last_processed
 
async def main():
    start = Start()
    await start.start_pool()
    start_time = time.time()
    log_batch = []
    start.priority_stocks_tpl = await start.db.get_priority_instruments_to_trade()
    start.df_priority_stocks = pd.DataFrame(start.priority_stocks_tpl, columns=['instrument_token', 'symbol', 'basket_id'])   

    r = redis.from_url('redis://localhost', decode_responses=True)
    interval = 'minute'
    table_name = 'one_min_ohlc'
    current_time = datetime.now().time()
    target_time = tm(9, 16)
    if current_time < target_time:
        print('download historical data')
        for row in start.df_priority_stocks.itertuples(index=False):
            instrument_token = row.instrument_token
            symbol = row.symbol
            # Fetch old data from MySQL for the given symbol
            old_data_df = await start.db.get_old_data_by_symbol(table_name, symbol)
            if len(old_data_df) > 0:
                # Loop through the data and store it in Redis
                for row2 in old_data_df.itertuples(index=False):
                    datetime_str = row2.datetime.strftime("%Y-%m-%d %H:%M:%S")
                    final_ohlc = {
                        "timestamp": datetime_str,  
                        "open": float(row2.open),
                        "high": float(row2.high),
                        "low": float(row2.low),
                        "close": float(row2.close)  
                    }
                    #print(final_ohlc)
                    sorted_set_key = f"ohlc_sorted:{instrument_token}"
                    timestamp = row2.datetime
                    timestamp_score = int(timestamp.timestamp())
                    print(f"{sorted_set_key=}")
                    try:
                        # Add the OHLC data to Redis as a sorted set with timestamp as the score
                        await r.zadd(sorted_set_key, {json.dumps(final_ohlc): timestamp_score})
                    except Exception as e:
                        info = f"Error inserting OHLC data into Redis for {symbol}: {e}"
                        print(info)
                        log_batch.append((start.today, 'main', 'cache redis', info, 2, datetime.now())) 
            else:
                info = f'Data not found for {symbol} {table_name}'
                print(info)
                log_batch.append((start.today, 'main', 'cache creation', info, 2, datetime.now())) 
    end_time = time.time()
    total_time = end_time - start_time
    print(f"total_time loading hist data to redis: {total_time}")
    await r.close()

    start.df_scan_items = await start.db.get_scan_items()
    start.df_custom_indicators = await start.db.get_custom_indicators()
    start.df_conditions = await start.db.get_conditions()
    start.df_HLFP = await start.db.get_hlfp()

    if log_batch:
        batch_insert_trade_logs.apply_async(args=[log_batch], queue='low_priority')
        log_batch= []

    last_run_minute = None  
    while True:
        CurrentDateTime = datetime.now()
        current_time = CurrentDateTime.time()
        current_minute = CurrentDateTime.minute
        #print(current_time)
        if current_time > start.initiate_time and current_time < start.exit_time and current_time.second < 50:
            # Ensure the code runs only if the minute has changed
            if last_run_minute is None or current_minute != last_run_minute:
                last_run_minute = current_minute
                await start.start_pool()
                start_time = time.time()
                end_time = await start.process_current_minute()
                #end_time = time.time()
                total_time = end_time - start_time
                print(f"{total_time=}")
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