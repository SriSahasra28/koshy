# Main bot that Generates Alerts
import os

import requests
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


def send_telegram_message(stock, price, date, time, tf, sn):
    """Format and send a message to a Telegram chat via the bot with only 'Alert' in bold."""
    # Only 'Alert' is formatted as bold
    message = f"*Alert*\nStock : {stock}\nPrice : Rs. {price}\nDate : {date}\nTime : {time}\nTF — {tf} min \nSN — {sn}"
    
    bot_token = '1936528227:AAFQwZV4z5AgSKEU9FW25Plnq-mTzXJY7Qw'
    chat_id = '@koshi_alerts'
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': message}

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()  # Raises HTTPError for bad requests
        print("MESSAGE FROM MAIN RDIS PY")
        print("Message sent successfully")
    except requests.exceptions.RequestException as e:
        print(f"Error sending message: {e}")

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
        self.log = False
        self.alertLog = False
        self.loglevel = 0 # Low 0, Medium 1, High 2
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

    # async def process_alert(self, exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r):
    #     alert_timestamp_str = str(alert_timestamp)
    #     alert_timestamp_str = alert_timestamp_str[:26]  
    #     if 'T' in alert_timestamp_str:
    #         alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
    #     else:
    #         alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%d %H:%M:%S")

    #     print('alert_timestamp type:', type(alert_timestamp))
    #     print('alert_timestamp_dt', alert_timestamp_dt)
    #     if lrcangletype == 'custom' and lrcanglestart < angle_degrees < lrcangleend:
    #         print("lrcangletype is custom")
    #         info = f"Alert {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
    #         print(info)
    #         await self.db.insert_trade_log(date_log=self.today, module='alert custom angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id='', timestamp=datetime.now())
    #         await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now(), conditionID)
            
    #         alert_data = {
    #             "symbol": exchange_code,
    #             "datetime": alert_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S"),
    #             "scanid": str(scanID),  # Convert Int64 to string
    #             "timeframe": str(digit_name),  # Convert to string if needed
    #             "bottime": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
    #             "conditionID": str(conditionID)  # Convert Int64 to string
    #         }
    #         print(alert_data)
    #         sorted_set_key = "Alerts"
    #         timestamp_score = datetime.now().timestamp() 
    #         await r.zadd(sorted_set_key, {json.dumps(alert_data): timestamp_score})
    #         alert = {
    #                 'type': 'info',
    #                 'message': 'new alert',
    #                 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #             }
    #         alert_json = json.dumps(alert)
    #         print("publishing alert to r",alert_json)
    #         await r.publish('alerts', alert_json)
    #         return 1
    #     elif lrcangletype != 'custom':
    #         print("lrcangletype is not custom")
    #         info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
    #         print(info)
    #         await self.db.insert_trade_log(date_log=self.today, module='alert normal angle', activity='Alert Generated', important_data=info, priority=5, strategy_trade_id='', timestamp=datetime.now())
    #         await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now(), conditionID)
            
    #         alert_data = {
    #             "symbol": exchange_code,
    #             "datetime": alert_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S"),
    #             "scanid": str(scanID),  # Convert Int64 to string
    #             "timeframe": str(digit_name),  # Convert to string if needed
    #             "bottime": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
    #             "conditionID": str(conditionID)  # Convert Int64 to string
    #         }
    #         print(alert_data)
    #         sorted_set_key = "Alerts"
    #         timestamp_score = datetime.now().timestamp() 
    #         await r.zadd(sorted_set_key, {json.dumps(alert_data): timestamp_score})
    #         alert = {
    #                 'type': 'info',
    #                 'message': 'new alert',
    #                 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #             }
    #         alert_json = json.dumps(alert)
    #         print("publishing alert to r",alert_json)
    #         await r.publish('alerts', alert_json)
    #         return 1
    #     else:
    #         if self.loglevel >= 1:
    #             info = f"NOT {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
    #             await self.db.insert_trade_log(date_log=self.today, module='checkAlerts_interval', activity='no angle', important_data=info, priority=2, strategy_trade_id = '', timestamp=datetime.now())
    #         return 0

    #Logs added for testing and understanding missed alerts :
  

    async def process_alert(self, exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, 
                        lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, 
                        conditionID, r, close_ha, scan_name):
    
        alert_timestamp_str = str(alert_timestamp)[:26]  
        if 'T' in alert_timestamp_str:
            alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%d %H:%M:%S")

        print(f"[DEBUG] Processing Alert: {exchange_code} at {alert_timestamp_dt}")
        print(f"[DEBUG] PSAR Signal: {psar_signal}, Candle Color: {candle_color}, High HA: {high_ha}")
        
        # Check why alerts are failing
        if not (lrcangletype == 'custom' and lrcanglestart < angle_degrees < lrcangleend) and not (1 == 1):
            print(f"[DEBUG] Alert NOT Triggered for {exchange_code}. Condition mismatch.")
            return 0

        # Log successful alert trigger
        info = f"[ALERT TRIGGERED] {exchange_code} {alert_timestamp} K crossover: {crossover_index} " \
            f"PSAR: {psar_signal} Color: {candle_color} High HA: {high_ha} < LRL:{LRL_value}"
        print(info)

        await self.db.insert_trade_log(date_log=self.today, module='alert debug', activity='Alert Triggered',
                                    important_data=info, priority=5, strategy_trade_id='', timestamp=datetime.now())
        
        await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now(), conditionID)

        # Telegram Alert Debugging
        print(f"[DEBUG] Sending Telegram Alert: {exchange_code} at {alert_timestamp_dt}")
        send_telegram_message(exchange_code, round(close_ha, 2), alert_timestamp_dt.strftime("%d %b %Y"),
                            alert_timestamp_dt.strftime("%H:%M"), digit_name, scan_name)
        
        # Redis Debugging
        sorted_set_key = "Alerts"
        timestamp_score = datetime.now().timestamp()
        alert_data = {
            "symbol": exchange_code,
            "datetime": alert_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "scanid": str(scanID),
            "timeframe": str(digit_name),
            "bottime": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
            "conditionID": str(conditionID)
        }
        print(f"[DEBUG] Storing Alert in Redis: {alert_data}")

        await r.zadd(sorted_set_key, {json.dumps(alert_data): timestamp_score})
        
        return 1


    async def process_symbol(self, exchange_code, instrument_token, interval, data_combined, dates_combined, basket_id, r):
        #table_name =  self.interval_to_table.get(interval, None)
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
                for cond in [1,2]  :
                    if cond == 1 :
                        condition = condition_filtered['condition1'].iloc[0]
                        cand_type = condition_filtered['candle1'].iloc[0] 
                        psarid = condition_filtered['psar1'].iloc[0] 

                    elif cond == 2 :
                        condition = condition_filtered['condition2'].iloc[0]
                        cand_type = condition_filtered['candle2'].iloc[0] 
                        psarid = condition_filtered['psar2'].iloc[0] 

                    else :
                        break

                    if condition != 1 :
                        break

                    # common params ____                                                     

                    kline_start = condition_filtered['kline_start'].iloc[0]
                    kline_end = condition_filtered['kline_end'].iloc[0]

                    scanID = df_items.loc[(df_items[column_name] == 1) & (df_items['conditionID'] == conditionID), 'scanID'].iloc[0] 
                    scan_name = 'Test'  
                    scan_name = self.df_scan_names.loc[self.df_scan_names['id'] == scanID, 'name'].iloc[0]  

                    # lrcid = condition_filtered['lrcid'].iloc[0]
                    # lrc_filtered = self.df_custom_indicators[self.df_custom_indicators.id == lrcid]
                    # lrc_values = lrc_filtered['value'].iloc[0]
                    # period_str, standard_deviation_str = lrc_values.split(',')
                    # lrc_period = int(period_str.strip())
                    # lrc_stdev = float(standard_deviation_str.strip())                    
                    
                    # PSAR ___
                    psar_filtered = self.df_custom_indicators[self.df_custom_indicators.id == psarid]
                    psar_values = psar_filtered['value'].iloc[0]
                    acceleration_str, max_acceleration_str = psar_values.split(',')
                    PSAR_acceleration = float(acceleration_str.strip())
                    PSAR_max_acceleration = float(max_acceleration_str) 

                    # Stoch ____
                    stochid = condition_filtered['stochid'].iloc[0] 
                    stoch_filtered = self.df_custom_indicators[self.df_custom_indicators.id == stochid]
                    stoch_values = stoch_filtered['value'].iloc[0]                    
                    period_str, k_avg_str, d_avg_str = stoch_values.split(',')
                    stoch_period = float(period_str)
                    k_avg = float(k_avg_str)
                    d_avg = float(d_avg_str)
                    
                    # lrcangletype = condition_filtered['lrcangletype'].iloc[0] 
                    # lrcanglestart = condition_filtered['lrcanglestart'].iloc[0] 
                    # lrcangleend = condition_filtered['lrcangleend'].iloc[0] 
                    signaldirection = condition_filtered['signaldirection'].iloc[0] 
                                                    
                    low = data_combined[:,2]
                    high = data_combined[:,1]
                    close = data_combined[:,3]
                    psar_data = psar(high, low, close, af0=float(PSAR_acceleration), af=float(PSAR_acceleration), max_af=float(PSAR_max_acceleration))
                    print(psar_data, "PSAR DATAA")
                    signals = get_psar_signals(close, psar_data)
                    #psar_signal = signals[-1]
                    # K, D = calc_fastStochastics(low, high, close, stoch_period, k_avg, d_avg)
                    K, D = calc_fastStochastics(low, high, close, lookback_period=stoch_period, d_period=d_avg, k_smoothing_period=k_avg)

                    # Disbaled cross_overs ____
                    # crossover_index = await self.get_crossover_index(K, LineThreshold, psarCandles)
                    # if crossover_index == -1 or crossover_index == psarCandles:
                    #     if self.loglevel >= 2:
                    #         info = f'{crossover_index=} {psarCandles=} {exchange_code} {interval}'
                    #         log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no crossover', info, 2, datetime.now()))
                    # else:
                    #     candles_to_check = 3
                    #     if crossover_index < candles_to_check:
                    #         candles_to_check = crossover_index

                    # Dummy params ___
                    LRL_value = lrcangletype = lrcanglestart = lrcangleend = angle_degrees = crossover_index = 0

                    candles_to_check = 1
                    for i in range(-candles_to_check, 0):
                        open_ha = ha_combined[i,0]
                        high_ha = ha_combined[i,1]
                        low_ha = ha_combined[i,2]
                        close_ha = ha_combined[i,3]

                        # Save data for troubleshooting ___
                        now = datetime.now()
                        formatted_time = now.strftime("%Y%m%d%H%M")
                        file_path = f"C:\\Users\\Administrator\\Desktop\\test_candle\\{exchange_code} {formatted_time} {interval}.txt"
                        
                        try :
                            candle_data = {
                                    "open_ha": open_ha,
                                    "high_ha": high_ha,
                                    "low_ha": low_ha,
                                    "close_ha": close_ha,
                                    "open": data_combined[-1][0],
                                    "high": data_combined[-1][1],
                                    "low": data_combined[-1][2],
                                    "close": data_combined[-1][3],                                   
                                    "data_combined": data_combined[-1].tolist(),
                                    "psar_data[i]": psar_data[i],
                                    "K[i]": K[i],
                                    "signals[i]": signals[i],
                                    "scanID": int(scanID), 
                                    "conditionID": int(conditionID), 
                                }
                            with open(file_path, 'w') as file:
                                json.dump(candle_data, file, indent=4)
                        except Exception as e:
                            print(e)
                            time.sleep(5)
                        # Save data for troubleshooting ___

                        if kline_start < K[i] < kline_end and cond == 1 or cond == 2 :
                            alert_timestamp = dates_combined[i-1]
                            print("alert timestamp: ",alert_timestamp)
                            print("K[I]: ", K[i])
                            psar_signal = signals[i] # psar_signal = signals[-1]
                            
                            # if self.loglevel >= 2:
                            #     info = f"index:{crossover_index} psig:{psar_signal} direction:{signaldirection} {exchange_code} {interval}"
                            #     log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'crossover', info, 2, datetime.now()))


                            print(f"close {close_ha} psar {psar_data[i]} psar_signal {psar_signal}  SignalDirection {signaldirection}")
                            if psar_signal == signaldirection : # signaldirection = 1 PSAR Signal is Long
                                if self.loglevel >= 1:
                                    info = f"psar_signal: {psar_signal} == signaldirection: {signaldirection}"
                                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'signal', info, 2, datetime.now()))
                                
                                print(f"psar_signal: {psar_signal} == signaldirection: {signaldirection}")

                                candle_color = 'g'
                                if close_ha < open_ha:
                                    candle_color = 'r'
                                
                                if i == -1:
                                    sliced_close = close
                                else:
                                    sliced_close = close[:i + 1]
                                # LRL, UCL, LCL, angle_degrees = linear_regression_channel_numba_sliding(sliced_close, lrc_period, lrc_stdev)        
                                # LRL_value = LRL[i]

                                if self.loglevel >= 2:
                                    info = f"{open_ha=} {high_ha=} {low_ha=} {close_ha=} {LRL_value=} {candle_color=} {cand_type=}"
                                    log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'imp data', info, 2, datetime.now()))
                                # and high_ha < LRL_value
                                print(f"candle_color {candle_color} cand_type {cand_type}")
                                if cand_type == 1:
                                    if candle_color == 'g' :
                                        print(f"Processing ALert for cand type 1 and green. Close_HA: {close_ha}  Open_HA: {open_ha}  Psar_signal: {psar_signal} K: {K[i]}  Signal Direction: {signaldirection}  Interval: {interval}")
                                        result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r,close_ha,scan_name)
                                    else:
                                        if self.loglevel >= 1:
                                            info = f"NOT color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value}"
                                            log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

                                elif cand_type == 2:
                                    no_lower_wick = low_ha == open_ha
                                    upper_wick = high_ha > close_ha
                                    if candle_color == 'g'  and no_lower_wick and upper_wick:
                                        print(f"processing alert for candle type 2 and green")
                                        result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r,close_ha,scan_name)
                                    else:
                                        if self.loglevel >= 1:
                                            info = f"Not color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {upper_wick=}"
                                            log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

                                elif cand_type == 3:
                                    no_upper_wick = high_ha == close_ha
                                    no_lower_wick = low_ha == open_ha
                                    if candle_color == 'g'  and no_lower_wick and no_upper_wick:
                                        print(f"Processing Alert for candle type 3 and green")
                                        result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r,close_ha,scan_name)
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
    # async def process_symbol(self, exchange_code, instrument_token, interval, data_combined, dates_combined, basket_id, r):
    #     #table_name =  self.interval_to_table.get(interval, None)
    #     print("process_symbol: ",exchange_code, interval,basket_id)
    #     #count_iter = 0
    #     log_batch = []
    #     #BATCH_SIZE = 500   
    #     ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(data_combined[:,0], data_combined[:,1], data_combined[:,2], data_combined[:,3])
    #     ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
    #     # if exchange_code in ['MARUTI24SEP11800PE', 'TRENT24SEP7200CE']:
    #     #     df = pd.DataFrame(data_combined, columns=['open', 'high', 'low', 'close'])
    #     #     df['timestamp'] = dates_combined
    #     #     df = df[::-1]
    #     #     datetime_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    #     #     filename = f"data/Hekin-{exchange_code}-{datetime_str}.csv"
    #     #     df.to_csv(filename, index=False)
    #         # -------------------  Alert Code here --------------------------------
    #     if self.loglevel >= 2:
    #         info = f'Alert code begin {exchange_code} {interval}'
    #         log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'start alert code', info, 2, datetime.now()))

    #     digit_name =  self.interval_to_digit.get(interval, None)
    #     column_name = str(digit_name) + 'min'
    #     print("Column Name: ",column_name)
    #     df_items = self.df_scan_items[self.df_scan_items[column_name] == 1]
    #     print("df item time frame filter column: ",df_items)
    #     alert_check = True
    #     if basket_id == None:
    #         info = f'basket_id None skip {exchange_code} {interval}'
    #         log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'basket_id None', info, 2, datetime.now()))
    #         alert_check = False
    #     else:
    #         df_items = df_items[df_items['basket_id'] == basket_id]
    #         print("df item basket filter",df_items)

    #     if df_items.empty:
    #         if self.loglevel >= 1:
    #             info = f'df_items empty skip {exchange_code} {interval}'
    #             log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no alert items', info, 2, datetime.now()))
    #         alert_check = False
    #         print("df_items empty")

    #     conditions = df_items.conditionID.unique()
    #     if len(conditions) == 0:
    #         if self.loglevel >= 1:
    #             info = f'No condition {exchange_code} {interval}'
    #             log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no conditions', info, 2, datetime.now()))

    #         alert_check = False
    #         print("no conditions")

    #     if alert_check:
    #         if self.loglevel >= 2:
    #             info = f'Alert check true {exchange_code} {interval}'
    #             log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'alert check', info, 2, datetime.now()))

    #         for conditionID in conditions:
    #             if self.loglevel >= 2:
    #                 info = f"{conditionID=} {exchange_code} {interval}"
    #                 log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'process cond', info, 2, datetime.now()))

    #             condition_filtered = self.df_conditions[self.df_conditions['id'] == conditionID]
    #             if len(condition_filtered) == 0:
    #                 info = f'len(condition_filtered) == 0 {conditionID=} {exchange_code} {interval}'
    #                 log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'conditionFilter', info, 2, datetime.now()))
    #                 continue

    #             scanID = df_items.loc[(df_items[column_name] == 1) & (df_items['conditionID'] == conditionID), 'scanID'].iloc[0] 
                    
    #             lrcid = condition_filtered['lrcid'].iloc[0]
    #             # Ensure lrc_filtered is not empty before accessing
    #             lrc_filtered = self.df_custom_indicators[self.df_custom_indicators.id == lrcid]
    #             # if lrc_filtered.empty:
    #             #     info = f"lrc_filtered is empty for lrcid: {lrcid}, {conditionID=}, {exchange_code=}, {interval=}"
    #             #     log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'lr(c_filtered empty', info, 2, datetime.now()))
    #             #     print("Skipping", lrcid, self.df_custom_indicators.id)
    #             #     continue  # Skip this iteration                
    #             # lrc_values = lrc_filtered['value'].iloc[0]
    #             # period_str, standard_deviation_str = lrc_values.split(',')
    #             # lrc_period = int(period_str.strip())
    #             # lrc_stdev = float(standard_deviation_str.strip())

    #             for cond in [1, 2]:
    #                 condition = condition_filtered[f'condition{cond}'].iloc[0]
    #                 cand_type = condition_filtered[f'candle{cond}'].iloc[0]
    #                 psarid = condition_filtered[f'psar{cond}'].iloc[0]
    #                 if condition != 1:
    #                     print("condition not active")
    #                     continue # Skip if inactive
 
    #                 try:
    #                     psar_filtered = self.df_custom_indicators[self.df_custom_indicators.id == psarid]
    #                     psar_values = psar_filtered['value'].iloc[0].split(',')
    #                     PSAR_acceleration, PSAR_max_acceleration = map(float,psar_values)
    #                     psar_data = psar(data_combined[: , 1], data_combined[:,2],data_combined[:,3],af0=PSAR_acceleration, af=PSAR_acceleration,max_af=PSAR_max_acceleration)
    #                     signals = get_psar_signals(data_combined[:,3], psar_data)
    #                     K, D = calc_fastStochastics(data_combined[:,2],data_combined[:,1], data_combined[:,3],lookback_period=14, d_period=3,k_smoothing_period=3)
    #                     psar_signal = signals[-1] if len(signals) > 0 else 1 # 🔥Default to 1 if missing
    #                     stoch_value = K[-1] if len(K) > 0 else 50 # 🔥 Default to mid-range
    #                 except Exception as e:
    #                     print(f"[ERROR] PSAR/Stochastics failed for {exchange_code}:{e}")
    #                     psar_signal = 1
    #                     stoch_value = 50

    #                 # --- 🔥 Relax Heikin-Ashi Candle Validation ---
    #                 ha_candle = ha_combined[-1] # Last Heikin-Ashi candle
    #                 valid_candle = True # 🔥 Default to true to prevent missing alerts
                    
    #                 # Get KLine values from condition
    #                 kline_start = condition_filtered['kline_start'].iloc[0]
    #                 kline_end = condition_filtered['kline_end'].iloc[0]
    #                 print(psar_signal, signals, stoch_value, kline_start, kline_end)
    #                 if kline_start < stoch_value < kline_end:
    #                     print("Process Alert...")
    #                     await self.process_alert(exchange_code, scanID,dates_combined[-1], 0, "none",
    #                                             0, 0, 0, 0, psar_signal, 'g', ha_candle[1],
    #                                                 digit_name, conditionID, r)
                                                    
    # async def process_symbol(self, exchange_code, instrument_token, interval, data_combined, dates_combined, basket_id, r):
    #     #table_name =  self.interval_to_table.get(interval, None)
    #     print("process_symbol: ",exchange_code, interval,basket_id)
    #     #count_iter = 0
    #     log_batch = []
    #     #BATCH_SIZE = 500   
    #     ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(data_combined[:,0], data_combined[:,1], data_combined[:,2], data_combined[:,3])
    #     ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
    #     # if exchange_code in ['MARUTI24SEP11800PE', 'TRENT24SEP7200CE']:
    #     #     df = pd.DataFrame(data_combined, columns=['open', 'high', 'low', 'close'])
    #     #     df['timestamp'] = dates_combined
    #     #     df = df[::-1]
    #     #     datetime_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    #     #     filename = f"data/Hekin-{exchange_code}-{datetime_str}.csv"
    #     #     df.to_csv(filename, index=False)
    #         # -------------------  Alert Code here --------------------------------
    #     if self.loglevel >= 2:
    #         info = f'Alert code begin {exchange_code} {interval}'
    #         log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'start alert code', info, 2, datetime.now()))

    #     digit_name =  self.interval_to_digit.get(interval, None)
    #     column_name = str(digit_name) + 'min'
    #     print("Column Name: ",column_name)
    #     df_items = self.df_scan_items[self.df_scan_items[column_name] == 1]
    #     print("df item time frame filter column: ",df_items)
    #     alert_check = True
    #     if basket_id == None:
    #         info = f'basket_id None skip {exchange_code} {interval}'
    #         log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'basket_id None', info, 2, datetime.now()))
    #         alert_check = False
    #     else:
    #         df_items = df_items[df_items['basket_id'] == basket_id]
    #         print("df item basket filter",df_items)

    #     if df_items.empty:
    #         if self.loglevel >= 1:
    #             info = f'df_items empty skip {exchange_code} {interval}'
    #             log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no alert items', info, 2, datetime.now()))
    #         alert_check = False
    #         print("df_items empty")

    #     conditions = df_items.conditionID.unique()
    #     if len(conditions) == 0:
    #         if self.loglevel >= 1:
    #             info = f'No condition {exchange_code} {interval}'
    #             log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no conditions', info, 2, datetime.now()))

    #         alert_check = False
    #         print("no conditions")

    #     print(" TTTTT " ,self.df_conditions['id'])
    #     print("conditionsss", conditions)
    #     if alert_check:
    #         if self.loglevel >= 2:
    #             info = f'Alert check true {exchange_code} {interval}'
    #             log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'alert check', info, 2, datetime.now()))

    #         for conditionID in conditions:
    #             if self.loglevel >= 2:
    #                 info = f"{conditionID=} {exchange_code} {interval}"
    #                 log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'process cond', info, 2, datetime.now()))

    #             condition_filtered = self.df_conditions[self.df_conditions['id'] == conditionID]
    #             if len(condition_filtered) == 0:
    #                 info = f'len(condition_filtered) == 0 {conditionID=} {exchange_code} {interval}'
    #                 log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'conditionFilter', info, 2, datetime.now()))
    #                 continue

    #             print("AAAAAAAAAA")
    #             scanID = df_items.loc[(df_items[column_name] == 1) & (df_items['conditionID'] == conditionID), 'scanID'].iloc[0] 
                    
    #             lrcid = condition_filtered['lrcid'].iloc[0]
    #             # Ensure lrc_filtered is not empty before accessing
    #             print(self.df_custom_indicators.id, lrcid, "HHHHHH")
    #             lrc_filtered = self.df_custom_indicators[self.df_custom_indicators.id == lrcid]
    #             # if lrc_filtered.empty:
    #             #     info = f"lrc_filtered is empty for lrcid: {lrcid}, {conditionID=}, {exchange_code=}, {interval=}"
    #             #     log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'lr(c_filtered empty', info, 2, datetime.now()))
    #             #     print("Skipping", lrcid, self.df_custom_indicators.id)
    #             #     continue  # Skip this iteration                
    #             # lrc_values = lrc_filtered['value'].iloc[0]
    #             # period_str, standard_deviation_str = lrc_values.split(',')
    #             # lrc_period = int(period_str.strip())
    #             # lrc_stdev = float(standard_deviation_str.strip())

 
    #             psarid = condition_filtered['psarid'].iloc[0] 
    #             psar_filtered = self.df_custom_indicators[self.df_custom_indicators.id == psarid]
    #             psar_values = psar_filtered['value'].iloc[0]
    #             acceleration_str, max_acceleration_str = psar_values.split(',')
    #             PSAR_acceleration = float(acceleration_str.strip())
    #             PSAR_max_acceleration = float(max_acceleration_str) 

    #             stochid = condition_filtered['stochid'].iloc[0] 
    #             stoch_filtered = self.df_custom_indicators[self.df_custom_indicators.id == stochid]
    #             stoch_values = stoch_filtered['value'].iloc[0]
                
    #             period_str, k_avg_str, d_avg_str = stoch_values.split(',')
    #             stoch_period = float(period_str)
    #             k_avg = float(k_avg_str)
    #             d_avg = float(d_avg_str)
                
    #             lrcangletype = condition_filtered['lrcangletype'].iloc[0] 
    #             lrcanglestart = condition_filtered['lrcanglestart'].iloc[0] 
    #             lrcangleend = condition_filtered['lrcangleend'].iloc[0] 
    #             signaldirection = condition_filtered['signaldirection'].iloc[0] 
    #             hlfpid = condition_filtered['hlfpid'].iloc[0]

    #             LineThreshold, psarCandles = await self.get_hlfp_values(hlfpid)
                
    #             low = data_combined[:,2]
    #             high = data_combined[:,1]
    #             close = data_combined[:,3]
                
    #             psar_data = psar(high, low, close, af0=float(PSAR_acceleration), af=float(PSAR_acceleration), max_af=float(PSAR_max_acceleration))
    #             signals = get_psar_signals(close, psar_data)
    #             #psar_signal = signals[-1]
    #             K, D = calc_fastStochastics(low, high, close, stoch_period, k_avg, d_avg)
    #             crossover_index = await self.get_crossover_index(K, LineThreshold, psarCandles)
    #             print("CROSSOVER INDEX",crossover_index, psarCandles, exchange_code, interval)
    #             if crossover_index == -1 or crossover_index == psarCandles:
    #                 if self.loglevel >= 2:
    #                     info = f'{crossover_index=} {psarCandles=} {exchange_code} {interval}'
    #                     log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no crossover', info, 2, datetime.now()))
    #             else:
    #                 candles_to_check = 3
    #                 if crossover_index < candles_to_check:
    #                     candles_to_check = crossover_index
    #                 for i in range(-candles_to_check, 0):
    #                     psar_signal = signals[i] # psar_signal = signals[-1]
    #                     if self.loglevel >= 2:
    #                         info = f"index:{crossover_index} psig:{psar_signal} direction:{signaldirection} {exchange_code} {interval}"
    #                         log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'crossover', info, 2, datetime.now()))

    #                     if psar_signal == signaldirection: # signaldirection = 1 PSAR Signal is Long
    #                         if self.loglevel >= 1:
    #                             info = f"psar_signal: {psar_signal} == signaldirection: {signaldirection}"
    #                             log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'signal', info, 2, datetime.now()))

    #                         open_ha = ha_combined[i,0]
    #                         high_ha = ha_combined[i,1]
    #                         low_ha = ha_combined[i,2]
    #                         close_ha = ha_combined[i,3]
    #                         candle_color = 'g'
    #                         if close_ha < open_ha:
    #                             candle_color = 'r'
                            
    #                         if i == -1:
    #                             sliced_close = close
    #                         else:
    #                             sliced_close = close[:i + 1]
    #                         LRL, UCL, LCL, angle_degrees = linear_regression_channel_numba(sliced_close, lrc_period, lrc_stdev)        
    #                         LRL_value = LRL[-1]

    #                         if self.loglevel >= 2:
    #                             info = f"{open_ha=} {high_ha=} {low_ha=} {close_ha=} {LRL_value=} {candle_color=} {hlfpid=}"
    #                             log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'imp data', info, 2, datetime.now()))

    #                         alert_timestamp = dates_combined[i]
    #                         if hlfpid == 1:
    #                             if candle_color == 'g' and high_ha < LRL_value:
    #                                 result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r)
    #                             else:
    #                                 if self.loglevel >= 1:
    #                                     info = f"NOT color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value}"
    #                                     log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

    #                         elif hlfpid == 2:
    #                             no_lower_wick = low_ha == open_ha
    #                             upper_wick = high_ha > close_ha
    #                             if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and upper_wick:
    #                                 result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r)
    #                             else:
    #                                 if self.loglevel >= 1:
    #                                     info = f"Not color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {upper_wick=}"
    #                                     log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

    #                         elif hlfpid == 3:
    #                             no_upper_wick = high_ha == close_ha
    #                             no_lower_wick = low_ha == open_ha
    #                             if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and no_upper_wick:
    #                                 result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, r)
    #                             else:
    #                                 if self.loglevel >= 1:
    #                                     info = f"Not Wickless color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {no_upper_wick=}"
    #                                     log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no match', info, 2, datetime.now()))

    #                     else:
    #                         if self.loglevel >= 1:
    #                             info = f"NOT psarsignal: {psar_signal} == signaldirection: {signaldirection}"
    #                             log_batch.append((self.today, 'd_ohlc_v2-Alerts', 'no signal', info, 2, datetime.now()))

    #         if log_batch:
    #             #batch_insert_trade_logs.delay(log_batch)
    #             batch_insert_trade_logs.apply_async(args=[log_batch], queue='low_priority')
    #             log_batch= []
    #         print('done ', exchange_code)

    #     return 1

    async def handle_resampling(self, df, interval, symbol, instrument_code, basket_id, r):
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
        log_batch_main = []
        current_datetime = datetime.now()
        r = redis.from_url('redis://localhost', decode_responses=True)
        # ohlc_dtype = [('timestamp', 'U20'), ('open', 'f8'), ('high', 'f8'), ('low', 'f8'), ('close', 'f8')]
        last_processed = time.time()
        print(f"process current minute")
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

                filtered_timeframes = self.df_basket_timeframes[self.df_basket_timeframes.basket_id == basket_id]
                min1 = filtered_timeframes['1min'].any()
                min2 = filtered_timeframes['2min'].any()
                min3 = filtered_timeframes['3min'].any()
                min5 = filtered_timeframes['5min'].any()
                min10 = filtered_timeframes['10min'].any()
                min15 = filtered_timeframes['15min'].any()
                min30 = filtered_timeframes['30min'].any()
                min60 = filtered_timeframes['60min'].any()
                any_true = any([min1, min2, min3, min5, min10, min15, min30, min60])
                if any_true == False:

                    print("No")
                    continue
                ohlc_sorted_data = await r.zrange('ohlc_sorted:' + instrument_code, 0, -1)
                ohlc_list = [json.loads(data) for data in ohlc_sorted_data]
                array_data = np.array([[entry['open'], entry['high'], entry['low'], entry['close']] for entry in ohlc_list])
                timestamps = np.array([entry['timestamp'] for entry in ohlc_list])
                if min1:
                    interval = 'minute'
                    await self.process_symbol(symbol, instrument_code, interval, array_data, timestamps, basket_id, r)

                df = pd.DataFrame(array_data, columns=['open', 'high', 'low', 'close'])
                df['timestamp'] = pd.to_datetime(timestamps)  
                datetime_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
                # filename = f"data/{symbol}-{datetime_str}.csv"
                # df.to_csv(filename, index=False)

                df.set_index('timestamp', inplace=True)  

                # intervals = [2, 3, 5, 10, 15, 30]
                # for interval in intervals:
                #     if current_datetime.minute % interval == 0:
                #         print(f"criteria match {interval}")
                #         await self.handle_resampling(df, interval, symbol, instrument_code, basket_id, r)

                # Check to see if we need to subtract 1 minute before %
                if min2 == True and current_datetime.minute % 2 == 0:
                    await self.handle_resampling(df, 2, symbol, instrument_code, basket_id, r)

                if min3 == True and current_datetime.minute % 3 == 0:
                    await self.handle_resampling(df, 3, symbol, instrument_code, basket_id, r)

                if min5 == True and current_datetime.minute % 5 == 0:
                    await self.handle_resampling(df, 5, symbol, instrument_code, basket_id, r)

                if min10 == True and current_datetime.minute % 10 == 0:
                    await self.handle_resampling(df, 10, symbol, instrument_code, basket_id, r)

                if min15 == True and current_datetime.minute % 15 == 0:
                    await self.handle_resampling(df, 15, symbol, instrument_code, basket_id, r)

                if min30 == True and current_datetime.minute % 30 == 0:
                    await self.handle_resampling(df, 30, symbol, instrument_code, basket_id, r)

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
    start.df_scan_names = await start.db.get_scan_names()
    print(start.df_scan_names)
    start.priority_stocks_tpl = await start.db.get_priority_instruments_to_trade()
    start.df_priority_stocks = pd.DataFrame(start.priority_stocks_tpl, columns=['instrument_token', 'symbol', 'basket_id'])   
    start.df_basket_timeframes = await start.db.get_timeframes()

    r = redis.from_url('redis://localhost', decode_responses=True)
    interval = 'minute'
    table_name = 'one_min_ohlc'
    current_time = datetime.now().time()
    for _, row in start.df_priority_stocks.iterrows():
        await r.hset('symbol_to_token', row['symbol'], row['instrument_token'])
        print(f"Added to symbol_to_token: {row['symbol']} -> {row['instrument_token']}")

        await r.hset('token_to_symbol', row['instrument_token'], row['symbol'])

    target_time = tm(21, 17)
    if current_time < target_time:
        print('download historical data')

        tasks = []

        async def cache_symbol_ohlc_data(row):
            instrument_token = row.instrument_token
            symbol = row.symbol
            print(row, "ROW")

            # Fetch old data from MySQL for the given symbol
            try:
                old_data_df = await start.db.get_old_data_by_symbol(table_name, symbol)
                print(f"OLD DATA {old_data_df}")
            except Exception as e:
                info = f"Error fetching data from MySQL for {symbol}: {e}"
                print(info)
                log_batch.append((start.today, 'main', 'mysql fetch', info, 2, datetime.now()))
                return

            if len(old_data_df) > 0:
                zadd_data = {}
                for row2 in old_data_df.itertuples(index=False):
                    datetime_str = row2.datetime.strftime("%Y-%m-%d %H:%M:%S")
                    final_ohlc = {
                        "timestamp": datetime_str,
                        "open": float(row2.open),
                        "high": float(row2.high),
                        "low": float(row2.low),
                        "close": float(row2.close)
                    }
                    score = int(row2.datetime.timestamp())
                    zadd_data[json.dumps(final_ohlc)] = score

                sorted_set_key = f"ohlc_sorted:{instrument_token}"

                try:
                    await r.zadd(sorted_set_key, zadd_data)
                except Exception as e:
                    info = f"Error inserting OHLC data into Redis for {symbol}: {e}"
                    print(info)
                    log_batch.append((start.today, 'main', 'cache redis', info, 2, datetime.now()))
            else:
                info = f'Data not found for {symbol} in table {table_name}'
                print(info)
                log_batch.append((start.today, 'main', 'cache creation', info, 2, datetime.now()))

        # Schedule async tasks for all symbols
        for row in start.df_priority_stocks.itertuples(index=False):
            tasks.append(cache_symbol_ohlc_data(row))

        # Run all tasks concurrently
        await asyncio.gather(*tasks)
        print("GATHER DONE")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"total_time loading hist data to redis: {total_time}")
    await r.close()

    start.df_scan_items = await start.db.get_scan_items()
    start.df_custom_indicators = await start.db.get_custom_indicators()
    print("DF INDITCATORS",start.df_custom_indicators)
    start.df_conditions = await start.db.get_conditions()
    print("DF CONDITIONS",start.df_conditions)
    start.df_HLFP = await start.db.get_hlfp()
    print("LOG BATCH")
    if log_batch:
        batch_insert_trade_logs.apply_async(args=[log_batch], queue='low_priority')
        log_batch= []

    last_run_minute = None  
    print("STARTING WHILE LOOP")
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
                print("PROCESS POOL")
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