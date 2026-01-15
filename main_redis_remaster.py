import os
import asyncio
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime, date, timedelta, time as tm
from typing import Dict, List, Optional, Tuple
import requests
import warnings
from kiteconnect import KiteConnect
from background.instruments import instruments
from background.async_db import dbconnection
from background.indicators import *
from background.login import login
import pytz
warnings.filterwarnings('ignore')

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.timezone("UTC")
def load_security_config(file_path: str = "C:/Users/Administrator/Desktop/python/koshy_python/background/security.txt") -> dict:
    try:
        with open(file_path, 'r') as file:
            content = file.read().strip()
            if content.startswith('{'):
                return json.loads(content)
            else:
                config = {}
                for line in content.split('\n'):
                    if ':' in line:
                        key, value = line.split(':', 1)
                        config[key.strip().strip('"')] = value.strip().strip('"').strip(',')
                return config
    except Exception as e:
        print(f"Error loading security config: {e}")
        return {}


def send_telegram_message(stock, price, date, time, tf, sn):
    message = f"*Alert*\nStock : {stock}\nPrice : Rs. {price}\nDate : {date}\nTime : {time}\nTF — {tf} min \nSN — {sn}"
    bot_token = '1936528227:AAFQwZV4z5AgSKEU9FW25Plnq-mTzXJY7Qw'
    chat_id = '@koshi_alerts'
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': message}

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        print("Telegram message sent successfully")
    except requests.exceptions.RequestException as e:
        print(f"Error sending message: {e}")


def calc_psar_numpy(high, low, close, af=0.02, max_af=0.2):
    length = len(close)
    psar = np.zeros(length)
    bull = True
    af_step = af
    ep = high[0]
    psar[0] = low[0]

    for i in range(1, length):
        psar[i] = psar[i - 1] + af_step * (ep - psar[i - 1])

        if bull:
            if low[i] < psar[i]:
                bull = False
                psar[i] = ep
                ep = low[i]
                af_step = af
            else:
                if high[i] > ep:
                    ep = high[i]
                    af_step = min(af_step + af, max_af)
        else:
            if high[i] > psar[i]:
                bull = True
                psar[i] = ep
                ep = high[i]
                af_step = af
            else:
                if low[i] < ep:
                    ep = low[i]
                    af_step = min(af_step + af, max_af)

    return psar

def calc_stochastic_k_numpy(high, low, close, k_period=14):
    k_values = np.full_like(close, np.nan, dtype=np.float64)

    for i in range(k_period, len(close)):
        high_max = np.max(high[i - k_period:i])
        low_min = np.min(low[i - k_period:i])
        if high_max != low_min:
            k_values[i] = 100 * (close[i] - low_min) / (high_max - low_min)
        else:
            k_values[i] = 0

    return k_values

def calc_lrl_angle_numpy(series, window=20):
    """Calculate LRL angle in degrees for the last window"""
    if len(series) < window:
        return 0

    y = series[-window:]
    x = np.arange(len(y))
    A = np.vstack([x, np.ones(len(x))]).T
    m, c = np.linalg.lstsq(A, y, rcond=None)[0]

    # Convert slope to degrees
    angle = np.arctan(m) * (180 / np.pi)
    return angle


class DirectKiteConnect:
    def __init__(self):
        self.backtest = 0

        # Zerodha login
        l = login(False)
        self.status, self.kite, self.kws, self.access_token = l.InitiateZerodha()
        print(f"Login Status: {self.status}")
        if not self.status:
            print('Login Failed')
            exit()

        self.config = load_security_config()
        self.db = dbconnection()
        self.instruments = instruments()

        self.ordertype = 'market'
        self.exchange = 'NFO'
        self.userid = 'koshy'
        self.sdate = datetime.now()
        self.log = False
        self.alertLog = False
        self.loglevel = 0
        self.trade = True
        self.run_job = True
        self.initiate_time = tm(9, 15, 59)
        self.exit_time = tm(15, 40)
        self.today = date.today()
        self.today_str = self.today.strftime('%Y-%m-%d')

        self.yesterday = self.today - timedelta(days=1)
        self.last_working_day = self.yesterday
        if self.last_working_day.weekday() == 5:
            self.last_working_day -= timedelta(days=1)
        elif self.last_working_day.weekday() == 6:
            self.last_working_day -= timedelta(days=2)

        self.interval_to_table = {
            'minute': 'one_min_ohlc', '2minute': 'two_min_ohlc', '5minute': 'five_min_ohlc',
            '3minute': 'three_min_ohlc', '10minute': 'ten_min_ohlc', '15minute': 'fifteen_min_ohlc',
            '30minute': 'thirty_min_ohlc', '60minute': 'one_hour_ohlc'
        }

        self.interval_to_digit = {
            'minute': '1', '2minute': '2', '5minute': '5', '3minute': '3',
            '10minute': '10', '15minute': '15', '30minute': '30', '60minute': '60'
        }

        self.kite_intervals = {
            'minute': 'minute',
            '2minute': '2minute',
            '3minute': '3minute',
            '5minute': '5minute',
            '10minute': '10minute',
            '15minute': '15minute',
            '30minute': '30minute',
            '60minute': '60minute',
            'day': 'day'  # optional if needed
        }

        self.ohlc_cache: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.last_update_time: Dict[str, datetime] = {}

        self.df_priority_stocks = None
        self.df_basket_timeframes = None
        self.df_scan_items = None
        self.df_scan_names = None
        self.df_custom_indicators = None
        self.df_conditions = None
        self.df_HLFP = None

    async def start_pool(self):
        print('Starting database pool')
        loop = asyncio.get_event_loop()

        db_config = {
            'host': self.config.get('hostname', '103.160.145.141'),
            'port': int(self.config.get('port', 3306)),
            'user': self.config.get('username', 'root'),
            'password': self.config.get('password', 'Airforce*123'),
            'database': self.config.get('database_name', 'algo')
        }

        await self.db.create_pool(loop=loop)

    async def close_pool(self):
        await self.db.close_pool()

    def get_historical_data(self, instrument_token: int, interval: str, from_date: datetime, to_date: datetime) -> pd.DataFrame:
        try:
            kite_interval = self.kite_intervals.get(interval, 'minute')
            print(f"kite interval: {kite_interval}  from_date: {from_date}   to_date: {to_date}    instrument_token: {instrument_token}")
            historical_data = self.kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval=kite_interval
            )
            if not historical_data:
                return pd.DataFrame()

            df = pd.DataFrame(historical_data)
            df['timestamp'] = pd.to_datetime(df['date'])
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            df.set_index('timestamp', inplace=True)
            return df

        except Exception as e:
            print(f"Error fetching historical data for {instrument_token}: {e}")
            return pd.DataFrame()

    def get_live_data(self, instrument_token: int) -> Optional[Dict]:
        try:
            quotes = self.kite.quote([instrument_token])
            if instrument_token in quotes:
                quote_data = quotes[instrument_token]
                return {
                    'timestamp': datetime.now(),
                    'open': quote_data.get('ohlc', {}).get('open', 0),
                    'high': quote_data.get('ohlc', {}).get('high', 0),
                    'low': quote_data.get('ohlc', {}).get('low', 0),
                    'close': quote_data.get('last_price', 0),
                    'volume': quote_data.get('volume', 0)
                }
        except Exception as e:
            print(f"Error fetching live data for {instrument_token}: {e}")
        return None

    # ... [Keep rest of the methods like get_ohlc_data, resample_data, process_symbol, etc. unchanged]

    # NOTE: You already had all the methods for alert generation, data processing, and main loop.
    # You can keep them as-is after this point since nothing else needs changing regarding authentication.

    def get_ohlc_data(self, instrument_token: int, symbol: str, interval: str, lookback_days: int = 30) -> Tuple[np.ndarray, np.ndarray]:
        """Get OHLC data for indicator calculations"""
        cache_key = f"{instrument_token}_{interval}"
        current_time = datetime.now()
        
        # Check if we need to update cache (every minute for live data)
        if (cache_key not in self.ohlc_cache or 
            cache_key not in self.last_update_time or 
            (current_time - self.last_update_time[cache_key]).seconds > 60):

            # Calculate date range
            to_date = current_time 
            from_date = current_time - timedelta(days=10)
            
            # Get historical data
            historical_df = self.get_historical_data(instrument_token, interval, from_date, to_date)
            
            if historical_df.empty == False:
                print(f"[INFO] Fetched historical data for {symbol} ({interval}) from {from_date} to {to_date}")
            
            # For live trading, append current live data if available
            if interval == 'minute':
                live_data = self.get_live_data(instrument_token)
                if live_data:
                    print(f"[INFO] Fetched live data for {symbol} ({interval}) at {live_data['timestamp']}")
                    # Create current minute candle
                    current_minute = current_time.replace(second=0, microsecond=0)
                    if current_minute not in historical_df.index:
                        live_row = pd.DataFrame([live_data]).set_index('timestamp')
                        historical_df = pd.concat([historical_df, live_row])
            
            # Update cache
            self.ohlc_cache[cache_key] = historical_df
            self.last_update_time[cache_key] = current_time
            
        else:
            historical_df = self.ohlc_cache[cache_key]
        
        if historical_df.empty:
            return np.array([]), np.array([])
        

        # Convert to numpy arrays
        ohlc_data = historical_df[['open', 'high', 'low', 'close']].values
        timestamps = historical_df.index.values
        
        return ohlc_data, timestamps

    def resample_data(self, df: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
        """Resample data to different timeframes"""
        if df.empty:
            return df
            
        # Resample with market opening offset
        resampled_df = df.resample(f'{interval_minutes}T', offset='15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        return resampled_df

    async def get_crossover_index(self, K, LineThreshold, psarCandles):
        """Calculate crossover index for stochastic indicator"""
        if len(K) < psarCandles:
            return -1
            
        last_n_elements = K[-psarCandles:]
        crossover_index = -1
        
        for i in range(len(last_n_elements) - 1):
            if last_n_elements[i] > LineThreshold and last_n_elements[i + 1] <= LineThreshold:
                crossover_index = i + 1
            elif last_n_elements[i + 1] > LineThreshold:
                crossover_index = -1
                
        return psarCandles - crossover_index if crossover_index > -1 else crossover_index

    async def process_alert(self, exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype,
                          lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal,
                          candle_color, high_ha, digit_name, conditionID, close_ha, scan_name):
        """Process and send alerts, including improved logging and Redis integration."""

        # alert_timestamp_str = str(alert_timestamp)[:26]
        # if 'T' in alert_timestamp_str:
        #     alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
        # else:
        #     alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%d %H:%M:%S")

        alert_timestamp_dt = alert_timestamp.replace(tzinfo=None)

        print(f"[DEBUG] Processing Alert: {exchange_code} at {alert_timestamp_dt}")
        print(f"[DEBUG] PSAR Signal: {psar_signal}, Candle Color: {candle_color}, High HA: {high_ha}")

        # Check alert conditions
        # The condition (1 == 1) means it will always pass, assuming other checks are done before calling this function.
        # If specific conditions from the second file need to be re-evaluated here, they should be explicitly added.
        if not (lrcangletype == 'custom' and lrcanglestart < angle_degrees < lrcangleend) and not (1 == 1):
            print(f"[DEBUG] Alert NOT Triggered for {exchange_code}. Condition mismatch.")
            return 0

        info = f"[ALERT TRIGGERED] {exchange_code} {alert_timestamp} K crossover: {crossover_index} " \
               f"PSAR: {psar_signal} Color: {candle_color} High HA: {high_ha} < LRL:{LRL_value}"
        print(info)

        await self.db.insert_trade_log(
            date_log=self.today,
            module='alert debug',
            activity='Alert Triggered',
            important_data=info,
            priority=5,
            strategy_trade_id='',
            timestamp=datetime.now()
        )

        await self.db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now(), conditionID)

        # Send Telegram alert
        print(f"[DEBUG] Sending Telegram Alert: {exchange_code} at {alert_timestamp_dt}")
        send_telegram_message(
            exchange_code,
            round(close_ha, 2),
            alert_timestamp_dt.strftime("%d %b %Y"),
            alert_timestamp_dt.strftime("%H:%M"),
            digit_name,
            scan_name
        )

        # Note: The Redis integration from the second file is not directly included here
        # because the first file doesn't seem to have a Redis connection initialized within this class.
        # If you need Redis integration, you'll need to add a Redis client to `DirectKiteConnect`'s `__init__`
        # and pass it to this function, similar to how it's done in the second file's `process_alert`.
        # For now, it's omitted to keep the fix isolated to the requested functions without adding new dependencies
        # that aren't present in the `DirectKiteConnect` class already.

        return 1

    async def process_symbol(self, exchange_code, instrument_token, interval, basket_id):
        """Process symbol for alerts using direct KiteConnect data, incorporating improved alert logic."""
        print(f"Processing {exchange_code} for {interval}")

        data_combined, dates_combined = self.get_ohlc_data(instrument_token, exchange_code, interval)
        if len(data_combined) == 0:
            print(f"No data available for {exchange_code}")
            return 0

        print(f"Data fetched for {exchange_code} with {len(data_combined)} records")

        # Calculate Heikin-Ashi
        ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(
            data_combined[:, 0], data_combined[:, 1], data_combined[:, 2], data_combined[:, 3]
        )
        ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))

        # Fetch applicable scan items
        digit_name = self.interval_to_digit.get(interval, None)
        column_name = str(digit_name) + 'min'
        df_items = self.df_scan_items[self.df_scan_items[column_name] == 1]
        if basket_id is None:
            print(f"Skipping {exchange_code} due to missing basket_id")
            return 0
        df_items = df_items[df_items['basket_id'] == basket_id]
        if df_items.empty:
            print(f"No scan items for {exchange_code}")
            return 0

        conditions = df_items.conditionID.unique()
        if len(conditions) == 0:
            print(f"No conditions for {exchange_code}")
            return 0

        for conditionID in conditions:
            condition_filtered = self.df_conditions[self.df_conditions['id'] == conditionID]
            if len(condition_filtered) == 0:
                continue

            for cond in [1, 2]:  # Iterate through condition1 and condition2
                if cond == 1:
                    condition = condition_filtered['condition1'].iloc[0]
                    cand_type = condition_filtered['candle1'].iloc[0]
                    psarid = condition_filtered['psar1'].iloc[0]
                elif cond == 2:
                    condition = condition_filtered['condition2'].iloc[0]
                    cand_type = condition_filtered['candle2'].iloc[0]
                    psarid = condition_filtered['psar2'].iloc[0]
                else:
                    break

                if condition != 1:  # Assuming condition == 1 means the condition is active
                    break

                # common params
                kline_start = condition_filtered['kline_start'].iloc[0]
                kline_end = condition_filtered['kline_end'].iloc[0]

                scanID = df_items.loc[(df_items[column_name] == 1) & (df_items['conditionID'] == conditionID), 'scanID'].iloc[0]
                scan_name = self.df_scan_names.loc[self.df_scan_names['id'] == scanID, 'name'].iloc[0]

                # PSAR
                psar_filtered = self.df_custom_indicators[self.df_custom_indicators.id == psarid]
                psar_values = psar_filtered['value'].iloc[0]
                acceleration_str, max_acceleration_str = psar_values.split(',')
                PSAR_acceleration = float(acceleration_str.strip())
                PSAR_max_acceleration = float(max_acceleration_str)

                # Stoch
                stochid = condition_filtered['stochid'].iloc[0]
                stoch_filtered = self.df_custom_indicators[self.df_custom_indicators.id == stochid]
                stoch_values = stoch_filtered['value'].iloc[0]
                period_str, k_avg_str, d_avg_str = stoch_values.split(',')
                stoch_period = float(period_str)
                k_avg = float(k_avg_str)
                d_avg = float(d_avg_str)

                # These values are taken from the second file's `process_symbol` function.
                # They are used for the `process_alert` call, even if currently set to 0 or None.
                lrcangletype = condition_filtered['lrcangletype'].iloc[0] if 'lrcangletype' in condition_filtered.columns else 'None'
                lrcanglestart = condition_filtered['lrcanglestart'].iloc[0] if 'lrcanglestart' in condition_filtered.columns else 0
                lrcangleend = condition_filtered['lrcangleend'].iloc[0] if 'lrcangleend' in condition_filtered.columns else 0
                signaldirection = condition_filtered['signaldirection'].iloc[0]

                low = data_combined[:, 2]
                high = data_combined[:, 1]
                close = data_combined[:, 3]

                psar_data = psar(high, low, close, af0=float(PSAR_acceleration), af=float(PSAR_acceleration), max_af=float(PSAR_max_acceleration))
                signals = get_psar_signals(close, psar_data)

                K, D = calc_fastStochastics(low, high, close, lookback_period=stoch_period, d_period=d_avg, k_smoothing_period=k_avg)

                # Dummy params for now as per second file's logic
                LRL_value = angle_degrees = crossover_index = 0

                candles_to_check = 1 # Only check the last candle for live alerts
                for i in range(-candles_to_check, 0):

                    open_ha = ha_combined[i, 0]
                    high_ha = ha_combined[i, 1]
                    low_ha = ha_combined[i, 2]
                    close_ha = ha_combined[i, 3]

                    if kline_start < K[i] < kline_end:
                        alert_timestamp = pd.to_datetime(dates_combined[i])
                        # Assume the original timestamp is in UTC, localize and convert
                        if alert_timestamp.tzinfo is None:
                            alert_timestamp = UTC.localize(alert_timestamp).astimezone(IST)
                        else:
                            alert_timestamp = alert_timestamp.astimezone(IST)
                        print(f"ALERT TIMESTAMPP: {alert_timestamp}")
                        psar_signal = signals[i]

                        print(f"close {close_ha} psar {psar_data[i]}")
                        print(f"psar_signal {psar_signal} signaldirection {signaldirection}")
                        if psar_signal == signaldirection:
                            candle_color = 'green' if close_ha > open_ha else 'red'

                            print(f"candle_color {candle_color} cand_type {cand_type}")
                            if cand_type == 1: # Basic color check
                                if candle_color == 'green':
                                    await self.process_alert(
                                        exchange_code=exchange_code,
                                        scanID=scanID,
                                        alert_timestamp=alert_timestamp,
                                        LRL_value=LRL_value,
                                        lrcangletype=lrcangletype,
                                        lrcanglestart=lrcanglestart,
                                        lrcangleend=lrcangleend,
                                        angle_degrees=angle_degrees,
                                        crossover_index=crossover_index,
                                        psar_signal=psar_signal,
                                        candle_color=candle_color,
                                        high_ha=high_ha,
                                        digit_name=digit_name,
                                        conditionID=conditionID,
                                        close_ha=close_ha,
                                        scan_name=scan_name
                                    )
                            elif cand_type == 2: # Green candle with no lower wick and an upper wick
                                no_lower_wick = low_ha == open_ha
                                upper_wick = high_ha > close_ha
                                if candle_color == 'green' and no_lower_wick and upper_wick:
                                    await self.process_alert(
                                        exchange_code=exchange_code,
                                        scanID=scanID,
                                        alert_timestamp=alert_timestamp,
                                        LRL_value=LRL_value,
                                        lrcangletype=lrcangletype,
                                        lrcanglestart=lrcanglestart,
                                        lrcangleend=lrcangleend,
                                        angle_degrees=angle_degrees,
                                        crossover_index=crossover_index,
                                        psar_signal=psar_signal,
                                        candle_color=candle_color,
                                        high_ha=high_ha,
                                        digit_name=digit_name,
                                        conditionID=conditionID,
                                        close_ha=close_ha,
                                        scan_name=scan_name
                                    )
                            elif cand_type == 3: # Green candle, wickless (no upper or lower wick)
                                no_upper_wick = high_ha == close_ha
                                no_lower_wick = low_ha == open_ha
                                if candle_color == 'green' and no_lower_wick and no_upper_wick:
                                    await self.process_alert(
                                        exchange_code=exchange_code,
                                        scanID=scanID,
                                        alert_timestamp=alert_timestamp,
                                        LRL_value=LRL_value,
                                        lrcangletype=lrcangletype,
                                        lrcanglestart=lrcanglestart,
                                        lrcangleend=lrcangleend,
                                        angle_degrees=angle_degrees,
                                        crossover_index=crossover_index,
                                        psar_signal=psar_signal,
                                        candle_color=candle_color,
                                        high_ha=high_ha,
                                        digit_name=digit_name,
                                        conditionID=conditionID,
                                        close_ha=close_ha,
                                        scan_name=scan_name
                                    )
        print(f'Completed processing {exchange_code}')
        return 1

    async def process_current_minute(self):
        """Process current minute data for all symbols"""
        print('Processing current minute data')
        
        current_datetime = datetime.now()
        processed_count = 0
        
        # Process each symbol in priority stocks
        for _, row in self.df_priority_stocks.iterrows():
            instrument_token = row['instrument_token']
            symbol = row['symbol']
            basket_id = row['basket_id']
            
            # Get applicable timeframes for this basket
            filtered_timeframes = self.df_basket_timeframes[self.df_basket_timeframes.basket_id == basket_id]
            
            if filtered_timeframes.empty:
                continue
                
            # Check which intervals to process based on current time
            intervals_to_process = []
            
            # Determine which intervals need processing
            min1 = filtered_timeframes['1min'].any()
            min2 = filtered_timeframes['2min'].any()
            min3 = filtered_timeframes['3min'].any()
            min5 = filtered_timeframes['5min'].any()
            min10 = filtered_timeframes['10min'].any()
            min15 = filtered_timeframes['15min'].any()
            min30 = filtered_timeframes['30min'].any()
            min60 = filtered_timeframes['60min'].any()
            
            # Add intervals based on current time
            if min60 and current_datetime.minute == 15:
                intervals_to_process.append('60minute')
            if min30 and (current_datetime.minute == 15 or current_datetime.minute == 45):
                intervals_to_process.append('30minute')
            if min15 and current_datetime.minute % 15 == 0:
                intervals_to_process.append('15minute')
            if min10 and current_datetime.minute % 10 != 0 and current_datetime.minute % 5 == 0:
                intervals_to_process.append('10minute')
            if min5 and current_datetime.minute % 5 == 0:
                intervals_to_process.append('5minute')
            if min3 and current_datetime.minute % 3 == 0:
                intervals_to_process.append('3minute')
            if min2 and current_datetime.minute % 2 == 0:
                intervals_to_process.append('2minute')
            if min1:
                intervals_to_process.append('minute')
            
            # Process each applicable interval
            for interval in intervals_to_process:
                await self.process_symbol(symbol, instrument_token, interval, basket_id)
                processed_count += 1
        
        print(f"Processed {processed_count} symbol-interval combinations")
        return processed_count

    async def initialize_data(self):
        """Initialize all required data from database"""
        print("Initializing configuration data...")
        
        self.df_scan_names = await self.db.get_scan_names()
        print(f"Loaded {len(self.df_scan_names)} scan names")
        
        self.priority_stocks_tpl = await self.db.get_priority_instruments_to_trade()
        basket_ids = set(self.df_scan_names['basket_id'])
        print(f"Loaded {len(self.priority_stocks_tpl)} priority stocks from database")
        # Filter priority stocks to include only those with valid basket IDs
        self.priority_stocks_tpl = [t for t in self.priority_stocks_tpl if t[2] in basket_ids]
        print(f"Filtered to {len(self.priority_stocks_tpl)} priority stocks")
        
        self.df_priority_stocks = pd.DataFrame(
            self.priority_stocks_tpl, 
            columns=['instrument_token', 'symbol', 'basket_id']
        )
        
        self.df_basket_timeframes = await self.db.get_timeframes()
        self.df_scan_items = await self.db.get_scan_items()
        self.df_custom_indicators = await self.db.get_custom_indicators()
        self.df_conditions = await self.db.get_conditions()
        self.df_HLFP = await self.db.get_hlfp()
        
        print("Configuration data initialized successfully")

    async def run_main_loop(self):
        """Main execution loop"""
        last_run_minute = None
        
        while True:
            current_datetime = datetime.now()
            current_time = current_datetime.time()
            current_minute = current_datetime.minute
            
            if (current_time > self.initiate_time and 
                current_time < self.exit_time and 
                current_time.second < 50):
                
                # Ensure the code runs only if the minute has changed
                if last_run_minute is None or current_minute != last_run_minute:
                    last_run_minute = current_minute
                    
                    await self.start_pool()
                    start_time = time.time()
                    
                    try:
                        await self.process_current_minute()
                    except Exception as e:
                        print(f"Error in process_current_minute: {e}")
                    
                    end_time = time.time()
                    total_time = end_time - start_time
                    print(f"Processing time: {total_time:.2f} seconds")
                    
                    await self.close_pool()
                    
                await asyncio.sleep(1)
                
            elif current_time <= self.initiate_time:
                print(f'Waiting for market to open. Current time: {current_time}')
                await asyncio.sleep(60)  # Wait 1 minute before checking again
                
            elif current_time >= self.exit_time:
                print('Market time over. Exiting...')
                break


async def main():
    system = DirectKiteConnect()

    try:
        await system.start_pool()
        await system.initialize_data()
        await system.run_main_loop()
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        await system.close_pool()


if __name__ == "__main__":
    asyncio.run(main())


