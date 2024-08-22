# Download Live market Data
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
print(os.getcwd())
from background.zerodha import zeroda
from datetime import datetime, date, timedelta, time as tm
from background.instruments import instruments
import pandas as pd
import numpy as np
import warnings
import os
import asyncio
from background.async_db import dbconnection
warnings.filterwarnings('ignore')
from numba import jit
import time
log = False

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
        self.loglevel = 0 # Low 0, Medium 1, High 2
        self.trade =True
        self.run_job = True
        self.initiate_time = tm(9,15,1)
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
        #print('start pool')
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

    async def process_alert(self, exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name):
        if lrcangletype == 'custom' and lrcanglestart < angle_degrees < lrcangleend:
            print("-  -" * 20)
            print(exchange_code, digit_name, ' min')
            info = f"Alert {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
            print(info)
            return 1
        elif lrcangletype != 'custom':
            print("-  -" * 20)
            print(exchange_code, digit_name, ' min')
            info = f'Alert {exchange_code} {alert_timestamp} K crossover {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}'
            print(info)
            return 1
        else:
            if log:
                info = f"NOT {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal=} color: {candle_color=} high_ha: {high_ha} < LRL:{LRL_value}"
                print(info)
            return 0

    async def test_alert(self, exchange_code, interval, start_datetime):
        table_name =  self.interval_to_table.get(interval, None)
        self.df_scan_items = await self.db.get_scan_items()
        self.df_custom_indicators = await self.db.get_custom_indicators()
        self.df_conditions = await self.db.get_conditions()
        self.df_HLFP = await self.db.get_hlfp()

        data_df = await self.db.get_old_data_by_symbol(table_name, exchange_code)
        data_df['datetime'] = pd.to_datetime(data_df['datetime'])
        data_df.reset_index(inplace=True, drop=True)
        #data_df = data_df[data_df.datetime >= start_datetime]
        total_rows = len(data_df)
        first_index = data_df[data_df['datetime'] >= start_datetime].index[0]
        # print("-  -" * 20)
        # print(exchange_code, interval)
        if log:
            print(f"data_df:{total_rows=} {first_index=}")

        if total_rows == 0:
            print('Candle Data not Available skip')
            return
        
        data_all = data_df[['open', 'high', 'low', 'close']].values.astype(float)
        dates_combined = data_df['datetime'].tolist()
        
        for j in range(first_index + 1, len(data_all) + 1): 
            
            data_partial = data_all[:j]  # Filter data from the first row up to the current index
            dates_partial = dates_combined[:j]
            if log:
                print(f"----------------------------  {dates_partial[-1]} ----------------------")
            ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(data_partial[:,0], data_partial[:,1], data_partial[:,2], data_partial[:,3])
            ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))

            digit_name =  self.interval_to_digit.get(interval, None)
            column_name = str(digit_name) + 'min'
            df_items = self.df_scan_items[self.df_scan_items[column_name] == 1]
            alert_check = True
            if df_items.empty:
                print('df_items empty skip')
                alert_check = False
                
            conditions = df_items.conditionID.unique()
            if len(conditions) == 0:
                print('No condition')
                alert_check = False
            if alert_check:
                for conditionID in conditions:
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
                    
                    low = data_partial[:,2]
                    high = data_partial[:,1]
                    close = data_partial[:,3]
                   
                    psar_data = psar(high, low, close, af0=float(PSAR_acceleration), af=float(PSAR_acceleration), max_af=float(PSAR_max_acceleration))
                    signals = get_psar_signals(close, psar_data)

                    K, D = calc_fastStochastics(low, high, close, stoch_period, k_avg, d_avg)
                    crossover_index = await self.get_crossover_index(K, LineThreshold, psarCandles)
                    if crossover_index == -1 or crossover_index == psarCandles:
                        if log:
                            print(f'{crossover_index=} {psarCandles=} {exchange_code}{interval}')
                    else:
                        candles_to_check = 1
                        if crossover_index < candles_to_check:
                            candles_to_check = crossover_index
                        for i in range(-candles_to_check, 0):
                            psar_signal = signals[i] # psar_signal = signals[-1]
                            if log:
                                info = f"{crossover_index=} {psar_signal=} {signaldirection=}"
                                print(info)
                            if psar_signal == signaldirection: # signaldirection = 1 PSAR Signal is Long
                                if log:
                                    info = f"psar_signal: {psar_signal} == signaldirection: {signaldirection}"
                                    print(info)
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
                                if log:
                                    info = f"{open_ha=} {high_ha=} {low_ha=} {close_ha=} {LRL_value=} {candle_color=} {hlfpid=}"
                                    print(info)    
                                alert_timestamp = dates_partial[i]
                                if hlfpid == 1:
                                    if candle_color == 'g' and high_ha < LRL_value:
                                        result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name)
                                    else:
                                        if log:
                                            info = f"NOT color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value}"
                                            print(info)
                                elif hlfpid == 2:
                                    no_lower_wick = low_ha == open_ha
                                    upper_wick = high_ha > close_ha
                                    if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and upper_wick:
                                        result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name)
                                    else:
                                        info = f"Not color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {upper_wick=}" 
                                elif hlfpid == 3:
                                    no_upper_wick = high_ha == close_ha
                                    no_lower_wick = low_ha == open_ha
                                    if candle_color == 'g' and high_ha < LRL_value and no_lower_wick and no_upper_wick:
                                        result = await self.process_alert(exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name)
                                    else:
                                        if log:
                                            info = f"Not Wickless color: {candle_color} == 'g' and high_ha: {high_ha} < LRL_value: {LRL_value} and {no_lower_wick=} and {no_upper_wick=}"
                                            print(info)    
                            else:
                                if log:
                                    info = f"NOT psarsignal: {psar_signal} == signaldirection: {signaldirection}"
                                    print(info)

async def main():
    start = Start()
    await start.start_pool()
    
    start_datetime = pd.Timestamp('2024-08-22 09:15:00')
    interval = 'minute'
    # start.priority_stocks_tpl = await start.db.get_priority_instruments_to_trade()
    # start.df_priority_stocks = pd.DataFrame(start.priority_stocks_tpl, columns=['instrument_token', 'symbol'])
    # all_symbols = start.df_priority_stocks['symbol'].to_list()
    symbol = 'RELIANCE24AUG2940CE'
    await start.test_alert(symbol, interval, start_datetime)
    # for symbol in all_symbols:
    #     await start.test_alert(symbol, interval, start_datetime)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.run(main())