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
        self.sdate = self.zerodha.getCurrentDate()
        #self.sdate_iso = self.sdate.isoformat()[:10] + 'T09:15:00.000Z'
        self.log = True
        self.trade =True
        self.run_job = True
        self.initiate_time = tm(9,15,1)
        self.exit_time = tm(22, 30)
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
    async def start_pool(self):
        print('start pool')
        loop = asyncio.get_event_loop()
        await self.db.create_pool(loop=loop)
    
    async def close_pool(self):
        await self.db.close_pool()

    async def get_data_zerodha(self, interval, sdate, edate, symbol):
        df_instrument = await self.db.get_instrument_token(symbol)
        if len(df_instrument) == 0:
            info = f"instrument token not found {symbol}"
            print(info)
            return pd.DataFrame()
        token = int(df_instrument.instrument_token.iloc[0])
        global log
        for retry in range(10):
            data = self.zerodha.gethistoricaldata(token, sdate, edate, interval)
            if len(data) > 0:
                if self.log == True:
                    print(f"Data Received in {retry} try")
                break  
            else:
                print(f'unable to get data {retry} of 10')
                asyncio.sleep(1) 
        if len(data) < 2:
            if self.log == True:
                print("Unable to fetch data after multiple retries.")
            return data
        else:
            if self.log == True:
                print('return new data')
            return data
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
        print('-------------------- process_indicators_5 min -----------------')
        await self.process_one_min_heikin(df)
        return 1
    
    async def process_indicators_five_min(self, df):
        print('-------------------- process_indicators_5 min -----------------')
        await self.process_fivemin_heikin(df)
        return 1
    async def process_indicators_fifteen_min(self, df):
        print('-------------------- process_indicators_15 min -----------------')
        return 1
    async def process_indicators_thirty_min(self, df):
        print('-------------------- process_indicators 30 minutes -----------------')
        return 1
    async def process_indicators_hour(self, df):
        print('-------------------- process_indicators 1 hour -----------------')
        return 1

    async def download_one_min(self, df_stocks, current_datetime):
        print('download_one_min')
        table_name = 'one_min_ohlc'
        missed_df = pd.DataFrame(columns=['symbol', 'exchange_code'])
        enddate = current_datetime
        #enddate_iso = current_datetime.isoformat()[:10] + 'T15:30:00.000Z'
        for index, row in df_stocks.iterrows():
            exchange_code = row['symbol']
            print(exchange_code)
            df_last_date = await self.db.get_ohlc_last_datetime(exchange_code, table_name)
            if len(df_last_date) == 0:
                days_prior = self.yesterday - timedelta(days=8)
                startdate = days_prior
            else:
                last_date = df_last_date.datetime.iloc[0]
                if last_date.date() < enddate.date():
                    startdate = last_date + timedelta(days=1)
                    startdate = startdate.replace(hour=9, minute=15)
                else:
                    startdate = last_date# + timedelta(minutes=5)
                print(f"{last_date=} {startdate=}")
            df = ""

            df = await self.get_data_zerodha('minute', startdate, enddate, exchange_code)
            if len(df) > 0:
                current_datetime = datetime.now()
                len_df = len(df)
                last_date_recd = ''
                if len_df > 0:
                    last_date_recd = df.date.iloc[-1]
                else:
                    blank_df = pd.DataFrame({'symbol': exchange_code, 'exchange_code': exchange_code}, index=[0])
                    missed_df = pd.concat([missed_df, blank_df], ignore_index=True)

                if len_df > 0:
                    df['date'] = pd.to_datetime(df['date'])
                  
                    for index, row in df.iterrows():
                        date_val = row['date']
                        open_val = row['open']
                        high_val = row['high']
                        low_val = row['low']
                        close_val = row['close']
                        volume_val = row['volume']
                        print('insert_one_min_ohlc ', exchange_code, date_val)
                        await self.db.insert_one_min_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)

        return missed_df

    async def download_five_min(self, df_stocks, current_datetime):
        print('download_five_min')
        missed_df = pd.DataFrame(columns=['symbol', 'exchange_code'])
        enddate = current_datetime
        #enddate_iso = current_datetime.isoformat()[:10] + 'T15:30:00.000Z'
        for index, row in df_stocks.iterrows():
            exchange_code = row['symbol']
            print(exchange_code)
            df_last_date = await self.db.get_fivemin_ohlc_last_datetime(exchange_code)
            if len(df_last_date) == 0:
                days_prior = self.yesterday - timedelta(days=8)
                startdate = days_prior
            else:
                last_date = df_last_date.datetime.iloc[0]
                if last_date.date() < enddate.date():
                    startdate = last_date + timedelta(days=1)
                    startdate = startdate.replace(hour=9, minute=15)
                else:
                    startdate = last_date# + timedelta(minutes=5)
                    #startdate = startdate.to_pydatetime().date()
                print(f"{last_date=} {startdate=}")
            df = ""

            df = await self.get_data_zerodha('5minute', startdate, enddate, exchange_code)
            if len(df) > 0:
                current_datetime = datetime.now()
                len_df = len(df)
                last_date_recd = ''
                if len_df > 0:
                    last_date_recd = df.date.iloc[-1]
                else:
                    blank_df = pd.DataFrame({'symbol': exchange_code, 'exchange_code': exchange_code}, index=[0])
                    missed_df = pd.concat([missed_df, blank_df], ignore_index=True)

                if len_df > 0:
                    df['date'] = pd.to_datetime(df['date'])
                  
                    for index, row in df.iterrows():
                        date_val = row['date']
                        open_val = row['open']
                        high_val = row['high']
                        low_val = row['low']
                        close_val = row['close']
                        volume_val = row['volume']
                        await self.db.insert_five_min_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)

        return missed_df
    async def download_fifteen_min(self, df_stocks, current_datetime):
        print('--------------- download 15 minutes ---------------------')

        enddate = current_datetime
        for index, row in df_stocks.iterrows():
            exchange_code = row['symbol']
            print(exchange_code)
            if current_datetime.minute % 15 == 0:
                df_last_date = await self.db.get_last_datetime_fifteen_min(exchange_code)
                if len(df_last_date) == 0:
                    days_prior = self.yesterday - timedelta(days=8)
                    startdate = days_prior
                else:
                    last_date = df_last_date.datetime.iloc[0]
                    if last_date.date() < enddate.date():
                        startdate = last_date + timedelta(days=1)
                        startdate = startdate.replace(hour=9, minute=15)
                    else:
                        startdate = last_date #+ timedelta(minutes=15)
                        
                    print(f"{last_date=} {startdate=}")

                df = await self.get_data_zerodha('15minute', startdate, enddate, exchange_code)
                if len(df) > 0:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)

                    for index, row in df.iterrows():
                        date_val = index
                        open_val = row['open']
                        high_val = row['high']
                        low_val = row['low']
                        close_val = row['close']
                        volume_val = row['volume']
                        await self.db.insert_fifteen_min_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)
    async def download_thirty_min(self, df_stocks, current_datetime):
        print('--------------- download 30 minutes ---------------------')
        
        for index, row in df_stocks.iterrows():
            exchange_code = row['symbol']
            print(exchange_code)
            if current_datetime.minute % 30 == 0:
                df_last_date = await self.db.get_last_datetime_thirty_min(exchange_code)
                if len(df_last_date) == 0:
                    days_prior = self.yesterday - timedelta(days=30)
                    startdate = days_prior
                    sdate_iso = days_prior.isoformat()[:10] + 'T09:15:00.000Z'
                else:
                    last_date = df_last_date.datetime.iloc[0]
                    if last_date.date() < current_datetime.date():
                        startdate = last_date + timedelta(days=1)
                        startdate = startdate.replace(hour=9, minute=15)
                    else:
                        startdate = last_date #+ timedelta(minutes=5)
                    print(f"{last_date=} {startdate=}")
                    sdate_iso = startdate.isoformat()
                df = ""
                try:
                    print('gethistorical_daily', exchange_code)
                    df = await self.get_data_zerodha('30minute', startdate, current_datetime, exchange_code)
                    if len(df) > 0:
                        df = df[(df['date'].dt.time >= pd.to_datetime('09:15:00').time()) & 
                        (df['date'].dt.time <= pd.to_datetime('15:30:00').time())]
                        print(f"First new date: {df['date'].iloc[0]}")
                        print(df)
                        for index, row in df.iterrows():
                            date_val = row['date']
                            open_val = row['open']
                            high_val = row['high']
                            low_val = row['low']
                            close_val = row['close']
                            volume_val = row['volume']
                            await self.db.insert_thirty_min_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)
                except Exception as e:
                    info = f"Error in downloading, {exchange_code}, {e}"
                    print(info)
                    await self.db.pre_process_logs(self.today_str, 'download_5 min_ohlc', 'main live data', info, 4)
                    continue
    async def download_hour(self, df_stocks, current_datetime):        
        for index, row in df_stocks.iterrows():
            exchange_code = row['symbol']
            print(exchange_code)
            if current_datetime.minute == 15:
                df_last_date = await self.db.get_last_datetime_one_hour_ohlc(exchange_code)
                if len(df_last_date) == 0:
                    days_prior = self.yesterday - timedelta(days=8)
                    startdate = days_prior
                else:
                    last_date = df_last_date.datetime.iloc[0]
                    if last_date.date() < current_datetime.date():
                        startdate = last_date + timedelta(days=1)
                        startdate = startdate.replace(hour=9, minute=15)
                    else:
                        startdate = last_date #+ timedelta(minutes=15)
                    print(f"{last_date=} {startdate=}")

                df = await self.get_data_zerodha('60minute', startdate, current_datetime, exchange_code)
                if len(df) > 0:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)

                    for index, row in df.iterrows():
                        date_val = index
                        open_val = row['open']
                        high_val = row['high']
                        low_val = row['low']
                        close_val = row['close']
                        volume_val = row['volume']
                        await self.db.insert_one_hour_ohlc(exchange_code, date_val, open_val, high_val, low_val, close_val, volume_val)
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
    async def download_current_data(self):
        current_datetime = datetime.now()
        print(f"{current_datetime=}")
        
        df_all_stocks = await self.db.get_monitor_symbols_to_trade()
        if len(df_all_stocks) == 0:
            print('No symbols to trade')
        count = 0
        missed_df = await self.download_one_min(df_all_stocks, current_datetime)
        await self.process_indicators_one_min(df_all_stocks)
        if current_datetime.minute % 5 == 0:
            missed_df = await self.download_five_min(df_all_stocks, current_datetime)
            if len(missed_df) > 0:
                print('reprocessing missed df')
                await self.download_five_min(missed_df, current_datetime)
        
        await self.process_indicators_five_min(df_all_stocks)
        if current_datetime.minute % 15 == 0:
            await self.download_fifteen_min(df_all_stocks, current_datetime)
            await self.process_indicators_fifteen_min(df_all_stocks)
        if current_datetime.minute % 30 == 0:
            await self.download_thirty_min(df_all_stocks, current_datetime)
            await self.process_indicators_thirty_min(df_all_stocks)
        if current_datetime.minute == 15:
            await self.download_hour(df_all_stocks, current_datetime)
            await self.process_indicators_hour(df_all_stocks)
    async def update_symbols_to_monitor(self):
        df_basket_stocks = await self.db.get_active_basket_symbols()
        symbols_list = df_basket_stocks['tradingsymbol'].unique()
        prefixed_symbols_list = ['NSE:' + symbol for symbol in symbols_list]
        df_ltp = self.zerodha.getLTPMulti(prefixed_symbols_list)
        def get_last_price(symbol):
            if symbol in df_ltp:
                return df_ltp[symbol]['last_price']
            else:
                return None  
        await self.db.run_query('truncate table monitor_symbols;')
        current_date_string = datetime.now().strftime("%Y-%m-%d")
        await self.db.pre_process_logs(current_date_string, 'update_symbols_to_monitor', 'symbols deleted', 'truncate table monitor_symbols', 1)
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
                    await self.db.insert_into_monitor_symbols(instrument_token, tradingsymbol, expiry, strike, instrument_type, ltp, symbol)
    async def final_download(self):
        current_datetime = datetime.now()
        df_all_stocks = await self.db.get_basket_symbols_to_trade()
        await self.download_fifteen_min(df_all_stocks, current_datetime)
        await self.process_indicators_fifteen_min(df_all_stocks)
        await self.download_thirty_min(df_all_stocks, current_datetime)
        await self.process_indicators_thirty_min(df_all_stocks)
        await self.download_hour(df_all_stocks, current_datetime)
        await self.process_indicators_hour(df_all_stocks)
async def main():
    start = Start()
    current_datetime = datetime.now()
    # await start.start_pool()
    # await start.download_current_data()
    # await asyncio.sleep(1)
    # await start.close_pool()
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
            await start.start_pool()
            #await start.final_download()
            await asyncio.sleep(1)
            await start.close_pool()
            print('Exitting Market time Over')
            break
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.run(main())