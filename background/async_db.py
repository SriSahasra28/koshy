import asyncio
import aiomysql
import pandas as pd
import time
from background.set import settings

class dbconnection:
    def __init__(self):
        setting = settings()
        self.set = setting.get_db()
    async def create_pool(self, loop):
        self.pool = await aiomysql.create_pool(maxsize=20, host=self.set[2], port=self.set[3], user=self.set[0], password=self.set[1], db=self.set[4], loop=loop)

    async def get_data(self, query):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_instrument_token(self, index_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT instrument_token FROM instruments where tradingsymbol = '{index_name}' Limit 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df    
    async def insert_into_monitor_symbols(self, instrument_token, symbol, expiry, strike, option_type, ltp, stock_symbol):
        print('in insert_into_monitor_symbols')
        print(f"{instrument_token=}, {symbol=}, {expiry=}, {strike=}, {option_type=}, {ltp=}, {stock_symbol=}")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "CALL InsertIntoMonitorSymbols(%s, %s, %s, %s, %s, %s, %s)",
                    (instrument_token, symbol, expiry, strike, option_type, ltp, stock_symbol)
                )
                await conn.commit()

    async def get_five_min_ohlc(self, symbol, start_date):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM five_min_ohlc where symbol = '{symbol}' and `datetime` > '{start_date}' order by `datetime`;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_thirty_min_ohlc(self, symbol, start_date):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM thirty_min_ohlc where symbol = '{symbol}' and `datetime` > '{start_date}' order by `datetime`;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df        

    async def truncate_pre_process_logs(self):      
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("truncate table pre_process_logs;")
                    await conn.commit()
                    return 1
        except Exception as e:
            raise e
    async def get_last_ohlc_date(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT `datetime` FROM daily_ohlc order by id desc LIMIT 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    async def get_last_fivemin_ohlc_date(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT `datetime` FROM five_min_ohlc order by id desc LIMIT 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    async def get_last_thirty_min_ohlc_date(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT `datetime` FROM thirty_min_ohlc order by id desc LIMIT 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_last_fifteen_min_ohlc_date(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT `datetime` FROM fifteen_min_ohlc order by id desc LIMIT 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    
    async def get_last_ohlc_date_symbol(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT `datetime` FROM daily_ohlc where symbol = '{symbol}' order by id desc LImit 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    async def get_last_datetime_five_min(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT `datetime` FROM five_min_ohlc where symbol = '{symbol}' order by id desc Limit 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df   
     
    async def get_last_datetime_fifteen_min(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT `datetime` FROM fifteen_min_ohlc where symbol = '{symbol}' order by id desc Limit 1;"
                print(query)
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        print(df)
        return df   
    
    async def get_last_datetime_thirty_min(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT `datetime` FROM thirty_min_ohlc where symbol = '{symbol}' order by id desc Limit 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df   
    
    async def get_last_datetime_one_hour_ohlc(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT `datetime` FROM one_hour_ohlc where symbol = '{symbol}' order by id desc Limit 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df  

    async def get_pre_market_steps(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT * FROM pre_market_steps where Date(last_execution) < curdate() and enabled = 1 order by priority;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    
    async def get_last_five_dates(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT distinct `date` FROM nifty_ohlc order by `date` desc Limit 5;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df  
    
    async def get_pre_market_steps_ignore_date(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT * FROM pre_market_steps order by priority;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
        
    async def insert_daily_ohlc(self, symbol, date, open, high, low, close, volume):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT IGNORE INTO daily_ohlc(symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (symbol, date, open, high, low, close, volume)
                    )
                    await conn.commit()
        except Exception as e:
            raise e
        
    async def insert_five_min_ohlc(self, symbol, datetime, open, high, low, close, volume):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT IGNORE INTO five_min_ohlc(symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (symbol, datetime, open, high, low, close, volume)
                    )
                    await conn.commit()
        except Exception as e:
            raise e
    async def insert_ohlc_data(self, table_name, symbol, datetime, open, high, low, close, volume):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"INSERT IGNORE INTO {table_name} (symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (symbol, datetime, open, high, low, close, volume)
                    )
                    await conn.commit()
        except Exception as e:
            raise e
    async def insert_one_min_ohlc(self, symbol, datetime, open, high, low, close, volume):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT IGNORE INTO one_min_ohlc(symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (symbol, datetime, open, high, low, close, volume)
                    )
                    await conn.commit()
        except Exception as e:
            raise e
    async def insert_thirty_min_ohlc(self, symbol, datetime, open, high, low, close, volume):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT IGNORE INTO thirty_min_ohlc(symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (symbol, datetime, open, high, low, close, volume)
                    )
                    await conn.commit()
        except Exception as e:
            raise e
    async def insert_fifteen_min_ohlc(self, symbol, datetime, open, high, low, close, volume):
        print(f"{symbol}, {datetime}, {open=}, {high=}, {low=}, {close=}, {volume=}")
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT IGNORE INTO fifteen_min_ohlc(symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (symbol, datetime, open, high, low, close, volume)
                    )
                    await conn.commit()
        except Exception as e:
            raise e

    async def insert_one_hour_ohlc(self, symbol, datetime, open, high, low, close, volume):
        print(f"{symbol}, {datetime}, {open=}, {high=}, {low=}, {close=}, {volume=}")
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT IGNORE INTO one_hour_ohlc(symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (symbol, datetime, open, high, low, close, volume)
                    )
                    await conn.commit()
        except Exception as e:
            raise e
        
    async def update_heikin_ashi(self, ha_open, ha_high, ha_low, ha_close, symbol, datetime_val, table):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"UPDATE {table} SET ha_open ='{ha_open}', ha_high ='{ha_high}', ha_low ='{ha_low}', ha_close ='{ha_close}' WHERE symbol = '{symbol}' AND datetime = '{datetime_val}';"
                    #print(query)
                    await cur.execute(query)
                    await conn.commit()
        except Exception as e:
            raise e   
                
    async def update_pre_market_steps(self, id, last_status, last_record_date):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"update pre_market_steps set last_execution = current_timestamp(), last_status = {last_status}, last_record_date  = '{last_record_date}' where id = {id};"
                    await cur.execute(query)
                    await conn.commit()
        except Exception as e:
            raise e

    async def get_basket_stocks_all(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT s.symbol, i.symbol as icici_code FROM basket_stocks s inner join all_stocks i on s.symbol = i.exchange_code;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df 
    
    async def get_unprocessed_dates(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT distinct `datetime` FROM daily_ohlc where processed = 0 order by `datetime` desc Limit 5;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_daily_ohlc_by_symbol(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT symbol, datetime, open, high, low, close, volume FROM daily_ohlc where symbol = '{symbol}' order by `datetime`;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
            
    async def get_null_ohlc(self, symbol, tablename):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime, open, high, low, close FROM {tablename} where symbol = '{symbol}' and ha_open is NULL order by `datetime`;"
                print(query)
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df        

    async def get_prior_rows(self, symbol, threshold, tablename):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime, open, high, low, close FROM {tablename} where symbol = '{symbol}' and datetime < '{threshold}' order by `datetime` desc Limit 5;"
                print(query)
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df  
    async def get_fivemin_ohlc_last_datetime(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime FROM five_min_ohlc where symbol = '{symbol}' order by datetime desc Limit 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_ohlc_last_datetime(self, symbol, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime FROM {table_name} where symbol = '{symbol}' order by datetime desc Limit 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_fifteen_min_ohlc_last_datetime(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime FROM fifteen_min_ohlc where symbol = '{symbol}' order by datetime desc Limit 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df  
    async def get_thirtymin_ohlc_by_symbol(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT symbol, datetime, open, high, low, close, volume FROM thirty_min_ohlc where symbol = '{symbol}' order by `datetime` Limit 205;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df   
    async def get_fifteenmin_ohlc_by_symbol(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT symbol, datetime, open, high, low, close, volume FROM fifteen_min_ohlc where symbol = '{symbol}' order by `datetime` Limit 205;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df   
    async def get_onehour_ohlc_by_symbol(self, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT symbol, datetime, open, high, low, close, volume FROM one_hour_ohlc where symbol = '{symbol}' order by `datetime` Limit 205;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df  
    async def get_basket_symbols_to_trade_all(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT symbol FROM basket_stocks;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df  

    async def get_monitor_symbols_to_trade(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT symbol FROM monitor_symbols;;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df  

    async def get_active_basket_symbols(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT distinct b.option_type, i.instrument_token, i.tradingsymbol FROM instruments i inner join basket_stocks b on i.tradingsymbol = b.symbol where exchange = 'NSE';"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
      
    async def pre_process_logs(self, date_log, module, activity, important_data, priority):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT INTO pre_process_logs(date_log, module, activity, important_data, priority) VALUES (%s, %s, %s, %s, %s)",
                    (date_log, module, activity, important_data, priority)
                    )
                    await conn.commit()
        except Exception as e:
            raise e

    async def insert_trade_log(self, date_log, module, activity, important_data, priority, strategy_trade_id, timestamp):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT INTO trade_logs(date_log, module, activity, important_data, priority, strategy_trade_id, `timestamp`) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (date_log, module, activity, important_data, priority, strategy_trade_id, timestamp)
                    )
                    await conn.commit()
        except Exception as e:
            raise e
    async def run_query(self, query):
        print(query)
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    await conn.commit()
        except Exception as e:
            raise e

    async def close_pool(self):
            self.pool.close()
            await self.pool.wait_closed()
            #print('Pool closed')

    async def test_NBCC(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime, open, high, low, close FROM fifteen_min_ohlc where symbol = 'NBCC' and histogram is NULL;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df