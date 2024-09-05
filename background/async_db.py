import asyncio
import aiomysql
import pandas as pd
import time
from background.set import settings
import numpy as np
from datetime import datetime

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
    async def insert_into_download_symbols(self, instrument_token, symbol, expiry, strike, option_type, ltp, stock_symbol):
        print('in InsertIntoDownload_symbols')
        print(f"{instrument_token=}, {symbol=}, {expiry=}, {strike=}, {option_type=}, {ltp=}, {stock_symbol=}")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "CALL InsertIntoDownload_symbols(%s, %s, %s, %s, %s, %s, %s)",
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
    async def get_last_min_ohlc_date(self, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT `datetime` FROM {table_name} order by id desc LIMIT 1;"
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
    async def insert_ohlc_data(self, table_name, symbol, datetime, open, high, low, close, volume, ha_open, ha_high, ha_low, ha_close):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"INSERT IGNORE INTO {table_name} (symbol, datetime, open, high, low, close, volume, ha_open, ha_high, ha_low, ha_close) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (symbol, datetime, open, high, low, close, volume, ha_open, ha_high, ha_low, ha_close)
                    )
                    await conn.commit()
        except Exception as e:
            raise e
    async def insert_batch_data(self, table_name, batch_data):
        print('batch_data', batch_data)
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(
                        f"INSERT IGNORE INTO {table_name} (symbol, datetime, open, high, low, close, ha_open, ha_high, ha_low, ha_close) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        batch_data
                    )
                    await conn.commit()
        except Exception as e:
            raise e

    # async def Insert_one_min_ohlc_proc_batch(self, batch_data):
    #     current_time = datetime.now().strftime("%H:%M:%S")
    #     print('in Insert_one_min_ohlc_proc_batch', current_time)
    #     try:
    #         async with self.pool.acquire() as conn:
    #             async with conn.cursor() as cur:
    #                 await cur.executemany(
    #                     "CALL Insert_one_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    #                     batch_data
    #                 )
    #                 await conn.commit()
    #     except Exception as e:
    #         raise e

    async def Insert_one_min_ohlc_proc_batch(self, batch_data, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(
                             "CALL Insert_one_min_ohlc(%s, %s, %s, %s, %s, %s)",
                            batch_data
                        )
                        await conn.commit()
                return
            except aiomysql.OperationalError as e:
                if e.args[0] == 1213:  # Error code for deadlock
                    retry_count += 1
                    await asyncio.sleep(0.1 * retry_count)  # Exponential backoff
                else:
                    print(f"Operational error occurred: {e}")
                    return

        print(f"Failed to execute batch after {max_retries} retries due to deadlocks.")

    async def Insert_three_min_ohlc_proc_batch(self, batch_data, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            "CALL Insert_three_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch_data
                        )
                        await conn.commit()
                return  # Exit the function if successful
            except aiomysql.OperationalError as e:
                if e.args[0] == 1213:  # Error code for deadlock
                    retry_count += 1
                    await asyncio.sleep(0.1 * retry_count)  # Exponential backoff
                else:
                    print(f"Operational error occurred: {e}")
                    return

        print(f"Failed to execute batch after {max_retries} retries due to deadlocks.")


    async def Insert_two_min_ohlc_proc_batch(self, batch_data, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            "CALL Insert_two_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch_data
                        )
                        await conn.commit()
                return
            except aiomysql.OperationalError as e:
                if e.args[0] == 1213:  # Error code for deadlock
                    retry_count += 1
                    await asyncio.sleep(0.1 * retry_count)  # Exponential backoff
                else:
                    print(f"Operational error occurred: {e}")
                    return

        print(f"Failed to execute batch after {max_retries} retries due to deadlocks.")

    async def Insert_five_min_ohlc_proc_batch(self, batch_data, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            "CALL Insert_five_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch_data
                        )
                        await conn.commit()
                return
            except aiomysql.OperationalError as e:
                if e.args[0] == 1213:  # Error code for deadlock
                    retry_count += 1
                    await asyncio.sleep(0.1 * retry_count)  # Exponential backoff
                else:
                    print(f"Operational error occurred: {e}")
                    return
        print(f"Failed to execute batch after {max_retries} retries due to deadlocks.")

    async def Insert_ten_min_ohlc_proc_batch(self, batch_data, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            "CALL Insert_ten_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch_data
                        )
                        await conn.commit()
                return
            except aiomysql.OperationalError as e:
                if e.args[0] == 1213:  # Error code for deadlock
                    retry_count += 1
                    await asyncio.sleep(0.1 * retry_count)  # Exponential backoff
                else:
                    print(f"Operational error occurred: {e}")
                    return
        print(f"Failed to execute batch after {max_retries} retries due to deadlocks.")

    async def Insert_fifteen_min_ohlc_proc_batch(self, batch_data, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            "CALL Insert_fifteen_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch_data
                        )
                        await conn.commit()
                return
            except aiomysql.OperationalError as e:
                if e.args[0] == 1213:  # Error code for deadlock
                    retry_count += 1
                    await asyncio.sleep(0.1 * retry_count)  # Exponential backoff
                else:
                    print(f"Operational error occurred: {e}")
                    return
        print(f"Failed to execute batch after {max_retries} retries due to deadlocks.")
    
    async def Insert_thirty_min_ohlc_proc_batch(self, batch_data, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            "CALL Insert_thirty_min_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch_data
                        )
                        await conn.commit()
                return
            except aiomysql.OperationalError as e:
                if e.args[0] == 1213:  # Error code for deadlock
                    retry_count += 1
                    await asyncio.sleep(0.1 * retry_count)  # Exponential backoff
                else:
                    print(f"Operational error occurred: {e}")
                    return

        print(f"Failed to execute batch after {max_retries} retries due to deadlocks.")

    async def Insert_hour_ohlc_proc_batch(self, batch_data, max_retries=5):
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(
                            "CALL Insert_hour_ohlc(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch_data
                        )
                        await conn.commit()
                return
            except aiomysql.OperationalError as e:
                if e.args[0] == 1213:  # Error code for deadlock
                    retry_count += 1
                    await asyncio.sleep(0.1 * retry_count)  # Exponential backoff
                else:
                    print(f"Operational error occurred: {e}")
                    return
        print(f"Failed to execute batch after {max_retries} retries due to deadlocks.")

    async def get_old_data(self, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"Select symbol, datetime, open, high, low, close FROM {table_name} ORDER BY id DESC")
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        if len(df) > 0:
            df = df[::-1] # reverse as its desc order
            df = df.sort_values(by='datetime')
        return df

    async def get_old_data_all(self, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"Select symbol, datetime, open, high, low, close FROM {table_name} ORDER BY datetime;")
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    
    async def get_old_data_limit(self, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"Select symbol, datetime, open, high, low, close FROM {table_name} ORDER BY datetime DESC LIMIT 1000000;")
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        if len(df) > 0:
            df = df[::-1] # reverse as its desc order
            df = df.sort_values(by='datetime')
        return df

    async def get_old_data_by_symbol(self, table_name, symbol):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"Select datetime, open, high, low, close FROM {table_name} where symbol = '{symbol}' ORDER BY datetime DESC LIMIT 1000;")
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        if len(df) > 0:
            df = df[::-1] # reverse as its desc order
            df = df.sort_values(by='datetime')
        return df

    async def get_old_data_priority(self, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"Select symbol, datetime, open, high, low, close from {table_name} where symbol in (SELECT m.symbol FROM monitor_symbols m left join instruments i on m.instrument_token = i.instrument_token where m.active = 1 and i.expiry >= curdate() and m.stock_symbol in (SELECT distinct symbol FROM basket_stocks where active = 1)) ORDER BY datetime;")
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_lastdate_symbols_all(self, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT symbol, max(datetime) as datetime FROM {table_name} group by symbol;")
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

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


    # insert_batch_ohlc
    async def insert_one_min_batch_ohlc(self, batch_data):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    INSERT IGNORE INTO one_min_ohlc
                    (symbol, datetime, open, high, low, close, volume) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    await cur.executemany(query, batch_data)
                    await conn.commit()
        except Exception as e:
            raise e


    async def insert_three_min_ohlc(self, symbol, datetime, open, high, low, close, volume):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT IGNORE INTO three_min_ohlc(symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (symbol, datetime, open, high, low, close, volume)
                    )
                    await conn.commit()
        except Exception as e:
            raise e

    # insert_batch_three_min_ohlc
    async def insert_batch_three_min_ohlc(self, batch_data):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    INSERT IGNORE INTO three_min_ohlc
                    (symbol, datetime, open, high, low, close, volume) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    await cur.executemany(query, batch_data)
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
    async def insert_ten_min_ohlc(self, symbol, datetime, open, high, low, close, volume):
        print(f"{symbol}, {datetime}, {open=}, {high=}, {low=}, {close=}, {volume=}")
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT IGNORE INTO ten_min_ohlc(symbol, datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)",
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

    async def update_PSAR(self, PSAR, PSAR_L, PSAR_S, symbol, datetime_val, table):
        # print(PSAR_L, type(PSAR_L))
        # print(PSAR_S, type(PSAR_S))
        try:
            PSAR_L_value = 'NULL' if PSAR_L == None else f"'{PSAR_L}'"
            PSAR_S_value = 'NULL' if PSAR_S == None else f"'{PSAR_S}'"
            
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"""
                    UPDATE {table} 
                    SET PSAR = '{PSAR}', 
                        PSAR_L = {PSAR_L_value}, 
                        PSAR_S = {PSAR_S_value} 
                    WHERE symbol = '{symbol}' 
                    AND datetime = '{datetime_val}';
                    """
                    print(query)
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
    
    async def get_one_min_datetime(self, symbol, sdate, edate):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT symbol, datetime, open, high, low, close FROM one_min_ohlc where symbol = '{symbol}' and datetime between '{sdate}' and '{edate}' order by `datetime`;"
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

    async def get_psar_null_ohlc(self, symbol, tablename):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime, open, high, low, close FROM {tablename} where symbol = '{symbol}' and PSAR is NULL order by `datetime`;"
                #print(query)
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_last_n_close(self, symbol, tablename, n=100):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime, close FROM {tablename} where symbol = '{symbol}' order by `datetime` desc LIMIT {n};"
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
    
    async def get_new_rows(self, symbol, threshold, tablename):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime, open, high, low, close, volume FROM {tablename} where symbol = '{symbol}' and datetime > '{threshold}' order by `datetime`;"
                #print(query)
                await cur.execute(query)
                data = await cur.fetchall()
                data_as_list = [list(row) for row in data]    
        return data_as_list

    async def get_all_rows(self, symbol, tablename):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime, open, high, low, close, volume FROM {tablename} where symbol = '{symbol}' order by `datetime`;"
                #print(query)
                await cur.execute(query)
                data = await cur.fetchall()
                data_as_list = [list(row) for row in data]    
        return data_as_list
    
    async def get_prior_rows_fifty(self, symbol, threshold, tablename):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime, open, high, low, close FROM {tablename} where symbol = '{symbol}' and datetime < '{threshold}' order by `datetime` desc Limit 50;"
                #print(query)
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
    async def get_ohlc_last_datetime_v2(self, symbol, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT datetime FROM {table_name} where symbol = '{symbol}' order by datetime desc Limit 1;"
                await cur.execute(query)
                data = await cur.fetchall()
        return data

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
                query = f"SELECT m.instrument_token, m.symbol FROM monitor_symbols m left join instruments i on m.instrument_token = i.instrument_token where m.active = 1 and i.expiry >= curdate();"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df  

    async def get_instrument_tokens(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT m.instrument_token, m.symbol FROM monitor_symbols m left join instruments i on m.instrument_token = i.instrument_token where m.active = 1 and i.expiry >= curdate();"
                await cur.execute(query)
                data = await cur.fetchall()
        return data
    
    async def get_priority_instruments_to_trade(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT distinct m.instrument_token, m.symbol, b.basket_id FROM monitor_symbols m left join instruments i on m.instrument_token = i.instrument_token inner join basket_stocks b on m.stock_symbol = b.symbol where m.active = 1 and i.expiry >= curdate() and m.stock_symbol in (SELECT distinct symbol FROM basket_stocks);"
                await cur.execute(query)
                data = await cur.fetchall()
        return data
    

    async def get_non_priority_instruments_to_trade(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT m.instrument_token, m.symbol FROM monitor_symbols m left join instruments i on m.instrument_token = i.instrument_token where m.active = 1 and i.expiry >= curdate() and m.stock_symbol not in (SELECT distinct symbol FROM basket_stocks);"
                await cur.execute(query)
                data = await cur.fetchall()
        return data

    async def get_table_metadata(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT table_name, token, last_datetime FROM table_metadata;"
                await cur.execute(query)
                data = await cur.fetchall()
        data_np = np.array(data)
        return data_np

    async def get_table_datetime_groupby(self, table_name):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT symbol, max(datetime) as datetime FROM {table_name} o left join instruments i on o.symbol = i.tradingsymbol group by symbol;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_download_symbols_to_trade(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT symbol FROM download_symbols;"
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
    async def get_all_stocks_token(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT distinct i.instrument_token, i.tradingsymbol FROM instruments i inner join all_stocks b on i.tradingsymbol = b.exchange_code where exchange = 'NSE';"
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
    
    async def insert_trade_logs_batch(self, logs_batch):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Batch insertion
                    query = """
                    INSERT INTO trade_logs(date_log, module, activity, important_data, priority, `timestamp`) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    await cur.executemany(query, logs_batch)
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
    async def reset_LRL(self, table_name, symbol):
        query = f"update {table_name} set LRL = null, UCL = null, LCL = null where symbol = '{symbol}';"
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
    async def update_PSAR_batch(self, updates, table):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"""
                    UPDATE {table} 
                    SET PSAR = %s, 
                        PSAR_L = CASE WHEN %s IS NULL THEN NULL ELSE %s END, 
                        PSAR_S = CASE WHEN %s IS NULL THEN NULL ELSE %s END 
                    WHERE symbol = %s AND datetime = %s;
                    """
                    await cur.executemany(query, updates)
                    await conn.commit()
        except Exception as e:
            raise e

    async def update_LRC_batch(self, updates, table):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"""
                    UPDATE {table} 
                    SET LRL = %s, UCL = %s, LCL = %s 
                    WHERE symbol = %s AND datetime = %s;
                    """
                    await cur.executemany(query, updates)
                    await conn.commit()
        except Exception as e:
            raise e
    async def upsert_table_metadata(self, batch_data):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Call the stored procedure with each batch of data
                    for data in batch_data:
                        table_name, token, datetime_val = data
                        await cur.callproc('upsert_table_metadata', (table_name, token, datetime_val))
                    await conn.commit()
        except Exception as e:
            raise e

    async def get_lrc_settings(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT period, standardDeviation FROM lrc_settings;"
                await cur.execute(query)
                data = await cur.fetchall()
        return data

    async def get_psar_settings(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT acceleration, max_acceleration FROM psar_settings;"
                await cur.execute(query)
                data = await cur.fetchall()
        return data
    
    async def get_scan_items(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("CALL get_scan_items()")
                data = await cur.fetchall()
                columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    
    async def get_custom_indicators(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("Select c.id, indicator_id, i.name, value from custom_indicators c inner join indicators i on c.indicator_id = i.id where c.active = 1;")
                data = await cur.fetchall()
                columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_conditions(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("Select * from conditions where active = 1;")
                data = await cur.fetchall()
                columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_hlfp(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("Select * from hlfp;")
                data = await cur.fetchall()
                columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def insert_alert(self, symbol, datetime, scanid, timeframe, bot_time):        
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "CALL insert_alert(%s, %s, %s, %s, %s)",
                    (symbol, datetime, scanid, timeframe, bot_time)
                )
                await conn.commit()
    async def insert_into_dashboard(self, total_symbol, skipped, processed, alerts_skip, alerts_process, alerts_gen, alerts_fail, cache_available, cache_unavailable, data_unavailable_db, data_unavailable_zerodha, invalid_token, bot_time, interval, total_time):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "CALL insert_into_dashboard(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (total_symbol, skipped, processed, alerts_skip, alerts_process, alerts_gen, alerts_fail, cache_available, cache_unavailable, data_unavailable_db, data_unavailable_zerodha, invalid_token, bot_time, interval, total_time)
                )
                await conn.commit()
