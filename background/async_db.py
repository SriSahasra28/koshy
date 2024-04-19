import asyncio
import aiomysql
import pandas as pd
import time
from background.set import settings

class dbconnection:
    def __init__(self):
        setting = settings()
        self.set = setting.get_db()
        print('db', self.set[4])
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
    async def get_open_positions_db(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM Trade_Book where `status` in (0, 1) and intent = 0"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df

    async def get_placedOrders(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM Trade_Book where `status` = 1"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    
    async def get_SL_placedOrders(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM Trade_Book where `status` = 0 and intent = 1"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    async def get_SL_Orders(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM Trade_Book where `status` < 2 and intent = 1"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    async def get_executed_Orders_forSL(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM Trade_Book where `status` = 1 and intent = 0 and StopLossStatus = 0"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    async def get_force_exit_orders(self, cur_date):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT SQL_NO_CACHE B.* FROM Trade_Book B inner join strategies S on B.strategy_id = S.ID where S.status = 1 and B.intent = 1 and B.status = 0 and S.force_exit = 1"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    async def get_monitor_symbols(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT m.id, m.instrument_token, i.tradingsymbol, m.symbol, m.expiry, m.strike, m.high, m.low, m.option_type, i.lot_size FROM monitor_symbols m left join instruments i on m.instrument_token = i.instrument_token where m.`active` = 1 and m.status = 0;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    
    async def get_stoplossbyMainOrder(self, main_order_id):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM Trade_Book where intent = 1 and main_order_id = {main_order_id};"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
        
    async def get_trade_book(self, cur_date):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM Trade_Book where date(`timestamp`) = '{cur_date}' and status >= 0;"
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    async def get_strategies(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = f"SELECT * FROM strategies;"
                #print(query)
                await cur.execute(query)
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    async def insert_nifty_ohlc(self, symbol, date, open, high, low, close):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT INTO nifty_ohlc(symbol, date, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s)",
                    (symbol, date, open, high, low, close)
                    )
                    await conn.commit()
        except Exception as e:
            raise e    
    async def insert_banknifty_ohlc(self, symbol, date, open, high, low, close):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT INTO bank_nifty_ohlc(symbol, date, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s)",
                    (symbol, date, open, high, low, close)
                    )
                    await conn.commit()
        except Exception as e:
            raise e
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
    async def get_strategy_trade_summary(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT T.id, S.strategy_name, S.Symbol, S.live, T.status, T.active, T.start_time FROM Strategy_trades T left join Strategies S on T.strategy_id = S.id where `date` = curdate();")
                data = await cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(data, columns=columns)
        return df
    # update function
    
    async def insert_trade_book(self, order_id, transaction_type, price_executed, status, intent, client_price, symbol, qty, stop_loss, main_order_id, target=0):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                    "INSERT INTO Trade_Book(broker_id, transaction_type, price_executed, status, intent, client_price, symbol, qty,  stop_loss,  main_order_id, target) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (order_id, transaction_type, price_executed, status, intent, client_price, symbol, qty, stop_loss,  main_order_id, target)
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
        
    async def updateOrder(self, status, price_executed, broker_id):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"update trade_book set `status` = '{status}', price_executed = '{price_executed}', execution_time = curtime() where broker_id = '{broker_id}';"
                    await cur.execute(query)
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
            print(e)
            raise e
    async def update_status_by_id(self, order_id, new_status):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"Update Trade_Book set status = {new_status}, execution_time = curtime() where id = {order_id};"
                    await cur.execute(query)
                    await conn.commit()
        except Exception as e:
            raise e
    async def update_tradebook_stoploss_status(self, orderid, StopLossStatus):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"Update Trade_Book set StopLossStatus = {StopLossStatus} where broker_id = '{orderid}'";
                    await cur.execute(query)
                    await conn.commit()
        except Exception as e:
            raise e
    async def update_tradebook_mainorderid(self, orderid, neworderid):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"Update Trade_Book set main_order_id = '{orderid}' where broker_id = '{neworderid}'"
                    await cur.execute(query)
                    await conn.commit()
        except Exception as e:
            raise e
    async def update_tradebook_status(self, orderid, status, price_executed):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f"Update Trade_Book set status = {status}, price_executed = '{price_executed}' where broker_id = '{orderid}'";
                    await cur.execute(query)
                    await conn.commit()
        except Exception as e:
            raise e
    async def close_pool(self):
            self.pool.close()
            await self.pool.wait_closed()
            #print('Pool closed')

    async def get_trade_book_by_strategy_trade_id(self, strategy_trades_id):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.callproc("GetTradeBookByStrategyTradeID", [strategy_trades_id])
                    if cur.rowcount:
                        data = await cur.fetchall()
                        if data is not None:
                            columns = [desc[0] for desc in cur.description]
                            df = pd.DataFrame(data, columns=columns)
                            return df
                        else:
                            print(' inner No data found get_trade_book_by_strategy_trade_id')
                            return pd.DataFrame()
                    else:
                        print('No data found get_trade_book_by_strategy_trade_id')
                        return pd.DataFrame()
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None
    
    async def insert_option_data_one_min(self, datetime_val, open_val, high, low, close, ha_open, ha_high, ha_low, ha_close, bol_up, bol_down):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    params = (datetime_val, open_val, high, low, close, ha_open, ha_high, ha_low, ha_close, bol_up, bol_down)
                    await cur.callproc('InsertOptionDataOneMin', params)
                    await conn.commit()
        except Exception as e:
            print(f"An error occurred: {str(e)}")