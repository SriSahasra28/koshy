# set strike price 0
# Database Related Functions
# ,auth_plugin='mysql_native_password'
import mysql.connector as sqlConnector
import pandas as pd
from sqlalchemy import create_engine
import datetime
import mysql.connector
import socket
import inspect
import warnings
import os
import ast
warnings.filterwarnings('ignore')
from background.set import settings

class DBHelper:
    strategies = pd.DataFrame()
    all_orders = pd.DataFrame()
    monitoring = 0
    def __init__(self):
        set = settings.get_db()
        self.user = set[0]
        self.passwd = set[1]
        self.server = set[2]
        self.port = set[3]
        self.database = set[4]
 
    def get_settings(self):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        df = pd.read_sql("SELECT * FROM settings", con=con)
        con.close()
        return df
    def get_credentials(self):
        try:
            con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
            df = pd.read_sql("SELECT * FROM credentials;", con=con)
            con.close()
            return df
        except Exception as e:
            print(f"Database connection failed: {e}")
            print("Using fallback credentials from security.txt...")
            return self._get_fallback_credentials()
    
    def _get_fallback_credentials(self):
        """Fallback method to get credentials from security.txt when database is unavailable"""
        try:
            import ast
            config_file_path = 'background/security.txt'
            with open(config_file_path, 'r') as file:
                content = file.read()
            config = ast.literal_eval(content)
            
            # Create a DataFrame with the credentials from security.txt
            # Assuming we need login_date and access_code columns
            # You may need to adjust this based on your actual requirements
            credentials_data = {
                'login_date': [pd.Timestamp.now().date()],  # Use current date as fallback
                'access_code': [config.get('api_pwd', '')]  # Use api_pwd as access_code
            }
            return pd.DataFrame(credentials_data)
        except Exception as e:
            print(f"Error reading fallback credentials: {e}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=['login_date', 'access_code'])

    def get_active_basket_symbols(self):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = "SELECT distinct b.option_type, i.instrument_token, i.tradingsymbol FROM instruments i inner join basket_stocks b on i.tradingsymbol = b.symbol where exchange = 'NSE'"
        df = pd.read_sql(query, con=con)
        con.close()
        return df

    def truncate_latest_price(self):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = "truncate table latest_price"
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        con.close()
        
    def get_tokens_for_tick(self):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        cursor = con.cursor()
        query = "SELECT i.instrument_token as token FROM instruments i inner join basket_stocks b on i.tradingsymbol = b.symbol"
        df = pd.read_sql(query, con=con)
        con.close()
        return df['token'].astype(int).values.tolist()
    def initialize_latest_price(self, tokens):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        cursor = con.cursor()
        query = "INSERT INTO latest_price(i) VALUES (%s)"
        for token in tokens:
            cursor.execute(query, (token,))
        con.commit()
        cursor.close()
        con.close()
    def get_nearest_three_expiry(self, index):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"SELECT distinct expiry FROM instruments i where i.exchange = 'NFO' and instrument_type = 'CE' and expiry >= curdate() and i.name = '{index}' order by expiry Limit 3;"
        df = pd.read_sql(query, con=con)
        con.close()
        return df
    def get_instrument_token(self, index_name, strike_price, option_type, expiry):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"SELECT instrument_token FROM instruments i where i.exchange = 'NFO' and instrument_type = '{option_type}' and expiry = '{expiry}' and i.name = '{index_name}' and strike = '{strike_price}' Limit 1;"
        print(query)
        df = pd.read_sql(query, con=con)
        con.close()
        return df
    def insert_market_data_intraday_V2(self, instrument_token, last_price, last_traded_quantity, average_traded_price, volume_traded, total_buy_quantity, total_sell_quantity, open, high, low, close, change, last_trade_time):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        cursor = con.cursor()
        params = (instrument_token, last_price, last_traded_quantity, average_traded_price, volume_traded, total_buy_quantity, total_sell_quantity, open, high, low, close, change, last_trade_time)
        cursor.callproc('InsertMarketData_v2', params)
        con.commit()
        cursor.close()
        con.close()

    def InsertIntoMonitorSymbols(self, instrument_token, symbol, expiry, strike, option_type, ltp, stock_symbol):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        cursor = con.cursor()
        params = (instrument_token, symbol, expiry, strike, option_type, ltp, stock_symbol)
        cursor.callproc('InsertIntoMonitorSymbols', params)
        con.commit()
        cursor.close()
        con.close()
        
    def update_latest_price(self, i_value, c_value, v_value, a_value):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        cursor = con.cursor()
        params = (i_value, c_value, v_value, a_value)
        cursor.callproc('UpdateLatestPrice', params)
        con.commit()
        cursor.close()
        con.close()
    def get_instrument_token_index(self, name):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"SELECT instrument_token FROM Instruments WHERE name = '{name}' AND instrument_type = 'FUT' AND expiry > CURDATE() ORDER BY expiry LIMIT 1;"
        df = pd.read_sql(query, con=con)
        con.close()
        return df
    def get_monitor_symbols(self, index_name, strike_price, option_type, expiry):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"SELECT * FROM monitor_symbols where symbol = '{index_name}' and expiry = '{expiry}' and strike = {strike_price} and option_type = '{option_type}' and `active` = 1;"
        print(query)
        df = pd.read_sql(query, con=con)
        con.close()
        return df
    def update_settings(self, capital, max_profit, max_loss, add_to_loss, divider, comparator, average_sl, mini_profit):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"Update settings set capital = '{capital}', max_profit = '{max_profit}', max_loss = '{max_loss}', add_to_loss = '{add_to_loss}', divider = '{divider}', comparator = '{comparator}', average_sl = '{average_sl}', mini_profit = '{mini_profit}'"
        print(query)
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        con.close()
    def insertmarketorders(self, strategy_id, symbol, start_time, end_time, live, active, lots):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"INSERT into Strategy_trades(strategy_id, symbol, start_time, end_time, live, `active`, lots) Values({strategy_id}, '{symbol}', '{start_time}', '{end_time}', {live}, {active}, {lots})"
        print(query)
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        con.close()
    def run_query_inst(self, query):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        con.close()

    def UpsertMonitorSymbol(self, p_instrument_token, p_symbol, p_expiry, p_strike, p_high, p_low, p_option_type, p_time, p_active):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        cur = con.cursor()
        cur.callproc('UpsertMonitorSymbol', [p_instrument_token, p_symbol, p_expiry, p_strike, p_high, p_low, p_option_type, p_time, p_active])
        con.commit()
        con.close()

    def update_instruments(self, df, exchange):
        #print('in update_instruments')
        #print(df)
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        cursor = con.cursor()
        for index, row in df.iterrows():
            instrument_token = row['instrument_token']
            exchange = row['exchange']
            expiry = row['expiry']
            tradingsymbol = row['tradingsymbol'] 
            name = row['name']
            strike = row['strike']
            tick_size = row['tick_size']
            lot_size = row['lot_size']
            instrument_type = row['instrument_type']
            #print(f"{instrument_type=}, {name=}, {tradingsymbol=}")
            cursor.callproc('InsertInstrument', [instrument_token, instrument_type, exchange, expiry, name, tradingsymbol, strike, tick_size, lot_size])
            con.commit()
        if cursor:
            cursor.close()
        con.close()
    def get_trades(self):
        try:
            con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
            cursor = con.cursor()
            cursor.callproc("GetTradeData")
            results = cursor.stored_results()
            first_result = next(results)
            df = pd.DataFrame(first_result.fetchall(), columns=first_result.column_names)
            return df
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None

    def get_trades_all(self):
        try:
            con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
            cursor = con.cursor()
            cursor.callproc("GetTradeDataAll")
            results = cursor.stored_results()
            first_result = next(results)
            df = pd.DataFrame(first_result.fetchall(), columns=first_result.column_names)
            return df
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None        
    def get_log(self):
        try:
            con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
            cursor = con.cursor()
            cursor.callproc("GetLogForCurrentDate")
            results = cursor.stored_results()
            first_result = next(results)
            df = pd.DataFrame(first_result.fetchall(), columns=first_result.column_names)
            return df
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None
    def get_title(self):
        try:
            con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
            query = f"SELECT symbol, strike_price, direction, expiry_date FROM strategies;"
            df = pd.read_sql(query, con=con)
            con.close()
            return df
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None    
    def get_timestamp(self):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = "SELECT `timestamp` FROM option_data_one_min order by id desc LIMIT 1;"
        df = pd.read_sql(query, con=con)
        con.close()
        return df

    def search_trade_log(self, stime, etime, imp_data):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"SELECT activity,important_data, `timestamp` FROM trade_logs where time(`timestamp`) between '{stime}' and '{etime}' and important_data like '%{imp_data}%'"
        df = pd.read_sql(query, con=con)
        print(query)
        con.close()
        return df

    def get_data(self, query):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        df = pd.read_sql(query, con=con)
        con.close()
        return df

    def getAllInstruments(self):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = "SELECT * FROM instruments where exchange = 'NFO' and expiry >= curdate() order by expiry;"
        df = pd.read_sql(query, con=con)
        con.close()
        return df    
    
    def getAllInstruments_by_type(self, name, instrument_type):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"SELECT instrument_token, tradingsymbol, name, expiry, strike, lot_size, instrument_type FROM instruments where `name` = '{name}' and instrument_type = '{instrument_type}';"
        df = pd.read_sql(query, con=con)
        con.close()
        return df   
    def get_instrument_token_option(self, index_code, right, expiry_date, strike_price):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"SELECT instrument_token FROM instruments where `name` = '{index_code}' and instrument_type = '{right}' and expiry = '{expiry_date}' and strike = '{strike_price}';"
        df = pd.read_sql(query, con=con)
        con.close()
        return df

    def get_tradingsymbol_option(self, index_code, right, expiry_date, strike_price):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = f"SELECT tradingsymbol FROM instruments where `name` = '{index_code}' and instrument_type = '{right}' and expiry = '{expiry_date}' and strike = '{strike_price}';"
        df = pd.read_sql(query, con=con)
        con.close()
        return df

    def get_orders(self):
        con = sqlConnector.connect(host=self.server, user=self.user, passwd=self.passwd, database=self.database, port=self.port, auth_plugin='mysql_native_password')
        query = "SELECT timestamp, transaction_type FROM trade_book;"
        df = pd.read_sql(query, con=con)
        con.close()
        return df   

    @staticmethod
    def run_query(query):
        set = settings.get_db()
        con = sqlConnector.connect(host=set[2], user=set[0], passwd=set[1], database=set[4], port=set[3], auth_plugin='mysql_native_password')
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        con.close()
    
    @staticmethod
    def update_access_token(access_token, login_date):
        set = settings.get_db()
        con = sqlConnector.connect(host=set[2], user=set[0], passwd=set[1], database=set[4], port=set[3], auth_plugin='mysql_native_password')
        query = f"update credentials set access_code = '{access_token}', login_date = '{login_date}';"
        print(query)
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        con.close()
        
    def get_strategy_details(self, id):
        set = settings.get_db()
        con = sqlConnector.connect(host=set[2], user=set[0], passwd=set[1], database=set[4], port=set[3], auth_plugin='mysql_native_password')
        df = pd.read_sql(f"SELECT * FROM strategies where id = {id};", con=con)
        con.close()
        return df
    def get_option_candle_data(self):
        set = settings.get_db()
        con = sqlConnector.connect(host=set[2], user=set[0], passwd=set[1], database=set[4], port=set[3], auth_plugin='mysql_native_password')
        df = pd.read_sql(f"SELECT datetime, HA_open, HA_high, HA_close, HA_low, bol_up, bol_down FROM option_data_one_min order by datetime;", con=con)
        con.close()
        return df
    
    def insert_one_min_batch_ohlc(self, batch_data):
        """Insert batch 1-minute OHLC data with duplicate handling"""
        try:
            set = settings.get_db()
            con = sqlConnector.connect(host=set[2], user=set[0], passwd=set[1], database=set[4], port=set[3], auth_plugin='mysql_native_password')
            cur = con.cursor()
            
            query = """
            INSERT INTO one_min_ohlc
            (symbol, datetime, open, high, low, close) 
            VALUES (%s, %s, %s, %s, %s, %s) AS new_values
            ON DUPLICATE KEY UPDATE 
            open = new_values.open, 
            high = new_values.high, 
            low = new_values.low, 
            close = new_values.close
            """
            
            cur.executemany(query, batch_data)
            con.commit()
            con.close()
            
        except Exception as e:
            print(f"Error inserting batch OHLC data: {e}")
            raise e