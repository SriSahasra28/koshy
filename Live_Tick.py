#Download Realtime Tick Data from Zerodha
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
print(os.getcwd())
import json
import logging
from kiteconnect import KiteTicker, KiteConnect
from background.database import DBHelper
from background.login import login
from datetime import date, datetime, time as tm
import time
import sys
import warnings
warnings.filterwarnings('ignore')
db = DBHelper() 
l = login(False)
global kws, tokens
status, kite, kws, access_token = l.InitiateZerodha()
import signal

def initialize():
    print('initialize')
    global kws, tokens
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_connect = on_connect
    kws.on_reconnect = on_reconnect
    kws.on_noreconnect = on_noreconnect
    global tokens, nifty_fut, banknifty_fut, fut_list
    tokens = []
    tokens = db.get_tokens_for_tick()
    if len(tokens) < 10:
        print(f"Error Number of {tokens=} < 10")
    db.truncate_latest_price()
    db.initialize_latest_price(tokens)
    
    df_fut = db.get_instrument_token_index('NIFTY')
    if len(df_fut) == 0:
        print('Error Nifty Index future not found')
    else:
        nifty_fut = df_fut.instrument_token.iloc[0]
    # # Banknifty future
    # df_fut = db.get_instrument_token_index('BANKNIFTY')
    # if len(df_fut) == 0:
    #     print('Error Bank Nifty Index future not found')
    # else:
    #     banknifty_fut = df_fut.instrument_token.iloc[0]
    # fut_list = [nifty_fut, banknifty_fut, 260105, 256265]
    
def on_ticks(ws, ticks):
    global fut_list
    for scripdata in ticks:
        now = datetime.now()
        now_time = now.time()
        desired_time = tm(9, 15)
        if now_time > desired_time:
            #print(scripdata)
            instrument_token_value = scripdata.get('instrument_token', 0)
            last_price_value = scripdata.get('last_price', 0.0)
            last_traded_quantity_value = scripdata.get('last_traded_quantity', 0)
            average_traded_price_value = scripdata.get('average_traded_price', 0.0)
            volume_traded_value = scripdata.get('volume_traded', 0)
            total_buy_quantity_value = scripdata.get('total_buy_quantity', 0)
            total_sell_quantity_value = scripdata.get('total_sell_quantity', 0)
            open_value = scripdata['ohlc'].get('open', 0.0)
            high_value = scripdata['ohlc'].get('high', 0.0)
            low_value = scripdata['ohlc'].get('low', 0.0)
            close_value = scripdata['ohlc'].get('close', 0.0)
            change_value = scripdata.get('change', 0.0)
            last_trade_time_value = exchange_timestamp_value =  int(datetime.now().timestamp())            
            print(instrument_token_value, last_price_value)

            #if int(instrument_token_value)  in fut_list:
            db.insert_market_data_intraday_V2(instrument_token_value, last_price_value, last_traded_quantity_value, average_traded_price_value, volume_traded_value, total_buy_quantity_value, total_sell_quantity_value, open_value, high_value, low_value, close_value, change_value, last_trade_time_value)
            #db.update_latest_price(instrument_token_value,  last_price_value, volume_traded_value, average_traded_price_value)
            if now.hour == 15 and now.minute >= 30:
                ws.close()
def on_connect(ws, response):
    global tokens
    if len(tokens) > 0:
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_QUOTE, tokens)
    else:
        print('No tokens to subscribe')

def on_close(ws, code, reason):
    logging.info("Connection closed: {code} - {reason}".format(code=code, reason=reason))
    now = datetime.now()
    if now.hour == 15 and now.minute >= 30:
        ws.stop()
        sys.exit() 
        os._exit(0)
        pid = os.getpid()
        os.kill(pid, signal.CTRL_BREAK_EVENT)

def on_error(ws, code, reason):
    logging.info("Connection error: {code} - {reason}".format(code=code, reason=reason))

def on_reconnect(ws, attempts_count):
    logging.info("Reconnecting: {}".format(attempts_count))

def on_noreconnect(ws):
    logging.info("Reconnect failed.")
    # mixer.init() 
    # sound=mixer.Sound("background\\alarm.wav")
    # sound.play()
    
initialize()
kws.connect()