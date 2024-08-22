from kiteconnect import KiteConnect, KiteTicker
from selenium import webdriver
import pandas as pd
from datetime import datetime as dtm, date, time, timedelta
import os
import time
from pyotp import TOTP
from background.database import DBHelper
import platform
from background.set import settings

class login():
    def __init__(self, force_login=False):
        set = settings.get_api_details()
        self.api_key = set[0]
        self.key_secret = set[1]
        self.userID = set[2]
        self.pwd = set[3]
        self.totp_key = set[4]
        self.force_login = force_login
        self.helper = DBHelper()
        
    def InitiateZerodha(self):
        # file = ''
        # if platform.system() == "Windows":
        #     file = open("background\\access.txt", "r")
        # else:
        #     file = open("background/access.txt", "r")
        # keys = file.read().split()
        # last_login_date_str = keys[0].replace('date:','')
        # token = keys[1].replace('token:','')
        # last_login_date = dtm.strptime(last_login_date_str, "%Y-%m-%d").date()
        df_cred = self.helper.get_credentials()
        last_login_date = df_cred.login_date.iloc[0]
        token = df_cred.access_code.iloc[0]
        #print(f"In Login {token =} {last_login_date=}")
        self.kite = KiteConnect(api_key=self.api_key)
        if self.force_login is False and last_login_date == dtm.now().date():
            #print('Access Key Available. Skipping fresh login')            
            try:
                self.kite.set_access_token(token)
                kws = KiteTicker(self.api_key, token, debug=True, reconnect=True, reconnect_max_tries=150)
                return True, self.kite, kws, token
            except Exception as e:
                print(f"Error during login to kite:{e}")
                return False, None, None
        else:
            try:
                print('Attempting Fresh Login')
                browser = webdriver.Chrome()
                browser.get(self.kite.login_url())
                browser.implicitly_wait(5)
                username = browser.find_element("xpath", '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input')
                password = browser.find_element("xpath", '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input')                     
                username.send_keys(self.userID)
                password.send_keys(self.pwd)
                browser.find_element("xpath", '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button').click()
                time.sleep(2)
                pin = browser.find_element("xpath", '/html/body/div[1]/div/div[2]/div[1]/div[2]/div/div[2]/form/div[1]/input')
                totp = TOTP(self.totp_key)
                token = totp.now()
                pin.send_keys(token)
                #browser.find_element("xpath", '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[3]/button').click()
                time.sleep(1)
                temp_token=browser.current_url.split('request_token=')[1][:32]
                print(f"{temp_token=}")
                data = self.kite.generate_session(temp_token, api_secret=self.key_secret)
                AccessToken = data["access_token"]
                print('AccessToken', AccessToken)
                self.kite.set_access_token(AccessToken)
                print('Ready to trade')
                if platform.system() == "Windows":
                    with open('background\\access.txt', 'w') as f:
                        content = 'date:' + dtm.now().date().strftime("%Y-%m-%d") + '\ntoken:' + str(AccessToken)
                        f.write(content)
                        f.flush()
                else:
                    with open('background/access.txt', 'w') as f:
                        content = 'date:' + dtm.now().date().strftime("%Y-%m-%d") + '\ntoken:' + str(AccessToken)
                        f.write(content)
                        f.flush()
                print('access token saved to file')
                kws = KiteTicker(self.api_key, token, debug=True, reconnect=True, reconnect_max_tries=150)
                return True, self.kite, kws, AccessToken
            except Exception as e:
                print(f"Error initial login to kite: {e}")
                return False, None, None, None
            
    def download_instruments(self, exch):
        print('downloading instruments')
        lst = []
        df = pd.DataFrame()
        if exch == 'NSE':
            exchange = self.kite.EXCHANGE_NSE
        elif exch == 'NFO':
            exchange = self.kite.EXCHANGE_NFO
        lst = self.kite.instruments(exchange=exchange)
        df = pd.DataFrame(lst)
        if exch == 'NFO': 
            df = df[df.segment == 'NFO-OPT']
            #df = df[(df.name == 'NIFTY') | (df.name == 'BANKNIFTY') | (df.name == 'FINNIFTY')]

        if len(df) == 0:
            print('No data returned')
            return
        elif exch == 'NSE':
            df.expiry = '2024-01-01'
        print(df.tail())
        self.helper.update_instruments(df, exch)
        print('downloaded instruments')

