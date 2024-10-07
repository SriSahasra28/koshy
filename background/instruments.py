
import pandas as pd
import warnings
from background.database import DBHelper
from datetime import datetime, timedelta
warnings.filterwarnings('ignore')

class instruments():
    def __init__(self):
        self.helper = DBHelper()
        self.scrip = self.helper.getAllInstruments()
        #print(self.scrip.tail())
        #self.scrip = scrip[['Token', 'InstrumentName', 'ShortName', 'Series','ExpiryDate', 'StrikePrice', 'OptionType', 'ExchangeCode']]
    
    def get_nearest_expiry(self, index_name):
        scrip = self.scrip[self.scrip.name == index_name]
        scrip = scrip[scrip.exchange == 'NFO']
        scrip.expiry = pd.to_datetime(self.scrip.expiry)
        scrip['expiry'] = [x.date() for x in scrip.expiry]
        return sorted(scrip.expiry)[0]
    def get_last_expiry_month(self, index_name):
        scrip = self.scrip[self.scrip.name == index_name]
        scrip = scrip[scrip.exchange == 'NFO']
        scrip.expiry = pd.to_datetime(self.scrip.expiry)
        scrip['expiry'] = [x.date() for x in scrip.expiry]
        current_year = datetime.now().year
        current_month = datetime.now().month
        scrip_current_month = scrip[scrip['expiry'].apply(lambda x: x.year == current_year and x.month == current_month)]
        last_expiry = None
        if not scrip_current_month.empty:
            last_expiry = max(scrip_current_month.expiry)
        return last_expiry
    def get_last_expiry_next_month(self, index_name):
        scrip = self.scrip[self.scrip.name == index_name]
        scrip = scrip[scrip.exchange == 'NFO']
        scrip.expiry = pd.to_datetime(self.scrip.expiry)
        scrip['expiry'] = [x.date() for x in scrip.expiry]
        current_year = datetime.now().year
        current_month = datetime.now().month
        if current_month == 12:
            next_month = 1
            next_year = current_year + 1
        else:
            next_month = current_month + 1
            next_year = current_year

        scrip_next_month = scrip[scrip['expiry'].apply(lambda x: x.year == next_year and x.month == next_month)]

        last_expiry_next_month = None
        if not scrip_next_month.empty:
            last_expiry_next_month = max(scrip_next_month.expiry)
        return last_expiry_next_month
    def get_next_month_expiry(self, index_name): 
        print(f'{index_name=}')
        scrip = self.scrip[self.scrip.name == index_name]
        scrip = scrip[scrip.exchange == 'NFO']
        scrip.expiry = pd.to_datetime(scrip.expiry)
        current_date = datetime.now()
        first_day_next_month = (current_date.replace(day=28) + timedelta(days=4)).replace(day=1)
        first_day_next_month = pd.Timestamp(first_day_next_month).normalize()
        scrip = scrip[scrip['expiry'].dt.date >= first_day_next_month.date()]
        first_day_following_month = (first_day_next_month + timedelta(days=32)).replace(day=1)
        last_day_next_month = first_day_following_month - timedelta(days=1)
        scrip = scrip[scrip['expiry'].dt.date <= last_day_next_month.date()]
        scrip['expiry'] = [x.date() for x in scrip.expiry]
        if len(scrip) > 0:
            return sorted(scrip.expiry)[-1]
        else:
            return None
    def get_nearest_ten_strikes(self, stock_name, LTP, option_type, next_month=False):
        expiry = None
        if next_month == True:
            print(stock_name)
            expiry = self.get_last_expiry_next_month(stock_name)
        else:
            expiry = self.get_last_expiry_month(stock_name)
        print(f"{expiry=}")
        if expiry == None:
            return -1, pd.DataFrame()
        scrip = self.scrip[self.scrip.name == stock_name]
        scrip = scrip[scrip.exchange == 'NFO']
        scrip = scrip[scrip.instrument_type == option_type]
        scrip.expiry = pd.to_datetime(scrip.expiry)
        scrip = scrip[scrip.expiry == expiry.strftime('%Y-%m-%d')]
        scrip['diff'] = abs(scrip.strike - LTP)
        scrip = scrip.sort_values(by=['diff'])
        scrip = scrip[:10]
        return 1, scrip
    # def find_nearest_strike_price(LTP, strike_prices):
    #     nearest_strike_price = min(strike_prices, key=lambda x: abs(x - LTP))
    #     return nearest_strike_price