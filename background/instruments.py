
import pandas as pd
import warnings
from background.database import DBHelper
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
    def get_nearest_ten_strikes(self, stock_name, LTP, option_type):
        expiry = self.get_nearest_expiry(stock_name)
        print(f"{expiry=}")
        scrip = self.scrip[self.scrip.name == stock_name]
        scrip = scrip[scrip.exchange == 'NFO']
        scrip = scrip[scrip.instrument_type == option_type]
        scrip.expiry = pd.to_datetime(scrip.expiry)
        scrip = scrip[scrip.expiry == expiry.strftime('%Y-%m-%d')]
        scrip['diff'] = abs(scrip.strike - LTP)
        scrip = scrip.sort_values(by=['diff'])
        scrip = scrip[:10]
        return scrip
    # def find_nearest_strike_price(LTP, strike_prices):
    #     nearest_strike_price = min(strike_prices, key=lambda x: abs(x - LTP))
    #     return nearest_strike_price