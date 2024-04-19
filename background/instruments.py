
import pandas as pd
import warnings
from background.database import DBHelper
warnings.filterwarnings('ignore')

class instruments():
    def __init__(self):
        self.helper = DBHelper()
        self.scrip = self.helper.getAllInstruments()
        #self.scrip = scrip[['Token', 'InstrumentName', 'ShortName', 'Series','ExpiryDate', 'StrikePrice', 'OptionType', 'ExchangeCode']]
    
    def get_nearest_expiry(self, index_name):
        scrip = self.scrip[self.scrip.name == index_name]
        scrip = scrip[scrip.segment == 'NFO-OPT']
        scrip.expiry = pd.to_datetime(self.scrip.expiry)
        scrip['expiry'] = [x.date() for x in scrip.expiry]
        return sorted(scrip.expiry)[0]
    
    def get_nearest_expiry_fut(self, index_name):
        scrip = self.scrip[self.scrip.name == index_name]
        scrip = scrip[scrip.segment == 'FUT']
        scrip.expiry = pd.to_datetime(self.scrip.expiry)
        scrip['expiry'] = [x.date() for x in scrip.expiry]
        return sorted(scrip.expiry)[0]
