#functions to communicate with Zerodha API
from kiteconnect import KiteConnect
from background.database import DBHelper
import datetime
import pandas as pd
import time
from background.login import login
from background.fake_clock import Clock
class zeroda():
    helper = DBHelper()
    global ClientCode, SDate, file
    accesstoken = ''
    global clock
    def __init__(self, mode, test_date):
        l = login(False)
        self.mode = mode
        self.clock = Clock(test_date)
        status, self.kite, kws = l.InitiateZerodha()
        self.status = status
        print('zerodha: ', self.getCurrentDateTime())
        
    def initiate(self):
        return self.kite
    def PlaceBuyOrderMarketNFO(self, tradingsymbol, qty, price):
        exchange_code = self.kite.EXCHANGE_NFO
        orderid = -1
        try:
            orderid = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                            exchange=exchange_code,
                                            product=self.kite.PRODUCT_NRML,
                                            order_type=self.kite.ORDER_TYPE_MARKET,
                                            tradingsymbol= tradingsymbol,
                                            transaction_type=self.kite.TRANSACTION_TYPE_BUY,
                                            quantity = qty,
                                            price= price
                                            )
        except Exception as e:
            print(f"{tradingsymbol=}, {qty=}, {price=}")
            print("Order placement failed: {}".format(e))
        return orderid
  
    def PlaceSellOrderMarketNFO(self, tradingsymbol, qty, price, exchange):
        exchange_code = self.kite.EXCHANGE_NFO
        orderid = -1
        try:
            orderid = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                            exchange=exchange_code,
                                            product=self.kite.PRODUCT_NRML,
                                            order_type=self.kite.ORDER_TYPE_MARKET,
                                            tradingsymbol= tradingsymbol,
                                            transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                                            quantity = qty,
                                            price = price
                                            )
        except Exception as e:
            print(f"{tradingsymbol=}, {qty=}, {price=}, {exchange=}")
            print("Order placement failed: {}".format(e))
        return orderid
    def PlaceStopLossSellOrderMarket(self, tradingsymbol, qty, triggerPrice):
        orderid = -1
        try:
            orderid = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                            exchange=self.kite.EXCHANGE_NFO,
                                            product=self.kite.PRODUCT_MIS,
                                            order_type=self.kite.ORDER_TYPE_SLM,
                                            tradingsymbol= tradingsymbol,
                                            transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                                            quantity = qty,
                                            trigger_price = triggerPrice 
                                            )
        except Exception as e:
            print("Order placement failed: {}".format(e))
        return orderid    

    def gettrades(self):
        trades = self.kite.trades()
        return trades
    
    def gettradeby_order(self, orderid):
        trades = self.kite.order_trades(order_id = orderid)
        return trades
    
    def cancel_order(self, orderid):
        self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR, order_id=orderid)
        
    def getpositions(self):
        positions = self.kite.positions()
        return positions
    def get_orders(self):
        orders = self.kite.orders()
        return orders
    def exit_order(self, orderid):
        self.kite.exit_order(variety=self.kite.VARIETY_REGULAR, order_id=orderid)
        return 1
    def get_holdings(self):
        holdings = self.kite.holdings()
        return holdings
    def get_margins(self):
        return self.kite.margins()
    
    # def getallorders():
    #     return self.kite.orders()

    def getOrderHistory(self, orderid):
        return self.kite.order_history(orderid)
    
    def gethistoricaldata(self, token, from_date, to_date, interval):
        print('gethistoricaldata Real zerodha')
        try:
            records = self.kite.historical_data(token, from_date=from_date, to_date=to_date, interval=interval)
            #print(records)
            df = pd.DataFrame(records)
            return df
        except Exception as e:
            print("Error getting Data: {}".format(e))
            df = pd.DataFrame()
            return df
    
    def getCurrentLTP(self, scripCode):
        try:
            return self.kite.ltp(scripCode)[scripCode]['last_price']
        except Exception as e:
            print(f"Error getting LTP {scripCode}")
            return -1
    
    def getLTPMulti(self, scrip_list):
        return self.kite.ltp(scrip_list)
    
    def getCurrentDateTime(self):
        return datetime.datetime.now()
    
    def getCurrentDate(self):
        tdate = datetime.datetime.today().date()
        print('getCurrentDate', tdate)
        return tdate

    def getCurrentTime(self):
        CurrentDateTime = datetime.datetime.now()
        CurrentTime = datetime.time(CurrentDateTime.hour, CurrentDateTime.minute, CurrentDateTime.second)
        return CurrentTime
    
    def advance_time(self, duration):
        pass
    
    def executeStopLossOrderSELL(self, orderid, tradingsymbol):
        pass
    
    def executeStopLossOrderBUY(self, orderid, tradingsymbol):
        pass
    
    def modifyStopLossOrder_market(self, order_id):
        orderid = self.kite.modify_order(variety=self.kite.VARIETY_REGULAR,order_id=order_id, trigger_price=0, order_type=self.kite.ORDER_TYPE_MARKET, price=0)

    def modifyStopLossOrderTriggerPrice(self, order_id, triggerPrice, parent_order):
        result = self.kite.modify_order(variety=self.kite.VARIETY_REGULAR,order_id=order_id, trigger_price=triggerPrice, order_type=self.kite.ORDER_TYPE_SLM, parent_order_id=parent_order)

    def addOneDay(self):
        pass
    def setCurrentDate(self):
        pass
    def getinstruments(self, exchange):
        if exchange == 'NSE':
            return self.kite.instruments(exchange=self.kite.EXCHANGE_NSE)
        else:
            return self.kite.instruments(exchange=self.kite.EXCHANGE_NFO)
    
