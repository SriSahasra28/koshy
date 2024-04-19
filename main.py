# cd "/Users/satya2/Desktop/Projects/OBEL"
# Trade_Book status 0: placed, 1: executed, 2: closed, -1: rejected, -2: Cancelled, -3: Expired, -4: Freezed
# order_type 0:market, 1: limit
# transaction_type: 1: buy, -1: sell
# intent: 0: main order, 1: stoploss order
# StopLossStatus 0: not placed, 1: placed
# Strategy_trades 0: 'waiting', 1: 'running', 2: 'finished' 
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)

from background.zerodha import zeroda
from background.async_db import dbconnection
from datetime import datetime, date, timedelta, time as tm
import time
import pandas as pd
import warnings
import os
import asyncio
import random
import math
warnings.filterwarnings('ignore')

class Start(object):
    def __init__(self, live, current_date, backtest=False):
        self.live = live
        self.today = current_date
        self.env = 'Real'
        if backtest == True:
            print('************** Backtesting with old data **************')
            self.zerodha = zeroda('test', self.today)
            self.env = 'Test'
            self.kite = self.zerodha.initiate()
        if self.live == 1:
            print('$$$$$$$$$$$$$$$$$ Live Trading $$$$$$$$$$$$$$$$$$$$$$$')
            self.zerodha = zeroda('live', self.today)
        else:
            print('------ Paper Trading ------------')
            self.zerodha = zeroda('paper', self.today)
        self.backtest = backtest
        self.current_date_string = self.today.strftime("%Y-%m-%d")
        self.strategy_name = 'strategy1'
        self.db = dbconnection()
        self.run_job = True
        self.ordertype = 'market'
        self.exchange = 'NSE'
        self.userid = 'Jiva'
        self.interval = '1minute' 
        self.loop_interval = 1  * 60  # 1 minutes
        self.sdate = self.zerodha.getCurrentDate()
        self.log = True
        self.trade =True
        self.run_job = True
        self.strategy_start = False
        self.position = 0

    async def initiate(self):
        df_strategies = await self.db.get_data("Select * from settings")
        if len(df_strategies) == 0:
            print('Settings not found')
            return
        row = df_strategies.iloc[0]
        start_time = str(row['start_time'])[-8:]
        self.start_time = datetime.strptime(start_time, '%H:%M:%S').time()
        exit_time = str(row['exit_time'])[-8:]
        self.exit_time = datetime.strptime(exit_time, '%H:%M:%S').time()
        self.capital = int(row['capital'])
        self.max_profit  = int(row['max_profit'])
        self.mini_profit  = int(row['mini_profit'])
        self.max_loss  = int(row['max_loss'])
        self.add_to_loss  = int(row['add_to_loss'])
        self.max_trades  = int(row['max_trades'])
        self.status = int(row['status'])
        self.divider = float(row['divider'])
        self.comparator = float(row['comparator'])
        self.average_sl  = bool(row['average_sl'])
        self.active = row['active'].astype(bool)
        self.force_exit = row['force_exit']#.astype(bool)
        self.live = int(row['live'])
        # print(f"{self.start_time=} {self.exit_time=} {self.capital=} {self.max_profit=} {self.max_loss=} {self.add_to_loss=} {self.max_trades=} {self.status=} {self.divider=} {self.comparator=}")
        print(f"{self.average_sl=} {self.active=} {self.force_exit=} {self.live=}")
        
    async def Resetdb(self):
        if self.env == 'Test':
            await self.db.run_query("Call Resetdb();")

    async def start_pool(self):
        loop = asyncio.get_event_loop()
        await self.db.create_pool(loop=loop)
    async def close_pool(self):
        await self.db.close_pool()
    
    async def buy(self, stock_code, qty, price, stoploss, target=0):
        print(f"Buy {stock_code}, {qty=}, {price=}, {stoploss=} {target=}")
        if self.live == 0:
            random_number = random.randint(1, 1000000)
            broker_id = int(datetime.now().timestamp() * 1000) + random_number
            broker_id =  str(broker_id)
        else:
            # Place actual order
            broker_id = self.zerodha.PlaceBuyOrderMarketNFO(stock_code, qty, price)
        if broker_id != 1:
            print(f'buy order placed {broker_id}')
            info = f"{broker_id=}, {price=}"
            await self.db.insert_trade_log(date_log=self.CurrentDateTime, module='Buy order', activity='buy order placed', important_data=info, priority=4, strategy_trade_id = '', timestamp=self.CurrentDateTime_str)
            order_type = stop_loss_stratus = 0
            status = intent = 0
            transaction_type = 1 # Long
            await self.db.insert_trade_book(broker_id, transaction_type, price, status, intent, price, stock_code, qty, stoploss, None, target)
            if self.live == 0:
                await self.db.run_query("update Trade_Book set status = 1, execution_time = curtime() where broker_id = " + str(broker_id))
            elif broker_id == -1:
                await self.db.run_query("update Trade_Book set status = '-1' where broker_id = '" + str(broker_id) + "'")
            return 1
        else:
            return 0

    async def sell(self, stock_code, qty, price, main_order_id = None):
        print('in Sell')
        if self.live == 0:
            random_number = random.randint(1, 1000000)
            broker_id = int(datetime.now().timestamp() * 1000) + random_number
            broker_id =  str(broker_id)
        else:
            # Place Actual Order
            broker_id = self.zerodha.PlaceSellOrderMarketNFO(stock_code, qty, price, self.exchange)
        if broker_id != 1:
            print(f'Sell order placed {broker_id}')
            close = 0
            info = f"{broker_id=}, {price=}"
            await self.db.insert_trade_log(date_log=self.CurrentDateTime, module='Sell order', activity='sell order placed', important_data=info, priority=4, strategy_trade_id = '', timestamp=self.CurrentDateTime_str)
            stoploss = order_type = stop_loss_stratus = 0
            transaction_type = -1 # Sell
            status = intent = 0            
            await self.db.insert_trade_book(broker_id, transaction_type, price, status, intent, price, stock_code, qty, stoploss, None, 0)
            if self.live == 0:
                await self.db.run_query(f"update Trade_Book set status = 1 where broker_id = {broker_id}")
            if main_order_id is not None:
                await self.db.run_query(f"update Trade_Book set main_order_id = {main_order_id} where broker_id = {broker_id}")
            return 1
        else:
            return 0

    async def sell_Stoploss(self, stock_code, qty, price, main_order_id = None):
        print('in Sell stoploss')
        if self.live == 0:
            random_number = random.randint(1, 1000000)
            broker_id = int(datetime.now().timestamp() * 1000) + random_number
            broker_id =  str(broker_id)
        else:
            # Place Actual Order
            #broker_id = self.zerodha.PlaceSellOrderMarketNFO(stock_code, qty, price, self.exchange)
            broker_id = self.zerodha.PlaceStopLossSellOrderMarket(stock_code, qty, price)
        if broker_id != 1:
            print(f'Stoploss order placed {broker_id}')
            close = 0
            info = f"{broker_id=}, {price=}"
            await self.db.insert_trade_log(date_log=self.CurrentDateTime, module='Stoploss order', activity='sell order placed', important_data=info, priority=4, strategy_trade_id = '', timestamp=self.CurrentDateTime_str)
            stoploss = order_type = stop_loss_stratus = 0
            transaction_type = -1 # Sell
            status = 0
            intent = 1            
            await self.db.insert_trade_book(broker_id, transaction_type, price, status, intent, price, stock_code, qty, 0, None, 0)
            if self.live == 0:
                await self.db.run_query(f"update Trade_Book set status = 1 where broker_id = {broker_id}")
            if main_order_id is not None:
                await self.db.run_query(f"update Trade_Book set main_order_id = {main_order_id} where broker_id = {broker_id}")
            return 1
        else:
            return 0

    async def process_entry(self):
        print('In process Entry')
        CurrentDateTime = self.zerodha.getCurrentDateTime()
        self.CurrentDateTime = CurrentDateTime
        print('--- Check Entry ---', CurrentDateTime)
        CurrentDate = self.zerodha.getCurrentDate()
        date_string = CurrentDate.strftime("%Y-%m-%d")
        current_time = CurrentDateTime.time()
        curr_time = current_time.strftime('%H:%M')
        self.current_time = current_time
        self.current_time_str = curr_time
        self.current_date_string = date_string
        self.CurrentDateTime_str = self.CurrentDateTime.strftime("%Y-%m-%d %H:%M:%S")
        self.stoploss = 0
        self.qty = 1
        df_open_pos_db = await self.db.get_open_positions_db()
        if len(df_open_pos_db) == 0:
            self.position = 0
        if self.active == True and self.force_exit == False and self.current_time < self.exit_time and self.current_time >= self.start_time:
            self.strategy_name = curr_time
            print(f"check Entry {self.current_time.hour}:{self.current_time.minute}:{self.current_time.second}")
            df_monitor_symbols = await self.db.get_monitor_symbols() # status 0
            for index, row in df_monitor_symbols.iterrows():
                id = row['id']
                tradingsymbol = row['tradingsymbol']
                high = float(row['high'])
                low = float(row['low'])
                lot_size = int(row['lot_size'])
                # instrument_token = row['instrument_token']
                # symbol = row['symbol']                
                # expiry = row['expiry']
                # strike = row['strike'] 
                # option_type = row['option_type']
                print(f"{tradingsymbol=}")
                # monitor price and compare with High & Low
                # Get LTP
                LTP = loss_value = target = 0
                for retry in range(10):
                    LTP = float(self.zerodha.getCurrentLTP('NFO:' + tradingsymbol))
                    if LTP > 0:
                        break  
                    else:
                        asyncio.sleep(1)
                if LTP == 0:
                    info = f"Couldn't get LTP for {tradingsymbol}"
                    print(info)
                else:
                    print(f"{tradingsymbol}, {LTP=}")
                if LTP < high:
                    info = f"Not LTP: {LTP} > high: {high} {tradingsymbol}"
                    await self.db.insert_trade_log(date_log=self.CurrentDateTime, module='LTP checked', activity='process Entry', important_data=info, priority=2, strategy_trade_id = '', timestamp=self.CurrentDateTime_str)
                elif LTP > high:
                    self.initiate()
                    print(f"LTP: {LTP} > high: {high}")
                    if self.average_sl:
                        self.stoploss = (high + low) / 2
                    else:
                        self.stoploss = low

                    if self.position == 0:
                        self.qty = math.floor(self.capital/ (LTP * lot_size))
                        #self.qty = math.floor(self.capital/ LTP)
                        print(f"{self.qty=}, {self.capital=}, {LTP=}")
                    else:
                        pnl = 0
                        df_positions = self.zerodha.getpositions()
                        if len(df_positions) == 0:
                            print('no positions received from zerodha')
                            continue
                        net_df = pd.DataFrame(df_positions['net'])
                        if len(net_df) > 0:
                            df_pos = net_df[['tradingsymbol', 'sell_value', 'buy_value', 'quantity', 'last_price', 'multiplier', 'pnl']]
                            pnl = int(df_pos['pnl'].sum())
                        if pnl < 0: # net loss
                            loss_value = pnl
                            if self.comparator > loss_value:
                                # Qty = (Capital / EntryPrice*Lotsize) 	if 		(Comparator < BookedLoss)	 
                                self.qty = math.floor(self.capital/ (LTP * lot_size))
                            elif self.comparator < loss_value:
                                # Qty = (((LossValue + Add loss) / Divider)/EntryPrice*LotSize)	if 	(Comparator > BookedLoss) 	
                                self.qty = math.floor(((loss_value + self.add_to_loss) / self.divider) / (LTP * lot_size))

                    # Calculate target
                    if self.comparator < loss_value:
                        print(f"{self.comparator=}  {loss_value=} {self.max_profit=}")
                        target = self.max_profit
                    else:
                        print(f"{self.comparator=}  {loss_value=} {self.mini_profit=}")
                        target = self.mini_profit

                    result = await self.buy(tradingsymbol, self.qty, LTP, self.stoploss, target)
                    if result == 1:
                        self.position = 1
                        query = f"update monitor_symbols set status = 1 where id = {id}"
                        await self.db.run_query(query)
                        info = f"Buy order placed {tradingsymbol}"
                        await self.db.insert_trade_log(date_log=self.CurrentDateTime, module='status changed', activity='monitor_symbols status 1', important_data=info, priority=4, strategy_trade_id = '', timestamp=self.CurrentDateTime_str)

    async def process_exit(self):
        CurrentDateTime = self.zerodha.getCurrentDateTime()
        self.CurrentDateTime = CurrentDateTime
        print('--- process exit ---', CurrentDateTime)
        CurrentDate = self.zerodha.getCurrentDate()
        date_string = CurrentDate.strftime("%Y-%m-%d")
        current_time = CurrentDateTime.time()
        curr_time = current_time.strftime('%H:%M')
        exit_time = current_time.replace(hour=15, minute=20, second=0, microsecond=0)
        self.current_time = current_time
        self.current_time_str = curr_time
        self.current_date_string = date_string
        self.CurrentDateTime_str = self.CurrentDateTime.strftime("%Y-%m-%d %H:%M:%S")
        df_orders = await self.db.get_open_positions_db()
        
        if (current_time >= exit_time) or (self.force_exit == True):
            print(f"Exit Strategy current_time:{current_time}  exit_time:{exit_time} {self.force_exit=}")
            # Exit All positions at 3:20 PM
            for index, row in df_orders.iterrows():
                id = row['id']
                tradingsymbol = row['symbol']
                stop_loss = row['stop_loss']
                qty = int(row['qty'])
                if self.live == 1:
                    # change stop loss price to market price
                    df_SL_order = self.db.get_stoplossbyMainOrder(id)
                    sl_order_id = df_SL_order.id.iloc[0]
                    # Update sl_order_id
                    new_orderid = self.zerodha.modifyStopLossOrder_market(sl_order_id)
                    #modifyStopLossOrderTriggerPrice(self, order_id, triggerPrice, parent_order)
                    print(f'Change Order Status {id=}')
                    await self.db.update_status_by_id(id, 2)
                    await self.db.update_status_by_id(sl_order_id, 2)
                    await self.db.insert_trade_log(self.current_date_string, self.strategy_name, 'Convert SL-Market', str(new_orderid), 4, '', self.CurrentDateTime_str)
                else:
                    LTP = 0
                    for retry in range(10):
                        LTP = float(self.zerodha.getCurrentLTP('NFO:' + tradingsymbol))
                        if LTP > 0:
                            break  
                        else:
                            asyncio.sleep(1)
                    if LTP == 0:
                        info = f"Couldn't get LTP for {tradingsymbol}"
                        print(info)
                        await self.db.insert_trade_log(self.current_date_string, self.strategy_name, 'process exit', 'Could not get price' + tradingsymbol, 4, '', self.CurrentDateTime_str)
                    else:
                        result = await self.sell(tradingsymbol, qty, LTP, id)
                        if result == 1:
                            print(f'Change Order Status {id=}')
                            await self.db.update_status_by_id(id, 2)
                        else:
                            print('Could not place order', tradingsymbol)
                            await self.db.insert_trade_log(self.current_date_string, self.strategy_name, 'place SELL trade', 'Could not place order', 4, '', self.CurrentDateTime_str)
                            
            self.run_job = False
            self.status = 2
            return 1
        elif len(df_orders) == 0:
            print('No open positions')
            return 1
        elif self.live == 0:
            print('in self.live == 0 check for stoploss')
            # Fetch Stop loss orders
            df_sl_orders = await self.db.get_SL_Orders()
            for index, row in df_sl_orders.iterrows():
                id = row['id']
                tradingsymbol = row['symbol']
                qty = int(row['qty'])
                price_executed = float(row['price_executed'])
                main_order_id = int(row['main_order_id'])
                print(f'In exit orders {tradingsymbol}')
                LTP = 0
                for retry in range(10):
                    LTP = float(self.zerodha.getCurrentLTP('NFO:' + tradingsymbol))
                    if LTP > 0:
                        break  
                    else:
                        asyncio.sleep(1)
                if LTP == 0:
                    info = f"Couldn't get LTP for {tradingsymbol}"
                    print(info)
                else:
                    print(f"{tradingsymbol}, {LTP=}")
                if LTP < price_executed:
                    info = f"LTP:{LTP} < SL price_executed:{price_executed}"
                    print(info)
                    print(f"change status of main order to 2 {id=}")
                    await self.db.update_status_by_id(main_order_id, 2)
                    await self.db.update_status_by_id(id, 2)
                    await self.db.run_query(f"update trade_book set current_price = '{LTP}' where id = {main_order_id}")
                    await self.db.insert_trade_log(self.current_date_string, self.strategy_name, 'stop loss hit', info, 4, '', self.CurrentDateTime_str)
                else:
                    await self.db.run_query(f"update trade_book set current_price = '{LTP}' where id = {main_order_id}")
        # Check target
        print('Checking Target')
        for index, row in df_orders.iterrows():
            id = row['id']
            tradingsymbol = row['symbol']
            target = row['target']
            qty = int(row['qty'])
            LTP = 0
            for retry in range(10):
                LTP = float(self.zerodha.getCurrentLTP('NFO:' + tradingsymbol))
                if LTP > 0:
                    break  
                else:
                    asyncio.sleep(1)
            if LTP == 0:
                info = f"Couldn't get LTP for {tradingsymbol}"
                print(info)
            else:
                print(f"{tradingsymbol}, {LTP=}")
            if LTP > target:
                # Fetch Stoploss order

                # Execute Stoploss order

                info = f"LTP:{LTP} > target:{target}"
                print(info)
                print(f"change status of main order to 2 {id=}")
                await self.db.update_status_by_id(main_order_id, 2)
                await self.db.update_status_by_id(id, 2)
                await self.db.run_query(f"update trade_book set current_price = '{LTP}' where id = {main_order_id}")
                
                await self.db.insert_trade_log(self.current_date_string, self.strategy_name, 'target hit', info, 4, '', self.CurrentDateTime_str)
            else:
                await self.db.run_query(f"update trade_book set current_price = '{LTP}' where id = {main_order_id}")
    async def MonitorCurrentPosition(self):
        print('*************** Monitor Position ********')            
        nw = self.zerodha.getCurrentDateTime()
        tdate = self.zerodha.getCurrentDate()
        if self.live == 1:
            placed_orders = await self.db.get_placedOrders()
            for index, row in placed_orders.iterrows():
                orderid = row['broker_id']
                #symbol = row['symbol']
                #qty = int(row['qty'])
                #transaction = row['transaction_type']
                orderhistory = self.zerodha.getOrderHistory(orderid)
                k = 0
                while k < len(orderhistory):
                    item_dic = orderhistory[k]
                    status = item_dic["status"]
                    info = item_dic['status_message']
                    if status == 'REJECTED':
                        print('Attention! Order rejected by Zerodha', info)
                        await self.db.updateOrder(-2, 0, orderid) 
                        await self.db.insert_trade_log(date_log=tdate, module='monitor', activity='Fresh Order rejected', important_data=info, priority=4, strategy_trade_id = '', timestamp=nw)
                    elif status == 'COMPLETE':
                        trades = self.zerodha.gettradeby_order(orderid)
                        for i in trades:
                            price = i['average_price']
                            time = i['exchange_timestamp']     
                            await self.db.updateOrder(1, price, orderid) 
                            info = 'orderid: ' + str(orderid) + ' price:' + str(price) + ' time:' + str(time)
                            await self.db.insert_trade_log(date_log=tdate, module='monitor', activity='Fresh Order rejected', important_data=info, priority=4, strategy_trade_id = '', timestamp=nw)
                    elif status == 'CANCELLED':
                        await self.db.updateOrder(-3, 0, orderid) 
                        info = 'orderid: ' + str(orderid) 
                        await self.db.insert_trade_log(date_log=tdate, module='monitor', activity='Fresh Order rejected', important_data=info, priority=4, strategy_trade_id = '', timestamp=nw)
                    k = k + 1
            # Confirm Stop Loss Order Executed
            SL_placed_orders = await self.db.get_SL_placedOrders()
            for index, row in SL_placed_orders.iterrows():
                orderid = row['broker_id']
                orderhistory = self.zerodha.getOrderHistory(orderid)
                k = 0
                while k < len(orderhistory):
                    item_dic = orderhistory[k]
                    status = item_dic["status"]
                    info = item_dic['status_message']
                    if status == 'REJECTED':
                        print('Attention! Order rejected by Zerodha', info)
                        await self.db.updateOrder(-2, 0, orderid) 
                        await self.db.insert_trade_log(date_log=tdate, module='monitor', activity='Fresh Order rejected', important_data=info, priority=4, strategy_trade_id = '', timestamp=nw)
                    elif status == 'COMPLETE':
                        trades = self.zerodha.gettradeby_order(orderid)
                        for i in trades:
                            price = i['average_price']
                            time = i['exchange_timestamp']     
                            await self.db.updateOrder(1, price, orderid) 
                            info = 'orderid: ' + str(orderid) + ' price:' + str(price) + ' time:' + str(time)
                            await self.db.insert_trade_log(date_log=tdate, module='monitor', activity='Fresh Order rejected', important_data=info, priority=4, strategy_trade_id = '', timestamp=nw)
                    elif status == 'CANCELLED':
                        await self.db.updateOrder(-3, 0, orderid) 
                        info = 'orderid: ' + str(orderid) 
                        await self.db.insert_trade_log(date_log=tdate, module='monitor', activity='Fresh Order rejected', important_data=info, priority=4, strategy_trade_id = '', timestamp=nw)
                    k = k + 1
        # Place Stoploss Orders
        print('Placing Stop Loss orders')
        executed_orders = await self.db.get_executed_Orders_forSL()
        print('Executed orders for SL', executed_orders)
        for index, row in executed_orders.iterrows():
            id = int(row['id'])
            orderid = row['broker_id']
            tradingsymbol = row['symbol']
            qty = int(row['qty'])
            transaction = int(row['transaction_type'])
            stop_loss = float(row['stop_loss'])
            if transaction == 1:
                result = await self.sell_Stoploss(tradingsymbol, qty, stop_loss, id)
                #orderid = self.zerodha.PlaceStopLossSellOrderMarket(tradingsymbol, qty, stop_loss)
                if result == 1:
                    await self.db.run_query(f"update Trade_Book set StopLossStatus = 1 where id = {id}")

async def main():
    today = date.today()
    start = Start(0, today, False)
    await start.start_pool()
    await start.initiate()
    await start.close_pool()
    while True:
        if start.backtest == 1:
            start.zerodha.advance_time('second', 60)
        CurrentDateTime = start.zerodha.getCurrentDateTime()
        current_time = CurrentDateTime.time()
        if current_time < start.exit_time:
            await start.start_pool()
            await start.process_exit()
            await start.process_entry()
            await start.MonitorCurrentPosition()
            await start.close_pool()
            if start.backtest == 1:
                pass
            else:
                await asyncio.sleep(1)
        # elif current_time.second % 10 == 0 and current_time < start.exit_time:
        #     if start.live == 1:
        #         await start.start_pool()
        #         await start.MonitorCurrentPosition()
        #         await start.close_pool()
        #     if start.backtest == 1:
        #         pass
        #     else:
        #         await asyncio.sleep(1)
        elif start.position== 0 and current_time >= start.exit_time:
            print('Exitting Market time Over')
            break
        else:
            #print('Jiva bot ', CurrentDateTime.time())
            pass
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    asyncio.run(main())


