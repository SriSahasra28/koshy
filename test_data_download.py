from background.zerodha import zeroda
from datetime import datetime, timedelta, date, time as tm
zerodha = zeroda('live', datetime.today())
import pandas as pd
def get_data_zerodha_recursive_list(interval, from_date, edate, token, symbol):
    print(f" ---------- in get_data_zerodha_recursive_list {symbol} {interval} {from_date=} {edate=}")
    to_date = edate
    data_list = []  
    days = 95 
    if interval == 'minute':
        days = 10
    invalid_token = False
    error = ''
    
    while from_date < edate:
        if from_date >= (edate - timedelta(days)):
            status, data, Error = zerodha.gethistoricaldata_v3(token, from_date, edate, interval)
            if status == 0:
                error = "{}".format(Error)
                print(f"get_data_zerodha_recursive {error=}")
                if error == 'invalid token':
                    invalid_token = True
                    info = f"invalid token {symbol}"
                elif Error == 'Too many requests':
                    info = f"{Error}"
                break
            else:
                data_list.extend(data)  # Append the list of dictionaries
            break
        else:
            to_date = from_date + timedelta(days)
            status, data, Error = zerodha.gethistoricaldata_v3(token, from_date, to_date, interval)
            if status == 0:
                if Error == 'invalid token':
                    info = f"invalid token {symbol}"
                    print(info)
                elif Error == 'Too many requests':
                    info = f"{Error}"
                break
            else:
                data_list.extend(data)  # Append the list of dictionaries
            from_date = to_date
    
    if data_list:
        return 1, data_list, None  # Return the merged list of dictionaries
    else:
        print("No data to process")
        return 0, None, error
interval = 'minute'
last_datetime = datetime.today() - timedelta(days=5)
end_date_today = datetime.today().replace(hour=15, minute=30, second=0, microsecond=0)
last_datetime = last_datetime.replace(hour=9, minute=15, second=0, microsecond=0)
instrument_token = 21104130
exchange_code = 'BAJFINANCE24OCT6900PE'
# print(f"{last_datetime=}")
# print(f"{end_date_today=}")
# status, data, Error = get_data_zerodha_recursive_list(interval, last_datetime, end_date_today, instrument_token, exchange_code)
# dates_new = [entry['date'].replace(tzinfo=None) for entry in data]  # Convert datetime to timezone-naive
# opens = [entry['open'] for entry in data]
# highs = [entry['high'] for entry in data]
# lows = [entry['low'] for entry in data]
# closes = [entry['close'] for entry in data]
# ohlc_df = pd.DataFrame({
#     'datetime': dates_new,
#     'open': opens,
#     'high': highs,
#     'low': lows,
#     'close': closes
# })
# ohlc_df.to_csv('BAJFINANCE6900PE_API.csv', index=False)
# print(ohlc_df)

datetime_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
print(datetime_str)