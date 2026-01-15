import mysql.connector
import pandas as pd
import time
from datetime import datetime, timedelta
import os
current_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_directory)
from background.login import login
import traceback

import requests
def send_telegram_message(message):
    """Send a message to a Telegram chat via the bot."""
    bot_token = '1936528227:AAFQwZV4z5AgSKEU9FW25Plnq-mTzXJY7Qw'
    chat_id = '@koshi_alerts' 
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': message}

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()  # Raises HTTPError for bad requests
        print("MESSAGE FROM FILTER OPTIONS")
        print("Message sent successfully")
    except requests.exceptions.RequestException as e:
        print(f"Error sending message: {e}")      

try:
    # Database connection parameters
    config = {
        'user': 'root',
        'password': 'Airforce*123',
        'host': '103.160.145.141',
        'port': '3306',
        'database': 'algo'
    }
    # Your script logic here
    print("Script started...")

    l = login(False)
    zerodha_login_status, kite, kws, token = l.InitiateZerodha()

    def get_historical_close(symbol, nse_nfo , instrument_token,expiry):
        try:
            # Get today's date and time
            today = datetime.now()

            # Target time to include (15:29:00+05:30)
            target_time = "15:29:00+05:30"
            formatted_target_time = datetime.strptime(target_time, "%H:%M:%S%z").timetz()

            # Check if today is Saturday (5) or Sunday (6)
            if today.weekday() in [5, 6]:
                # Adjust to last Friday (weekday 4)
                days_to_subtract = today.weekday() - 4
                target_date = today - timedelta(days=days_to_subtract)
            else:
                # If not Saturday or Sunday, use today's date
                target_date = today
            target_date = target_date.replace(hour=15, minute=29, second=0, microsecond=0, tzinfo=formatted_target_time.tzinfo)

            print(nse_nfo)
            if nse_nfo == 'NSE' :
                target_days = 30
                interval = 'day'
                folder_path = f"C:/Users/Administrator/Desktop/React/build/price_data/{nse_nfo}"
            else :
                target_days = 60
                interval = 'minute'
                expiry = expiry.strftime('%Y%m')
                folder_path = f"C:/Users/Administrator/Desktop/React/build/price_data/{nse_nfo}/{expiry}"

            from_date = target_date - timedelta(days=target_days)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            file_name = f"{folder_path}/{symbol}.csv"

            if os.path.exists(file_name):
                last_modified_time = datetime.fromtimestamp(os.path.getmtime(file_name))
                if datetime.now() - last_modified_time > timedelta(hours=4):
                    pass
                else :
                    print('skipping_____')
                    return

                # Read the existing CSV
                df = pd.read_csv(file_name)
                
                if not df.empty:
                    # Get the last row and value from the 'date' column
                    last_row = df.iloc[-1]
                    from_date = last_row['date']
                    from_date = datetime.fromisoformat(from_date)

            
            if target_date == from_date :
                print('skipping_____')
                return

            # Adjust target_date to one day before                    
            # Get the historical data (assuming 'symbol' is in the format 'NSE:<symbol>')
            try:
                historical_data = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=from_date.strftime('%Y-%m-%d'),
                    to_date=target_date.strftime('%Y-%m-%d'),
                    interval=interval
                )
                # print(target_date,from_date)
                # print(historical_data)

            except Exception as e:
                print(f"Error fetching historical data: {e}")
                historical_data = None  # Or handle it in another way, like setting a default value or retry logic
            
            time.sleep(1)

            # Assuming `historical_data` is a list of dictionaries with the fetched data
            if historical_data:
                new_data_df = pd.DataFrame(historical_data)
                new_data_df['symbol'] = symbol  # Add the symbol column
                new_data_df['date'] = pd.to_datetime(new_data_df['date'], errors='coerce')  # Ensure date format is correct

                if os.path.exists(file_name):
                    existing_df = pd.read_csv(file_name)
                    existing_df['date'] = pd.to_datetime(existing_df['date'], errors='coerce')  # Convert the 'date' column to datetime

                    # print("Existing Data Before Merge:", existing_df.tail())
                    # print("New Data Before Merge:", new_data_df.head())

                    combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
                    combined_df.drop_duplicates(subset='date', inplace=True)
                    combined_df.dropna(subset=['date'], inplace=True)
                    combined_df.sort_values(by='date', inplace=True)

                    combined_df.to_csv(file_name, index=False)
                    print(f"Historical data for {symbol} updated and saved to {file_name}")
                    # print("Combined Data After Merge:", combined_df.tail())  # Show last few rows after merge
                else:
                    new_data_df.sort_values(by='date', inplace=True)
                    new_data_df.to_csv(file_name, index=False)
                    print(f"New historical data file created for {symbol} at {file_name}")

            else:
                # If no historical data is found, create a blank file with the appropriate columns
                columns =  ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']
                blank_df = pd.DataFrame(columns=columns)
                blank_df.to_csv(file_name, index=False)
                print(f"Blank file created for {symbol} at {file_name}")
                print(f"No historical data found for {symbol}.")

        except Exception as e:
            print(f"Error fetching historical close for {symbol}: {e}")
            traceback.print_exc()  # Print the traceback information
            # input("Press Enter to exit...")

    def fetch_and_store_tables(tables):

        # Establish a database connection
        db = mysql.connector.connect(**config)
        table_data = {}  # Dictionary to store table data

        try:
            for table_name in tables:
                print(f"Fetching data from table: {table_name}")

                # Read data into a DataFrame
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, db)

                # Store the DataFrame in a dictionary for later use
                table_data[table_name] = df
                print(f"Data from {table_name} stored in variable.")

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            # Close database connection
            db.close()
            print("Database connection closed.")

        return table_data

    # List of tables to fetch
    tables_to_fetch = ['basket_stocks', 'instruments','baskets','scans']

    # Call the function and store the tables in a variable
    tables_data = fetch_and_store_tables(tables_to_fetch)

    # Accessing the stored DataFrames
    basket_stocks_df = tables_data['basket_stocks']
    instruments_df = tables_data['instruments']
    groups = tables_data['baskets']
    scans_df = tables_data['scans']
    basket_id_at_scans = scans_df['basket_id'].unique().tolist()

    # saves all instrumnet data ____
    send_telegram_message('Options Filtering Started') 
    date = datetime.now()
    if 1 == 1  :        
        under_lying = {}
        for index, row in instruments_df.iterrows():
            if row['exchange'] == 'NFO' :
                # donwload futues data ___
                symbol = row['tradingsymbol']
                # Check if the expiry is in the current month
                expiry_date = row['expiry']
                current_date = datetime.now()
       
                # print(symbol)
                if row['instrument_type'] == 'FUT':
                    print(" FUT INSTRUMENT FOUND ") 
                    print(symbol ,expiry_date.month, current_date.month, expiry_date.year, expiry_date.year)
                    if expiry_date.month - 1 == current_date.month and expiry_date.year == current_date.year:
                        print("GETTING FUT DATAAA")
                        get_historical_close(symbol, row['exchange'] , row['instrument_token'],row['expiry'])


                if row['name'] in basket_stocks_df['symbol'].values :                         
                    expiry_date = row['expiry']  # datetime.date object
                    current_date = datetime.now()
                    four_months_away = current_date + timedelta(days=4 * 30)  # Approximation of 4 months

                    # Convert expiry_date to datetime for comparison
                    expiry_date = datetime.combine(expiry_date, datetime.min.time())

                    # Check if expiry date is 4 months or more away
                    if expiry_date >= four_months_away:
                        continue  # Skip this row                
                                                                    
                    symbol = row['name']

                    if symbol == 'NIFTY':
                        symbol = 'NIFTY 50'
                    elif symbol == 'BANKNIFTY':
                        symbol = 'NIFTY BANK'
                    elif symbol == 'FINNIFTY':
                        symbol = 'NIFTY FIN SERVICE'
                    elif symbol == 'MIDCPNIFTY':
                        symbol = 'NIFTY MIDCAP 50'
                    under_lying[symbol] = 0
        for symbol in under_lying :
            result = instruments_df[(instruments_df['exchange'] == 'NSE') & (instruments_df['tradingsymbol'] == symbol)]
            if result.empty:
                continue
            result = result.iloc[0]
            get_historical_close(symbol, result['exchange'] , result['instrument_token'],result['expiry'])

    def get_ltp (symbol,nse_nfo,expiry) :
        folder_path = f"C:/Users/Administrator/Desktop/React/build/price_data/{nse_nfo}"
        if nse_nfo == 'NFO' :
            expiry = expiry.strftime('%Y%m')
            folder_path = f"C:/Users/Administrator/Desktop/React/build/price_data/{nse_nfo}/{expiry}"
        file_name = f"{folder_path}/{symbol}.csv"
        print(file_name)

        # Check if the CSV file exists
        previous_close = None
        if os.path.exists(file_name):
            # Read the CSV file
            df = pd.read_csv(file_name)
            if not df.empty:
                # Get the last row
                last_row = df.iloc[-1]
                # Get the value from the 'close' column
                previous_close = last_row['close']  
        return previous_close         

    filtered_strikes = {}
    for index, row in basket_stocks_df.iterrows():
        basket_id = row['basket_id']
        group_name = groups.loc[groups['id'] == row['basket_id'], 'group_name'].iloc[0]
        symbol = row['symbol']
        date_A = row['date_A'] if 'date_A' in row else None  # Ensure date_A exists
        date_B = row['date_B'] if 'date_B' in row else None  # Ensure date_B exists
        config_1m = row['config_1m'] if 'config_1m' in row else None
        config_2m = row['config_2m'] if 'config_2m' in row else None
        config_3m = row['config_3m'] if 'config_3m' in row else None
        ce_lower_val = row['ce_lower_val'] if 'ce_lower_val' in row else None
        ce_upper_val = row['ce_upper_val'] if 'ce_upper_val' in row else None
        pe_lower_val = row['pe_lower_val'] if 'pe_lower_val' in row else None
        pe_upper_val = row['pe_upper_val'] if 'pe_upper_val' in row else None
        filter_val = row['filter'] if 'filter' in row else None
        filter_from = row['filter_from'] if 'filter_from' in row else None
        filter_to = row['filter_to'] if 'filter_to' in row else None
        scan_1m = row['scan_1m'] if 'scan_1m' in row else None
        scan_2m = row['scan_2m'] if 'scan_2m' in row else None
        scan_3m = row['scan_3m'] if 'scan_3m' in row else None
        scan_ce_value = row['scan_ce_value'] if 'scan_ce_value' in row else None
        scan_pe_value = row['scan_pe_value'] if 'scan_pe_value' in row else None
        scan_range_from = row['scan_range_from'] if 'scan_range_from' in row else None
        scan_range_to = row['scan_range_to'] if 'scan_range_to' in row else None

        checkbox_1mf = row['checkbox_1mf']
        checkbox_2mf = row['checkbox_2mf']
        checkbox_3mf = row['checkbox_3mf']

        if pd.isna(symbol) or symbol.strip() == '':
            continue

        # Determine the option_symbol based on the symbol
        option_symbol = symbol
        csv_symbol = symbol
        if option_symbol == 'NIFTY':
            csv_symbol = 'NIFTY 50'
        elif option_symbol == 'BANKNIFTY':
            csv_symbol = 'NIFTY BANK'
        elif option_symbol == 'FINNIFTY':
            csv_symbol = 'NIFTY FIN SERVICE'
        elif option_symbol == 'MIDCPNIFTY':
            csv_symbol = 'NIFTY MIDCAP 50'

        # Print the current symbol and option_symbol
        print(symbol)

        this_month = date_A.replace(day=1).strftime('%Y-%m')
        next_month = (date_A.replace(day=28) + timedelta(days=4)).replace(day=1).strftime('%Y-%m')
        next_next_month = (date_A.replace(day=28) + timedelta(days=35)).replace(day=1).strftime('%Y-%m')

        instruments_df['expiry'] = pd.to_datetime(instruments_df['expiry'], errors='coerce')
        instruments_df['expiry_ym'] = instruments_df['expiry'].dt.strftime('%Y-%m')

        filtered_instruments = instruments_df[
            (instruments_df['name'] == option_symbol) &
            (instruments_df['instrument_type'] == 'FUT') ]
        filtered_instruments = filtered_instruments.sort_values(by='strike', ascending=True)

        # Add Fututres ___
        temp_array_c = {}
        for _, instrument_row in filtered_instruments.iterrows():
            if instrument_row['instrument_type'] == 'FUT' :
                print(instrument_row['expiry_ym'],this_month,checkbox_1mf)
                if instrument_row['expiry_ym'] == this_month and checkbox_1mf == 1 :
                    temp_array_c[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}
                if instrument_row['expiry_ym'] == next_month and checkbox_2mf == 1 :
                    temp_array_c[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}
                if instrument_row['expiry_ym'] == next_next_month and checkbox_3mf == 1 :
                    temp_array_c[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}
        filtered_strikes.update(temp_array_c)

        option_array = []
        if row['ceChecked'] == 1 :
            option_array.append('CE')
        if row['peChecked'] == 1 :
            option_array.append('PE')

        for option_type in option_array :
            temp_array_a = {}
            if row['checkbox_config'] == 1 :
                previous_close  = get_ltp(csv_symbol,'NSE','NONE')     
                                
                # strikes filtering___
                for conditions in [[config_1m,this_month],[config_2m,next_month],[config_3m,next_next_month]] :
                    print(conditions,date_A)
                    if conditions[0] is not None :
                        if conditions[0] == 1 :

                            filtered_instruments = instruments_df[
                                (instruments_df['name'] == option_symbol) &
                                (instruments_df['instrument_type'] == option_type)  &
                                (instruments_df['expiry_ym'] == conditions[1]) ]
                            filtered_instruments = filtered_instruments.sort_values(by='strike', ascending=True)
                            if filtered_instruments.empty:
                                print(option_symbol,'Empty')
                                continue

                            # ATM and strike identification_____
                            ATM, prev_strike = 0 , 0 
                            for _, instrument_row in filtered_instruments.iterrows():
                                if previous_close is not None and instrument_row['strike'] > previous_close:
                                    ATM = instrument_row['strike']
                                    continue
                                prev_strike = instrument_row['strike']
                            strike_size = abs(ATM-prev_strike)

                            # Adding stikes basis range____
                            for _, instrument_row in filtered_instruments.iterrows():
                                if ATM == 0 or strike_size == 0 :
                                    continue
                                if instrument_row['expiry'].date() <= date_A.date():
                                    continue
                                if instrument_row['strike'] == ATM and 'CE' == option_type :
                                    temp_array_a[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}
                                elif instrument_row['strike'] < ATM and abs(instrument_row['strike'] -ATM) <= strike_size*(ce_lower_val-1) and 'CE' == option_type :
                                    temp_array_a[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}
                                elif instrument_row['strike'] > ATM and abs(instrument_row['strike'] -ATM) <= strike_size*(ce_upper_val-1) and 'CE' == option_type  :
                                    temp_array_a[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}

                                elif instrument_row['strike'] == ATM and 'PE' == option_type :
                                    temp_array_a[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}                                    
                                elif instrument_row['strike'] < ATM and abs(instrument_row['strike'] -ATM) <= strike_size*(pe_lower_val-1) and 'PE' == option_type  :
                                    temp_array_a[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}
                                elif instrument_row['strike'] > ATM and abs(instrument_row['strike'] -ATM) <= strike_size*(pe_upper_val-1) and 'PE' == option_type  :
                                    temp_array_a[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}
                print(temp_array_a)
                array_a = temp_array_a.copy()  # Create a copy of the original dictionary    
                # applying filter ___
                if filter_val is not None :
                    if filter_val == 1 :
                        for key in list(temp_array_a.keys()):  # Iterate over a list of the keys
                            try:
                                get_historical_close(key, 'NFO' , temp_array_a[key]['instrument_token'],temp_array_a[key]['expiry'])   
                                ltp = get_ltp(key,'NFO',temp_array_a[key]['expiry'])    
                                print(ltp) 
                                # Check if the LTP is within the desired range
                                if ltp is not None and not (filter_from < ltp < filter_to):
                                    del array_a[key]
                            except Exception as e:
                                print(f"Error fetching quote for {key}: {e}")                            
                filtered_strikes.update(array_a)

            # Scan ___
            if row['checkbox_scan'] == 1 :
                this_month = date_B.replace(day=1).strftime('%Y-%m')
                next_month = (date_B.replace(day=28) + timedelta(days=4)).replace(day=1).strftime('%Y-%m')
                next_next_month = (date_B.replace(day=28) + timedelta(days=35)).replace(day=1).strftime('%Y-%m')

                temp_arr_b = {}
                for conditions in [[scan_1m,this_month],[scan_2m,next_month],[scan_3m,next_next_month]] :
                    if conditions[0] is not None :
                        if conditions[0] == 1 :
                            instruments_df['expiry'] = pd.to_datetime(instruments_df['expiry'], errors='coerce')
                            instruments_df['expiry_ym'] = instruments_df['expiry'].dt.strftime('%Y-%m')
                            filtered_instruments = instruments_df[
                                (instruments_df['name'] == option_symbol) &
                                (instruments_df['instrument_type'] == option_type) &
                                (instruments_df['expiry_ym'] == conditions[1]) ]
                            filtered_instruments = filtered_instruments.sort_values(by='strike', ascending=True)
                            if filtered_instruments.empty:
                                continue
                            for _, instrument_row in filtered_instruments.iterrows():  
                                if instrument_row['expiry'].date() <= date_B.date():
                                    continue                        
                                temp_arr_b[instrument_row['tradingsymbol']] = {'group':group_name, 'symbol' : symbol, 'expiry' : instrument_row['expiry'], 'instrument_token' : instrument_row['instrument_token'], 'basket_id' : basket_id}
            
                arr_b = {}  # Initialize an empty dictionary to store filtered LTP values
                for key in temp_arr_b:
                    try:
                        get_historical_close(key, 'NFO' , temp_arr_b[key]['instrument_token'],temp_arr_b[key]['expiry'])  
                        ltp = get_ltp(key,'NFO',temp_arr_b[key]['expiry']) 
                        print(key,ltp)
                        if ltp is not None and scan_range_from <= ltp <= scan_range_to:
                            arr_b[key] = ltp  # Add key and LTP to temp_arr_b if within range
                    except Exception as e:
                        print(f"Error fetching quote for {key}: {e}")                    
                arr_b = dict(sorted(arr_b.items(), key=lambda item: item[1], reverse=True))
                
                if 'CE' == option_type :
                    arr_b = dict(list(arr_b.items())[-int(scan_ce_value):])
                if 'PE' == option_type :
                    arr_b = dict(list(arr_b.items())[-int(scan_pe_value):])
                print(arr_b)
                temp_arr_b = {key: temp_arr_b[key] for key in temp_arr_b if key in arr_b}
                print(temp_arr_b)

                filtered_strikes.update(temp_arr_b)


    print(filtered_strikes)
    # time.sleep(200)

    # Save the instruments DataFrame to a CSV file
    output_data = []
    for key, value in filtered_strikes.items():
        output_data.append({
            'group_name': value['group'],
            'symbol': value['symbol'],
            'option': key ,
            'expiry': value['expiry'] ,
            'instrument_token': value['instrument_token'],
            'basket_id': value['basket_id']
        })

    # Insert the data into the filter_options table

    # Establish a database connection
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    # Delete existing entries from the filter_options table
    delete_query = "DELETE FROM filter_options WHERE mode = 'Auto'"
    cursor.execute(delete_query)
    print("Existing entries in filter_options table have been deleted.")

    # Deleting deleted groups ___
    unique_group_names = groups['group_name'].unique().tolist()
    group_names_str = ', '.join(f"'{name}'" for name in unique_group_names)
    print(group_names_str)
    delete_query = f"DELETE FROM filter_options WHERE group_name NOT IN ({group_names_str})"
    # Execute the query using your cursor
    cursor.execute(delete_query)
    print("Entries not present in the DataFrame have been deleted from filter_options table.")

    # Insert new data into the filter_options table
    insert_query = """
        INSERT INTO filter_options (group_name, symbol, option_name, expiry, instrument_token,basket_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    insert_data = [(row['group_name'], row['symbol'], row['option'], row['expiry'], row['instrument_token'],row['basket_id']) for row in output_data]
    cursor.executemany(insert_query, insert_data)

    # Commit the transaction
    db.commit()
    print(f"{cursor.rowcount} rows have been inserted into the filter_options table.")

    # Save the filtered DataFrame to a CSV file
    tables_to_fetch = ['filter_options']
    tables_data = fetch_and_store_tables(tables_to_fetch)
    filter_options = tables_data['filter_options']   
    filtered_df = filter_options[filter_options['basket_id'].isin(basket_id_at_scans)]    
    filtered_df.to_csv('C:/Users/Administrator/Desktop/React/build/price_data/scan_instruments.csv', index=False)
    print("Filtered DataFrame has been saved to 'scan_instruments.csv'.")

    # Close the database connection
    if db.is_connected():
        cursor.close()
        db.close()
        print("Database connection closed.")
    send_telegram_message('Options Filtering Completed') 
except Exception as e:
    print("An error occurred:", str(e))
    traceback.print_exc()  # Print the traceback information
    send_telegram_message('Options Filtering # Error') 
    input("Press Enter to exit...")

# input("Press Enter to exit...")