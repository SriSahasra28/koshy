import os
import requests
import mysql.connector
import pandas as pd
from datetime import datetime
import traceback
import time
import io  # Import StringIO from io module
from background.indicators_rolling import *
import json

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
        print("MESSAGE FROM BACK TEST")
        print("Message sent successfully")
    except requests.exceptions.RequestException as e:
        print(f"Error sending message: {e}") 

# File paths
BACK_TEST_FILE = r"C:\Users\Administrator\Desktop\React\build\price_data\back_test.txt"
LOG_FILE = r"C:\Users\Administrator\Desktop\React\build\price_data\back_test_log.txt"
OUTPUT_DIR = "C:/Users/Administrator/Desktop/React/build/price_data/back_test"

config = {
    'user': 'root',
    'password': 'Airforce*123',
    'host': '103.160.145.141',
    'port': '3306',
    'database': 'algo'
}


def clear_log(msg):
    """Clears the log file at the start of execution."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'w') as log_file:
        log_file.write(f"[{timestamp}] {msg}\n")  # Include timestamp
    print(f"Log file cleared at {timestamp}.")

def log_message(message):
    """Logs a message to back_test_log.txt."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'a') as log_file:  # Changed to append mode
        log_file.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

def read_back_test_id():
    """Reads the back_test_id from back_test.txt."""
    if os.path.exists(BACK_TEST_FILE):
        with open(BACK_TEST_FILE, 'r') as file:
            back_test_id = file.read().strip()
            return back_test_id
    else:
        print(f"Error: {BACK_TEST_FILE} not found.")
        return None

def fetch_and_store_tables(tables):
    """Fetches data from specified database tables and stores them in DataFrames."""
    print("Connecting to the database...")
    db = mysql.connector.connect(**config)
    table_data = {}

    try:
        for table_name in tables:
            print(f"Fetching data from table: {table_name}")
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, db)
            table_data[table_name] = df
            print(f"Data from {table_name} stored successfully.")
    except Exception as e:
        print(f"An error occurred while fetching data: {e}")
    finally:
        db.close()
        print("Database connection closed.")

    return table_data

def save_csv_as_excel(csv_data, strike_name, frame, output_dir, period, std_multiplier, psar_af, psar_max_af, stoch_k_period, stoch_d_period, stoch_smoothing,condition_name,kline_thres,psar_candles,hlfpid,back_test_name):
    """Saves CSV data in Excel format for a specific time frame with indicator values."""
    global total_alerts
    try:
        current_datetime = datetime.now()

        # Use io.StringIO to read CSV data into a DataFrame
        df = pd.read_csv(io.StringIO(csv_data))
        
        # Ensure the time column is in datetime format and timezone-unaware
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        df.set_index('date', inplace=True)

        # Resample the data to the given time frame
        resampled_df = df.resample(f'{frame}min', offset='15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

        resampled_df['heikin_open'], resampled_df['heikin_high'], resampled_df['heikin_low'], resampled_df['heikin_close'] = heikin_ashi_numpy(
            resampled_df['open'].values,
            resampled_df['high'].values,
            resampled_df['low'].values,
            resampled_df['close'].values            )
        resampled_df['heikin_open'] = resampled_df['heikin_open'].round(2)
        resampled_df['heikin_high'] = resampled_df['heikin_high'].round(2)
        resampled_df['heikin_low'] = resampled_df['heikin_low'].round(2)
        resampled_df['heikin_close'] = resampled_df['heikin_close'].round(2)

        # Determine Heikin-Ashi candle color and type
        resampled_df['heikin_color'] = resampled_df.apply(
            lambda row: 'Green' if row['heikin_close'] > row['heikin_open'] else 'Red',
            axis=1
        )

        def classify_candle(row):
            body_size = abs(row['heikin_close'] - row['heikin_open'])
            upper_wick = row['heikin_high'] - max(row['heikin_close'], row['heikin_open'])
            lower_wick = min(row['heikin_close'], row['heikin_open']) - row['heikin_low']
            
            if row['heikin_close'] > row['heikin_open']:  # Green candles only
                if upper_wick == 0 and lower_wick == 0:
                    return "Green Type 3 (Wickless)"
                elif upper_wick > 2 * body_size and lower_wick < body_size:
                    return "Green Type 2 (Pin Bar)"
                return "Green Type 1 (Normal)"
            return None  # No classification for red or other candles

        resampled_df['heikin_candle_type'] = resampled_df.apply(classify_candle, axis=1)

        # Add indicators using custom values
        resampled_df['fast_k'], resampled_df['fast_d'] = calc_fastStochastics(
            resampled_df['low'].values,
            resampled_df['high'].values,
            resampled_df['close'].values,
            lookback_period=stoch_k_period,
            d_period=stoch_d_period,
            k_smoothing_period=stoch_smoothing
        )

        resampled_df['psar'] = psar(
            resampled_df['high'].values,
            resampled_df['low'].values,
            resampled_df['close'].values,
            af0=psar_af,
            max_af=psar_max_af
        )

        resampled_df['psar_signal'] = get_psar_signals(
            resampled_df['close'].values,
            resampled_df['psar'].values
        )
        
        resampled_df['lr_line'], resampled_df['lr_ucl'], resampled_df['lr_lcl'], resampled_df['lr_angle'] = linear_regression_channel_numba_sliding(
            resampled_df['close'].values,
            period=period,
            std_multiplier=std_multiplier
        )
                        
        # Check conditions for alert   
        alert_condition = 0     

        # Define a new column for the K line cross condition
        resampled_df['k_cross_below'] = (resampled_df['fast_k'] < kline_thres) & (resampled_df['fast_k'].shift(1) >= kline_thres)

        # Define a function to check all conditions
        def check_conditions(df, idx, psar_candles, hlfpid):
            current_row = df.iloc[idx]
            # Check if necessary columns are not NA or empty
            if pd.notna(current_row['psar']) and pd.notna(current_row['heikin_color']) and pd.notna(current_row['heikin_high']) and pd.notna(current_row['heikin_candle_type']):
                # Check if current conditions are met
                if (current_row['psar'] < current_row['close'] and
                    current_row['heikin_color'] == 'Green' and
                    current_row['heikin_high'] < current_row['lr_line']):
                    # Look back within the last psar_candles to check for a k_cross_below
                    start_idx = max(0, idx - psar_candles)
                    past_range = df.iloc[start_idx:idx]  # Look back range from current index

                    # Ensure past_range is not empty and then check for k_cross_below
                    if not past_range.empty and past_range['k_cross_below'].any():
                        # Check for specific Heikin-Ashi candle types based on hlfpid
                        if ((hlfpid == 1 and current_row['heikin_candle_type'] == "Green Type 1 (Normal)") or
                            (hlfpid == 2 and current_row['heikin_candle_type'] == "Green Type 2 (Pin Bar)") or
                            (hlfpid == 3 and current_row['heikin_candle_type'] == "Green Type 3 (Wickless)")):

                            if df.index[idx].date() == current_datetime.date() :
                                send_telegram_message(f'Today Alert : back_test : {back_test_name} condition:{condition_name} frame : {frame} strike : {strike_name} ')

                            return True
            return False


        # Assuming 'resampled_df' is your DataFrame
        resampled_df['alert'] = False  # Initialize the column to store condition met status

        for idx in range(len(resampled_df)):
            # Ensure the 'close' column is not empty or NA before checking conditions
            if pd.notna(resampled_df.iloc[idx]['close']):  # Using `iloc` instead of `at`
                if check_conditions(resampled_df, idx, psar_candles, hlfpid):
                    resampled_df.at[resampled_df.index[idx], 'alert'] = True  # Use 'at' with the correct index for label-based setting
                    alert_condition = 1  # Set alert_condition only if 'close' is not NA and conditions met
                    total_alerts[frame] = total_alerts[frame] + 1

        if alert_condition == 1 :
            output_dir = f"{output_dir}/alert/{frame} min"
        else :
            output_dir = f"{output_dir}/{frame} min"

        output_path = f"{output_dir}/{strike_name}_{back_test_name}_{condition_name}_{frame}min.csv"

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        resampled_df.to_csv(output_path, index=True)

        print(f"Saved Excel file: {output_path}")
    except Exception as e:
        print(f"Failed to save Excel file for {strike_name} at: {str(e)}")
        traceback.print_exc()
        # input("Press Enter to exit...")


# Main logic
def main():
    global total_alerts
    total_alerts = {}
    
    clear_log("")

    log_message("Starting the back test process...")

    base_url = "C:/Users/Administrator/Desktop/React/build/price_data/NFO/"

    # Read the back_test_id
    back_test_id = read_back_test_id()
    if back_test_id:
        print(f"Read back_test_id: {back_test_id}")
    else:
        print("No back_test_id to process.")
        return

    # List of tables to fetch
    tables_to_fetch = ['back_test', 'baskets', 'filter_options', 'conditions', 'custom_indicators','hlfp']
    tables_data = fetch_and_store_tables(tables_to_fetch)    

    if not tables_data:
        print("Failed to fetch table data. Exiting.")
        return

    # Extracting and processing data
    try:
        # back_test_id = 25

        back_test_df = tables_data['back_test']
        back_test_df['id'] = back_test_df['id'].astype(int)
        back_test_df = back_test_df[back_test_df['id'] == int(back_test_id)]

        groups = tables_data['baskets']
        filter_options_df = tables_data['filter_options']

        back_test_row = back_test_df.loc[back_test_df['id'] == int(back_test_id)]
        back_test_name = back_test_row.iloc[0]['back_test_name']

        send_telegram_message(f'Back_test started___ : {back_test_name}')

        conditions_df = tables_data['conditions']
        custom_indicators_df = tables_data['custom_indicators']
        hlfp_df = tables_data['hlfp']

        condition_id = back_test_df.loc[back_test_df['id'] == int(back_test_id), 'condition_name'].values[0] if not back_test_df.loc[back_test_df['id'] == int(back_test_id)].empty else None
        
        condition_row = conditions_df.loc[conditions_df['id'] == int(condition_id)]
        
        condition_name = condition_row.iloc[0]['name']

        threshold_value_one = hlfp_df.iloc[0]['kLineThresholdOne']
        threshold_value_two = hlfp_df.iloc[0]['kLineThresholdTwo']
        threshold_value_three = hlfp_df.iloc[0]['kLineThresholdThree']

        psar_value_one = hlfp_df.iloc[0]['psarCandlesOne']
        psar_value_two = hlfp_df.iloc[0]['psarCandlesTwo']
        psar_value_three = hlfp_df.iloc[0]['psarCandlesThree']

        hlfpid = condition_row.iloc[0]['hlfpid']

        if hlfpid == 1:
            kline_thres = threshold_value_one
            psar_candles = psar_value_one
        elif hlfpid == 2:
            kline_thres = threshold_value_two
            psar_candles = psar_value_two
        elif hlfpid == 3:
            kline_thres = threshold_value_three
            psar_candles = psar_value_three

        lrcid = condition_row.iloc[0]['lrcid']
        lrc_value = custom_indicators_df.loc[custom_indicators_df['id'] == int(lrcid), 'value'].values[0] if not custom_indicators_df.loc[custom_indicators_df['id'] == int(lrcid)].empty else None
        lrc_parts = lrc_value.split(',')        
        # Extract period and std_multiplier
        period = int(lrc_parts[0].strip())  # Convert the first part to an integer
        std_multiplier = float(lrc_parts[1].strip())  # Convert the second part to a float

        psarid = condition_row.iloc[0]['psarid']
        psar_value = custom_indicators_df.loc[custom_indicators_df['id'] == int(psarid), 'value'].values[0] if not custom_indicators_df.loc[custom_indicators_df['id'] == int(psarid)].empty else None
        psar_parts = psar_value.split(',')
        psar_af = float(psar_parts[0].strip())  # Acceleration factor
        psar_max_af = float(psar_parts[1].strip())  # Maximum acceleration factor

        stochid = condition_row.iloc[0]['stochid']
        stoch_value = custom_indicators_df.loc[custom_indicators_df['id'] == int(stochid), 'value'].values[0] if not custom_indicators_df.loc[custom_indicators_df['id'] == int(stochid)].empty else None
        stoch_parts = stoch_value.split(',')
        stoch_k_period = int(stoch_parts[0].strip())  # %K period
        stoch_d_period = int(stoch_parts[1].strip())  # %D period
        stoch_smoothing = int(stoch_parts[2].strip())  # Smoothing period for %K

        print(f"lrcid: {lrcid}, psarid: {psarid}, stochid: {stochid}")

        group_ids = back_test_df.iloc[0]['group_ids'].split(',') if 'group_ids' in back_test_df.columns else []

        group_ids = [id_.strip() for id_ in group_ids]

        filtered_groups = groups[groups['id'].astype(str).isin(group_ids)]
        group_names = filtered_groups['group_name'].tolist()

        filtered_filters = filter_options_df[filter_options_df['group_name'].isin(group_names)]

        time_frames = back_test_df.iloc[0]['time_frames'].split(',') if 'time_frames' in back_test_df.columns else []
        time_frames = [int(tf.strip()) for tf in time_frames]

        if not filtered_filters.empty:
            for _, filter_item in filtered_filters.iterrows():
                strike_name = filter_item['option_name']
                expiry = filter_item['expiry']
                print(f"expiry {expiry}")

                try:
                    expiry_date = expiry.strftime("%Y%m")
                    file_path = f"{base_url}{expiry_date}/{strike_name}.csv"

                    print(f"Fetching CSV from {file_path}")
                    if os.path.exists(file_path):
                        with open(file_path, 'r') as file:
                            original_csv = file.read()
                        print(f"Successfully read CSV for {strike_name}")
                    else:
                        raise FileNotFoundError(f"CSV file not found at {file_path}")

                    print(f"Processing CSV for {strike_name}...")

                    for frame in time_frames:        
                        if total_alerts.get(frame) == None :
                            total_alerts[frame] = 0
                        save_csv_as_excel(original_csv, strike_name, frame, OUTPUT_DIR, period, std_multiplier, psar_af, psar_max_af, stoch_k_period, stoch_d_period, stoch_smoothing,condition_name,kline_thres,psar_candles,hlfpid,back_test_name)

                except Exception as e:
                    print(f"Error processing CSV for {strike_name}: {str(e)}")
                    # input("Press Enter to exit...")
        else:
            print("No matching filters found for group names.")

    except Exception as e:
        log_message(f"An error occurred during processing: {e}")
        send_telegram_message('Back_test Error___')
        # input("Press Enter to exit...")
        # time.sleep(5)

    
    log_message('Total Alerts')
    log_message(json.dumps(total_alerts))  
    log_message("Back test process completed.")      
    send_telegram_message('Total Alerts')
    print(total_alerts)
    send_telegram_message(json.dumps(total_alerts))
    send_telegram_message('Back_test Completed___')
    # input("Press Enter to exit...")
    

if __name__ == "__main__":
    main()
