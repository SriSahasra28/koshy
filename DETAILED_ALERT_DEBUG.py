"""
Detailed debug for Alert ID 22202 - why was it generated with PSAR signal=0?
"""
import pymysql
import redis
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from background.set import settings
import json
import asyncio
import talib
from concurrent.futures import ThreadPoolExecutor

def get_db_connection():
    db_config = settings.get_db()
    return pymysql.connect(
        host=db_config[2],
        port=db_config[3],
        user=db_config[0],
        password=db_config[1],
        database=db_config[4],
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor
    )

def get_redis_connection():
    return redis.from_url('redis://localhost', decode_responses=True)

def heikin_ashi_numpy(open_, high, low, close):
    """Calculate Heikin Ashi"""
    ha_close = (open_ + high + low + close) / 4
    ha_open = np.zeros_like(open_)
    ha_open[0] = (open_[0] + close[0]) / 2
    for i in range(1, len(open_)):
        ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2
    ha_high = np.maximum.reduce([high, ha_open, ha_close])
    ha_low = np.minimum.reduce([low, ha_open, ha_close])
    return ha_open, ha_high, ha_low, ha_close

def psar(high, low, close, af0=0.02, af=None, max_af=0.2):
    """Calculate PSAR using TA-Lib"""
    psar_values = talib.SAR(high, low, acceleration=af0, maximum=max_af)
    return psar_values

def get_psar_signals(close, psar_values):
    """Calculate PSAR signals based on crossovers"""
    signals = np.zeros(len(close))
    for i in range(1, len(close)):
        if close[i] > psar_values[i] and close[i-1] <= psar_values[i-1]:
            signals[i] = 1  # Long signal
        elif close[i] < psar_values[i] and close[i-1] >= psar_values[i-1]:
            signals[i] = -1  # Short signal
    return signals

async def calc_fastStochastics_async(low, high, close, lookback_period=14, d_period=3, k_smoothing_period=3):
    """Calculate Stochastic"""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        def calc():
            lb_period = max(1, int(lookback_period))
            d_per = max(1, int(d_period))
            k_smooth = max(1, int(k_smoothing_period))
            high_arr = np.asarray(high, dtype=np.float64)
            low_arr = np.asarray(low, dtype=np.float64)
            close_arr = np.asarray(close, dtype=np.float64)
            slowk, slowd = talib.STOCH(
                high_arr, low_arr, close_arr,
                fastk_period=lb_period,
                slowk_period=k_smooth,
                slowk_matype=0,
                slowd_period=d_per,
                slowd_matype=0
            )
            slowk = np.nan_to_num(slowk, nan=0.0)
            slowd = np.nan_to_num(slowd, nan=0.0)
            return slowk, slowd
        k, d = await loop.run_in_executor(executor, calc)
    return k, d

def fetch_ohlc_from_mysql(symbol, alert_timestamp, lookback_minutes=250):
    """Fetch OHLC data from MySQL"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        alert_dt = pd.to_datetime(alert_timestamp)
        start_time = alert_dt - timedelta(minutes=lookback_minutes)
        end_time = alert_dt + timedelta(minutes=5)
        
        query = """
            SELECT datetime, open, high, low, close 
            FROM one_min_ohlc 
            WHERE symbol = %s AND datetime BETWEEN %s AND %s 
            ORDER BY datetime
        """
        cursor.execute(query, (symbol, start_time, end_time))
        rows = cursor.fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('timestamp').reset_index(drop=True)
        return df
    finally:
        cursor.close()
        conn.close()

def fetch_ohlc_from_redis(redis_client, symbol, interval="1minute", lookback_candles=250):
    """Fetch OHLC data from Redis"""
    try:
        zset_key = f"resampled_ohlc_sorted:{symbol}:{interval}"
        entries = redis_client.zrange(zset_key, -lookback_candles, -1)
        
        if not entries:
            return pd.DataFrame()
        
        arr = [json.loads(x) for x in entries]
        if not arr:
            return pd.DataFrame()
        
        df = pd.DataFrame(arr)
        if 'datetime' in df.columns:
            df['timestamp'] = pd.to_datetime(df['datetime'])
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df = df.sort_values('timestamp').reset_index(drop=True)
        return df
    except Exception as e:
        print(f"Error fetching from Redis: {e}")
        return pd.DataFrame()

def debug_alert(alert_id):
    """Debug a specific alert"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Get alert details
        cursor.execute("""
            SELECT id, symbol, datetime, timeframe, scanid, conditionID
            FROM alerts
            WHERE id = %s
        """, (alert_id,))
        alert = cursor.fetchone()
        
        if not alert:
            print(f"Alert {alert_id} not found")
            return
        
        # Get condition details
        cursor.execute("""
            SELECT name, candle1, psar1, stochid, kline_start, kline_end, signaldirection
            FROM conditions
            WHERE id = %s
        """, (alert['conditionID'],))
        condition = cursor.fetchone()
        
        # Get custom indicator configs
        cursor.execute("""
            SELECT id, name, value FROM custom_indicators WHERE id IN (%s, %s)
        """, (condition['psar1'], condition['stochid']))
        indicators = {row['id']: row for row in cursor.fetchall()}
        
        psar_config = indicators.get(condition['psar1'])
        stoch_config = indicators.get(condition['stochid'])
        
        print("\n" + "="*80)
        print(f"DETAILED DEBUG FOR ALERT ID {alert_id}")
        print("="*80)
        print(f"\nAlert Details:")
        print(f"  Symbol: {alert['symbol']}")
        print(f"  Alert Time: {alert['datetime']}")
        print(f"  Timeframe: {alert['timeframe']}min")
        print(f"  Condition: {condition['name']}")
        print(f"  Signal Direction Required: {condition['signaldirection']}")
        print(f"  K-line Range: {condition['kline_start']} - {condition['kline_end']}")
        print(f"  Candle Type: {condition['candle1']}")
        
        print(f"\nIndicator Configs:")
        print(f"  PSAR ({psar_config['name']}): {psar_config['value']}")
        print(f"  Stochastic ({stoch_config['name']}): {stoch_config['value']}")
        
        # Fetch data from MySQL
        print(f"\n{'='*80}")
        print("FETCHING DATA FROM MYSQL")
        print("="*80)
        mysql_df = fetch_ohlc_from_mysql(alert['symbol'], alert['datetime'], lookback_minutes=250)
        print(f"MySQL: Fetched {len(mysql_df)} candles")
        
        if mysql_df.empty:
            print("No MySQL data available!")
            return
        
        # Find alert candle in MySQL
        alert_dt = pd.to_datetime(alert['datetime'])
        alert_candle_idx = None
        for idx, row in mysql_df.iterrows():
            if row['timestamp'] == alert_dt:
                alert_candle_idx = idx
                break
        
        if alert_candle_idx is None:
            # Find closest
            mysql_df['time_diff'] = abs(mysql_df['timestamp'] - alert_dt)
            alert_candle_idx = mysql_df['time_diff'].idxmin()
            closest_time = mysql_df.loc[alert_candle_idx, 'timestamp']
            time_diff = abs((closest_time - alert_dt).total_seconds())
            print(f"  Alert candle not exact match, closest is {time_diff}s away: {closest_time}")
        
        print(f"  Alert candle index in MySQL data: {alert_candle_idx}")
        
        # Fetch data from Redis
        print(f"\n{'='*80}")
        print("FETCHING DATA FROM REDIS")
        print("="*80)
        redis_client = get_redis_connection()
        redis_df = fetch_ohlc_from_redis(redis_client, alert['symbol'], "1minute", lookback_candles=250)
        print(f"Redis: Fetched {len(redis_df)} candles")
        
        if not redis_df.empty:
            # Find alert candle in Redis
            alert_candle_idx_redis = None
            for idx, row in redis_df.iterrows():
                if row['timestamp'] == alert_dt:
                    alert_candle_idx_redis = idx
                    break
            if alert_candle_idx_redis is not None:
                print(f"  Alert candle index in Redis data: {alert_candle_idx_redis}")
            else:
                print(f"  Alert candle not found in Redis data")
        
        # Calculate indicators on MySQL data
        print(f"\n{'='*80}")
        print("CALCULATING INDICATORS (MySQL Data)")
        print("="*80)
        
        high = mysql_df['high'].values.astype(float)
        low = mysql_df['low'].values.astype(float)
        close = mysql_df['close'].values.astype(float)
        open_vals = mysql_df['open'].values.astype(float)
        
        # Parse PSAR config
        psar_values_config = psar_config['value'].split(',')
        psar_acc = float(psar_values_config[0].strip())
        psar_max = float(psar_values_config[1].strip())
        
        # Parse Stochastic config
        stoch_values = stoch_config['value'].split(',')
        stoch_period = float(stoch_values[0])
        k_avg = float(stoch_values[1])
        d_avg = float(stoch_values[2])
        
        # Calculate indicators
        psar_values = psar(high, low, close, af0=psar_acc, max_af=psar_max)
        psar_signals = get_psar_signals(close, psar_values)
        
        # Calculate Heikin-Ashi
        ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(open_vals, high, low, close)
        
        # Calculate Stochastic
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            K, D = loop.run_until_complete(
                calc_fastStochastics_async(low, high, close, 
                                         lookback_period=stoch_period,
                                         d_period=d_avg,
                                         k_smoothing_period=k_avg)
            )
        finally:
            loop.close()
        
        # Show alert candle details
        print(f"\n{'='*80}")
        print(f"ALERT CANDLE ANALYSIS (Index {alert_candle_idx})")
        print("="*80)
        
        alert_candle = mysql_df.iloc[alert_candle_idx]
        print(f"\nOHLC:")
        print(f"  Open: {alert_candle['open']}")
        print(f"  High: {alert_candle['high']}")
        print(f"  Low: {alert_candle['low']}")
        print(f"  Close: {alert_candle['close']}")
        print(f"  Timestamp: {alert_candle['timestamp']}")
        
        print(f"\nPSAR:")
        print(f"  PSAR Value: {psar_values[alert_candle_idx]}")
        print(f"  PSAR Signal: {int(psar_signals[alert_candle_idx])}")
        print(f"  Close Price: {close[alert_candle_idx]}")
        print(f"  Required Signal: {condition['signaldirection']}")
        
        # Show PSAR signals around alert candle
        print(f"\nPSAR Signals around alert candle (±5 candles):")
        start_idx = max(0, alert_candle_idx - 5)
        end_idx = min(len(psar_signals), alert_candle_idx + 6)
        for i in range(start_idx, end_idx):
            marker = " <-- ALERT" if i == alert_candle_idx else ""
            print(f"  [{i}] {mysql_df.iloc[i]['timestamp']}: PSAR={psar_values[i]:.2f}, Close={close[i]:.2f}, Signal={int(psar_signals[i])}{marker}")
        
        print(f"\nStochastic:")
        print(f"  K: {K[alert_candle_idx]:.2f}")
        print(f"  D: {D[alert_candle_idx]:.2f}")
        print(f"  Required Range: {condition['kline_start']} - {condition['kline_end']}")
        print(f"  In Range: {condition['kline_start'] < K[alert_candle_idx] < condition['kline_end']}")
        
        print(f"\nHeikin-Ashi:")
        print(f"  HA Open: {ha_open[alert_candle_idx]:.2f}")
        print(f"  HA High: {ha_high[alert_candle_idx]:.2f}")
        print(f"  HA Low: {ha_low[alert_candle_idx]:.2f}")
        print(f"  HA Close: {ha_close[alert_candle_idx]:.2f}")
        print(f"  Candle Type: {condition['candle1']}")
        print(f"  Is Green: {ha_close[alert_candle_idx] > ha_open[alert_candle_idx]}")
        
        # Validation
        print(f"\n{'='*80}")
        print("VALIDATION CHECKS")
        print("="*80)
        
        psar_signal_int = int(psar_signals[alert_candle_idx]) if not np.isnan(psar_signals[alert_candle_idx]) else 0
        signaldirection_int = int(condition['signaldirection'])
        
        checks_passed = []
        checks_failed = []
        
        # Check 1: K value
        if condition['kline_start'] < K[alert_candle_idx] < condition['kline_end']:
            checks_passed.append(f"K value {K[alert_candle_idx]:.2f} in range")
        else:
            checks_failed.append(f"K value {K[alert_candle_idx]:.2f} NOT in range [{condition['kline_start']}, {condition['kline_end']}]")
        
        # Check 2: PSAR signal
        if psar_signal_int == 0 and signaldirection_int != 0:
            checks_failed.append(f"PSAR signal is 0 (no crossover) but required {signaldirection_int}")
        elif psar_signal_int == signaldirection_int:
            checks_passed.append(f"PSAR signal {psar_signal_int} matches required {signaldirection_int}")
        else:
            checks_failed.append(f"PSAR signal {psar_signal_int} != required {signaldirection_int}")
        
        # Check 3: Candle type
        candle_type = condition['candle1']
        ha_open_val = ha_open[alert_candle_idx]
        ha_high_val = ha_high[alert_candle_idx]
        ha_low_val = ha_low[alert_candle_idx]
        ha_close_val = ha_close[alert_candle_idx]
        
        candle_valid = False
        if candle_type == 1:
            candle_valid = ha_close_val > ha_open_val
        elif candle_type == 2:
            is_green = ha_close_val > ha_open_val
            no_lower_wick = abs(ha_low_val - ha_open_val) <= 0.001
            has_upper_wick = ha_high_val > ha_close_val
            candle_valid = is_green and no_lower_wick and has_upper_wick
        elif candle_type == 3:
            is_green = ha_close_val > ha_open_val
            no_lower_wick = abs(ha_low_val - ha_open_val) <= 0.001
            no_upper_wick = abs(ha_high_val - ha_close_val) <= 0.001
            candle_valid = is_green and no_lower_wick and no_upper_wick
        
        if candle_valid:
            checks_passed.append(f"Candle type {candle_type} check passed")
        else:
            checks_failed.append(f"Candle type {candle_type} check failed")
        
        print(f"\n[PASSED] ({len(checks_passed)}):")
        for check in checks_passed:
            print(f"  - {check}")
        
        print(f"\n[FAILED] ({len(checks_failed)}):")
        for check in checks_failed:
            print(f"  - {check}")
        
        print(f"\n{'='*80}")
        print("CONCLUSION")
        print("="*80)
        if len(checks_failed) == 0:
            print("[OK] Alert should have been generated (all checks pass)")
        else:
            print(f"[ERROR] Alert should NOT have been generated ({len(checks_failed)} check(s) failed)")
            print(f"\nThis suggests either:")
            print(f"  1. The engine saw different data than MySQL has")
            print(f"  2. The engine checked a different candle index")
            print(f"  3. There's a bug in the engine's validation logic")
            print(f"  4. The fix wasn't active when this alert was generated")
        
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    alert_id = int(sys.argv[1]) if len(sys.argv) > 1 else 22202
    debug_alert(alert_id)
