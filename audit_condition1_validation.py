"""
Audit Validation: Validate each alert against Condition 1 requirements

WHAT THIS SCRIPT DOES:
1. Takes a symbol and condition ID as input
2. Fetches actual alerts for that symbol/condition from database
3. For EACH alert:
   a. Fetches OHLC data from Redis for the alert timestamp and timeframe
   b. Calculates indicators (PSAR, Stochastic K/D, Heikin-Ashi candles)
   c. Validates Condition 1 requirements:
      - K value in range [kline_start, kline_end]
      - PSAR signal matches signaldirection
      - Candle type matches (candle1)
4. Reports which alerts are VALID (correctly triggered) vs INVALID (shouldn't have triggered)

Usage: python audit_condition1_validation.py <symbol> <condition_id> [max_alerts]
Example: python audit_condition1_validation.py ASIANPAINT26JANFUT 6 10
"""
import pymysql
import redis
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from background.set import settings
import json
import sys
import os

import asyncio
import talib
from concurrent.futures import ThreadPoolExecutor

# Copy required functions from redis_alert_engine to avoid dependency issues
def heikin_ashi_numpy(open_, high, low, close):
    """Calculate Heikin Ashi on entire dataset"""
    ha_close = (open_ + high + low + close) / 4
    ha_open = np.zeros_like(open_)
    ha_open[0] = (open_[0] + close[0]) / 2
    for i in range(1, len(open_)):
        ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2
    ha_high = np.maximum.reduce([high, ha_open, ha_close])
    ha_low = np.minimum.reduce([low, ha_open, ha_close])
    return ha_open, ha_high, ha_low, ha_close

async def psar_async(high, low, close, af0=0.02, af=0.02, max_af=0.2):
    """Calculate PSAR asynchronously"""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        psar_data = await loop.run_in_executor(
            executor,
            lambda: talib.SAR(high, low, acceleration=af, maximum=max_af)
        )
    return psar_data

async def calc_fastStochastics_async(low, high, close, lookback_period=14, d_period=3, k_smoothing_period=3):
    """Calculate Stochastic asynchronously - matches original engine logic (uses STOCH, not STOCHF)"""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        def calc():
            # Match original engine: uses STOCH (not STOCHF) with these parameters
            # Coerce to safe integers (use different variable names to avoid scoping issues)
            try:
                lb_period = max(1, int(lookback_period))
            except Exception:
                lb_period = 14
            try:
                d_per = max(1, int(d_period))
            except Exception:
                d_per = 3
            try:
                k_smooth = max(1, int(k_smoothing_period))
            except Exception:
                k_smooth = 3
            
            # Convert to numpy arrays
            high_arr = np.asarray(high, dtype=np.float64)
            low_arr = np.asarray(low, dtype=np.float64)
            close_arr = np.asarray(close, dtype=np.float64)
            
            # Use STOCH (not STOCHF) - matches original engine
            slowk, slowd = talib.STOCH(
                high_arr, low_arr, close_arr,
                fastk_period=lb_period,
                slowk_period=k_smooth,
                slowk_matype=0,  # Simple Moving Average
                slowd_period=d_per,
                slowd_matype=0   # Simple Moving Average
            )
            
            # Replace NaN values with 0 (matches original)
            slowk = np.nan_to_num(slowk, nan=0.0)
            slowd = np.nan_to_num(slowd, nan=0.0)
            
            return slowk, slowd
        k, d = await loop.run_in_executor(executor, calc)
    return k, d

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

def get_db_connection():
    """Get database connection using settings"""
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
    """Get Redis connection"""
    return redis.from_url('redis://localhost', decode_responses=True)

def get_custom_indicator_config(indicator_id):
    """Get custom indicator configuration from database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = "SELECT id, name, value FROM custom_indicators WHERE id = %s"
        cursor.execute(query, (indicator_id,))
        result = cursor.fetchone()
        return result
    finally:
        cursor.close()
        conn.close()

def check_mysql_data_available(symbol, alert_datetime, timeframe, min_candles=20):
    """Check if MySQL has sufficient data for validation"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if data exists around alert timestamp
        alert_dt = pd.to_datetime(alert_datetime)
        start_time = alert_dt - timedelta(hours=2)  # Check 2 hours before
        end_time = alert_dt + timedelta(minutes=timeframe)
        
        query = """
            SELECT COUNT(*) as count 
            FROM one_min_ohlc 
            WHERE symbol = %s AND datetime BETWEEN %s AND %s
        """
        cursor.execute(query, (symbol, start_time, end_time))
        result = cursor.fetchone()
        count = result['count'] if result else 0
        
        cursor.close()
        conn.close()
        
        # Need at least min_candles * timeframe minutes of 1-minute data to resample
        min_required = min_candles * timeframe
        if count < min_required:
            return False, f"MySQL has only {count} 1-minute candles (need {min_required} for {timeframe}min timeframe)"
        
        return True, f"MySQL has {count} 1-minute candles available"
    except Exception as e:
        if conn:
            try:
                conn.close()
            except:
                pass
        return False, f"Error checking MySQL: {e}"

def fetch_ohlc_from_mysql(symbol, timeframe, alert_timestamp, lookback_minutes=250):
    """Fetch OHLC data from MySQL and resample to required timeframe"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Calculate start time: fetch enough 1-minute candles to resample (need at least lookback_minutes)
        alert_dt = pd.to_datetime(alert_timestamp)
        start_time = alert_dt - timedelta(minutes=lookback_minutes)
        end_time = alert_dt + timedelta(minutes=timeframe)
        
        # Fetch 1-minute data from MySQL
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
        
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['datetime'])
        df = df.drop(columns=['datetime'])
        
        if df.empty or len(df) < 2:
            return pd.DataFrame()
        
        # If timeframe is 1 minute, return as-is
        if timeframe == 1:
            df = df.sort_values('timestamp').reset_index(drop=True)
            return df
        
        # Resample to required timeframe
        df = df.set_index('timestamp')
        df_resampled = df.resample(f'{timeframe}min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }).dropna()
        
        # Reset index to get timestamp as column
        df_resampled = df_resampled.reset_index()
        df_resampled['timestamp'] = df_resampled['timestamp']
        
        return df_resampled.sort_values('timestamp').reset_index(drop=True)
        
    except Exception as e:
        print(f"Error fetching OHLC data from MySQL: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            cursor.close()
            conn.close()

def fetch_ohlc_from_redis(redis_client, symbol, timeframe, alert_timestamp, lookback_candles=250):
    """Fetch OHLC data from Redis for a symbol and timeframe around the alert timestamp (DEPRECATED - use MySQL)"""
    try:
        # Convert timeframe to interval string (Redis uses "1minute", "2minute", etc.)
        interval = f"{timeframe}minute"
        
        # Fetch resampled OHLC data
        zset_key = f"resampled_ohlc_sorted:{symbol}:{interval}"
        entries = redis_client.zrange(zset_key, -lookback_candles, -1)
        
        if not entries:
            return pd.DataFrame()
        
        # Parse entries
        arr = [json.loads(x) for x in entries]
        if not arr:
            return pd.DataFrame()
        
        df = pd.DataFrame(arr)
        
        # Normalize timestamp column
        if 'datetime' in df.columns:
            df['timestamp'] = pd.to_datetime(df['datetime'])
            df = df.drop(columns=['datetime'])
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Ensure required columns
        required_cols = ['timestamp', 'open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            return pd.DataFrame()
        
        df = df.sort_values('timestamp')
        df = df.reset_index(drop=True)
        
        # Filter to candles around alert timestamp (within 1 hour before/after)
        alert_dt = pd.to_datetime(alert_timestamp)
        start_time = alert_dt - timedelta(hours=1)
        end_time = alert_dt + timedelta(minutes=timeframe)
        
        df_filtered = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
        
        if df_filtered.empty:
            return df.tail(50)  # Return last 50 if no match found
        
        return df_filtered
        
    except Exception as e:
        print(f"Error fetching OHLC data: {e}")
        return pd.DataFrame()

async def calculate_indicators(ohlc_df, psar_config, stoch_config):
    """Calculate PSAR and Stochastic indicators"""
    error_details = []
    try:
        if ohlc_df.empty:
            error_details.append("OHLC dataframe is empty")
            return None, None, None, None, error_details
        
        if len(ohlc_df) < 20:
            error_details.append(f"Not enough candles: {len(ohlc_df)} < 20")
            return None, None, None, None, error_details
        
        # Prepare data
        try:
            high = ohlc_df['high'].values.astype(float)
            low = ohlc_df['low'].values.astype(float)
            close = ohlc_df['close'].values.astype(float)
            open_vals = ohlc_df['open'].values.astype(float)
        except Exception as e:
            error_details.append(f"Error preparing OHLC data: {e}")
            return None, None, None, None, error_details
        
        # Parse PSAR config
        try:
            psar_values = psar_config['value'].split(',')
            psar_acceleration = float(psar_values[0].strip())
            psar_max_acceleration = float(psar_values[1].strip())
        except Exception as e:
            error_details.append(f"Error parsing PSAR config '{psar_config['value']}': {e}")
            return None, None, None, None, error_details
        
        # Parse Stochastic config
        try:
            stoch_values = stoch_config['value'].split(',')
            stoch_period = float(stoch_values[0])
            k_avg = float(stoch_values[1])
            d_avg = float(stoch_values[2])
        except Exception as e:
            error_details.append(f"Error parsing Stochastic config '{stoch_config['value']}': {e}")
            return None, None, None, None, error_details
        
        # Calculate Heikin-Ashi
        try:
            ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(open_vals, high, low, close)
        except Exception as e:
            error_details.append(f"Error calculating Heikin-Ashi: {e}")
            return None, None, None, None, error_details
        
        # Calculate PSAR
        try:
            psar_data = await psar_async(high, low, close, 
                                         af0=psar_acceleration,
                                         af=psar_acceleration,
                                         max_af=psar_max_acceleration)
            if psar_data is None:
                error_details.append("PSAR calculation returned None")
                return None, None, None, None, error_details
        except Exception as e:
            error_details.append(f"Error calculating PSAR: {e}")
            return None, None, None, None, error_details
        
        # Calculate Stochastic
        try:
            K, D = await calc_fastStochastics_async(low, high, close,
                                                    lookback_period=stoch_period,
                                                    d_period=d_avg,
                                                    k_smoothing_period=k_avg)
            if K is None or D is None:
                error_details.append("Stochastic calculation returned None")
                return None, None, None, None, error_details
        except Exception as e:
            error_details.append(f"Error calculating Stochastic: {e}")
            return None, None, None, None, error_details
        
        return psar_data, K, D, (ha_open, ha_high, ha_low, ha_close), error_details
        
    except Exception as e:
        error_details.append(f"Unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None, None, error_details

def get_psar_signal(psar_value, close_price):
    """Determine PSAR signal: 1 for bullish (PSAR below price), -1 for bearish (PSAR above price)"""
    if psar_value is None or close_price is None:
        return None
    if psar_value < close_price:
        return 1  # Bullish
    else:
        return -1  # Bearish

def check_candle_type(candle_type, ha_open, ha_high, ha_low, ha_close):
    """Check if candle matches the required type"""
    if candle_type == 1:
        # Type 1: Green candle (close_ha > open_ha)
        return ha_close > ha_open
    elif candle_type == 2:
        # Type 2: Green candle + no lower wick + upper wick
        is_green = ha_close > ha_open
        no_lower_wick = abs(ha_low - ha_open) <= 0.001
        has_upper_wick = ha_high > ha_close
        return is_green and no_lower_wick and has_upper_wick
    elif candle_type == 3:
        # Type 3: Green candle + no lower wick + no upper wick
        is_green = ha_close > ha_open
        no_lower_wick = abs(ha_low - ha_open) <= 0.001
        no_upper_wick = abs(ha_high - ha_close) <= 0.001
        return is_green and no_lower_wick and no_upper_wick
    return False

def validate_alert(alert_row, condition_details, redis_client):
    """Validate a single alert against Condition 1 requirements"""
    symbol = alert_row['symbol']
    alert_datetime = alert_row['alert_datetime']
    timeframe = alert_row['timeframe']
    
    # Get condition parameters
    candle_type = condition_details['candle1']
    psar_id = condition_details['psar1']
    stoch_id = condition_details['stochid']
    kline_start = condition_details['kline_start']
    kline_end = condition_details['kline_end']
    signaldirection = condition_details['signaldirection']
    
    # Get indicator configs
    psar_config = get_custom_indicator_config(psar_id)
    stoch_config = get_custom_indicator_config(stoch_id)
    
    if not psar_config or not stoch_config:
        return {
            'valid': False,
            'reason': f"Missing indicator configs (PSAR: {psar_id}, Stoch: {stoch_id})"
        }
    
    # Fetch OHLC data from MySQL (resampled to required timeframe)
    ohlc_df = fetch_ohlc_from_mysql(symbol, timeframe, alert_datetime, lookback_minutes=250)
    
    if ohlc_df.empty:
        return {
            'valid': False,
            'reason': "No OHLC data found in Redis"
        }
    
    # Find the exact candle for the alert timestamp
    alert_dt = pd.to_datetime(alert_datetime)
    matching_candles = ohlc_df[ohlc_df['timestamp'] == alert_dt]
    
    if matching_candles.empty:
        # Try to find closest candle
        ohlc_df['time_diff'] = abs(ohlc_df['timestamp'] - alert_dt)
        closest_idx = ohlc_df['time_diff'].idxmin()
        matching_candle = ohlc_df.loc[closest_idx]
        time_diff_seconds = abs((matching_candle['timestamp'] - alert_dt).total_seconds())
        if time_diff_seconds > timeframe * 60:  # More than one candle period away
            return {
                'valid': False,
                'reason': f"No matching candle found (closest is {time_diff_seconds:.0f}s away)"
            }
    else:
        matching_candle = matching_candles.iloc[0]
    
    # Calculate indicators
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(
            calculate_indicators(ohlc_df, psar_config, stoch_config)
        )
        if len(result) == 5:
            psar_data, K, D, ha_data, error_details = result
        else:
            # Backward compatibility
            psar_data, K, D, ha_data = result
            error_details = []
    finally:
        loop.close()
    
    if psar_data is None or K is None or D is None or ha_data is None:
        error_msg = "Failed to calculate indicators"
        if error_details:
            error_msg += f": {', '.join(error_details)}"
        return {
            'valid': False,
            'reason': error_msg
        }
    
    # Get values for the alert candle
    candle_idx = ohlc_df.index.get_loc(matching_candle.name)
    if candle_idx >= len(psar_data) or candle_idx >= len(K) or candle_idx >= len(D):
        return {
            'valid': False,
            'reason': "Candle index out of range for indicators"
        }
    
    psar_value = psar_data[candle_idx]
    stoch_k = K[candle_idx]
    stoch_d = D[candle_idx]
    ha_open, ha_high, ha_low, ha_close = ha_data
    ha_open_val = ha_open[candle_idx]
    ha_high_val = ha_high[candle_idx]
    ha_low_val = ha_low[candle_idx]
    ha_close_val = ha_close[candle_idx]
    
    # Validate Condition 1
    validation_results = []
    
    # Check 1: K value in range
    if stoch_k is None or np.isnan(stoch_k):
        validation_results.append("K value is NaN/None")
    elif not (kline_start < stoch_k < kline_end):
        validation_results.append(f"K value {stoch_k:.2f} not in range [{kline_start}, {kline_end}]")
    
    # Check 2: PSAR signal direction
    close_price = matching_candle['close']
    psar_signal = get_psar_signal(psar_value, close_price)
    if psar_signal != signaldirection:
        validation_results.append(f"PSAR signal {psar_signal} != required {signaldirection}")
    
    # Check 3: Candle type
    candle_valid = check_candle_type(candle_type, ha_open_val, ha_high_val, ha_low_val, ha_close_val)
    if not candle_valid:
        validation_results.append(f"Candle type {candle_type} check failed")
    
    # Return result
    if len(validation_results) == 0:
        return {
            'valid': True,
            'reason': "All conditions met",
            'details': {
                'stoch_k': stoch_k,
                'stoch_d': stoch_d,
                'psar_value': psar_value,
                'psar_signal': psar_signal,
                'ha_open': ha_open_val,
                'ha_high': ha_high_val,
                'ha_low': ha_low_val,
                'ha_close': ha_close_val
            }
        }
    else:
        return {
            'valid': False,
            'reason': "; ".join(validation_results),
            'details': {
                'stoch_k': stoch_k,
                'stoch_d': stoch_d,
                'psar_value': psar_value,
                'psar_signal': psar_signal,
                'ha_open': ha_open_val,
                'ha_high': ha_high_val,
                'ha_low': ha_low_val,
                'ha_close': ha_close_val
            }
        }

def validate_alerts_batch(actual_alerts_df, condition_details, redis_client=None, max_alerts=10):
    """Validate a batch of alerts - uses MySQL for OHLC data"""
    results = []
    skipped = []
    
    print(f"\nPre-filtering alerts (checking MySQL data availability)...")
    
    # Pre-filter: Only validate alerts where MySQL has sufficient data
    alerts_to_validate = []
    for idx, alert in actual_alerts_df.head(max_alerts).iterrows():
        available, msg = check_mysql_data_available(alert['symbol'], alert['alert_datetime'], alert['timeframe'], min_candles=20)
        if available:
            alerts_to_validate.append((idx, alert))
        else:
            skipped.append({
                'alert_id': alert['alert_id'],
                'alert_datetime': alert['alert_datetime'],
                'timeframe': alert['timeframe'],
                'reason': f"Skipped: {msg}"
            })
    
    print(f"Found {len(alerts_to_validate)} alerts with MySQL data available, {len(skipped)} skipped")
    
    if not alerts_to_validate:
        print("No alerts have sufficient MySQL data for validation!")
        return results, skipped
    
    print(f"\nValidating {len(alerts_to_validate)} alerts...")
    
    for validate_idx, (orig_idx, alert) in enumerate(alerts_to_validate):
        print(f"Validating alert {validate_idx+1}/{len(alerts_to_validate)}: {alert['alert_datetime']} ({alert['timeframe']}min)...", end=" ")
        
        validation = validate_alert(alert, condition_details, redis_client)
        
        result = {
            'alert_id': alert['alert_id'],
            'alert_datetime': alert['alert_datetime'],
            'timeframe': alert['timeframe'],
            'valid': validation['valid'],
            'reason': validation['reason'],
            'details': validation.get('details', {})
        }
        
        results.append(result)
        
        status = "[VALID]" if validation['valid'] else "[INVALID]"
        print(status)
    
    return results, skipped

def print_validation_report(validation_results, skipped_alerts=None):
    """Print validation report"""
    print("\n" + "="*80)
    print("VALIDATION REPORT")
    print("="*80)
    
    valid_count = sum(1 for r in validation_results if r['valid'])
    invalid_count = len(validation_results) - valid_count
    skipped_count = len(skipped_alerts) if skipped_alerts else 0
    
    print(f"\nSummary:")
    print(f"  Total Alerts Validated: {len(validation_results)}")
    print(f"  Valid Alerts: {valid_count}")
    print(f"  Invalid Alerts: {invalid_count}")
    if len(validation_results) > 0:
        print(f"  Accuracy: {valid_count/len(validation_results)*100:.1f}%")
    if skipped_count > 0:
        print(f"  Skipped (no Redis data): {skipped_count}")
    
    print("\n" + "-"*80)
    print("INVALID ALERTS:")
    print("-"*80)
    
    invalid_alerts = [r for r in validation_results if not r['valid']]
    if invalid_alerts:
        for alert in invalid_alerts:
            print(f"\nAlert ID {alert['alert_id']}: {alert['alert_datetime']} ({alert['timeframe']}min)")
            print(f"  Reason: {alert['reason']}")
            if alert['details']:
                print(f"  K={alert['details'].get('stoch_k', 'N/A'):.2f}, "
                      f"PSAR_signal={alert['details'].get('psar_signal', 'N/A')}, "
                      f"PSAR_value={alert['details'].get('psar_value', 'N/A'):.2f}")
    else:
        print("  No invalid alerts found!")
    
    print("\n" + "="*80)
    
    # Print skipped alerts section
    if skipped_alerts and len(skipped_alerts) > 0:
        print("\n" + "-"*80)
        print("SKIPPED ALERTS (No Redis Data Available):")
        print("-"*80)
        for skip in skipped_alerts[:10]:  # Show first 10 skipped
            print(f"  Alert ID {skip['alert_id']}: {skip['alert_datetime']} ({skip['timeframe']}min)")
            print(f"    Reason: {skip['reason']}")
        if len(skipped_alerts) > 10:
            print(f"  ... and {len(skipped_alerts) - 10} more skipped alerts")
        print("="*80)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python audit_condition1_validation.py <symbol> <condition_id> [max_alerts]")
        print("Example: python audit_condition1_validation.py ASHOKLEY26JANFUT 6 10")
        sys.exit(1)
    
    symbol = sys.argv[1]
    condition_id = int(sys.argv[2])
    max_alerts = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    
    # Get condition details
    from audit_condition1_comparison import get_condition_details, get_actual_alerts
    
    print(f"Fetching condition details for conditionID {condition_id}...")
    condition_details = get_condition_details(condition_id)
    
    if not condition_details:
        print(f"Condition {condition_id} not found")
        sys.exit(1)
    
    print(f"Condition: {condition_details.get('name', 'N/A')}")
    
    # Get alerts
    print(f"\nFetching alerts for {symbol}...")
    actual_alerts_df = get_actual_alerts(symbol, condition_id)
    
    if actual_alerts_df.empty:
        print(f"No alerts found for {symbol} with conditionID {condition_id}")
        sys.exit(1)
    
    print(f"Found {len(actual_alerts_df)} alerts")
    
    # Connect to Redis
    print("\nConnecting to Redis...")
    redis_client = get_redis_connection()
    
    # Validate alerts
    validation_results, skipped_alerts = validate_alerts_batch(actual_alerts_df, condition_details, redis_client, max_alerts)
    
    # Print report
    print_validation_report(validation_results, skipped_alerts)
