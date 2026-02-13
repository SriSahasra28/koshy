#!/usr/bin/env python3
"""
Debug LRC NaN Issues - Inspect the exact 20-candle window causing NaN

This script will:
1. Fetch the same OHLC data that the alert engine uses
2. Extract the exact 20-candle window for LRC calculation
3. Show which candles have NaN/invalid close values
4. Test LRC calculation on that window to reproduce the NaN

Usage: python debug_lrc_nan.py ADANIENT26FEBFUT "2026-02-09 14:44:00"
"""

import asyncio
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# Import from the alert engine
from background.async_db import dbconnection
from background.set import settings
import redis

async def get_ohlc_data_from_redis(symbol, interval, target_timestamp, window_size=500):
    """Get OHLC data directly from Redis the same way the alert engine does"""
    try:
        # Connect to Redis
        redis_client = redis.from_url('redis://localhost', decode_responses=True)
        
        # Convert target timestamp
        target_dt = pd.to_datetime(target_timestamp)
        
        # Use the correct Redis key format
        zset_key = f"candle_data:{symbol}:{interval}"
        entries = redis_client.zrange(zset_key, -window_size, -1)
        
        if not entries:
            print(f"[ERROR] No data found in Redis key: {zset_key}")
            return None
            
        print(f"[OK] Found {len(entries)} entries in Redis key: {zset_key}")
        
        # Parse entries
        data = []
        for entry in entries:
            try:
                parsed = json.loads(entry)
                data.append(parsed)
            except Exception as e:
                print(f"[WARN] Failed to parse entry: {entry[:100]}... Error: {e}")
                continue
        
        if not data:
            print(f"[ERROR] No valid data entries found")
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Ensure timestamp column
        if 'datetime' in df.columns:
            df['timestamp'] = pd.to_datetime(df['datetime'])
            df = df.drop(columns=['datetime'])
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        else:
            print(f"[ERROR] No timestamp column found in data")
            return None
            
        # Ensure required columns exist
        required_cols = ['timestamp', 'open', 'high', 'low', 'close']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"[ERROR] Missing required columns: {missing_cols}")
            return None
            
        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        print(f"[OK] Fetched {len(df)} candles from Redis")
        print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] Error fetching OHLC data from Redis: {e}")
        import traceback
        traceback.print_exc()
        return None

def find_target_candle_index(df, target_timestamp):
    """Find the index of the target timestamp candle"""
    target_dt = pd.to_datetime(target_timestamp)
    
    # Look for exact match first
    exact_matches = df[df['timestamp'] == target_dt]
    if not exact_matches.empty:
        return exact_matches.index[0]
    
    # Find closest candle
    df['time_diff'] = abs(df['timestamp'] - target_dt)
    closest_idx = df['time_diff'].idxmin()
    closest_candle = df.loc[closest_idx]
    
    time_diff_seconds = abs((closest_candle['timestamp'] - target_dt).total_seconds())
    print(f"[WARN] No exact match for {target_timestamp}")
    print(f"   Closest candle: {closest_candle['timestamp']} (diff: {time_diff_seconds:.0f}s)")
    
    if time_diff_seconds > 120:  # More than 2 minutes away
        print(f"[ERROR] Closest candle is too far away ({time_diff_seconds:.0f}s)")
        return None
        
    return closest_idx

def inspect_lrc_window(df, target_idx, lrc_period=20):
    """Inspect the LRC calculation window for the target candle"""
    print(f"\n[INSPECT] Inspecting LRC window for candle at index {target_idx}")
    print(f"   Target timestamp: {df.loc[target_idx, 'timestamp']}")
    print(f"   LRC period: {lrc_period}")
    
    # Calculate the window bounds
    start_idx = max(0, target_idx - lrc_period + 1)
    end_idx = target_idx + 1
    
    print(f"   Window: index {start_idx} to {end_idx-1} ({end_idx - start_idx} candles)")
    
    if end_idx - start_idx < lrc_period:
        print(f"[ERROR] Not enough candles for LRC calculation")
        print(f"   Need {lrc_period}, have {end_idx - start_idx}")
        return None
        
    # Extract the window
    window_df = df.iloc[start_idx:end_idx].copy()
    
    print(f"\n[ANALYSIS] LRC Window Analysis:")
    print(f"   Time range: {window_df['timestamp'].min()} to {window_df['timestamp'].max()}")
    
    # Check for NaN/invalid values in close prices
    close_values = window_df['close'].values
    
    print(f"\n[PRICES] Close Price Analysis:")
    print(f"   Total candles: {len(close_values)}")
    print(f"   NaN count: {np.isnan(close_values).sum()}")
    print(f"   Inf count: {np.isinf(close_values).sum()}")
    print(f"   Zero count: {(close_values == 0).sum()}")
    print(f"   Min value: {np.nanmin(close_values)}")
    print(f"   Max value: {np.nanmax(close_values)}")
    
    # Show problematic candles
    problematic_indices = []
    for i, close_val in enumerate(close_values):
        if np.isnan(close_val) or np.isinf(close_val) or close_val <= 0:
            actual_idx = start_idx + i
            problematic_indices.append(actual_idx)
            print(f"   [BAD] Index {actual_idx}: close={close_val}, timestamp={df.loc[actual_idx, 'timestamp']}")
    
    if not problematic_indices:
        print(f"   [OK] All close values appear valid")
        
        # Test LRC calculation
        print(f"\n[TEST] Testing LRC calculation on this window...")
        try:
            # Simple linear regression calculation for testing
            close_array = close_values.astype(np.float64)
            
            print(f"   Close array shape: {close_array.shape}")
            print(f"   Close array dtype: {close_array.dtype}")
            print(f"   Close array sample: [{close_array[0]:.2f}, {close_array[1]:.2f}, ..., {close_array[-2]:.2f}, {close_array[-1]:.2f}]")
            
            # Basic linear regression on the 20 values
            x = np.arange(len(close_array))
            
            # Check for any problematic values
            if np.any(np.isnan(close_array)):
                print(f"   [ERROR] Found NaN in close_array")
            elif np.any(np.isinf(close_array)):
                print(f"   [ERROR] Found Inf in close_array")
            elif np.any(close_array <= 0):
                print(f"   [ERROR] Found zero/negative in close_array")
            else:
                # Simple linear regression
                A = np.vstack([x, np.ones(len(x))]).T
                try:
                    slope, intercept = np.linalg.lstsq(A, close_array, rcond=None)[0]
                    print(f"   Linear regression: slope={slope:.6f}, intercept={intercept:.2f}")
                    
                    # Calculate regression line value at the end
                    reg_value = slope * (len(x) - 1) + intercept
                    print(f"   Regression value at end: {reg_value:.2f}")
                    
                    # Calculate standard deviation of residuals
                    reg_line = slope * x + intercept
                    residuals = close_array - reg_line
                    std_dev = np.std(residuals)
                    print(f"   Standard deviation of residuals: {std_dev:.4f}")
                    
                    # Simulate LRC bands (2 std dev)
                    upper_band = reg_value + 2 * std_dev
                    lower_band = reg_value - 2 * std_dev
                    
                    print(f"   Simulated LRC bands:")
                    print(f"     Upper: {upper_band:.2f}")
                    print(f"     Middle: {reg_value:.2f}")
                    print(f"     Lower: {lower_band:.2f}")
                    
                    if np.isnan(upper_band) or np.isnan(reg_value) or np.isnan(lower_band):
                        print(f"   [ERROR] Simple LRC calculation returned NaN!")
                    else:
                        print(f"   [OK] Simple LRC calculation succeeded")
                        
                except np.linalg.LinAlgError as e:
                    print(f"   [ERROR] Linear algebra error: {e}")
                except Exception as e:
                    print(f"   [ERROR] Calculation error: {e}")
                
        except Exception as e:
            print(f"   [ERROR] Error during LRC test: {e}")
            import traceback
            traceback.print_exc()
    
    # Show the full window for manual inspection
    print(f"\n[WINDOW] Full LRC Window Data:")
    print("Index | Timestamp           | Open     | High     | Low      | Close")
    print("-" * 70)
    for i, row in window_df.iterrows():
        marker = "[BAD]" if i in problematic_indices else "     "
        print(f"{marker}{i:4d} | {row['timestamp']} | {row['open']:8.2f} | {row['high']:8.2f} | {row['low']:8.2f} | {row['close']:8.2f}")
    
    return window_df

async def main():
    if len(sys.argv) < 3:
        print("Usage: python debug_lrc_nan.py <symbol> <timestamp>")
        print("Example: python debug_lrc_nan.py ADANIENT26FEBFUT '2026-02-09 14:44:00'")
        sys.exit(1)
    
    symbol = sys.argv[1]
    target_timestamp = sys.argv[2]
    
    print(f"[DEBUG] Debugging LRC NaN for {symbol} at {target_timestamp}")
    print("=" * 60)
    
    # Fetch OHLC data
    df = await get_ohlc_data_from_redis(symbol, "1minute", target_timestamp)
    if df is None:
        print("[ERROR] Failed to fetch OHLC data")
        return
    
    # Find target candle
    target_idx = find_target_candle_index(df, target_timestamp)
    if target_idx is None:
        print("[ERROR] Could not find target candle")
        return
    
    # Inspect LRC window
    window_df = inspect_lrc_window(df, target_idx, lrc_period=20)
    
    print(f"\n" + "=" * 60)
    print(f"[COMPLETE] Analysis complete for {symbol} at {target_timestamp}")

if __name__ == "__main__":
    asyncio.run(main())