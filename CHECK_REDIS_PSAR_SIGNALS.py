"""
Check what PSAR signals were stored in Redis for this alert
"""
import redis
import json
from datetime import datetime
import pandas as pd

def get_redis_connection():
    return redis.from_url('redis://localhost', decode_responses=True)

def check_redis_psar_signals(symbol, alert_timestamp):
    redis_client = get_redis_connection()
    
    alert_dt = pd.to_datetime(alert_timestamp)
    
    # PSAR key format: psar:{symbol}:{interval}
    psar_key = f"psar:{symbol}:1minute"
    
    print("\n" + "="*80)
    print("CHECKING REDIS PSAR SIGNALS")
    print("="*80)
    print(f"\nSymbol: {symbol}")
    print(f"Alert Timestamp: {alert_timestamp}")
    print(f"PSAR Key: {psar_key}")
    
    # Check if key exists
    if not redis_client.exists(psar_key):
        print("\n[INFO] PSAR key does not exist in Redis")
        return
    
    # Get all entries (stored as hash)
    entries = redis_client.hgetall(psar_key)
    
    if not entries:
        print("\n[INFO] PSAR key exists but is empty")
        return
    
    print(f"\n[INFO] Found {len(entries)} PSAR entries in Redis")
    
    # Parse and find entries around alert time
    parsed_entries = []
    for timestamp_str, value_str in entries.items():
        try:
            value_data = json.loads(value_str)
            ts = pd.to_datetime(timestamp_str)
            parsed_entries.append({
                'timestamp': ts,
                'psar_value': value_data.get('value'),
                'signal': value_data.get('signal', 0)
            })
        except:
            continue
    
    parsed_entries.sort(key=lambda x: x['timestamp'])
    
    # Find alert candle and show surrounding candles
    alert_idx = None
    for idx, entry in enumerate(parsed_entries):
        if entry['timestamp'] == alert_dt:
            alert_idx = idx
            break
    
    if alert_idx is None:
        # Find closest
        time_diffs = [abs((e['timestamp'] - alert_dt).total_seconds()) for e in parsed_entries]
        alert_idx = time_diffs.index(min(time_diffs))
        closest_time = parsed_entries[alert_idx]['timestamp']
        print(f"\n[WARNING] Exact alert time not found, using closest: {closest_time}")
    else:
        print(f"\n[OK] Found alert candle in Redis")
    
    print(f"\nPSAR Signals around alert candle (from Redis, ±5 candles):")
    print("-" * 80)
    start_idx = max(0, alert_idx - 5)
    end_idx = min(len(parsed_entries), alert_idx + 6)
    
    for i in range(start_idx, end_idx):
        entry = parsed_entries[i]
        marker = " <-- ALERT" if i == alert_idx else ""
        print(f"  [{i}] {entry['timestamp']}: PSAR={entry['psar_value']:.2f}, Signal={int(entry['signal'])}{marker}")
    
    alert_entry = parsed_entries[alert_idx]
    print(f"\n{'='*80}")
    print("ALERT CANDLE FROM REDIS:")
    print("="*80)
    print(f"  Timestamp: {alert_entry['timestamp']}")
    print(f"  PSAR Value: {alert_entry['psar_value']}")
    print(f"  PSAR Signal: {int(alert_entry['signal'])}")
    print(f"  Required Signal: 1")
    
    if int(alert_entry['signal']) == 0:
        print(f"\n[ERROR] Redis also shows signal=0 - this confirms the alert should NOT have been generated")
    elif int(alert_entry['signal']) == 1:
        print(f"\n[WARNING] Redis shows signal=1, but MySQL calculation shows signal=0")
        print(f"  This suggests Redis had stale or incorrect data")

if __name__ == "__main__":
    check_redis_psar_signals("ADANIENT26JANFUT", "2026-01-20 11:54:00")
