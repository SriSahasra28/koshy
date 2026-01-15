"""Check Redis data availability for validation"""
import redis
import json
from datetime import datetime, timedelta

def check_redis_data():
    r = redis.from_url('redis://localhost', decode_responses=True)
    
    # Get sample keys
    keys = r.keys('resampled_ohlc_sorted:*')
    print(f"Total Redis keys (resampled_ohlc_sorted): {len(keys)}")
    
    if not keys:
        print("No Redis keys found!")
        return
    
    # Check a few sample keys
    print("\n" + "=" * 80)
    print("SAMPLE REDIS KEYS ANALYSIS")
    print("=" * 80)
    
    for key in keys[:5]:
        print(f"\nKey: {key}")
        ttl = r.ttl(key)
        count = r.zcard(key)
        print(f"  Count: {count} candles")
        print(f"  TTL: {ttl} seconds ({ttl/3600:.1f} hours)" if ttl > 0 else "  TTL: No expiration")
        
        # Get first and last entry
        first_entry = r.zrange(key, 0, 0)
        last_entry = r.zrange(key, -1, -1)
        
        if first_entry:
            try:
                first_data = json.loads(first_entry[0])
                first_ts = first_data.get('datetime') or first_data.get('timestamp')
                print(f"  First candle: {first_ts}")
            except:
                pass
        
        if last_entry:
            try:
                last_data = json.loads(last_entry[0])
                last_ts = last_data.get('datetime') or last_data.get('timestamp')
                print(f"  Last candle: {last_ts}")
            except:
                pass
    
    # Check for a specific symbol (one we know exists)
    print("\n" + "=" * 80)
    print("CHECKING SPECIFIC SYMBOL: ASHOKLEY26JANFUT")
    print("=" * 80)
    
    test_symbol = "ASHOKLEY26JANFUT"
    timeframes = ['1minute', '2minute', '5minute', '10minute', '15minute']
    
    for tf in timeframes:
        key = f"resampled_ohlc_sorted:{test_symbol}:{tf}"
        exists = r.exists(key)
        count = r.zcard(key) if exists else 0
        print(f"{tf:15s} - Exists: {str(exists):5s} - Count: {count:5d} candles")

if __name__ == "__main__":
    check_redis_data()
