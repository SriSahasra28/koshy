import redis

# Connect to Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Use SCAN to find all keys matching 'ohlc:*'
cursor = 0
while True:
    cursor, keys = r.scan(cursor=cursor, match='ohlc:*', count=100)
    
    if keys:
        # Delete the matched keys
        r.delete(*keys)
    
    # If the cursor is 0, we've scanned the entire keyspace
    if cursor == 0:
        break

print("All 'ohlc:' keys deleted.")
