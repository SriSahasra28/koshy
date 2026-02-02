# Alert Flow Analysis

## Current Alert Flow

### 1. **Alert Generation** (`redis_alert_engine.py` - `process_alert()`)

**Flow:**
```
1. Check if should_generate_alert = True
2. If True:
   a. Insert trade_log to MySQL
   b. Insert alert to MySQL via stored procedure `insert_alert()`
   c. If db_success = True:
      - Send Telegram notification
      - Store in Redis sorted sets (Alerts, alerts_simple)
      - Publish to Redis pub/sub channel 'alerts'
   d. If db_success = False:
      - Skip Telegram and Redis (alert NOT stored anywhere except logs)
```

### 2. **Frontend Display**

**Flow:**
```
1. Initial Load:
   - Frontend calls GET /alerts API
   - Backend queries MySQL alerts table directly
   - Returns alerts to frontend

2. Real-time Updates:
   - WebSocket connects to ws-server.js (port 8080)
   - ws-server.js subscribes to Redis 'alerts' channel
   - When alert is published, WebSocket pushes to frontend
   - Frontend receives message and calls GET /alerts API again
   - Fresh data loaded from MySQL
```

### 3. **The Problem**

**Issue:** Frontend shows alerts but MySQL doesn't have them

**Possible Causes:**

1. **MySQL Insert Failing Silently**
   - `insert_alert()` stored procedure might be failing
   - Errors might not be logged properly
   - `db_success` might be False but Redis is being populated somehow (unlikely based on code)

2. **Stored Procedure Issue**
   - `CALL insert_alert(symbol, datetime, scanid, timeframe, bot_time, conditionID)` might have issues
   - Parameters might be wrong format
   - Procedure might be rejecting duplicates silently

3. **Query Issue**
   - Frontend might be querying a different database
   - Or query might have wrong WHERE clause

4. **Redis Only Storage (Old Code)**
   - If old code is still running, it might store only in Redis
   - Need to check if main-consumer.py is using latest redis_alert_engine.py

5. **Timing Issue**
   - Alerts might be stored but with old timestamps
   - MySQL query might be filtering them out

## Diagnostic Steps

### Step 1: Run Diagnostic Script
```bash
python DIAGNOSE_ALERT_STORAGE.py
```

This will show:
- Latest alerts in MySQL
- Latest alerts in Redis
- Comparison of both
- Alerts in Redis but not in MySQL

### Step 2: Check Logs
Look for these log messages in `main_consumer.log`:
- `"✅ Alert inserted successfully"` - MySQL insert succeeded
- `"CRITICAL: Failed to save alert to database"` - MySQL insert failed
- `"Database insertion failed for {symbol}"` - Retry attempts
- `"[ALERT_CHECK] ... PASSED"` - Alert condition passed

### Step 3: Check MySQL Stored Procedure
```sql
-- Check if stored procedure exists
SHOW PROCEDURE STATUS WHERE Db = 'algo' AND Name = 'insert_alert';

-- Check latest alerts
SELECT * FROM alerts 
WHERE deleted = 0 
ORDER BY datetime DESC, system_time DESC 
LIMIT 20;

-- Check if alerts are being inserted but with wrong datetime
SELECT 
    symbol,
    datetime,
    system_time,
    TIMESTAMPDIFF(SECOND, datetime, system_time) AS time_diff_seconds
FROM alerts 
WHERE deleted = 0 
ORDER BY system_time DESC 
LIMIT 20;
```

### Step 4: Check Redis
```bash
# Connect to Redis
redis-cli

# Check alerts count
ZCARD Alerts
ZCARD alerts_simple

# Get latest alerts
ZRANGE Alerts -10 -1 WITHSCORES
ZRANGE alerts_simple -10 -1 WITHSCORES
```

## Expected Fixes

1. **Add Better Logging**
   - Log every step of alert insertion
   - Log stored procedure return values
   - Log any exceptions

2. **Verify Stored Procedure**
   - Check if it handles duplicates
   - Check if it validates parameters
   - Check if it returns success/failure

3. **Fix Code Flow**
   - Ensure alerts are ONLY stored in Redis if MySQL succeeds
   - Add fallback mechanism if MySQL fails
   - Better error reporting

4. **Check Database Connection**
   - Verify MySQL connection is active
   - Check if connection pool is working
   - Verify credentials
