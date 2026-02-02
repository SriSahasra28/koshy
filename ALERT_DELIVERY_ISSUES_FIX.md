# Alert Delivery Issues & Fixes

## Issues Found

### 1. Telegram: Silent Failures (No Retry)
**Location:** `redis_alert_engine.py:628-639`
- ❌ Telegram errors are logged but not retried
- ❌ Alert silently lost if API fails

### 2. Frontend WebSocket: No Error Handling
**Location:** `ws-server.js:38-44`
- ❌ `client.send()` can fail silently
- ❌ No verification message was received

### 3. Redis Pub/Sub: No Retry
**Location:** `redis_alert_engine.py:678`
- ❌ Redis publish failures not handled
- ❌ Frontend doesn't get notified if Redis fails

### 4. Delivery Order: Proceeds Even if DB Fails
**Location:** `redis_alert_engine.py:573-681`
- ❌ Telegram/Redis sent even if DB save fails
- ❌ Inconsistent state (alerts in Telegram but not DB)

## Quick Fixes

### Fix 1: Add Telegram Retry
```python
# Around line 628
max_telegram_retries = 3
telegram_sent = False
for attempt in range(max_telegram_retries):
    try:
        success = await send_telegram_message(...)
        if success:
            telegram_sent = True
            break
    except Exception as e:
        logger.warning(f"Telegram attempt {attempt+1}/{max_telegram_retries} failed: {e}")
        if attempt < max_telegram_retries - 1:
            await asyncio.sleep(0.5 * (attempt + 1))
if not telegram_sent:
    logger.error(f"CRITICAL: Failed to send Telegram after {max_telegram_retries} attempts")
```

### Fix 2: Add WebSocket Error Handling
```javascript
// ws-server.js:38-44
for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
        try {
            client.send(message);
            console.log('sent message');
        } catch (error) {
            console.error('WebSocket send failed:', error);
            clients.delete(client); // Remove broken connection
        }
    }
}
```

### Fix 3: Add Redis Pub/Sub Retry
```python
# Around line 678
max_redis_retries = 3
redis_published = False
for attempt in range(max_redis_retries):
    try:
        await redis_client.publish('alerts', alert_json)
        redis_published = True
        break
    except Exception as e:
        logger.warning(f"Redis publish attempt {attempt+1}/{max_redis_retries} failed: {e}")
        if attempt < max_redis_retries - 1:
            await asyncio.sleep(0.1 * (attempt + 1))
if not redis_published:
    logger.error(f"CRITICAL: Failed to publish to Redis after {max_redis_retries} attempts")
```

### Fix 4: Only Send After DB Success
```python
# Around line 607-681
if db_success:
    # Only send Telegram and Redis if DB save succeeded
    # Move Telegram and Redis code inside this block
    try:
        await send_telegram_message(...)  # With retry (Fix 1)
    except Exception as e:
        logger.error(f"Error sending Telegram: {e}")
    
    try:
        await redis_client.publish('alerts', alert_json)  # With retry (Fix 3)
    except Exception as e:
        logger.error(f"Error publishing to Redis: {e}")
else:
    logger.error("Skipping Telegram/Redis - DB save failed")
```

## Priority
**HIGH** - Users may miss alerts silently

## Expected Outcome
- ✅ Telegram alerts retry on failure
- ✅ Frontend WebSocket errors handled
- ✅ Redis pub/sub retries on failure  
- ✅ Consistent state (DB must succeed before notifications)
