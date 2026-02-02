# Why Longer Timeframes Have Delayed Alerts

## The Problem
- **1min and 2min timeframes**: Alerts arrive within 3 minutes ✅
- **3min, 5min, 10min, 15min timeframes**: Alerts arrive after 10-15 minutes ❌

## Root Cause Analysis

### 1. **Candle Completion Logic** (`closed='right'`)

In `main-consumer.py` line 397, resampling uses:
```python
resampled = df_resample.resample(
    f'{interval_minutes}min', origin='start_day', offset='15min', label='left', closed='right'
)
```

**`closed='right'`** means a candle is only considered **"complete"** when the **next candle starts**.

**Example for 5-minute candles:**
- Candle starts at `09:20:00`
- Candle is **incomplete** until `09:25:00` (when next candle starts)
- System can only process alerts **after** `09:25:00`

**Example for 15-minute candles:**
- Candle starts at `09:15:00`
- Candle is **incomplete** until `09:30:00` (when next candle starts)
- System can only process alerts **after** `09:30:00`

### 2. **Processing Window Gate** (`should_process_timeframe`)

In `main-consumer.py` line 338-364, the system only processes at specific minute boundaries:

```python
def should_process_timeframe(current_time, interval):
    # ...
    minutes_since_start = int((last_closed - day_start).total_seconds() // 60)
    return minutes_since_start % interval_minutes == 0
```

**This means:**
- **1-minute**: Processes every minute (09:16, 09:17, 09:18, ...)
- **2-minute**: Processes at 09:16, 09:18, 09:20, 09:22, ...
- **5-minute**: Processes at 09:20, 09:25, 09:30, 09:35, ...
- **10-minute**: Processes at 09:20, 09:30, 09:40, 09:50, ...
- **15-minute**: Processes at 09:30, 09:45, 10:00, 10:15, ...

### 3. **The Delay Chain**

For a **5-minute timeframe**:
1. **Candle starts** at `09:20:00`
2. **Candle completes** at `09:25:00` (5 minutes later)
3. **System checks** at `09:25:00` → `should_process_timeframe` returns `True`
4. **Alert processing** happens at `09:25:00`
5. **Total delay**: ~5 minutes (minimum)

For a **15-minute timeframe**:
1. **Candle starts** at `09:15:00`
2. **Candle completes** at `09:30:00` (15 minutes later)
3. **System checks** at `09:30:00` → `should_process_timeframe` returns `True`
4. **Alert processing** happens at `09:30:00`
5. **Total delay**: ~15 minutes (minimum)

### 4. **Why 10-15 Minutes Extra Delay?**

The additional 5-10 minutes beyond the candle period could be due to:
- **Processing queue**: If many symbols/timeframes are queued, processing may be delayed
- **Network/Redis latency**: Fetching and storing data takes time
- **Database writes**: Saving 1-minute candles to MySQL
- **Alert engine computation**: Calculating indicators (PSAR, Stochastic, LRC, etc.)
- **Batch processing**: System processes in batches of 30, so if you're not in the first batch, you wait

## Why 1min and 2min Are Fast

- **1-minute**: Candle completes in 1 minute, processes every minute → **~1-2 minute delay**
- **2-minute**: Candle completes in 2 minutes, processes every 2 minutes → **~2-3 minute delay**

## Solutions to Reduce Delay

### Option 1: Process on "Current" Candle (Not Just Completed)
**Change**: Process alerts based on the **current forming candle** instead of waiting for completion.

**Pros**: Alerts arrive faster (within 1-2 minutes regardless of timeframe)
**Cons**: Alert data is based on incomplete candles (may change as candle forms)

### Option 2: Process More Frequently
**Change**: Check `should_process_timeframe` more often (e.g., every 30 seconds instead of every minute).

**Pros**: Reduces delay by catching completed candles sooner
**Cons**: More CPU usage, but minimal impact

### Option 3: Parallel Processing Optimization
**Change**: Optimize batch sizes and parallel processing to reduce queue delays.

**Pros**: Faster processing without changing alert logic
**Cons**: Requires performance tuning

### Option 4: Hybrid Approach (Recommended)
**Change**: 
- For **1min and 2min**: Keep current logic (fast enough)
- For **3min+**: Process on current forming candle OR check every 30 seconds

**Pros**: Best of both worlds
**Cons**: More complex logic

## Current Behavior Summary

| Timeframe | Candle Period | Processing Window | Minimum Delay | Actual Delay (Your Observation) |
|-----------|---------------|-------------------|---------------|----------------------------------|
| 1min      | 1 minute      | Every minute      | ~1 min        | ~1-3 min ✅                       |
| 2min      | 2 minutes     | Every 2 minutes   | ~2 min        | ~2-3 min ✅                       |
| 3min      | 3 minutes     | Every 3 minutes   | ~3 min        | ~10-15 min ❌                    |
| 5min      | 5 minutes     | Every 5 minutes   | ~5 min        | ~10-15 min ❌                    |
| 10min     | 10 minutes    | Every 10 minutes  | ~10 min       | ~10-15 min ❌                    |
| 15min     | 15 minutes    | Every 15 minutes  | ~15 min       | ~10-15 min ❌                    |

**Note**: The 3min timeframe showing 10-15 min delay suggests additional processing/queue delays beyond the minimum candle period.
