# Optimal Symbol-Timeframe Limits for Fastest Processing

## Current System Architecture

### Processing Pattern
- **Batch Size**: 30 symbol-timeframe combinations processed in parallel
- **Processing Flow**: Sequential batches (batch 1 → batch 2 → batch 3...)
- **Per Symbol Operations**:
  1. Fetch OHLC data from Redis (~50-200ms)
  2. Calculate indicators (PSAR, Stochastic, LRC) - **CPU intensive** (~500-2000ms)
  3. Check conditions against indicators (~100-500ms)
  4. Send Telegram messages if alerts trigger (~200-1000ms per alert)

### Bottlenecks
1. **CPU-bound**: Indicator calculations (TA-Lib, Numba LRC) - **biggest bottleneck**
2. **I/O-bound**: Redis fetches, MySQL writes, Telegram API calls
3. **Batch Queue**: Later batches wait for earlier batches to complete

## Performance Estimates

### Per Symbol-Timeframe Processing Time
- **Best case** (no alerts): ~500-1000ms
- **Average case** (1-2 alerts): ~1000-2000ms
- **Worst case** (multiple alerts + Telegram delays): ~2000-5000ms

### Batch Processing Time
- **Batch of 30** (parallel): ~1-3 seconds (if no alerts)
- **Batch of 30** (with alerts): ~3-10 seconds (depending on alert count)

### Timeframe Window Constraints
- **1-minute timeframe**: Must complete within 60 seconds
- **2-minute timeframe**: Must complete within 120 seconds
- **5-minute timeframe**: Must complete within 300 seconds
- **10-minute timeframe**: Must complete within 600 seconds
- **15-minute timeframe**: Must complete within 900 seconds

## Recommended Limits

### 🚀 **FASTEST Processing (Sub-3 minute alerts)**

**Target**: Complete all processing within 30-60 seconds per processing window

| Timeframe | Max Symbol-Timeframe Combinations | Recommended Symbols (if 3 timeframes) | Recommended Symbols (if 5 timeframes) |
|-----------|-----------------------------------|--------------------------------------|----------------------------------------|
| 1min      | **20-30**                         | **6-10 symbols**                     | **4-6 symbols**                         |
| 2min      | **30-50**                         | **10-16 symbols**                    | **6-10 symbols**                        |
| 5min      | **50-100**                        | **16-33 symbols**                    | **10-20 symbols**                      |
| 10min     | **100-150**                       | **33-50 symbols**                    | **20-30 symbols**                      |
| 15min     | **150-200**                       | **50-66 symbols**                    | **30-40 symbols**                       |

**Formula**: `Max Combinations = (Timeframe Minutes × 60) / (Avg Processing Time Per Symbol)`

### ⚡ **OPTIMAL Processing (3-5 minute alerts)**

**Target**: Complete all processing within 60-120 seconds per processing window

| Timeframe | Max Symbol-Timeframe Combinations | Recommended Symbols (if 3 timeframes) | Recommended Symbols (if 5 timeframes) |
|-----------|-----------------------------------|--------------------------------------|----------------------------------------|
| 1min      | **30-50**                         | **10-16 symbols**                    | **6-10 symbols**                        |
| 2min      | **50-80**                         | **16-26 symbols**                    | **10-16 symbols**                       |
| 5min      | **100-200**                       | **33-66 symbols**                    | **20-40 symbols**                      |
| 10min     | **200-300**                       | **66-100 symbols**                   | **40-60 symbols**                      |
| 15min     | **300-400**                       | **100-133 symbols**                  | **60-80 symbols**                      |

### 🐢 **ACCEPTABLE Processing (5-10 minute alerts)**

**Target**: Complete all processing within 120-300 seconds per processing window

| Timeframe | Max Symbol-Timeframe Combinations | Recommended Symbols (if 3 timeframes) | Recommended Symbols (if 5 timeframes) |
|-----------|-----------------------------------|--------------------------------------|----------------------------------------|
| 1min      | **50-100**                        | **16-33 symbols**                    | **10-20 symbols**                      |
| 2min      | **80-150**                        | **26-50 symbols**                    | **16-30 symbols**                      |
| 5min      | **200-400**                       | **66-133 symbols**                   | **40-80 symbols**                      |
| 10min     | **400-600**                       | **133-200 symbols**                  | **80-120 symbols**                     |
| 15min     | **600-800**                       | **200-266 symbols**                  | **120-160 symbols**                    |

## Practical Recommendations

### 🎯 **For Your Use Case (Fastest Processing)**

**If you want alerts within 3 minutes:**

1. **Focus on 1-2 timeframes** (not all 5-8 timeframes)
   - Example: Only `1min` and `5min` instead of `1min, 2min, 3min, 5min, 10min, 15min`

2. **Limit to 10-15 high-priority symbols**
   - Use basket priorities to filter
   - Focus on most liquid/volatile symbols

3. **Reduce conditions per symbol**
   - Each condition adds processing time
   - Aim for 1-3 conditions per symbol-timeframe

**Example Configuration:**
```
Symbols: 10-15
Timeframes: 2 (1min, 5min)
Total Combinations: 20-30
Expected Processing Time: 30-60 seconds
Alert Delay: 1-3 minutes ✅
```

### 📊 **Calculation Example**

**Scenario**: 20 symbols × 3 timeframes = 60 combinations

**Processing**:
- Batch 1 (30 combinations): ~2-5 seconds
- Batch 2 (30 combinations): ~2-5 seconds
- **Total**: ~4-10 seconds per processing window ✅

**Result**: Fast processing, alerts within 3-5 minutes

### ⚠️ **Warning Signs (Too Many Combinations)**

If you see these in logs:
- `[BATCH] Batch X/Y | Duration: >10s` → Too many combinations
- `[BATCH] Processing >100 alert tasks` → Need to reduce
- Alerts arriving 10-15+ minutes late → System overloaded

**Action**: Reduce symbols or timeframes

### 📍 **Where to See These Logs**

The logs appear in the **console/terminal window** where `main-consumer.py` is running.

**To view logs:**

1. **If running in a terminal window:**
   - Open the PowerShell/Command Prompt window where you started `main-consumer.py`
   - Logs appear in real-time with timestamps
   - Look for lines starting with `[BATCH]`

2. **If running in background (Windows Task Scheduler or service):**
   - Check the console output if redirected to a file
   - Or run `main-consumer.py` in a visible terminal window to see logs

3. **Example log output:**
   ```
   2025-01-17 10:25:00 | INFO | [BATCH] Processing 45 alert tasks in 2 batches
   2025-01-17 10:25:00 | INFO | [BATCH] Executing batch 1/2 | Tasks: 30
   2025-01-17 10:25:03 | INFO | [BATCH] Batch 1/2 | Duration: 2.45s | Success: 30 | Errors: 0
   2025-01-17 10:25:03 | INFO | [BATCH] Executing batch 2/2 | Tasks: 15
   2025-01-17 10:25:05 | INFO | [BATCH] Batch 2/2 | Duration: 1.89s | Success: 15 | Errors: 0
   ```

4. **To run and see logs:**
   ```powershell
   cd C:\Users\Administrator\Desktop\koshy-trading-app-client_2025\koshy_python
   python main-consumer.py
   ```
   The logs will stream in real-time in this terminal window.

## Optimization Strategies

### 1. **Prioritize Timeframes**
- **Fast alerts**: Use `1min` and `2min` only
- **Medium alerts**: Use `5min` and `10min`
- **Slow alerts**: Use `15min` and `30min`

### 2. **Use Basket Priorities**
- Enable only high-priority symbols in baskets
- Disable low-priority symbols during market hours

### 3. **Condition Optimization**
- Reduce number of conditions per symbol
- Disable complex conditions (LRC filters, multiple PSAR checks)

### 4. **Batch Size Tuning** (Advanced)
If you have powerful hardware, you can increase `BATCH_SIZE` in `main-consumer.py`:
```python
BATCH_SIZE = 50  # Instead of 30 (requires more CPU/RAM)
```

**Warning**: Only increase if you have:
- 8+ CPU cores
- 16GB+ RAM
- Fast SSD storage

## Summary

### 🎯 **Recommended Starting Point**

**For fastest processing (1-3 minute alerts):**
- **10-15 symbols**
- **2-3 timeframes** (e.g., `1min`, `5min`)
- **1-2 conditions per symbol**
- **Total combinations: 20-45**

**This gives you:**
- Processing time: 30-90 seconds per window
- Alert delay: 1-3 minutes ✅
- System load: Moderate (sustainable)

### 📈 **Scaling Up**

If you need more symbols:
1. **Reduce timeframes** (e.g., only `5min` instead of `1min, 2min, 5min`)
2. **Use basket priorities** to enable/disable symbols dynamically
3. **Monitor batch durations** in logs - keep under 5 seconds per batch

### 🔍 **Monitoring**

Watch these log patterns:
```
[BATCH] Processing X alert tasks in Y batches
[BATCH] Batch 1/Y | Duration: X.XXs
```

**Good**: Duration < 5s per batch, total batches < 5
**Bad**: Duration > 10s per batch, total batches > 10
