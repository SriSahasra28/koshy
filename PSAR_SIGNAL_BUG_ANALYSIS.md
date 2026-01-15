# PSAR Signal Bug Analysis

## Summary

The original `redis_alert_engine.py` has a bug where alerts are being triggered even when `psar_signal == 0` (no crossover), which should be rejected when `signaldirection != 0`.

## Code Flow

### 1. PSAR Signal Calculation (Line 335-343)

```python
def get_psar_signals(close, psar_values):
    signals = np.zeros(len(close))   
    for i in range(1, len(close)):
        if close[i] > psar_values[i] and close[i-1] <= psar_values[i-1]:
            signals[i] = 1  # Long signal (crossover above)
        elif close[i] < psar_values[i] and close[i-1] >= psar_values[i-1]:
            signals[i] = -1  # Short signal (crossover below)
    return signals
```

**Behavior:**
- Returns `1` when price crosses ABOVE PSAR
- Returns `-1` when price crosses BELOW PSAR
- Returns `0` otherwise (no crossover)

### 2. Signal Retrieval (Lines 1611, 1654)

Signals are retrieved from Redis or calculated fresh:

```python
# Line 1611: When PSAR key doesn't exist (fresh calculation)
signals = np.array([signals_dict.get(ts, 0) for ts in dates_str])

# Line 1654: When PSAR key exists (from Redis)
signals = np.array([stored_psar_normalized.get(normalize_timestamp(ts), {}).get('signal', 0) for ts in dates_str])
```

**Note:** Both default to `0` if signal not found, which is correct.

### 3. Signal Check (Lines 1899, 1992-2004)

```python
# Line 1899: Get signal for current candle
psar_signal = signals[i]

# Lines 1992-2004: Check if signal matches required direction
psar_signal_match = psar_signal == signaldirection
if not psar_signal_match:
    failure_reasons.append(f"PSAR signal {psar_signal} does not match required direction {signaldirection}")
    result_status = "REJECTED"
    result_reason = failure_reasons[-1]
    logger.info(...)
    continue  # Should skip this alert
```

**Expected Behavior:**
- If `psar_signal = 0` and `signaldirection = 1`: `0 == 1` → False → Should `continue` (skip alert)
- If `psar_signal = 0` and `signaldirection = -1`: `0 == -1` → False → Should `continue` (skip alert)
- If `psar_signal = 1` and `signaldirection = 1`: `1 == 1` → True → Should proceed
- If `psar_signal = -1` and `signaldirection = -1`: `-1 == -1` → True → Should proceed

## The Bug

**Validation Results Show:**
- 2 alerts were triggered with `psar_signal = 0.0` when `signaldirection = 1`
- These alerts should have been rejected by the check at line 1992-2004
- But they were still triggered and saved to the database

## Possible Root Causes

### Hypothesis 1: Signal Mismatch Between Calculation and Check
The signal might be calculated on one dataset (e.g., full OHLC data) but checked on a different dataset (e.g., windowed data), causing a mismatch.

**Evidence:**
- Line 1740: `signals_window = get_psar_signals(close_window, psar_data_window)` - calculates on window
- Line 1746: `signals = np.concatenate([np.zeros(padding_size, dtype=int), signals_window])` - pads with zeros
- Line 1899: `psar_signal = signals[i]` - uses padded array

**Issue:** If `i` is in the padding region (first `padding_size` indices), `signals[i] = 0`, which might not reflect the actual signal for that candle.

### Hypothesis 2: Stale Redis Data
Signals stored in Redis might be stale or incorrect, causing the check to use wrong values.

**Evidence:**
- Line 1654: Signals are retrieved from Redis: `stored_psar_normalized.get(normalize_timestamp(ts), {}).get('signal', 0)`
- If Redis has stale or incorrect signals, the check will use wrong values

### Hypothesis 3: Index Mismatch
The index `i` used to retrieve `signals[i]` might not correspond to the correct candle due to array alignment issues.

**Evidence:**
- Multiple padding operations (lines 1746, 1773) could cause misalignment
- If `data_combined` and `signals` arrays are not properly aligned, `signals[i]` might not correspond to the correct candle

### Hypothesis 4: Multiple Conditions / Code Path
There might be a different code path that bypasses the PSAR signal check, or multiple conditions are evaluated and one passes while another fails.

**Evidence:**
- The code structure shows a single condition evaluation path
- But there might be edge cases or exception handling that bypasses the check

## Validation Evidence

**From `audit_condition1_validation_original_logic.py`:**
```
Alert ID 21378: 2026-01-14 15:15:00 (2min)
  Reason: PSAR signal 0.0 != required 1 (crossover-based)
  K=59.75, PSAR_signal=0.0 (crossover-based), PSAR_value=186.65
```

**This confirms:**
- The alert was triggered when `psar_signal = 0.0`
- The required `signaldirection = 1`
- The check `0 == 1` should have failed and skipped the alert
- But the alert was still saved to the database

## Code Structure Analysis

### Exception Handling (Line 2121-2123)
```python
except Exception as e:
    logger.error(f"Error processing candle {i} for {symbol}: {e}")
    continue
```

**Note:** If an exception occurs during candle processing, it's caught and logged, then the loop continues. This should NOT cause alerts to be triggered - it should skip the candle.

### The Check Logic (Lines 1992-2004)
The code structure is correct:
1. Check `psar_signal == signaldirection`
2. If not equal, log rejection and `continue` (skip alert)
3. If equal, proceed to candle type check

**The check should work correctly**, but alerts are still being triggered with `psar_signal = 0` when `signaldirection = 1`.

## Most Likely Root Cause

### Hypothesis: Data Type or Comparison Issue
The comparison `psar_signal == signaldirection` might have a subtle issue:
- `psar_signal` is from `signals[i]` which is a numpy array element (could be `numpy.int64` or `numpy.float64`)
- `signaldirection` is from database (likely `int` or `float`)
- Python's `==` should handle this, but there might be edge cases

### Hypothesis: Signal Calculation Timing
Signals might be calculated on a different dataset than the one being checked:
- Line 1740: Signals calculated on `close_window` (windowed data)
- Line 1746: Signals padded with zeros: `np.concatenate([np.zeros(padding_size, dtype=int), signals_window])`
- Line 1899: Signal retrieved: `psar_signal = signals[i]`

**If `i` is in the padding region**, `signals[i] = 0`, which might not reflect the actual signal for that candle.

### Hypothesis: Redis Data Staleness
Signals retrieved from Redis (line 1654) might be stale or incorrect:
- Signals are stored after calculation (line 1631)
- But then immediately retrieved (line 1636)
- If there's a race condition or timestamp mismatch, wrong signals might be retrieved

## Recommended Fix

1. **Add Debug Logging:** Log the exact values and types of `psar_signal`, `signaldirection`, and the comparison result:
   ```python
   logger.debug(f"[PSAR_CHECK] psar_signal={psar_signal} (type={type(psar_signal)}), signaldirection={signaldirection} (type={type(signaldirection)}), match={psar_signal == signaldirection}")
   ```

2. **Verify Array Alignment:** Ensure `signals` array index `i` corresponds to the correct candle:
   ```python
   # Before line 1899, verify alignment
   if i < len(signals) and i < len(dates_combined):
       expected_timestamp = dates_combined[i]
       logger.debug(f"[ALIGNMENT] i={i}, timestamp={expected_timestamp}, signal={signals[i]}")
   ```

3. **Add Type Coercion:** Ensure both values are the same type before comparison:
   ```python
   psar_signal_match = int(psar_signal) == int(signaldirection)
   ```

4. **Add Assertion:** Add an assertion before `process_alert` to ensure the check passed:
   ```python
   assert psar_signal == signaldirection, f"PSAR signal check failed: {psar_signal} != {signaldirection}"
   ```

5. **Check Padding Logic:** Verify that padding doesn't cause index misalignment:
   - If `padding_size > 0`, ensure `i >= padding_size` before using `signals[i]`
   - Or recalculate signals on the full dataset instead of padding

## Next Steps

1. **Add detailed logging** around the PSAR signal check to capture exact values, types, and comparison results
2. **Verify array alignment** - ensure `signals[i]` corresponds to the correct candle at `dates_combined[i]`
3. **Check padding logic** - verify that padded zeros don't cause false matches
4. **Test with known invalid case** - run the engine with a test case where `signal = 0` and `signaldirection = 1` to see if the check is executed
5. **Review logs** - check the actual log files to see if the rejection is being logged but alerts are still being triggered (indicating a bug in `process_alert` or database insertion)
