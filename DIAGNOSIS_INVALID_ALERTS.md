# Diagnosis: Why Alerts Were Incorrectly Triggered

## Summary
Alert ID 21386 (2026-01-14 15:29:00, 1min) was incorrectly triggered 8 times for ASHOKLEY26JANFUT with Condition 6 (CN-Test).

## Condition 6 Requirements
- **Candle Type**: 2 (Green candle + no lower wick + upper wick)
- **PSAR Signal Direction**: 1 (Bullish - PSAR below price)
- **K Range**: [10, 100]

## Validation Results
- **K value**: 27.62 ✅ PASSED (within range)
- **PSAR signal**: -1 ❌ FAILED (required 1, got -1)
- **PSAR value**: 186.64
- **Candle type 2 check**: ❌ FAILED

## Root Cause Analysis

### 1. PSAR Signal Mismatch (CRITICAL)
- **Expected**: PSAR signal = 1 (bullish/PSAR below price)
- **Actual**: PSAR signal = -1 (bearish/PSAR above price)
- **Impact**: This should have **prevented the alert** according to the original code logic

### 2. Candle Type 2 Check Failed
Candle Type 2 requires ALL of:
- ✅ Green candle (close_ha > open_ha)
- ❌ No lower wick (abs(low_ha - open_ha) <= 0.001)
- ❌ Upper wick (high_ha > close_ha)

At least one of these conditions failed.

## Original Code Logic (redis_alert_engine.py)

### Condition Check Order (lines 1976-2040):
1. **K value check** (line 1977-1989): Checks if K is in range, `continue` if fails
2. **PSAR signal check** (line 1991-2004): Checks if `psar_signal == signaldirection`, `continue` if fails
3. **Candle type check** (line 2006-2037): Sets `alert_triggered = True` if conditions met
4. **Alert processing** (line 2040-2045): Calls `process_alert()` if `alert_triggered == True`

### Expected Behavior
According to the code logic:
- Line 1992: `psar_signal_match = psar_signal == signaldirection`
- Line 1993: `if not psar_signal_match:`
- Line 2004: `continue` (skip this alert)

**The alert should NOT have been triggered because PSAR signal was -1 but required 1.**

## Possible Explanations

### Theory 1: PSAR Signal Calculation Issue
- The `signals` array (line 1654, 1899) might have incorrect values
- `get_psar_signals()` function (line 335-343) calculates signals based on crossover logic
- If PSAR signal was incorrectly calculated as 1 (instead of -1), the check would pass

### Theory 2: Data Timing Issue
- The alert might have been triggered at a different timestamp when conditions were met
- PSAR signal might have changed between alert time and validation time
- However, this is unlikely as we're validating the exact alert timestamp

### Theory 3: Code Version Mismatch
- The code we're examining might be a newer version
- The actual production code that generated these alerts might have different logic
- Different code path or bug that bypassed the PSAR signal check

### Theory 4: Candle Type Check Bypass
- The candle type check might have incorrectly passed
- Tolerance issues (0.001 for wick checks) might allow invalid candles
- However, our validation shows candle type 2 check failed

## Recommendation

1. **Check logs** for this alert timestamp to see what PSAR signal value was used
2. **Compare code versions** - check if production code differs from current code
3. **Add debug logging** to capture PSAR signal values at alert time
4. **Verify PSAR calculation** - check if `get_psar_signals()` logic is correct
5. **Review alert generation logic** - ensure all conditions are checked in the correct order

## Conclusion

The original code logic **appears correct** - it should reject alerts where PSAR signal doesn't match. However, alerts were triggered with PSAR signal -1 when 1 was required, suggesting:
- Either a bug in the PSAR signal calculation/storage
- Or a different code version was running
- Or conditions were checked in a different order/branch

Our validation correctly identifies these as **invalid alerts** that should not have been triggered.
