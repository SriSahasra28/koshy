# PSAR Signal Calculation: Original vs Validation - CRITICAL DIFFERENCE FOUND

## Summary
**We found a FUNDAMENTAL difference in how PSAR signals are calculated!**

## Original Code (redis_alert_engine.py)

### Signal Calculation Method: **CROSSOVER-BASED**
- Uses `get_psar_signals()` function (line 335-343)
- Calculates signals based on **crossover logic**:
  ```python
  def get_psar_signals(close, psar_values):
      signals = np.zeros(len(close))   # Default: 0 (no signal)
      for i in range(1, len(close)):
          if close[i] > psar_values[i] and close[i-1] <= psar_values[i-1]:
              signals[i] = 1  # Long signal (price crossed ABOVE PSAR)
          elif close[i] < psar_values[i] and close[i-1] >= psar_values[i-1]:
              signals[i] = -1  # Short signal (price crossed BELOW PSAR)
      return signals
  ```
- **Signal values**: 1 (crossover up), -1 (crossover down), **0 (no crossover)**
- Signal is set to 1/-1 **only when price crosses PSAR**
- Signal remains 0 if price doesn't cross (even if PSAR is above/below price)
- Uses pre-calculated signals array: `psar_signal = signals[i]` (line 1899)

## Our Validation Code (audit_condition1_validation.py)

### Signal Calculation Method: **POSITION-BASED**
- Uses `get_psar_signal()` function (line 366-373) - **DIFFERENT FUNCTION NAME!**
- Calculates signal based on **simple position comparison**:
  ```python
  def get_psar_signal(psar_value, close_price):
      if psar_value < close_price:
          return 1  # Bullish (PSAR below price)
      else:
          return -1  # Bearish (PSAR above price)
  ```
- **Signal values**: Always 1 or -1 (never 0)
- Signal = 1 when PSAR < close (PSAR below price)
- Signal = -1 when PSAR >= close (PSAR above price)
- **No crossover logic** - just checks current position

## The Problem

**These are COMPLETELY DIFFERENT logics!**

1. **Original**: Signal = 1 only when price **crosses above** PSAR (crossover event)
   - Signal = 0 if price is above PSAR but didn't cross
   - Signal = 0 if price is below PSAR but didn't cross

2. **Validation**: Signal = 1 when PSAR < close (PSAR is below price)
   - Always 1 or -1, never 0
   - No crossover requirement

## Impact on Validation

This explains why our validation shows alerts as invalid:
- Original code might have had signal = 1 (from a previous crossover) 
- But our validation calculates signal = -1 (because PSAR > close at alert time)
- This mismatch causes validation to fail

## What We Need to Do

We need to match the original code's logic exactly:
1. Use `get_psar_signals()` (crossover-based) instead of `get_psar_signal()` (position-based)
2. Calculate signals array for all candles
3. Use `signals[i]` from the array, which can be 0, 1, or -1
4. Handle signal = 0 case (no signal, should reject alert)

## Next Steps

1. Update validation to use `get_psar_signals()` crossover logic
2. Calculate signals array for the entire OHLC dataset
3. Use `signals[candle_idx]` instead of `get_psar_signal(psar_value, close_price)`
4. Re-run validation to see if results match original behavior
