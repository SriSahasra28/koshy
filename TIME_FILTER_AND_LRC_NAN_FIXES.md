# Time Filter and LRC NaN Error Fixes

## Issues Fixed

### 1. Time Filter Type Error
**Error:** `'<=' not supported between instances of 'Timedelta' and 'datetime.time'`

**Root Cause:**
- MySQL `TIME` columns are sometimes returned as `Timedelta` objects by pandas when reading from the database
- The code was only handling string and `datetime.time` types
- When comparing `Timedelta` with `datetime.time`, Python throws a type error

**Fix:**
- Enhanced `parse_time()` function to handle multiple input types:
  - String (e.g., `"09:30:00"`)
  - `datetime.time` objects
  - `Timedelta` objects (converts to time by extracting hours/minutes/seconds)
  - Other types (converts to string first, then parses)
- Added validation to skip time filter check if parsing fails (logs warning but doesn't crash)

**Code Location:** `redis_alert_engine.py` lines 2117-2142

---

### 2. LRC NaN Error
**Error:** `RESULT: REJECTED | REASON: LRC values are NaN`

**Root Cause:**
- LRC filter was enabled (`lrc_filter_enabled=1`, `lrc_filter_type` set)
- But LRC values were not calculated or stored (e.g., invalid LRC config like `"20, 50, 50"` instead of `"20, 2"`)
- Code was checking `if lrc_filter_enabled and lrc_filter_type and LRL is not None...`
- If LRC wasn't calculated, `LRL`, `UCL`, `LCL` remained `None`
- Code tried to access `LRL[i]` which failed or returned NaN

**Fix:**
- Added check: if LRC filter is enabled but no LRC data is available, log a warning and **skip the LRC filter check** (don't reject the alert)
- This allows alerts to proceed even if LRC calculation failed
- Previously, alerts were being rejected when LRC data was missing

**Code Location:** 
- `redis_alert_engine.py` lines 1681-1706 (LRC data population)
- `redis_alert_engine.py` lines 2185-2238 (LRC filter check)

---

## Testing

### Test Time Filter Fix
1. Create a condition with time filter enabled (e.g., exclude 09:30-10:00)
2. Check logs - should no longer see `'<=' not supported` error
3. Alerts should be properly filtered by time

### Test LRC NaN Fix
1. Check conditions with LRC filter enabled
2. Verify LRC config values are valid (format: `"period,std_dev"` like `"20,2"`)
3. If LRC config is invalid, alerts should still proceed (with warning log)
4. If LRC config is valid but calculation fails, alerts proceed with warning

---

## Impact

- **Time Filter:** Now handles all time value types from database, no more crashes
- **LRC Filter:** Alerts no longer rejected when LRC data is unavailable (graceful degradation)
- **Logging:** Better error messages to diagnose issues

---

## Next Steps

1. Check conditions with invalid LRC config values (e.g., `"20, 50, 50"`)
2. Fix LRC config values in `custom_indicators` table to match expected format: `"period,std_dev"`
3. Monitor logs for LRC warnings to identify conditions that need LRC config fixes
