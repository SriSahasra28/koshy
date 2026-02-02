# LRC & Time Filter Testing Guide

## Prerequisites
- ✅ Database columns added (`lrc_filter_enabled`, `lrc_filter_type`, `time_filter_enabled`, `time_filter_start`, `time_filter_end`)
- ✅ Frontend code updated (`Condition.jsx`, `charts.apis.js`)
- ✅ Backend API updated (`data.controller.js`)
- ✅ Alert engine updated (`redis_alert_engine.py`)

---

## Step 1: Start the Application

### 1.1 Start Backend Server
```powershell
cd koshy-trading-app-server
npm start
# Server should run on port 1000
```

### 1.2 Start Frontend
```powershell
cd koshy-trading-app-client
npm start
# App should run on port 1100
```

### 1.3 Verify Backend Connection
- Open browser: `http://localhost:1100`
- Login to the app
- Navigate to "Condition" page
- Verify you can see existing conditions

---

## Step 2: Test LRC Filter - Create New Condition

### 2.1 Create Condition with LRC Filter
1. Go to **Condition** page
2. Click **"Add Condition"** (if not already in add mode)
3. Fill in basic condition details:
   - Name: `Test-LRC-Filter`
   - Select LRC, PSAR, Stochastic, HLFP settings
4. Scroll to **"LRC Filter"** section
5. ✅ **Check "Enable LRC Filter"**
6. Select one option:
   - **"High is below middle LRC"** OR
   - **"High is below lower LRC"**
7. Click **"Save"**

### 2.2 Verify Database Storage
```sql
SELECT id, name, lrc_filter_enabled, lrc_filter_type 
FROM conditions 
WHERE name = 'Test-LRC-Filter';
```

**Expected Result:**
- `lrc_filter_enabled` = 1
- `lrc_filter_type` = `'high_below_middle'` or `'high_below_lower'`

### 2.3 Verify Frontend Display (Edit Mode)
1. Click **Edit** icon on the condition you just created
2. Verify:
   - ✅ "Enable LRC Filter" checkbox is checked
   - Radio button for selected type is selected
3. Change filter type and save
4. Verify changes persist

---

## Step 3: Test Time Filter - Create New Condition

### 3.1 Create Condition with Time Filter
1. Create a new condition: `Test-Time-Filter`
2. Fill in basic condition details
3. Scroll to **"Time Filter"** section
4. ✅ **Check "Enable Time Filter"**
5. Set time range:
   - **Start Time:** `09:30` (market opening)
   - **End Time:** `10:00` (exclude first 30 minutes)
6. Click **"Save"**

### 3.2 Verify Database Storage
```sql
SELECT id, name, time_filter_enabled, time_filter_start, time_filter_end 
FROM conditions 
WHERE name = 'Test-Time-Filter';
```

**Expected Result:**
- `time_filter_enabled` = 1
- `time_filter_start` = `'09:30:00'`
- `time_filter_end` = `'10:00:00'`

### 3.3 Test Cross-Midnight Time Range
1. Edit the condition
2. Set time range:
   - **Start Time:** `22:00` (10 PM)
   - **End Time:** `02:00` (2 AM next day)
3. Save and verify in database

---

## Step 4: Test Combined Filters

### 4.1 Create Condition with Both Filters
1. Create condition: `Test-Combined-Filters`
2. Enable **both** LRC Filter and Time Filter
3. Configure:
   - LRC Filter: "High is below middle LRC"
   - Time Filter: `09:30` to `10:00`
4. Save

### 4.2 Verify Database
```sql
SELECT 
    id, name,
    lrc_filter_enabled, lrc_filter_type,
    time_filter_enabled, time_filter_start, time_filter_end
FROM conditions 
WHERE name = 'Test-Combined-Filters';
```

---

## Step 5: Test Backend Alert Engine

### 5.1 Verify Alert Engine Reads Filters
1. Check alert engine logs when processing symbols
2. Look for log messages indicating filter checks

**Expected Log Messages:**
- `"LRC filter active: ..."`
- `"Time filter active: ..."`
- `"LRC filter failed: High X not below Middle LRC Y"`
- `"Time filter active: HH:MM:SS is within excluded range [HH:MM:SS-HH:MM:SS]"`

### 5.2 Monitor Alert Generation
1. Add the test condition to a **Scan** with active symbols
2. Enable the scan
3. Monitor Redis alerts channel or Telegram for alerts
4. Verify alerts **only** trigger when:
   - LRC condition is met (if enabled)
   - Time is **outside** excluded range (if enabled)

---

## Step 6: End-to-End Testing

### 6.1 Test LRC Filter Behavior
**Setup:**
- Create condition with LRC filter: "High is below middle LRC"
- Add to scan with active symbol (e.g., `NIFTY`, `RELIANCE`)
- Enable scan

**Expected Behavior:**
- Alerts **only** trigger when candle high < middle LRC
- No alerts when high >= middle LRC
- Check alert engine logs for "LRC filter failed" messages

### 6.2 Test Time Filter Behavior
**Setup:**
- Create condition with Time filter: `09:30` to `10:00`
- Add to scan with active symbol
- Enable scan

**Expected Behavior:**
- **No alerts** between 09:30:00 and 10:00:00
- Alerts **only** trigger outside this range
- Check logs for "Time filter active" messages

### 6.3 Test Combined Filters
**Setup:**
- Use `Test-Combined-Filters` condition
- Add to scan

**Expected Behavior:**
- Alerts trigger **only** when:
  1. LRC condition is met (high < middle LRC)
  2. **AND** time is outside excluded range (not 09:30-10:00)
- Both conditions must be satisfied

---

## Step 7: Verification Queries

### 7.1 Check All Conditions with Filters
```sql
SELECT 
    id, name,
    lrc_filter_enabled, lrc_filter_type,
    time_filter_enabled, time_filter_start, time_filter_end
FROM conditions
WHERE lrc_filter_enabled = 1 OR time_filter_enabled = 1;
```

### 7.2 Check Recent Alerts for Filtered Condition
```sql
SELECT 
    a.id, a.symbol, a.alert_time, a.timeframe,
    c.name as condition_name,
    c.lrc_filter_enabled, c.time_filter_enabled
FROM alerts a
JOIN scanitems si ON a.scanitem_id = si.id
JOIN conditions c ON si.condition_id = c.id
WHERE c.lrc_filter_enabled = 1 OR c.time_filter_enabled = 1
ORDER BY a.alert_time DESC
LIMIT 20;
```

### 7.3 Verify Alert Time Distribution (Time Filter Test)
```sql
SELECT 
    HOUR(alert_time) as hour,
    MINUTE(alert_time) as minute,
    COUNT(*) as alert_count
FROM alerts a
JOIN scanitems si ON a.scanitem_id = si.id
JOIN conditions c ON si.condition_id = c.id
WHERE c.time_filter_enabled = 1
  AND DATE(alert_time) = CURDATE()
GROUP BY HOUR(alert_time), MINUTE(alert_time)
ORDER BY hour, minute;
```

**Expected:** No alerts in the excluded time range.

---

## Step 8: Troubleshooting

### Issue: Filters not showing in frontend
- ✅ Check browser console for errors
- ✅ Verify API response includes filter fields
- ✅ Check `fetchConditionById` returns all fields

### Issue: Filters not saving
- ✅ Check backend API logs
- ✅ Verify SQL query includes new columns
- ✅ Check database columns exist

### Issue: Alerts still triggering with filters enabled
- ✅ Check alert engine logs for filter check messages
- ✅ Verify condition data is being read correctly
- ✅ Check LRC values are being calculated and stored in Redis
- ✅ Verify time parsing (check for timezone issues)

### Issue: LRC filter always failing
- ✅ Verify LRC indicator is configured for the condition
- ✅ Check LRC values are stored in Redis
- ✅ Verify LRC calculation is running (check logs)
- ✅ Check Redis keys: `indicator:SYMBOL:timeframe:lrc`

---

## Step 9: Performance Check

### 9.1 Monitor Alert Engine Performance
- Check if LRC calculation adds significant latency
- Verify parallel calculation (PSAR, Stochastic, LRC) is working
- Monitor Redis operations for LRC data storage/retrieval

### 9.2 Expected Performance
- LRC calculation should run in parallel with PSAR/Stochastic
- Filter checks should add minimal overhead (< 1ms per alert check)
- No noticeable delay in alert generation

---

## Success Criteria

✅ **Frontend:**
- Can create/edit conditions with LRC and Time filters
- Filter settings persist after save
- Filter UI displays correctly in edit mode

✅ **Backend API:**
- Filter fields saved to database correctly
- Filter fields retrieved correctly when fetching conditions

✅ **Alert Engine:**
- Reads filter settings from database
- Applies LRC filter correctly (blocks alerts when condition not met)
- Applies Time filter correctly (blocks alerts in excluded time range)
- Logs filter decisions for debugging

✅ **End-to-End:**
- Alerts only trigger when all conditions (including filters) are met
- No alerts trigger when filters block them
- System performance remains acceptable

---

## Next Steps After Testing

1. **If all tests pass:** Deploy to production
2. **If issues found:** Check logs, verify database, review code changes
3. **Monitor in production:** Watch alert patterns, verify filter effectiveness
