-- Query to see complete Condition → Scan → Alert data flow for one instrument
-- This shows all the data stored in the database for a single symbol

-- Step 1: Pick one symbol from alerts (use the most recent one)
SET @symbol = (SELECT symbol FROM alerts WHERE deleted = 0 ORDER BY datetime DESC LIMIT 1);
-- OR manually set: SET @symbol = 'RELIANCE';

SELECT CONCAT('Using symbol: ', @symbol) AS info;

-- ============================================================
-- PART 1: ALERTS DATA (Final Output)
-- ============================================================
SELECT 
    '=== ALERTS (Final Output) ===' AS section,
    a.id AS alert_id,
    a.symbol,
    DATE_FORMAT(a.datetime, '%Y-%m-%d %H:%i:%s') AS alert_datetime,
    DATE_FORMAT(a.system_time, '%Y-%m-%d %H:%i:%s') AS system_time,
    a.scanid,
    a.timeframe,
    a.conditionID,
    a.deleted
FROM alerts a
WHERE a.symbol = @symbol 
  AND a.deleted = 0
ORDER BY a.datetime DESC
LIMIT 10;

-- ============================================================
-- PART 2: SCAN DATA (What scan generated these alerts)
-- ============================================================
SELECT 
    '=== SCANS (Scan Configuration) ===' AS section,
    s.id AS scan_id,
    s.name AS scan_name,
    s.basket_id,
    s.active
FROM scans s
WHERE s.id IN (
    SELECT DISTINCT scanid FROM alerts WHERE symbol = @symbol AND deleted = 0
);

-- ============================================================
-- PART 3: SCAN ITEMS (Which conditions & timeframes are enabled)
-- ============================================================
SELECT 
    '=== SCAN ITEMS (Condition + Timeframe Mapping) ===' AS section,
    si.id AS scan_item_id,
    si.scanID,
    si.conditionID,
    si.1min AS `1min_enabled`,
    si.2min AS `2min_enabled`,
    si.3min AS `3min_enabled`,
    si.5min AS `5min_enabled`,
    si.10min AS `10min_enabled`,
    si.15min AS `15min_enabled`,
    si.30min AS `30min_enabled`,
    si.60min AS `60min_enabled`,
    si.active
FROM scanitems si
WHERE si.scanID IN (
    SELECT DISTINCT scanid FROM alerts WHERE symbol = @symbol AND deleted = 0
)
AND si.conditionID IN (
    SELECT DISTINCT conditionID FROM alerts WHERE symbol = @symbol AND deleted = 0
)
AND si.active = 1;

-- ============================================================
-- PART 4: CONDITIONS DATA (Trading Condition Configuration)
-- ============================================================
SELECT 
    '=== CONDITIONS (Trading Rules Configuration) ===' AS section,
    c.id AS condition_id,
    c.name AS condition_name,
    c.condition1 AS `condition1_enabled`,
    c.condition2 AS `condition2_enabled`,
    c.candle1,
    c.candle2,
    c.psar1,
    c.psar2,
    c.stochid,
    c.lrcid,
    c.kline_start,
    c.kline_end,
    c.signaldirection,
    c.signalColor,
    c.lrcangletype,
    c.lrcanglestart,
    c.lrcangleend,
    c.hlfpid,
    c.active
FROM conditions c
WHERE c.id IN (
    SELECT DISTINCT conditionID FROM alerts WHERE symbol = @symbol AND deleted = 0
)
AND c.active = 1;

-- ============================================================
-- PART 5: COMPLETE JOINED VIEW (All data together)
-- ============================================================
SELECT 
    '=== COMPLETE VIEW (Condition → Scan → Alert) ===' AS section,
    a.id AS alert_id,
    a.symbol,
    DATE_FORMAT(a.datetime, '%Y-%m-%d %H:%i:%s') AS alert_datetime,
    a.timeframe,
    s.name AS scan_name,
    s.id AS scan_id,
    c.name AS condition_name,
    c.id AS condition_id,
    c.condition1,
    c.candle1,
    c.kline_start,
    c.kline_end,
    c.signaldirection,
    c.signalColor,
    si.id AS scan_item_id,
    si.1min AS `1min`,
    si.5min AS `5min`,
    si.15min AS `15min`,
    si.30min AS `30min`,
    si.60min AS `60min`
FROM alerts a
LEFT JOIN scans s ON a.scanid = s.id
LEFT JOIN conditions c ON a.conditionID = c.id
LEFT JOIN scanitems si ON a.scanid = si.scanID AND a.conditionID = si.conditionID
WHERE a.symbol = @symbol 
  AND a.deleted = 0
ORDER BY a.datetime DESC
LIMIT 20;

-- ============================================================
-- PART 6: SUMMARY STATISTICS
-- ============================================================
SELECT 
    '=== SUMMARY STATISTICS ===' AS section,
    COUNT(DISTINCT a.id) AS total_alerts,
    COUNT(DISTINCT a.scanid) AS unique_scans,
    COUNT(DISTINCT a.conditionID) AS unique_conditions,
    COUNT(DISTINCT a.timeframe) AS unique_timeframes,
    MIN(a.datetime) AS first_alert,
    MAX(a.datetime) AS last_alert
FROM alerts a
WHERE a.symbol = @symbol 
  AND a.deleted = 0;
