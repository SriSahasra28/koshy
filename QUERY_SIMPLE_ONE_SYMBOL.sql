-- Simple Query: See all Condition → Scan → Alert data for one symbol
-- Run this to see the complete data flow

-- Change this symbol to any symbol you want to inspect
SET @symbol = 'RELIANCE';

-- Or uncomment to use the most recent symbol from alerts:
-- SET @symbol = (SELECT symbol FROM alerts WHERE deleted = 0 ORDER BY datetime DESC LIMIT 1);

-- COMPLETE DATA VIEW
SELECT 
    a.id AS alert_id,
    a.symbol,
    DATE_FORMAT(a.datetime, '%Y-%m-%d %H:%i:%s') AS alert_datetime,
    a.timeframe,
    s.name AS scan_name,
    s.id AS scan_id,
    c.name AS condition_name,
    c.id AS condition_id,
    c.condition1 AS cond1_enabled,
    c.condition2 AS cond2_enabled,
    c.candle1,
    c.candle2,
    c.psar1,
    c.stochid,
    c.kline_start,
    c.kline_end,
    c.signaldirection,
    c.signalColor,
    si.1min AS `1min`,
    si.2min AS `2min`,
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
LIMIT 50;
