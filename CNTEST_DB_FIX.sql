-- ============================================================
-- CNTEST DATABASE FIXES AND MONITORING QUERIES
-- ============================================================

-- 1. NULL OUT LRC ANGLE FIELDS FOR CNTEST
-- ============================================================
UPDATE conditions 
SET 
    lrcangletype = NULL,
    lrcanglestart = NULL,
    lrcangleend = NULL
WHERE name = 'CNtest';

-- Verify the update
SELECT 
    id,
    name,
    lrcangletype,
    lrcanglestart,
    lrcangleend
FROM conditions 
WHERE name = 'CNtest';

-- ============================================================
-- 2. FULL CNTEST CONDITION DETAILS FOR MONITORING TOMORROW
-- ============================================================
SELECT 
    '=== CNTEST CONDITION CONFIGURATION ===' AS section,
    c.id AS condition_id,
    c.name AS condition_name,
    c.active,
    
    -- Condition 1 Settings
    c.condition1 AS condition1_enabled,
    c.candle1 AS condition1_candle_type,
    c.psar1 AS condition1_psar_id,
    c.kline_start,
    c.kline_end,
    c.signaldirection,
    
    -- Condition 2 Settings
    c.condition2 AS condition2_enabled,
    c.candle2 AS condition2_candle_type,
    c.psar2 AS condition2_psar_id,
    
    -- Indicator IDs
    c.lrcid,
    c.stochid,
    c.psar1,
    c.psar2,
    
    -- LRC Settings (should be NULL after update)
    c.lrcangletype,
    c.lrcanglestart,
    c.lrcangleend,
    
    -- Custom Indicators (FS-22 and PS-22 values)
    (SELECT value FROM custom_indicators WHERE id = 105) AS FS22_value,
    (SELECT value FROM custom_indicators WHERE id = 107) AS PS22_value,
    
    -- HLFP Settings
    c.hlfpid,
    
    -- Signal Color
    c.signalColor
    
FROM conditions c
WHERE c.name = 'CNtest';

-- ============================================================
-- 3. CNTEST CUSTOM INDICATORS DETAILS
-- ============================================================
SELECT 
    '=== CNTEST CUSTOM INDICATORS ===' AS section,
    ci.id,
    ci.name,
    ci.indicator_id,
    i.name AS indicator_name,
    ci.value,
    ci.active
FROM custom_indicators ci
INNER JOIN indicators i ON ci.indicator_id = i.id
WHERE ci.id IN (105, 107);

-- Expected values:
-- ID 105: FS-22, indicator_id 3, value '14,3,3'
-- ID 107: PS-22, indicator_id 2, value '0.002, 0.05'

-- ============================================================
-- 4. CNTEST SCAN ITEMS (Which scans use CNtest condition)
-- ============================================================
SELECT 
    '=== CNTEST SCAN ITEMS (Timeframes) ===' AS section,
    si.id AS scan_item_id,
    si.scanID,
    s.name AS scan_name,
    si.conditionID,
    c.name AS condition_name,
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
INNER JOIN scans s ON si.scanID = s.id
INNER JOIN conditions c ON si.conditionID = c.id
WHERE c.name = 'CNtest'
AND si.active = 1
AND s.active = 1;

-- ============================================================
-- 5. DIAGNOSE BASKET_ID 44 ISSUE FOR GROUP XYZ
-- ============================================================

-- Check if group "xyz" exists and has basket_id 44
SELECT 
    '=== GROUP XYZ DETAILS ===' AS section,
    b.id AS basket_id,
    b.name AS basket_name,
    b.active
FROM baskets b
WHERE b.name = 'xyz';

-- Check scans that use basket_id 44
SELECT 
    '=== SCANS USING BASKET_ID 44 ===' AS section,
    s.id AS scan_id,
    s.name AS scan_name,
    s.basket_id,
    s.active
FROM scans s
WHERE s.basket_id = 44;

-- Check if basket_stocks has any entries for basket_id 44
SELECT 
    '=== BASKET_STOCKS FOR BASKET_ID 44 ===' AS section,
    bs.id,
    bs.basket_id,
    bs.symbol,
    bs.active
FROM basket_stocks bs
WHERE bs.basket_id = 44;

-- Count of basket_stocks entries per basket_id (to see if 44 is missing)
SELECT 
    '=== BASKET_STOCKS COUNT BY BASKET_ID ===' AS section,
    bs.basket_id,
    COUNT(*) AS stock_count,
    GROUP_CONCAT(DISTINCT bs.symbol ORDER BY bs.symbol SEPARATOR ', ') AS symbols
FROM basket_stocks bs
WHERE bs.active = 1
GROUP BY bs.basket_id
ORDER BY bs.basket_id;

-- Check all baskets to see which ones have stocks
SELECT 
    '=== ALL BASKETS WITH STOCK COUNT ===' AS section,
    b.id AS basket_id,
    b.name AS basket_name,
    COUNT(bs.id) AS stock_count,
    b.active AS basket_active
FROM baskets b
LEFT JOIN basket_stocks bs ON b.id = bs.basket_id AND bs.active = 1
GROUP BY b.id, b.name, b.active
ORDER BY b.id;

-- Check if basket_id 44 should exist (maybe it was deleted but scan still references it)
SELECT 
    '=== ORPHANED SCAN REFERENCES ===' AS section,
    s.id AS scan_id,
    s.name AS scan_name,
    s.basket_id,
    b.name AS basket_name,
    CASE 
        WHEN b.id IS NULL THEN 'MISSING - Basket does not exist!'
        ELSE 'EXISTS'
    END AS basket_status
FROM scans s
LEFT JOIN baskets b ON s.basket_id = b.id
WHERE s.basket_id = 44;

-- ============================================================
-- 6. COMPLETE CNTEST MONITORING SUMMARY FOR TOMORROW
-- ============================================================
SELECT 
    '=== COMPLETE CNTEST MONITORING SUMMARY ===' AS section,
    
    -- Condition Info
    c.id AS condition_id,
    c.name AS condition_name,
    c.active AS condition_active,
    
    -- Condition 1
    CONCAT('C1: ', 
        CASE WHEN c.condition1 = 1 THEN 'ENABLED' ELSE 'DISABLED' END,
        ' | Candle:', c.candle1,
        ' | PSAR:', c.psar1,
        ' | K-range:', c.kline_start, '-', c.kline_end,
        ' | Signal Dir:', c.signaldirection
    ) AS condition1_summary,
    
    -- Condition 2
    CONCAT('C2: ',
        CASE WHEN c.condition2 = 1 THEN 'ENABLED' ELSE 'DISABLED' END,
        ' | Candle:', c.candle2,
        ' | PSAR:', c.psar2
    ) AS condition2_summary,
    
    -- Custom Indicators
    CONCAT('FS-22 (ID 105): ', (SELECT value FROM custom_indicators WHERE id = 105)) AS fs22_summary,
    CONCAT('PS-22 (ID 107): ', (SELECT value FROM custom_indicators WHERE id = 107)) AS ps22_summary,
    
    -- Active Scans
    (SELECT COUNT(*) FROM scanitems si 
     INNER JOIN scans s ON si.scanID = s.id 
     WHERE si.conditionID = c.id AND si.active = 1 AND s.active = 1) AS active_scan_count,
    
    -- Active Timeframes
    (SELECT GROUP_CONCAT(
        CASE WHEN si.1min = 1 THEN '1min' END,
        CASE WHEN si.2min = 1 THEN '2min' END,
        CASE WHEN si.3min = 1 THEN '3min' END,
        CASE WHEN si.5min = 1 THEN '5min' END,
        CASE WHEN si.10min = 1 THEN '10min' END,
        CASE WHEN si.15min = 1 THEN '15min' END,
        CASE WHEN si.30min = 1 THEN '30min' END,
        CASE WHEN si.60min = 1 THEN '60min' END
        SEPARATOR ', '
    ) FROM scanitems si 
    INNER JOIN scans s ON si.scanID = s.id 
    WHERE si.conditionID = c.id AND si.active = 1 AND s.active = 1
    LIMIT 1) AS enabled_timeframes
    
FROM conditions c
WHERE c.name = 'CNtest';
