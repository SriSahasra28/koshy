# Database Schema Analysis: Signals, Conditions, and Alerts

Based on code analysis, here's how signals, conditions, and alerts are stored:

## Table: `conditions`

Stores trading condition configurations:

```sql
CREATE TABLE conditions (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    lrcid INT,                    -- LRC indicator ID reference
    psarid INT,                   -- PSAR indicator ID reference  
    stochid INT,                  -- Stochastic indicator ID reference
    lrcangletype VARCHAR(50),     -- LRC angle type (e.g., 'custom')
    lrcanglestart DECIMAL,        -- LRC angle start value
    lrcangleend DECIMAL,          -- LRC angle end value
    signaldirection INT,          -- PSAR signal direction (1 or -1)
    signalColor VARCHAR(50),      -- Color for alerts (e.g., '#000000')
    hlfpid INT,                   -- HLFP (High Low First Pattern) ID
    condition1 INT,               -- Enable/disable Condition 1 (1=enabled, 0=disabled)
    condition2 INT,               -- Enable/disable Condition 2 (1=enabled, 0=disabled)
    candle1 INT,                  -- Candle type for Condition 1 (1, 2, or 3)
    candle2 INT,                  -- Candle type for Condition 2 (1, 2, or 3)
    psar1 INT,                    -- PSAR ID for Condition 1
    psar2 INT,                    -- PSAR ID for Condition 2
    kline_start DECIMAL,          -- Stochastic K line start threshold
    kline_end DECIMAL,            -- Stochastic K line end threshold
    active INT DEFAULT 1          -- Active flag (1=active, 0=inactive)
);
```

**Key Fields:**
- `condition1`: 1 = enabled, 0 = disabled
- `condition2`: 1 = enabled, 0 = disabled  
- `candle1`: Candle type for Condition 1 (1=green, 2=green+upper wick, 3=green no wicks)
- `candle2`: Candle type for Condition 2
- `kline_start` / `kline_end`: Stochastic K range (e.g., 40-80)
- `signaldirection`: PSAR direction (1=up, -1=down)

## Table: `scans`

Stores scan configurations (containers for scan items):

```sql
CREATE TABLE scans (
    id INT PRIMARY KEY,
    name VARCHAR(255),            -- Scan name (e.g., "My Trading Scan")
    basket_id INT,                -- Reference to groups/baskets table
    active INT DEFAULT 1          -- Active flag
);
```

## Table: `scanitems` (or `scan_items`)

Links scans to conditions and specifies which timeframes are enabled:

```sql
CREATE TABLE scanitems (
    id INT PRIMARY KEY,
    scanID INT,                   -- Foreign key to scans.id
    conditionID INT,              -- Foreign key to conditions.id
    1min INT DEFAULT 0,           -- Enable 1-minute timeframe (1=enabled, 0=disabled)
    2min INT DEFAULT 0,           -- Enable 2-minute timeframe
    3min INT DEFAULT 0,           -- Enable 3-minute timeframe
    5min INT DEFAULT 0,           -- Enable 5-minute timeframe
    10min INT DEFAULT 0,          -- Enable 10-minute timeframe
    15min INT DEFAULT 0,          -- Enable 15-minute timeframe
    30min INT DEFAULT 0,          -- Enable 30-minute timeframe
    60min INT DEFAULT 0,          -- Enable 60-minute timeframe
    active INT DEFAULT 1          -- Active flag
);
```

**Key Fields:**
- Each timeframe column (`1min`, `2min`, etc.) is a boolean (1=enabled, 0=disabled)
- Multiple scan items can exist per scan (different conditions for different timeframes)

## Table: `alerts`

Stores generated alerts:

```sql
CREATE TABLE alerts (
    id INT PRIMARY KEY AUTO_INCREMENT,
    symbol VARCHAR(50),           -- Stock symbol (e.g., 'RELIANCE')
    datetime DATETIME,            -- Alert timestamp (candle close time)
    scanid INT,                   -- Foreign key to scans.id
    timeframe VARCHAR(10),        -- Timeframe (e.g., '1min', '5min', '60min')
    conditionID INT,              -- Foreign key to conditions.id
    system_time DATETIME,         -- System timestamp when alert was created (bot_time)
    deleted INT DEFAULT 0,        -- Soft delete flag (0=active, 1=deleted)
    INDEX idx_symbol_timeframe (symbol, timeframe),
    INDEX idx_datetime (datetime),
    INDEX idx_scanid (scanid)
);
```

**Alert Insertion (Stored Procedure):**
```sql
CALL insert_alert(symbol, datetime, scanid, timeframe, bot_time, conditionID);
```

**Alert Query Example:**
```sql
SELECT 
    DATE_FORMAT(a.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
    a.id,
    a.symbol,
    s.name AS scan_name,
    a.timeframe,
    s.id AS scanid,
    c.signalColor
FROM alerts a
LEFT JOIN scans s ON a.scanid = s.id
LEFT JOIN conditions c ON a.conditionID = c.id
WHERE a.deleted = 0
ORDER BY a.datetime DESC, a.system_time DESC;
```

## Data Flow: Condition → Scan → Alert

1. **Condition Created** (CN page):
   - User creates condition with parameters (PSAR, Stochastic, LRC settings, candle types)
   - Stored in `conditions` table with `condition1=1` (enabled)

2. **Scan Created** (Scan page):
   - User creates scan and links it to a basket/group
   - Stored in `scans` table

3. **Scan Items Added**:
   - User adds scan items (links condition to scan, enables timeframes)
   - Stored in `scanitems` table with timeframe columns set to 1 (enabled)

4. **Alert Generated** (Backend Python):
   - Alert engine evaluates conditions for each symbol/timeframe
   - When Condition 1 passes all checks, alert is inserted:
     ```python
     insert_alert(symbol, alert_timestamp, scanID, timeframe, bot_time, conditionID)
     ```

## Relationships

```
conditions (1) ──< (many) scanitems (many) ──< (1) scans
                                                    │
                                                    │
                                              (many)│
                                                    ▼
                                                 alerts
```

- One condition can be used in multiple scan items
- One scan can have multiple scan items (different conditions/timeframes)
- One scan generates many alerts (one per trigger)

## Key Points for Audit

1. **Condition Evaluation**:
   - Backend checks `condition1` field (1=enabled)
   - Uses `candle1`, `psar1`, `stochid`, `kline_start`, `kline_end`, `signaldirection`

2. **Scan Item Filtering**:
   - Backend filters scan items where `{timeframe}min = 1` (e.g., `1min=1`)
   - Gets `conditionID` from matching scan items

3. **Alert Storage**:
   - Alerts store: `symbol`, `datetime` (candle time), `scanid`, `timeframe`, `conditionID`
   - `system_time` (bot_time) records when alert was processed
   - `datetime` is the actual candle close time

4. **Audit Considerations**:
   - Compare alert `datetime` with actual candle close time
   - Verify `conditionID` matches the condition used
   - Check all enabled timeframes are generating alerts
   - Verify alert timing (`system_time` vs `datetime` delay)
