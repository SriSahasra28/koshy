# Scan Feature Audit Plan

## Objective
Audit the scan feature to verify alert accuracy based on conditions, specifically:
- **Wrong candle alerts** (False Positives): Alerts that triggered but shouldn't have
- **Missed alerts** (False Negatives): Alerts that should have triggered but didn't
- **Delayed alerts**: Timing issues in alert generation
- **Timeframe coverage**: Verify all enabled timeframes are working

## Focus Areas
1. **Condition 1 (CN page)**: Primary condition evaluation logic
2. **Scan Page Function**: Scan item configuration and timeframe activation

## Alert Evaluation Criteria (Condition 1)

Based on `redis_alert_engine.py`, Condition 1 evaluates:

1. **Stochastic K Range Check**: `kline_start < K < kline_end`
2. **PSAR Signal Direction**: `psar_signal == signaldirection`
3. **Candle Type** (candle1):
   - **Type 1**: Green candle only (`close_ha > open_ha`)
   - **Type 2**: Green candle + no lower wick + upper wick present
   - **Type 3**: Green candle + no lower wick + no upper wick

All three conditions must pass for alert to trigger.

## Audit Methodology

### Phase 1: Data Collection & Analysis
1. **Extract Test Data**
   - Select a specific condition (Condition 1 enabled)
   - Select a specific scan with known timeframes enabled
   - Get historical OHLC data for test period
   - Get actual alerts from database for comparison

2. **Condition Configuration**
   - Verify condition1 = 1 (enabled)
   - Extract: candle1, psar1, stochid, kline_start, kline_end, signaldirection
   - Extract indicator configurations (PSAR, Stochastic parameters)

### Phase 2: Replay & Validation
1. **Replay Alert Logic**
   - Feed historical OHLC data through alert evaluation logic
   - Calculate indicators (PSAR, Stochastic, LRC)
   - Evaluate Condition 1 for each candle
   - Generate expected alerts

2. **Compare Results**
   - Expected alerts vs Actual alerts from database
   - Identify discrepancies:
     - **False Positives**: In database but not in expected
     - **False Negatives**: In expected but not in database
     - **Timing issues**: Same alert but different timestamps

### Phase 3: Timeframe Coverage Check
1. **Verify Timeframe Processing**
   - For each scan item, check all enabled timeframes (1min, 2min, etc.)
   - Verify alerts generated for all enabled timeframes
   - Check for missing timeframes

### Phase 4: Detailed Logging
1. **Enhanced Logging**
   - Log all condition evaluation steps
   - Log why alerts passed/failed
   - Log indicator values at alert time
   - Log candle characteristics (color, wicks, etc.)

## Implementation Steps

### Step 1: Create Audit Script
Create `audit_scan_feature.py` that:
- Connects to database
- Fetches condition, scan, and scan_item configurations
- Fetches historical OHLC data
- Replays alert logic
- Compares with actual alerts
- Generates audit report

### Step 2: Enhanced Logging in Alert Engine
Add detailed logging to `redis_alert_engine.py`:
- Log all condition evaluation steps
- Log indicator values
- Log why alerts passed/failed
- Store evaluation details for audit

### Step 3: Create Test Cases
- Test Case 1: Condition 1 with Candle Type 1
- Test Case 2: Condition 1 with Candle Type 2
- Test Case 3: Condition 1 with Candle Type 3
- Test Case 4: Multiple timeframes
- Test Case 5: Edge cases (boundary values)

### Step 4: Generate Audit Report
Report should include:
- Summary statistics (total alerts, false positives, false negatives)
- Detailed breakdown by timeframe
- Detailed breakdown by condition
- Timestamp comparison
- Recommendations

## Files to Create/Modify

1. **New Files**:
   - `audit_scan_feature.py` - Main audit script
   - `audit_utils.py` - Utility functions for audit
   - `SCAN_AUDIT_PLAN.md` - This document

2. **Files to Modify**:
   - `redis_alert_engine.py` - Add enhanced logging (optional, for future audits)

## Testing Strategy

1. **Unit Testing**: Test individual condition checks
2. **Integration Testing**: Test full alert evaluation pipeline
3. **Regression Testing**: Run on historical data
4. **Performance Testing**: Ensure audit doesn't impact production

## Success Criteria

- Zero false positives (within acceptable tolerance)
- Zero false negatives (within acceptable tolerance)
- All enabled timeframes generating alerts
- Alert timestamps matching candle close times (within 1-2 minutes)
- Clear audit trail for all alerts
