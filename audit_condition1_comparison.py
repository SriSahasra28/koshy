"""
Audit Script: Compare Actual Alerts vs Expected Alerts for Condition 1

WHAT THIS SCRIPT DOES:
1. Fetches condition configuration (Condition ID 6 - CN-Test)
2. Finds symbols that have alerts for this condition
3. Retrieves actual alerts from database
4. Displays:
   - Condition 1 settings (candle type, PSAR, Stochastic, K-range, etc.)
   - List of actual alerts found
   - Timeframes and date ranges

This is STEP 1: Data Collection
Next step: Run audit_condition1_validation.py to validate each alert
"""
import pymysql
from background.set import settings
import pandas as pd
from datetime import datetime, timedelta

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

def get_db_connection():
    """Get database connection using settings"""
    try:
        db_config = settings.get_db()
        conn = pymysql.connect(
            host=db_config[2],
            port=db_config[3],
            user=db_config[0],
            password=db_config[1],
            database=db_config[4],
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        import traceback
        traceback.print_exc()
        raise

def find_symbol_by_condition(condition_id):
    """Find a symbol with alerts for the given condition ID"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT DISTINCT a.symbol, 
                   COUNT(*) as alert_count,
                   MAX(a.datetime) as latest_alert
            FROM alerts a
            WHERE a.conditionID = %s 
              AND a.deleted = 0
            GROUP BY a.symbol
            ORDER BY latest_alert DESC
            LIMIT 1
        """
        cursor.execute(query, (condition_id,))
        result = cursor.fetchone()
        return result
    except Exception as e:
        print(f"Error in find_symbol_by_condition: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_actual_alerts(symbol, condition_id):
    """Get actual alerts from database for symbol and condition"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                a.id AS alert_id,
                a.symbol,
                a.datetime AS alert_datetime,
                a.timeframe,
                s.name AS scan_name,
                s.id AS scan_id,
                c.name AS condition_name,
                c.id AS condition_id,
                c.condition1 AS cond1_enabled,
                c.candle1,
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
            WHERE a.symbol = %s 
              AND a.conditionID = %s
              AND a.deleted = 0
            ORDER BY a.datetime DESC
            LIMIT 100
        """
        cursor.execute(query, (symbol, condition_id))
        results = cursor.fetchall()
        return pd.DataFrame(results)
    except Exception as e:
        print(f"Error in get_actual_alerts: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_condition_details(condition_id):
    """Get condition configuration details"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                id, name, condition1, condition2,
                candle1, candle2, psar1, psar2, stochid,
                kline_start, kline_end, signaldirection, signalColor,
                lrcangletype, lrcanglestart, lrcangleend
            FROM conditions
            WHERE id = %s AND active = 1
        """
        cursor.execute(query, (condition_id,))
        result = cursor.fetchone()
        return result
    except Exception as e:
        print(f"Error in get_condition_details: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def print_audit_report(symbol, condition_id, condition_details, actual_alerts_df):
    """Print comprehensive audit report"""
    print("\n" + "="*80)
    print("CONDITION 1 AUDIT REPORT")
    print("="*80)
    
    print(f"\nSYMBOL: {symbol}")
    print(f"CONDITION ID: {condition_id}")
    print(f"CONDITION NAME: {condition_details.get('name', 'N/A')}")
    
    print("\n" + "-"*80)
    print("CONDITION 1 CONFIGURATION:")
    print("-"*80)
    print(f"  Condition 1 Enabled: {condition_details.get('condition1', 'N/A')}")
    print(f"  Candle Type 1: {condition_details.get('candle1', 'N/A')}")
    print(f"  PSAR ID 1: {condition_details.get('psar1', 'N/A')}")
    print(f"  Stochastic ID: {condition_details.get('stochid', 'N/A')}")
    print(f"  K-line Range: {condition_details.get('kline_start', 'N/A')} to {condition_details.get('kline_end', 'N/A')}")
    print(f"  Signal Direction: {condition_details.get('signaldirection', 'N/A')}")
    print(f"  Signal Color: {condition_details.get('signalColor', 'N/A')}")
    
    print("\n" + "-"*80)
    print("ACTUAL ALERTS FROM DATABASE:")
    print("-"*80)
    print(f"  Total Alerts Found: {len(actual_alerts_df)}")
    
    if len(actual_alerts_df) > 0:
        timeframes = [str(tf) for tf in sorted(actual_alerts_df['timeframe'].unique())]
        print(f"\n  Timeframes: {', '.join(timeframes)}")
        print(f"  Date Range: {actual_alerts_df['alert_datetime'].min()} to {actual_alerts_df['alert_datetime'].max()}")
        
        print("\n  Alert Details:")
        display_cols = ['alert_id', 'alert_datetime', 'timeframe', 'scan_name']
        if HAS_TABULATE:
            print(tabulate(actual_alerts_df[display_cols], headers='keys', tablefmt='grid', showindex=False))
        else:
            print(actual_alerts_df[display_cols].to_string(index=False))
    else:
        print("  No alerts found for this symbol and condition")
    
    print("\n" + "-"*80)
    print("NEXT STEPS FOR COMPARISON:")
    print("-"*80)
    print("1. For each alert timestamp, fetch OHLC data from Redis/DB")
    print("2. Calculate indicators (PSAR, Stochastic K/D, Heikin-Ashi)")
    print("3. Evaluate Condition 1 logic:")
    print("   - Check if K is in range [kline_start, kline_end]")
    print("   - Check if PSAR signal matches signaldirection")
    print("   - Check candle type (candle1):")
    print("     * Type 1: Green candle (close_ha > open_ha)")
    print("     * Type 2: Green candle + no lower wick + upper wick")
    print("     * Type 3: Green candle + no lower wick + no upper wick")
    print("4. Compare: Should alert have triggered? (Yes/No/Missed/Delayed)")
    
    print("\n" + "="*80)

def main():
    condition_id = 6  # CN-Test (has alerts available for testing)
    
    print(f"Fetching condition details for conditionID {condition_id} (CN-Test)...")
    condition_details = get_condition_details(condition_id)
    
    if not condition_details:
        print(f"Condition {condition_id} (CN-PB1) not found or inactive")
        return
    
    condition_name = condition_details.get('name', 'N/A')
    print(f"\nCondition Found: {condition_name} (ID: {condition_id})")
    
    if condition_details.get('condition1') != 1:
        print(f"WARNING: Condition 1 is not enabled for conditionID {condition_id} ({condition_name})")
        return
    
    print(f"\nFinding symbol with alerts for conditionID {condition_id} ({condition_name})...")
    try:
        symbol_result = find_symbol_by_condition(condition_id)
    except Exception as e:
        print(f"ERROR in find_symbol_by_condition call: {e}")
        import traceback
        traceback.print_exc()
        return
    
    if not symbol_result:
        print(f"No symbols found with conditionID = {condition_id} ({condition_name})")
        return
    
    symbol = symbol_result['symbol']
    print(f"\nFound symbol: {symbol}")
    print(f"  Alert count: {symbol_result['alert_count']}")
    print(f"  Latest alert: {symbol_result['latest_alert']}")
    
    print(f"\nFetching actual alerts for {symbol} with conditionID {condition_id}...")
    actual_alerts_df = get_actual_alerts(symbol, condition_id)
    
    print_audit_report(symbol, condition_id, condition_details, actual_alerts_df)
    
    print("\nAudit data collection complete!")
    print(f"Symbol: {symbol} | Condition ID: {condition_id}")
    
    # Ask user if they want to run validation
    print("\n" + "="*80)
    print("READY FOR VALIDATION")
    print("="*80)
    print("\nTo validate alerts, run:")
    print(f"  python audit_condition1_validation.py {symbol} {condition_id}")
    print("\nOr import and use the validation functions directly.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
