"""
Run CNTest validation for ADANIENT26JANFUT
Gets all alerts from database (symbol=ADANIENT26JANFUT, scanid=26, conditionID=6)
and validates each one against the logic
"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from audit_condition1_comparison import get_condition_details, get_actual_alerts
from audit_condition1_validation import validate_alerts_batch, print_validation_report, get_redis_connection

def main():
    symbol = "ADANIENT26JANFUT"
    condition_id = 6  # CNTest
    scanid = None  # Will auto-detect from database
    
    print("="*80)
    print("CNTEST ALERT VALIDATION FOR ADANIENT26JANFUT")
    print("="*80)
    print(f"Symbol: {symbol}")
    print(f"Condition ID: {condition_id}")
    print(f"Scan ID: {scanid}")
    print("="*80)
    
    # Get condition details
    print(f"\n[INFO] Fetching condition details for conditionID {condition_id}...")
    condition_details = get_condition_details(condition_id)
    
    if not condition_details:
        print(f"[ERROR] Condition {condition_id} not found or inactive")
        return
    
    print(f"[OK] Condition: {condition_details.get('name', 'N/A')}")
    print(f"   Condition 1 Enabled: {condition_details.get('condition1')}")
    print(f"   Candle Type: {condition_details.get('candle1')}")
    print(f"   K-line Range: {condition_details.get('kline_start')} - {condition_details.get('kline_end')}")
    print(f"   Signal Direction: {condition_details.get('signaldirection')}")
    
    # Get alerts from database (filtered by scanid=26)
    print(f"\n[INFO] Fetching alerts from database...")
    print(f"   Symbol: {symbol}")
    print(f"   Condition ID: {condition_id}")
    print(f"   Scan ID: {scanid}")
    
    actual_alerts_df = get_actual_alerts(symbol, condition_id, scanid=scanid)
    
    if actual_alerts_df.empty:
        print(f"\n[ERROR] No alerts found for {symbol} with conditionID={condition_id} and scanid={scanid}")
        print("\nChecking if alerts exist without scanid filter...")
        actual_alerts_df_all = get_actual_alerts(symbol, condition_id)
        if not actual_alerts_df_all.empty:
            print(f"   Found {len(actual_alerts_df_all)} alerts without scanid filter")
            scan_ids = actual_alerts_df_all['scan_id'].unique().tolist()
            print(f"   Scan IDs in alerts: {scan_ids}")
            if len(scan_ids) == 1:
                print(f"\n[INFO] Using scanid={scan_ids[0]} from database")
                actual_alerts_df = get_actual_alerts(symbol, condition_id, scanid=scan_ids[0])
            else:
                print(f"\n[INFO] Multiple scan IDs found, using all alerts")
                actual_alerts_df = actual_alerts_df_all
        else:
            return
    
    print(f"[OK] Found {len(actual_alerts_df)} alerts")
    print(f"   Timeframes: {sorted(actual_alerts_df['timeframe'].unique().tolist())}")
    print(f"   Date Range: {actual_alerts_df['alert_datetime'].min()} to {actual_alerts_df['alert_datetime'].max()}")
    
    # Show first few alerts
    print(f"\n[INFO] Sample alerts (first 5):")
    for idx, alert in actual_alerts_df.head(5).iterrows():
        print(f"   ID: {alert['alert_id']} | {alert['alert_datetime']} | {alert['timeframe']}min")
    
    # Connect to Redis
    print(f"\n[INFO] Connecting to Redis...")
    redis_client = get_redis_connection()
    print("[OK] Redis connected")
    
    # Validate all alerts (no limit)
    print(f"\n[INFO] Validating all {len(actual_alerts_df)} alerts...")
    validation_results, skipped_alerts = validate_alerts_batch(
        actual_alerts_df, 
        condition_details, 
        redis_client, 
        max_alerts=len(actual_alerts_df)  # Validate all alerts
    )
    
    # Print comprehensive report
    print_validation_report(validation_results, skipped_alerts)
    
    # Summary
    valid_count = sum(1 for r in validation_results if r['valid'])
    invalid_count = len(validation_results) - valid_count
    total = len(validation_results)
    
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    print(f"Total Alerts Validated: {total}")
    print(f"[OK] Valid Alerts: {valid_count}")
    print(f"[ERROR] Invalid Alerts: {invalid_count}")
    if total > 0:
        accuracy = (valid_count / total) * 100
        print(f"[STATS] Accuracy: {accuracy:.1f}%")
        if accuracy == 100.0:
            print("[SUCCESS] PERFECT MATCH! All alerts are valid according to logic!")
        elif accuracy >= 95.0:
            print("[OK] Excellent accuracy! (>95%)")
        elif accuracy >= 90.0:
            print("[WARN] Good accuracy, but some invalid alerts found")
        else:
            print("[ERROR] Low accuracy - investigation needed")
    print("="*80)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
