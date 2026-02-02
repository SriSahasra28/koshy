"""
Validate CNTest alerts for ADANIENT26FEBFUT from today (2026-01-21)
"""
import sys
import pandas as pd
from datetime import datetime
from audit_condition1_validation import (
    validate_alerts_batch,
    print_validation_report,
    get_redis_connection
)
from audit_condition1_comparison import get_condition_details, get_actual_alerts

def validate_cntest_adanient26febfut():
    """Validate CNTest (condition 6) alerts for ADANIENT26FEBFUT from today"""
    
    symbol = "ADANIENT26FEBFUT"
    condition_id = 6  # CNTest
    target_date = "2026-01-21"
    
    print("\n" + "="*80)
    print(f"CNTEST ALERT VALIDATION FOR {symbol}")
    print("="*80)
    print(f"Symbol: {symbol}")
    print(f"Condition ID: {condition_id}")
    print(f"Date: {target_date}")
    print("="*80)
    
    # Get condition details
    print(f"\n[INFO] Fetching condition details for conditionID {condition_id}...")
    condition_details = get_condition_details(condition_id)
    
    if not condition_details:
        print(f"[ERROR] Condition {condition_id} not found")
        return
    
    print(f"[OK] Condition: {condition_details.get('name', 'N/A')}")
    print(f"   Condition 1 Enabled: {condition_details.get('condition1', 'N/A')}")
    print(f"   Candle Type: {condition_details.get('candle1', 'N/A')}")
    print(f"   K-line Range: {condition_details.get('kline_start', 'N/A')} - {condition_details.get('kline_end', 'N/A')}")
    print(f"   Signal Direction: {condition_details.get('signaldirection', 'N/A')}")
    
    # Get alerts from today
    print(f"\n[INFO] Fetching alerts from database...")
    print(f"   Symbol: {symbol}")
    print(f"   Condition ID: {condition_id}")
    print(f"   Date: {target_date}")
    
    # Get all alerts for this symbol and condition
    actual_alerts_df = get_actual_alerts(symbol, condition_id)
    
    if actual_alerts_df.empty:
        print(f"[INFO] No alerts found for {symbol} with conditionID {condition_id}")
        return
    
    # Filter to today's alerts only
    actual_alerts_df['alert_datetime'] = pd.to_datetime(actual_alerts_df['alert_datetime'])
    target_date_dt = pd.to_datetime(target_date).date()
    today_alerts_df = actual_alerts_df[actual_alerts_df['alert_datetime'].dt.date == target_date_dt].copy()
    
    if today_alerts_df.empty:
        print(f"[INFO] No alerts found for {symbol} on {target_date}")
        print(f"[INFO] Total alerts in database: {len(actual_alerts_df)}")
        if len(actual_alerts_df) > 0:
            print(f"[INFO] Date range: {actual_alerts_df['alert_datetime'].min()} to {actual_alerts_df['alert_datetime'].max()}")
        return
    
    print(f"[OK] Found {len(today_alerts_df)} alerts from {target_date}")
    print(f"   Timeframes: {sorted(today_alerts_df['timeframe'].unique())}")
    print(f"   Date Range: {today_alerts_df['alert_datetime'].min()} to {today_alerts_df['alert_datetime'].max()}")
    
    # Show sample alerts
    print(f"\n[INFO] Sample alerts (first 5):")
    for idx, alert in today_alerts_df.head(5).iterrows():
        print(f"   ID: {alert['alert_id']} | {alert['alert_datetime']} | {alert['timeframe']}min")
    
    # Connect to Redis
    print("\n[INFO] Connecting to Redis...")
    redis_client = get_redis_connection()
    print("[OK] Redis connected")
    
    # Validate all alerts
    print(f"\n[INFO] Validating all {len(today_alerts_df)} alerts...")
    
    validation_results, skipped_alerts = validate_alerts_batch(
        today_alerts_df, 
        condition_details, 
        redis_client, 
        max_alerts=len(today_alerts_df)  # Validate all
    )
    
    # Print report
    print_validation_report(validation_results, skipped_alerts)
    
    # Print summary
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    valid_count = sum(1 for r in validation_results if r['valid'])
    invalid_count = len(validation_results) - valid_count
    total_validated = len(validation_results)
    
    print(f"Total Alerts Validated: {total_validated}")
    print(f"[OK] Valid Alerts: {valid_count}")
    print(f"[ERROR] Invalid Alerts: {invalid_count}")
    
    if total_validated > 0:
        accuracy = (valid_count / total_validated) * 100
        print(f"[STATS] Accuracy: {accuracy:.1f}%")
        
        if accuracy < 100:
            print(f"[ERROR] Low accuracy - investigation needed")
        else:
            print(f"[OK] Perfect match - all alerts are valid!")
    
    print("="*80)

if __name__ == "__main__":
    validate_cntest_adanient26febfut()
