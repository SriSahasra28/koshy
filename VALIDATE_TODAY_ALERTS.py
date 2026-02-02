"""
Validate all alerts generated today
"""
import pymysql
import pandas as pd
import sys
from datetime import datetime, timedelta
from background.set import settings
from audit_condition1_validation import (
    validate_alerts_batch,
    print_validation_report,
    get_redis_connection
)
from audit_condition1_comparison import get_condition_details

def get_db_connection():
    db_config = settings.get_db()
    return pymysql.connect(
        host=db_config[2],
        port=db_config[3],
        user=db_config[0],
        password=db_config[1],
        database=db_config[4],
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor
    )

def get_todays_alerts():
    """Get all alerts generated today"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Get today's date range (IST)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)
        
        query = """
            SELECT 
                a.id as alert_id,
                a.symbol,
                a.datetime as alert_datetime,
                a.system_time as alert_created_time,
                a.timeframe,
                a.scanid,
                a.conditionID,
                c.name as condition_name
            FROM alerts a
            LEFT JOIN conditions c ON a.conditionID = c.id
            WHERE a.deleted = 0
              AND a.datetime >= %s
              AND a.datetime < %s
            ORDER BY a.datetime DESC, a.system_time DESC
        """
        
        cursor.execute(query, (today, tomorrow))
        alerts = cursor.fetchall()
        return alerts
    finally:
        cursor.close()
        conn.close()

def validate_todays_alerts():
    """Validate all alerts from today"""
    print("\n" + "="*80)
    print("TODAY'S ALERT VALIDATION")
    print("="*80)
    
    # Get today's alerts
    print("\n[INFO] Fetching today's alerts...")
    todays_alerts = get_todays_alerts()
    
    if not todays_alerts:
        print("[INFO] No alerts found for today")
        return
    
    print(f"[OK] Found {len(todays_alerts)} alerts today")
    
    # Group by symbol and condition
    alerts_by_condition = {}
    for alert in todays_alerts:
        key = (alert['symbol'], alert['conditionID'])
        if key not in alerts_by_condition:
            alerts_by_condition[key] = []
        alerts_by_condition[key].append(alert)
    
    print(f"\n[INFO] Grouped into {len(alerts_by_condition)} symbol+condition combinations")
    
    # Connect to Redis
    print("\n[INFO] Connecting to Redis...")
    redis_client = get_redis_connection()
    print("[OK] Redis connected")
    
    # Validate each group
    all_results = []
    all_skipped = []
    
    for (symbol, condition_id), alerts in alerts_by_condition.items():
        print(f"\n{'='*80}")
        print(f"Validating: {symbol} | Condition ID {condition_id}")
        print("="*80)
        
        # Get condition details
        condition_details = get_condition_details(condition_id)
        if not condition_details:
            print(f"[ERROR] Condition {condition_id} not found, skipping")
            continue
        
        print(f"Condition: {condition_details.get('name', 'N/A')}")
        print(f"Alerts: {len(alerts)}")
        
        # Convert to DataFrame format
        import pandas as pd
        alerts_df = pd.DataFrame(alerts)
        
        # Validate (process all alerts for this symbol+condition)
        validation_results, skipped_alerts = validate_alerts_batch(
            alerts_df, 
            condition_details, 
            redis_client, 
            max_alerts=len(alerts)  # Process all
        )
        
        all_results.extend(validation_results)
        all_skipped.extend(skipped_alerts)
        
        # Print summary for this group
        valid_count = sum(1 for r in validation_results if r['valid'])
        invalid_count = len(validation_results) - valid_count
        print(f"\n[SUMMARY] {symbol} | Condition {condition_id}:")
        print(f"  Valid: {valid_count}, Invalid: {invalid_count}")
    
    # Print overall report
    print("\n" + "="*80)
    print("OVERALL VALIDATION REPORT")
    print("="*80)
    print_validation_report(all_results, all_skipped)
    
    # Show summary by condition
    print("\n" + "="*80)
    print("SUMMARY BY CONDITION")
    print("="*80)
    
    summary_by_condition = {}
    for result in all_results:
        condition_id = result.get('condition_id', 'Unknown')
        if condition_id not in summary_by_condition:
            summary_by_condition[condition_id] = {'valid': 0, 'invalid': 0}
        if result['valid']:
            summary_by_condition[condition_id]['valid'] += 1
        else:
            summary_by_condition[condition_id]['invalid'] += 1
    
    for condition_id, counts in summary_by_condition.items():
        total = counts['valid'] + counts['invalid']
        accuracy = (counts['valid'] / total * 100) if total > 0 else 0
        print(f"Condition {condition_id}: {counts['valid']}/{total} valid ({accuracy:.1f}%)")

if __name__ == "__main__":
    validate_todays_alerts()
