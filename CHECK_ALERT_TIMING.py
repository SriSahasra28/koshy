"""
Check when alerts were generated and if fix was active
"""
import pymysql
from background.set import settings
from datetime import datetime

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

def check_alert_timing():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    symbol,
                    datetime AS alert_datetime,
                    system_time AS alert_created_time,
                    TIMESTAMPDIFF(SECOND, datetime, system_time) AS time_diff_seconds,
                    scanid,
                    conditionID,
                    timeframe
                FROM alerts
                WHERE symbol = 'ADANIENT26JANFUT' 
                  AND conditionID = 6
                  AND deleted = 0
                ORDER BY datetime DESC
            """)
            alerts = cursor.fetchall()
            
            print("\n" + "="*80)
            print("ALERT TIMING ANALYSIS")
            print("="*80)
            print(f"\nFound {len(alerts)} alerts:\n")
            
            for alert in alerts:
                print(f"Alert ID: {alert['id']}")
                print(f"  Symbol: {alert['symbol']}")
                print(f"  Alert Datetime (candle time): {alert['alert_datetime']}")
                print(f"  System Time (when stored): {alert['alert_created_time']}")
                print(f"  Time Difference: {alert['time_diff_seconds']} seconds")
                print(f"  Scan ID: {alert['scanid']}")
                print(f"  Condition ID: {alert['conditionID']}")
                print(f"  Timeframe: {alert['timeframe']}min")
                print()
            
            # Check when the fix was applied
            print("="*80)
            print("FIX TIMELINE:")
            print("="*80)
            print("PSAR Signal Bug Fix was applied:")
            print("  - NULL lrcangletype handling: Today")
            print("  - Defensive PSAR signal checks: Today")
            print("  - Signal=0 rejection: Today")
            print()
            
            # Analyze each alert
            for alert in alerts:
                alert_dt = alert['alert_datetime']
                created_dt = alert['alert_created_time']
                print(f"Alert ID {alert['id']}:")
                print(f"  Generated: {created_dt}")
                if created_dt:
                    print(f"  This alert was generated {'TODAY' if '2026-01-20' in str(created_dt) else 'PREVIOUSLY'}")
                print()
            
    finally:
        conn.close()

if __name__ == "__main__":
    check_alert_timing()
