"""
Diagnostic script to understand alert storage issue
Checks MySQL alerts table, Redis alerts, and compares them
"""
import pymysql
import redis
import json
from datetime import datetime
from background.set import settings

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

def check_mysql_alerts():
    """Check latest alerts in MySQL"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Get latest alerts
            cursor.execute("""
                SELECT 
                    DATE_FORMAT(a.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
                    a.id,
                    a.symbol,
                    s.name AS scan_name,
                    a.timeframe,
                    s.id as scanid,
                    a.system_time,
                    a.deleted,
                    a.conditionID
                FROM alerts a 
                LEFT JOIN scans s ON a.scanid = s.id 
                LEFT JOIN conditions c ON a.conditionID = c.id 
                WHERE a.deleted = 0 
                ORDER BY a.datetime DESC, a.system_time DESC 
                LIMIT 20
            """)
            alerts = cursor.fetchall()
            
            # Get count and latest datetime
            cursor.execute("""
                SELECT 
                    COUNT(*) AS total_alerts,
                    MAX(a.datetime) AS latest_datetime,
                    MAX(a.system_time) AS latest_system_time
                FROM alerts a 
                WHERE a.deleted = 0
            """)
            stats = cursor.fetchone()
            
            return alerts, stats
    finally:
        conn.close()

def check_redis_alerts():
    """Check alerts in Redis"""
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        # Check Alerts sorted set
        alerts_key = "Alerts"
        alerts_count = r.zcard(alerts_key)
        
        # Get latest 20 alerts from Redis (highest scores = most recent)
        latest_alerts = r.zrange(alerts_key, -20, -1, withscores=True, desc=True)
        
        # Check alerts_simple
        alerts_simple_key = "alerts_simple"
        alerts_simple_count = r.zcard(alerts_simple_key)
        latest_simple = r.zrange(alerts_simple_key, -20, -1, withscores=True, desc=True)
        
        # Parse alerts
        parsed_alerts = []
        for alert_json, score in latest_alerts:
            try:
                alert_data = json.loads(alert_json)
                alert_data['redis_score'] = score
                alert_data['redis_timestamp'] = datetime.fromtimestamp(score).strftime('%Y-%m-%d %H:%M:%S')
                parsed_alerts.append(alert_data)
            except:
                pass
        
        parsed_simple = []
        for simple_alert, score in latest_simple:
            try:
                # Format: symbol,interval,candle_timestamp
                parts = simple_alert.split(',')
                if len(parts) >= 3:
                    parsed_simple.append({
                        'symbol': parts[0],
                        'interval': parts[1],
                        'candle_timestamp': parts[2],
                        'redis_score': score,
                        'redis_timestamp': datetime.fromtimestamp(score).strftime('%Y-%m-%d %H:%M:%S')
                    })
            except:
                pass
        
        return {
            'alerts_count': alerts_count,
            'alerts_simple_count': alerts_simple_count,
            'latest_alerts': parsed_alerts,
            'latest_simple': parsed_simple
        }
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        import traceback
        traceback.print_exc()
        return None

def compare_alerts():
    """Compare MySQL and Redis alerts"""
    print("\n" + "="*80)
    print("ALERT STORAGE DIAGNOSIS")
    print("="*80)
    
    # Check MySQL
    print("\n📊 MYSQL ALERTS TABLE:")
    print("-" * 80)
    try:
        mysql_alerts, mysql_stats = check_mysql_alerts()
        print(f"Total Alerts: {mysql_stats['total_alerts']}")
        print(f"Latest Alert Datetime: {mysql_stats['latest_datetime']}")
        print(f"Latest System Time: {mysql_stats['latest_system_time']}")
        
        print(f"\nLatest {len(mysql_alerts)} alerts from MySQL:")
        for alert in mysql_alerts:
            print(f"  ID: {alert['id']} | Symbol: {alert['symbol']} | "
                  f"Datetime: {alert['datetime']} | "
                  f"Timeframe: {alert['timeframe']} | "
                  f"Scan: {alert['scan_name']}")
    except Exception as e:
        print(f"❌ Error querying MySQL: {e}")
        mysql_alerts = []
        mysql_stats = {}
    
    # Check Redis
    print("\n📊 REDIS ALERTS:")
    print("-" * 80)
    redis_data = check_redis_alerts()
    if redis_data:
        print(f"Redis 'Alerts' count: {redis_data['alerts_count']}")
        print(f"Redis 'alerts_simple' count: {redis_data['alerts_simple_count']}")
        
        print(f"\nLatest {len(redis_data['latest_alerts'])} alerts from Redis 'Alerts':")
        for alert in redis_data['latest_alerts']:
            print(f"  Symbol: {alert.get('symbol')} | "
                  f"Datetime: {alert.get('datetime')} | "
                  f"Timeframe: {alert.get('timeframe')} | "
                  f"Redis Timestamp: {alert.get('redis_timestamp')}")
        
        print(f"\nLatest {len(redis_data['latest_simple'])} alerts from Redis 'alerts_simple':")
        for alert in redis_data['latest_simple']:
            print(f"  Symbol: {alert.get('symbol')} | "
                  f"Interval: {alert.get('interval')} | "
                  f"Candle Timestamp: {alert.get('candle_timestamp')} | "
                  f"Redis Timestamp: {alert.get('redis_timestamp')}")
    
    # Comparison
    print("\n🔍 COMPARISON:")
    print("-" * 80)
    if mysql_stats.get('latest_datetime'):
        mysql_latest = mysql_stats['latest_datetime']
        print(f"MySQL Latest: {mysql_latest}")
    else:
        print("MySQL Latest: No alerts found")
        mysql_latest = None
    
    if redis_data and redis_data['latest_alerts']:
        redis_latest = redis_data['latest_alerts'][0].get('datetime')
        print(f"Redis Latest: {redis_latest}")
        
        if mysql_latest:
            mysql_dt = datetime.strptime(str(mysql_latest), '%Y-%m-%d %H:%M:%S')
            redis_dt = datetime.strptime(redis_latest, '%Y-%m-%d %H:%M:%S')
            
            if redis_dt > mysql_dt:
                print(f"⚠️  ISSUE: Redis has newer alerts than MySQL!")
                print(f"   Redis is {redis_dt - mysql_dt} ahead of MySQL")
                print(f"   This suggests alerts are being stored in Redis but NOT in MySQL")
            elif mysql_dt > redis_dt:
                print(f"✅ MySQL has newer alerts than Redis")
            else:
                print(f"✅ MySQL and Redis are in sync")
        else:
            print(f"⚠️  ISSUE: Redis has alerts but MySQL has NONE!")
            print(f"   This confirms alerts are being stored in Redis but NOT in MySQL")
    else:
        print("Redis Latest: No alerts found")
    
    # Check for alerts in Redis but not in MySQL
    print("\n🔍 ALERTS IN REDIS BUT NOT IN MYSQL:")
    print("-" * 80)
    if redis_data and mysql_alerts:
        mysql_symbols_dt = {(a['symbol'], a['datetime']) for a in mysql_alerts}
        missing_count = 0
        for redis_alert in redis_data['latest_alerts']:
            redis_symbol = redis_alert.get('symbol')
            redis_dt = redis_alert.get('datetime')
            if (redis_symbol, redis_dt) not in mysql_symbols_dt:
                missing_count += 1
                print(f"  ❌ Missing: {redis_symbol} at {redis_dt}")
        
        if missing_count == 0:
            print("  ✅ All Redis alerts are present in MySQL")
        else:
            print(f"\n  ⚠️  Found {missing_count} alerts in Redis that are NOT in MySQL")
    
    print("\n" + "="*80)
    print("DIAGNOSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    compare_alerts()
