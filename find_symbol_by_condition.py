"""
Find a symbol that has alerts with a specific condition ID
"""
import pymysql
from background.set import settings

def find_symbol_by_condition(condition_id):
    """Find a symbol with alerts for the given condition ID"""
    conn = None
    cursor = None
    
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
        cursor = conn.cursor(dictionary=True)
        
        # Find most recent symbol with this condition ID
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
        
        if result:
            print(f"Found symbol: {result['symbol']}")
            print(f"  Alert count: {result['alert_count']}")
            print(f"  Latest alert: {result['latest_alert']}")
            return result['symbol']
        else:
            print(f"No symbols found with conditionID = {condition_id}")
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    import sys
    condition_id = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    symbol = find_symbol_by_condition(condition_id)
    if symbol:
        print(f"\nSymbol to use: {symbol}")
