"""
Query Condition to Alert Data
Run this script to see the complete Condition → Scan → Alert data flow for one symbol
"""
import pymysql
from background.set import settings
import pandas as pd

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False
    print("Note: tabulate not available, using simple table format")

def get_db_connection():
    """Get database connection using settings"""
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

def query_condition_to_alert_data(symbol=None, condition_id=None):
    """
    Query and display Condition → Scan → Alert data for a symbol
    
    Args:
        symbol: Symbol to query (if None, uses most recent symbol from alerts)
        condition_id: Condition ID to filter by (optional)
    """
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # If symbol not provided, get the most recent symbol from alerts (optionally filtered by condition_id)
        if symbol is None:
            if condition_id:
                cursor.execute("""
                    SELECT symbol FROM alerts 
                    WHERE deleted = 0 AND conditionID = %s
                    ORDER BY datetime DESC 
                    LIMIT 1
                """, (condition_id,))
            else:
                cursor.execute("""
                    SELECT symbol FROM alerts 
                    WHERE deleted = 0 
                    ORDER BY datetime DESC 
                    LIMIT 1
                """)
            result = cursor.fetchone()
            if result:
                symbol = result['symbol']
                print(f"Using most recent symbol from alerts: {symbol}")
            else:
                print("No alerts found in database")
                return
        
        print(f"\n{'='*80}")
        print(f"CONDITION -> SCAN -> ALERT DATA FOR: {symbol}")
        print(f"{'='*80}\n")
        
        # Main query - complete data view
        query = """
            SELECT 
                a.id AS alert_id,
                a.symbol,
                DATE_FORMAT(a.datetime, '%Y-%m-%d %H:%i:%s') AS alert_datetime,
                a.timeframe,
                s.name AS scan_name,
                s.id AS scan_id,
                c.name AS condition_name,
                c.id AS condition_id,
                c.condition1 AS cond1_enabled,
                c.condition2 AS cond2_enabled,
                c.candle1,
                c.candle2,
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
              AND a.deleted = 0
        """
        params = [symbol]
        
        if condition_id:
            query += " AND a.conditionID = %s"
            params.append(condition_id)
            
        query += " ORDER BY a.datetime DESC LIMIT 50"
        
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        
        if not results:
            print(f"No alerts found for symbol: {symbol}")
            return
        
        # Convert to DataFrame for better display
        df = pd.DataFrame(results)
        
        print(f"Found {len(df)} alerts\n")
        
        # Display summary first
        print("SUMMARY:")
        print(f"   Total Alerts: {len(df)}")
        print(f"   Unique Scans: {df['scan_id'].nunique()}")
        print(f"   Unique Conditions: {df['condition_id'].nunique()}")
        print(f"   Unique Timeframes: {df['timeframe'].nunique()}")
        print(f"   Timeframes: {', '.join(sorted(df['timeframe'].unique()))}")
        print()
        
        # Display all data in table format
        print("COMPLETE DATA (Condition -> Scan -> Alert):")
        print("="*80)
        
        # Select key columns for display
        display_columns = [
            'alert_id', 'symbol', 'alert_datetime', 'timeframe',
            'scan_name', 'scan_id', 
            'condition_name', 'condition_id',
            'cond1_enabled', 'candle1', 'kline_start', 'kline_end', 
            'signaldirection', 'signalColor',
            '1min', '5min', '15min', '30min', '60min'
        ]
        
        # Filter to only columns that exist
        display_columns = [col for col in display_columns if col in df.columns]
        
        if HAS_TABULATE:
            print(tabulate(df[display_columns], headers='keys', tablefmt='grid', showindex=False))
        else:
            # Simple table format without tabulate
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 30)
            print(df[display_columns].to_string(index=False))
        
        # Display detailed condition information
        print("\n" + "="*80)
        print("CONDITION DETAILS:")
        print("="*80)
        
        unique_conditions = df[['condition_id', 'condition_name', 'cond1_enabled', 'cond2_enabled', 
                                'candle1', 'candle2', 'psar1', 'stochid', 'kline_start', 
                                'kline_end', 'signaldirection', 'signalColor']].drop_duplicates()
        
        for idx, row in unique_conditions.iterrows():
            print(f"\nCondition ID {row['condition_id']}: {row['condition_name']}")
            print(f"   Condition 1 Enabled: {row['cond1_enabled']}")
            print(f"   Condition 2 Enabled: {row['cond2_enabled']}")
            print(f"   Candle Type 1: {row['candle1']} | Candle Type 2: {row['candle2']}")
            print(f"   PSAR ID 1: {row['psar1']} | Stochastic ID: {row['stochid']}")
            print(f"   K-line Range: {row['kline_start']} to {row['kline_end']}")
            print(f"   Signal Direction: {row['signaldirection']}")
            print(f"   Signal Color: {row['signalColor']}")
        
        print("\n" + "="*80)
        print("Query completed successfully!")
        print("="*80)
        
    except pymysql.Error as err:
        print(f"Database Error: {err}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    import sys
    
    # Get symbol and condition_id from command line if provided
    symbol = None
    condition_id = None
    
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    if len(sys.argv) > 2:
        condition_id = int(sys.argv[2])
    
    if symbol:
        print(f"Querying data for symbol: {symbol}")
    else:
        print("Querying data for most recent symbol...")
    if condition_id:
        print(f"Filtering by conditionID: {condition_id}")
    
    query_condition_to_alert_data(symbol, condition_id)
