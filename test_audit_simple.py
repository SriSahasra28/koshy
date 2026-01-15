"""Simple test script to debug the audit script"""
import sys
print("Script starting...", flush=True)

try:
    print("Importing pymysql...", flush=True)
    import pymysql
    print("pymysql imported", flush=True)
    
    print("Importing settings...", flush=True)
    from background.set import settings
    print("settings imported", flush=True)
    
    print("Getting DB config...", flush=True)
    db_config = settings.get_db()
    print(f"DB config retrieved: {len(db_config)} items", flush=True)
    
    print("Connecting to database...", flush=True)
    print(f"  Host: {db_config[2]}, Port: {db_config[3]}, User: {db_config[0]}, Database: {db_config[4]}", flush=True)
    
    try:
        conn = pymysql.connect(
            host=db_config[2],
            port=db_config[3],
            user=db_config[0],
            password=db_config[1],
            database=db_config[4],
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Connected!", flush=True)
    except pymysql.Error as err:
        print(f"MySQL Error: {err}", flush=True)
        print(f"Error Code: {err.args[0]}", flush=True)
        print(f"Error Message: {err.args[1]}", flush=True)
        raise
    except Exception as e:
        print(f"General Error: {type(e).__name__}: {e}", flush=True)
        raise
    
    print("Creating cursor...", flush=True)
    cursor = conn.cursor()
    print("Cursor created", flush=True)
    
    print("Executing query...", flush=True)
    query = "SELECT DISTINCT a.symbol FROM alerts a WHERE a.conditionID = %s AND a.deleted = 0 LIMIT 1"
    cursor.execute(query, (12,))
    result = cursor.fetchone()
    print(f"Query result: {result}", flush=True)
    
    cursor.close()
    conn.close()
    print("Test completed successfully!", flush=True)
    
except Exception as e:
    print(f"ERROR: {e}", flush=True)
    import traceback
    traceback.print_exc(file=sys.stdout)
    sys.exit(1)
