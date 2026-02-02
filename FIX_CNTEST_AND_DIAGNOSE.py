"""
Script to fix CNtest condition and diagnose basket_id 44 issue
"""
import pymysql
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

def find_cntest_condition():
    """Find CNtest condition (may have different name variations)"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Try different variations
            variations = ['CNtest', 'CN-test', 'CN Test', 'CN-Test', 'CNTEST']
            for variant in variations:
                cursor.execute("""
                    SELECT id, name
                    FROM conditions 
                    WHERE name = %s
                """, (variant,))
                result = cursor.fetchone()
                if result:
                    return result['name']
            return None
    finally:
        conn.close()

def fix_cntest_lrc_fields():
    """Null out lrcangletype, lrcanglestart, lrcangleend for CNtest"""
    conn = get_db_connection()
    try:
        # First find the actual condition name
        cntest_name = find_cntest_condition()
        if not cntest_name:
            print("❌ CNtest condition not found. Searching for similar conditions...")
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, name FROM conditions WHERE name LIKE '%CN%' OR name LIKE '%test%'")
                results = cursor.fetchall()
                if results:
                    print("   Similar conditions found:")
                    for r in results:
                        print(f"      ID: {r['id']}, Name: '{r['name']}'")
                return
        
        with conn.cursor() as cursor:
            # Update CNtest condition
            cursor.execute("""
                UPDATE conditions 
                SET 
                    lrcangletype = NULL,
                    lrcanglestart = NULL,
                    lrcangleend = NULL
                WHERE name = %s
            """, (cntest_name,))
            
            affected_rows = cursor.rowcount
            conn.commit()
            print(f"✅ Updated {affected_rows} row(s) - Nulled LRC angle fields for '{cntest_name}'")
            
            # Verify update
            cursor.execute("""
                SELECT id, name, lrcangletype, lrcanglestart, lrcangleend
                FROM conditions 
                WHERE name = %s
            """, (cntest_name,))
            result = cursor.fetchone()
            if result:
                print(f"   Verification: ID={result['id']}, Name='{result['name']}'")
                print(f"   lrcangletype={result['lrcangletype']}, lrcanglestart={result['lrcanglestart']}, lrcangleend={result['lrcangleend']}")
            return cntest_name
    finally:
        conn.close()

def get_cntest_monitoring_details(cntest_name=None):
    """Get complete CNtest condition details for monitoring"""
    if not cntest_name:
        cntest_name = find_cntest_condition()
        if not cntest_name:
            print("❌ CNtest condition not found. Cannot get monitoring details.")
            return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Get condition details (removed hlfpid as it doesn't exist)
            cursor.execute("""
                SELECT 
                    c.id AS condition_id,
                    c.name AS condition_name,
                    c.active,
                    c.condition1 AS condition1_enabled,
                    c.candle1 AS condition1_candle_type,
                    c.psar1 AS condition1_psar_id,
                    c.kline_start,
                    c.kline_end,
                    c.signaldirection,
                    c.condition2 AS condition2_enabled,
                    c.candle2 AS condition2_candle_type,
                    c.psar2 AS condition2_psar_id,
                    c.lrcid,
                    c.stochid,
                    c.lrcangletype,
                    c.lrcanglestart,
                    c.lrcangleend,
                    c.signalColor
                FROM conditions c
                WHERE c.name = %s
            """, (cntest_name,))
            condition = cursor.fetchone()
            
            # Get custom indicators
            cursor.execute("""
                SELECT 
                    ci.id,
                    ci.name,
                    ci.indicator_id,
                    i.name AS indicator_name,
                    ci.value,
                    ci.active
                FROM custom_indicators ci
                INNER JOIN indicators i ON ci.indicator_id = i.id
                WHERE ci.id IN (105, 107)
            """)
            custom_indicators = cursor.fetchall()
            
            # Get scan items
            cursor.execute("""
                SELECT 
                    si.id AS scan_item_id,
                    si.scanID,
                    s.name AS scan_name,
                    si.conditionID,
                    si.1min AS `1min_enabled`,
                    si.2min AS `2min_enabled`,
                    si.3min AS `3min_enabled`,
                    si.5min AS `5min_enabled`,
                    si.10min AS `10min_enabled`,
                    si.15min AS `15min_enabled`,
                    si.30min AS `30min_enabled`,
                    si.60min AS `60min_enabled`,
                    si.active
                FROM scanitems si
                INNER JOIN scans s ON si.scanID = s.id
                INNER JOIN conditions c ON si.conditionID = c.id
                WHERE c.name = %s
                AND si.active = 1
                AND s.active = 1
            """, (cntest_name,))
            scan_items = cursor.fetchall()
            
            # Print results
            print("\n" + "="*80)
            print("CNTEST MONITORING DETAILS FOR TOMORROW")
            print("="*80)
            
            if condition:
                print(f"\n📋 CONDITION: {condition['condition_name']} (ID: {condition['condition_id']})")
                print(f"   Active: {condition['active']}")
                
                print(f"\n🔧 CONDITION 1:")
                print(f"   Enabled: {condition['condition1_enabled']}")
                print(f"   Candle Type: {condition['condition1_candle_type']}")
                print(f"   PSAR ID: {condition['condition1_psar_id']}")
                print(f"   K-line Range: {condition['kline_start']} - {condition['kline_end']}")
                print(f"   Signal Direction: {condition['signaldirection']}")
                
                print(f"\n🔧 CONDITION 2:")
                print(f"   Enabled: {condition['condition2_enabled']}")
                print(f"   Candle Type: {condition['condition2_candle_type']}")
                print(f"   PSAR ID: {condition['condition2_psar_id']}")
                
                print(f"\n📊 INDICATOR IDs:")
                print(f"   LRC ID: {condition['lrcid']}")
                print(f"   Stochastic ID: {condition['stochid']}")
                
                print(f"\n🎨 Signal Color: {condition['signalColor']}")
                
                print(f"\n📐 LRC ANGLE (should be NULL):")
                print(f"   Type: {condition['lrcangletype']}")
                print(f"   Start: {condition['lrcanglestart']}")
                print(f"   End: {condition['lrcangleend']}")
            
            print(f"\n📈 CUSTOM INDICATORS:")
            for ci in custom_indicators:
                print(f"   ID {ci['id']}: {ci['name']} (Indicator: {ci['indicator_name']}) = {ci['value']}")
            
            print(f"\n📡 ACTIVE SCAN ITEMS ({len(scan_items)}):")
            enabled_tfs = set()
            for si in scan_items:
                tfs = []
                if si['1min_enabled']: tfs.append('1min')
                if si['2min_enabled']: tfs.append('2min')
                if si['3min_enabled']: tfs.append('3min')
                if si['5min_enabled']: tfs.append('5min')
                if si['10min_enabled']: tfs.append('10min')
                if si['15min_enabled']: tfs.append('15min')
                if si['30min_enabled']: tfs.append('30min')
                if si['60min_enabled']: tfs.append('60min')
                enabled_tfs.update(tfs)
                print(f"   Scan: {si['scan_name']} (ID: {si['scanID']}) | Timeframes: {', '.join(tfs) if tfs else 'NONE'}")
            
            print(f"\n✅ ENABLED TIMEFRAMES FOR MONITORING: {', '.join(sorted(enabled_tfs)) if enabled_tfs else 'NONE'}")
            print("="*80)
            
    finally:
        conn.close()

def diagnose_basket_44():
    """Diagnose basket_id 44 issue for group xyz"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            print("\n" + "="*80)
            print("BASKET_ID 44 DIAGNOSIS FOR GROUP XYZ")
            print("="*80)
            
            # Check group xyz
            cursor.execute("""
                SELECT id AS basket_id, name AS basket_name, active
                FROM baskets
                WHERE name = 'xyz'
            """)
            group_xyz = cursor.fetchone()
            
            if group_xyz:
                print(f"\n✅ GROUP 'xyz' FOUND:")
                print(f"   Basket ID: {group_xyz['basket_id']}")
                print(f"   Name: {group_xyz['basket_name']}")
                print(f"   Active: {group_xyz['active']}")
            else:
                print(f"\n❌ GROUP 'xyz' NOT FOUND in baskets table")
            
            # Check scans using basket_id 44
            cursor.execute("""
                SELECT id AS scan_id, name AS scan_name, basket_id, active
                FROM scans
                WHERE basket_id = 44
            """)
            scans_44 = cursor.fetchall()
            
            print(f"\n📋 SCANS USING BASKET_ID 44 ({len(scans_44)}):")
            for scan in scans_44:
                print(f"   Scan: {scan['scan_name']} (ID: {scan['scan_id']}) | Active: {scan['active']}")
            
            # Check basket_stocks for basket_id 44
            cursor.execute("""
                SELECT id, basket_id, symbol, active
                FROM basket_stocks
                WHERE basket_id = 44
            """)
            basket_stocks_44 = cursor.fetchall()
            
            print(f"\n📊 BASKET_STOCKS FOR BASKET_ID 44 ({len(basket_stocks_44)}):")
            if basket_stocks_44:
                for bs in basket_stocks_44:
                    print(f"   Symbol: {bs['symbol']} | Active: {bs['active']}")
            else:
                print(f"   ⚠️  NO STOCKS FOUND - Basket ID 44 has no entries in basket_stocks table!")
            
            # Check if basket_id 44 exists in baskets table
            cursor.execute("""
                SELECT id, name, active
                FROM baskets
                WHERE id = 44
            """)
            basket_44 = cursor.fetchone()
            
            if basket_44:
                print(f"\n✅ BASKET_ID 44 EXISTS in baskets table:")
                print(f"   ID: {basket_44['id']}")
                print(f"   Name: {basket_44['name']}")
                print(f"   Active: {basket_44['active']}")
            else:
                print(f"\n❌ BASKET_ID 44 DOES NOT EXIST in baskets table!")
                print(f"   ⚠️  ORPHANED REFERENCE - Scans reference basket_id 44 but basket doesn't exist")
            
            # Summary
            print(f"\n📝 DIAGNOSIS SUMMARY:")
            if group_xyz and group_xyz['basket_id'] == 44:
                if basket_stocks_44:
                    print(f"   ✅ Group xyz exists with basket_id 44")
                    print(f"   ✅ Basket_stocks has {len(basket_stocks_44)} entry/entries")
                    print(f"   ✅ Everything looks OK")
                else:
                    print(f"   ✅ Group xyz exists with basket_id 44")
                    print(f"   ⚠️  But basket_stocks has NO entries for basket_id 44")
                    print(f"   💡 ACTION: Add stocks to basket_stocks table for basket_id 44")
            elif group_xyz:
                print(f"   ⚠️  Group xyz exists but has basket_id {group_xyz['basket_id']}, not 44")
                print(f"   ⚠️  Scans reference basket_id 44 which may not match group xyz")
            else:
                print(f"   ❌ Group xyz does not exist")
                print(f"   ⚠️  Scans reference basket_id 44 but group may be missing or renamed")
            
            print("="*80)
            
    finally:
        conn.close()

if __name__ == "__main__":
    print("Starting CNtest fixes and diagnostics...\n")
    
    # Fix CNtest LRC fields
    print("STEP 1: Nulling out LRC angle fields for CNtest...")
    cntest_name = fix_cntest_lrc_fields()
    
    # Get monitoring details
    print("\nSTEP 2: Getting CNtest monitoring details...")
    get_cntest_monitoring_details(cntest_name)
    
    # Diagnose basket issue
    print("\nSTEP 3: Diagnosing basket_id 44 issue...")
    diagnose_basket_44()
    
    print("\n✅ All tasks completed!")
