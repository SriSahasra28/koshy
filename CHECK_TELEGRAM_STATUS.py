"""
Check Telegram delivery status for today's alerts
"""
import pymysql
from datetime import datetime, timedelta
from background.set import settings
import re

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

def check_telegram_delivery():
    """Check if Telegram messages were sent for today's alerts"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Get today's alerts
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)
        
        query = """
            SELECT 
                id,
                symbol,
                datetime as alert_datetime,
                system_time,
                conditionID,
                timeframe
            FROM alerts
            WHERE deleted = 0
              AND datetime >= %s
              AND datetime < %s
            ORDER BY datetime DESC
        """
        
        cursor.execute(query, (today, tomorrow))
        alerts = cursor.fetchall()
        
        print("\n" + "="*80)
        print("TELEGRAM DELIVERY STATUS CHECK")
        print("="*80)
        print(f"\nFound {len(alerts)} alerts today\n")
        
        if not alerts:
            print("No alerts found for today")
            return
        
        # Check if main-consumer.py is running
        print("="*80)
        print("SYSTEM STATUS")
        print("="*80)
        print("\n[INFO] To check if main-consumer.py is running:")
        print("  PowerShell: Get-Process python | Where-Object {$_.CommandLine -like '*main-consumer*'}")
        print("  Or check your terminal/process manager")
        
        # Check Telegram bot configuration
        print("\n" + "="*80)
        print("TELEGRAM CONFIGURATION")
        print("="*80)
        print("\nCurrent Configuration (from redis_alert_engine.py):")
        print("  Bot Token: 8213206702:AAGRu6r0ag2zjbvT8TyoBGBq0gx_I05uH6o")
        print("  Chat IDs:")
        print("    1. @koshy_alerts (channel)")
        print("    2. 1367653901 (your personal chat)")
        print("\n[NOTE] Both chat IDs should receive messages")
        
        # Show today's alerts
        print("\n" + "="*80)
        print("TODAY'S ALERTS")
        print("="*80)
        for alert in alerts:
            delay_seconds = (alert['system_time'] - alert['alert_datetime']).total_seconds()
            print(f"\nAlert ID: {alert['id']}")
            print(f"  Symbol: {alert['symbol']}")
            print(f"  Alert Time: {alert['alert_datetime']}")
            print(f"  Stored At: {alert['system_time']}")
            print(f"  Delay: {delay_seconds:.0f} seconds")
            print(f"  Condition ID: {alert['conditionID']}")
            print(f"  Timeframe: {alert['timeframe']}min")
        
        # Possible reasons for no Telegram notifications
        print("\n" + "="*80)
        print("POSSIBLE REASONS FOR NO TELEGRAM NOTIFICATIONS")
        print("="*80)
        print("\n1. **main-consumer.py not restarted after code changes**")
        print("   → The Telegram code change (personal chat ID) requires restart")
        print("   → Old process might still be running with old code")
        print("\n2. **Telegram bot not authorized for your personal chat**")
        print("   → Bot needs to be added to @koshy_alerts channel")
        print("   → For personal chat (1367653901), you need to start a chat with the bot first")
        print("   → Send /start to the bot to initialize chat")
        print("\n3. **Telegram API errors**")
        print("   → Check logs for 'Error sending Telegram message'")
        print("   → Bot token might be invalid/revoked")
        print("   → Network connectivity issues")
        print("\n4. **Database save failed**")
        print("   → Alerts only send Telegram if DB save succeeds")
        print("   → Check if alerts are in MySQL database")
        print("\n5. **Bot not authorized for channel**")
        print("   → Bot must be admin of @koshy_alerts channel")
        print("   → Channel must allow bot to post messages")
        
        print("\n" + "="*80)
        print("HOW TO FIX")
        print("="*80)
        print("\n1. **Restart main-consumer.py**")
        print("   → Stop current process (Ctrl+C)")
        print("   → Start fresh: python main-consumer.py")
        print("\n2. **Initialize bot for personal chat**")
        print("   → Find your bot username (e.g., @YourBotName)")
        print("   → Open Telegram and send /start to the bot")
        print("   → This initializes the chat so bot can send you messages")
        print("\n3. **Check logs**")
        print("   → Look for 'Error sending Telegram message' in logs")
        print("   → Check for 'CRITICAL: Failed to send Telegram' messages")
        print("\n4. **Verify bot permissions**")
        print("   → Ensure bot is admin in @koshy_alerts channel")
        print("   → Check channel settings allow bot posting")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_telegram_delivery()
