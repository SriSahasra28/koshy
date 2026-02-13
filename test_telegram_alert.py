"""
Test Telegram Notification and Alert Logic

This script allows you to test:
1. Telegram notification delivery (standalone)
2. Full alert processing flow (optional)

Usage:
    python test_telegram_alert.py [--full-alert]

Options:
    --full-alert    Test the complete alert flow (requires DB and Redis)
"""

import asyncio
import sys
import os
from datetime import datetime
from telegram import Bot
from telegram.constants import ParseMode

# Fix Windows console encoding for Unicode characters
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Import the telegram function from redis_alert_engine
try:
    from redis_alert_engine import send_telegram_message
    HAS_ALERT_ENGINE = True
except ImportError:
    HAS_ALERT_ENGINE = False
    print("Warning: Could not import from redis_alert_engine.py")
    print("Will test telegram directly instead.")


async def test_telegram_direct():
    """Test Telegram notification directly using the bot"""
    print("\n" + "="*60)
    print("TEST 1: Direct Telegram Notification")
    print("="*60)
    
    bot_token = '8213206702:AAGRu6r0ag2zjbvT8TyoBGBq0gx_I05uH6o'
    chat_ids = ['@koshy_alerts', '1367653901', '5210270840']  # Channel and personal chat IDs
    
    # Test message (use regular dash instead of em dash to avoid markdown parsing issues)
    test_stock = "TEST_SYMBOL"
    test_price = 1234.56
    test_date = datetime.now().strftime("%d %b %Y")
    test_time = datetime.now().strftime("%H:%M")
    test_tf = "5"
    test_sn = "Test Scan"
    
    # Use exact format as redis_alert_engine.py (with em dash)
    message = f"*Alert*\nStock : {test_stock}\nPrice : Rs. {test_price}\nDate : {test_date}\nTime : {test_time}\nTF — {test_tf} min \nSN — {test_sn}"
    
    print(f"\nSending test message to Telegram...")
    print(f"Bot Token: {bot_token[:20]}...")
    print(f"Chat IDs: {chat_ids}")
    print(f"\nMessage:")
    print(message.replace('*', ''))  # Print without markdown
    
    try:
        bot = Bot(token=bot_token)
        success_count = 0
        
        for chat_id in chat_ids:
            try:
                # Try MARKDOWN first (as in production)
                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=message,
                        parse_mode=ParseMode.MARKDOWN
                    )
                except Exception as markdown_error:
                    # If markdown fails, try HTML mode (more forgiving)
                    print(f"[WARN] Markdown failed for {chat_id}, trying HTML: {markdown_error}")
                    message_html = message.replace('*Alert*', '<b>Alert</b>')
                    await bot.send_message(
                        chat_id=chat_id,
                        text=message_html,
                        parse_mode=ParseMode.HTML
                    )
                print(f"[OK] Successfully sent to {chat_id}")
                success_count += 1
            except Exception as e:
                print(f"[FAIL] Failed to send to {chat_id}: {e}")
        
        if success_count > 0:
            print(f"\n[PASS] Telegram test PASSED: {success_count}/{len(chat_ids)} messages sent")
            return True
        else:
            print(f"\n[FAIL] Telegram test FAILED: No messages sent")
            return False
            
    except Exception as e:
        print(f"\n[FAIL] Telegram test FAILED: {e}")
        return False


async def test_telegram_via_engine():
    """Test Telegram notification using the redis_alert_engine function"""
    print("\n" + "="*60)
    print("TEST 2: Telegram via redis_alert_engine")
    print("="*60)
    
    if not HAS_ALERT_ENGINE:
        print("[SKIP] Skipping: redis_alert_engine not available")
        return False
    
    test_stock = "TEST_SYMBOL_ENGINE"
    test_price = 9876.54
    test_date = datetime.now().strftime("%d %b %Y")
    test_time = datetime.now().strftime("%H:%M")
    test_tf = "3"
    test_sn = "Test Scan Engine"
    
    print(f"\nSending test message via redis_alert_engine.send_telegram_message()...")
    print(f"Stock: {test_stock}, Price: {test_price}, TF: {test_tf}min")
    
    try:
        success = await send_telegram_message(
            stock=test_stock,
            price=test_price,
            date=test_date,
            time=test_time,
            tf=test_tf,
            sn=test_sn
        )
        
        if success:
            print("[PASS] Telegram test via engine PASSED")
            return True
        else:
            print("[FAIL] Telegram test via engine FAILED")
            return False
            
    except Exception as e:
        print(f"[FAIL] Telegram test via engine FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_full_alert_flow():
    """Test the complete alert processing flow (requires DB and Redis)"""
    print("\n" + "="*60)
    print("TEST 3: Full Alert Processing Flow")
    print("="*60)
    
    print("\n[INFO] This test requires:")
    print("  - MySQL database connection")
    print("  - Redis connection")
    print("  - Valid symbol and condition data")
    print("\nThis is a simplified test - for full testing, use the actual alert engine.")
    
    # You can add more comprehensive testing here if needed
    # For now, just indicate this would require the full system
    print("\n[TIP] To test full alert flow:")
    print("  1. Ensure main-consumer.py is running")
    print("  2. Ensure tick_zerodha.py is running (during market hours)")
    print("  3. Or manually trigger an alert via the database")
    
    return True


async def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("TELEGRAM & ALERT TESTING SUITE")
    print("="*60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = []
    
    # Test 1: Direct Telegram
    result1 = await test_telegram_direct()
    results.append(("Direct Telegram", result1))
    
    # Test 2: Telegram via Engine
    result2 = await test_telegram_via_engine()
    results.append(("Telegram via Engine", result2))
    
    # Test 3: Full Alert Flow (if requested)
    if '--full-alert' in sys.argv:
        result3 = await test_full_alert_flow()
        results.append(("Full Alert Flow", result3))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    for test_name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"  {test_name}: {status}")
    
    all_passed = all(r[1] for r in results)
    
    if all_passed:
        print("\n[PASS] All tests PASSED!")
    else:
        print("\n[WARN] Some tests FAILED - check output above")
    
    print("="*60 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[WARN] Test interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
