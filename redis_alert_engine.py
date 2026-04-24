# python/redis_alert_engine.py

import asyncio
import sys
import numpy as np
import pandas as pd
from datetime import datetime, time as dt_time
import pytz
import json
import os 
import requests
from telegram import Bot
from telegram.constants import ParseMode
import talib
from redis.asyncio.lock import Lock
from loguru import logger
from background.indicators import linear_regression_channel_numba_sliding

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.timezone("UTC")

# Reusable Telegram bot (singleton)
_TELEGRAM_BOT = None
_TELEGRAM_BOT_LOCK = asyncio.Lock()  # Lock for thread-safe bot initialization

# Async alert delivery queue — decouples DB insert + Telegram + Redis publish from main loop
_ALERT_DELIVERY_QUEUE = None  # Initialized lazily on first use
_ALERT_DELIVERY_TASK = None   # Background consumer task

# Helper function for safe TTL setting
async def set_ttl_safe(redis_client, key, ttl=30*24*60*60):
    """Set TTL on key, silently ignore errors (non-critical operation)"""
    try:
        await redis_client.expire(key, ttl)
    except Exception:
        pass  # TTL failure is non-critical

async def _ensure_alert_queue():
    """Lazily initialize the alert delivery queue and start background consumer."""
    global _ALERT_DELIVERY_QUEUE, _ALERT_DELIVERY_TASK
    if _ALERT_DELIVERY_QUEUE is None:
        _ALERT_DELIVERY_QUEUE = asyncio.Queue()
        _ALERT_DELIVERY_TASK = asyncio.create_task(_alert_delivery_consumer())
        logger.info("[ALERT_QUEUE] Alert delivery queue initialized with background consumer")
    return _ALERT_DELIVERY_QUEUE


async def _alert_delivery_consumer():
    """Background consumer that processes alert deliveries (DB insert, Telegram, Redis publish).
    Runs forever, draining the queue without blocking the main processing loop."""
    logger.info("[ALERT_QUEUE] Background alert delivery consumer started")
    while True:
        try:
            alert_job = await _ALERT_DELIVERY_QUEUE.get()
            try:
                await _process_alert_delivery(alert_job)
            except Exception as e:
                logger.error(f"[ALERT_QUEUE] Error processing alert delivery: {e}")
            finally:
                _ALERT_DELIVERY_QUEUE.task_done()
        except asyncio.CancelledError:
            logger.info("[ALERT_QUEUE] Alert delivery consumer cancelled")
            break
        except Exception as e:
            logger.error(f"[ALERT_QUEUE] Unexpected error in consumer loop: {e}")
            await asyncio.sleep(0.1)


async def _process_alert_delivery(job):
    """Execute the actual alert delivery work: DB insert → Telegram → Redis publish."""
    db = job['db']
    redis_client = job['redis_client']
    exchange_code = job['exchange_code']
    alert_timestamp = job['alert_timestamp']
    alert_timestamp_dt = job['alert_timestamp_dt']
    scanID = job['scanID']
    digit_name = job['digit_name']
    conditionID = job['conditionID']
    close_ha = job['close_ha']
    scan_name = job['scan_name']
    info = job['info']
    module_name = job['module_name']
    today = job['today']

    # Step 1: DB inserts with retry
    max_retries = 3
    db_success = False

    for attempt in range(max_retries):
        try:
            await db.insert_trade_log(
                date_log=today,
                module=module_name,
                activity='Alert Generated',
                important_data=info,
                priority=5,
                strategy_trade_id='',
                timestamp=datetime.now()
            )
            alert_success = await db.insert_alert(exchange_code, alert_timestamp, scanID, digit_name, datetime.now(), conditionID)
            if alert_success:
                db_success = True
                logger.info(f"[ALERT_QUEUE] ✅ DB insert success: {exchange_code} at {alert_timestamp}")
                break
            else:
                logger.warning(f"[ALERT_QUEUE] ❌ DB insert failed for {exchange_code} (attempt {attempt+1}/{max_retries})")
        except Exception as e:
            logger.error(f"[ALERT_QUEUE] ❌ DB exception (attempt {attempt+1}/{max_retries}): {e}")
        if attempt < max_retries - 1:
            await asyncio.sleep(0.1 * (2 ** attempt))

    if not db_success:
        logger.error(f"[ALERT_QUEUE] CRITICAL: Failed DB insert after {max_retries} attempts: {exchange_code}")
        return  # Don't send Telegram/Redis if DB failed

    # Step 2: Telegram (parallel to all chat IDs, already implemented in send_telegram_message)
    max_telegram_retries = 3
    telegram_sent = False
    for attempt in range(max_telegram_retries):
        try:
            success = await send_telegram_message(
                stock=exchange_code,
                price=round(close_ha, 2),
                date=alert_timestamp_dt.strftime("%d %b %Y"),
                time=alert_timestamp_dt.strftime("%H:%M"),
                tf=digit_name,
                sn=scan_name
            )
            if success:
                telegram_sent = True
                break
        except Exception as e:
            logger.warning(f"[ALERT_QUEUE] Telegram attempt {attempt+1}/{max_telegram_retries} failed: {e}")
            if attempt < max_telegram_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
    if not telegram_sent:
        logger.error(f"[ALERT_QUEUE] CRITICAL: Failed Telegram after {max_telegram_retries} attempts for {exchange_code}")

    # Step 3: Redis sorted set + publish
    try:
        alert_data = {
            "symbol": str(exchange_code),
            "datetime": alert_timestamp_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "scanid": str(int(scanID)) if pd.notna(scanID) else "0",
            "timeframe": str(digit_name),
            "bottime": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
            "conditionID": str(int(conditionID)) if pd.notna(conditionID) else "0"
        }

        sorted_set_key = "Alerts"
        timestamp_score = datetime.now().timestamp()
        existing_count = await redis_client.zcount(sorted_set_key, timestamp_score, timestamp_score)
        if existing_count == 0:
            await redis_client.zadd(sorted_set_key, {json.dumps(alert_data): timestamp_score})
        await set_ttl_safe(redis_client, sorted_set_key)

        # Simple alert sorted set
        try:
            simple_alert_key = "alerts_simple"
            candle_timestamp_score = int(alert_timestamp_dt.timestamp())
            simple_alert_value = f"{exchange_code},{digit_name},{alert_timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')}"
            existing_count = await redis_client.zcount(simple_alert_key, candle_timestamp_score, candle_timestamp_score)
            if existing_count == 0:
                await redis_client.zadd(simple_alert_key, {simple_alert_value: candle_timestamp_score})
            await set_ttl_safe(redis_client, simple_alert_key)
        except Exception as simple_err:
            logger.error(f"[ALERT_QUEUE] Error storing simple alert: {simple_err}")

        # Publish notification
        alert_notification = {
            'type': 'info',
            'message': 'new alert',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': exchange_code,
            'timeframe': digit_name
        }
        await redis_client.publish('alerts', json.dumps(alert_notification))
        logger.info(f"[ALERT_QUEUE] ✅ Full delivery complete: {exchange_code} {digit_name}")
    except Exception as e:
        logger.error(f"[ALERT_QUEUE] Error in Redis operations: {e}")


# --- Utility functions (copy from your main_redis_remaster.py) ---

CSV_OUTPUT_DIR = "C:/Users/Administrator/Desktop/React/koshy-trading-app-server/alerts_logs"
DEBUG_CSV_OUTPUT_DIR = "C:/Users/Administrator/Desktop/React/koshy-trading-app-server/debug_logs"

# --- Logging setup using loguru (OPTIMIZED: Console only, no file logging for performance) ---
def _setup_logging():
    """Setup loguru logger - console only (file logging removed for performance)"""
    try:
        # Remove default handler
        logger.remove()
        
        # Add console handler only (file logging removed to reduce I/O bottleneck)
        logger.add(
            sys.stdout,
            level="INFO",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
            colorize=True
        )
        
        logger.info("redis-alert-engine logging initialized (console only - file logging disabled for performance)")
    except Exception as e:
        # Fallback to basic console logging if setup fails
        logger.add(sys.stdout, level="INFO")
        logger.error(f"Failed to setup logging: {e}")

_setup_logging()

async def send_telegram_message(stock, price, date, time, tf, sn):
    """Format and send a message to Telegram chats (channel + personal) via the bot with only 'Alert' in bold."""
    # Escape underscores in dynamic values — Telegram Markdown treats _ as italic
    stock = str(stock).replace('_', r'\_')
    sn = str(sn).replace('_', r'\_')
    # Only 'Alert' is formatted as bold
    # Check if it's a crypto symbol (contains USDT)
    if 'USDT' in stock:
        message = f"*Crypto Alert*\nSymbol : {stock}\nPrice : ${price}\nDate : {date}\nTime : {time}\nTF - {tf} min \nSN - {sn}"
    else:
        message = f"*Alert*\nStock : {stock}\nPrice : Rs. {price}\nDate : {date}\nTime : {time}\nTF - {tf} min \nSN - {sn}"
    
    bot_token = '8213206702:AAGRu6r0ag2zjbvT8TyoBGBq0gx_I05uH6o'
    # Send to both channel and personal chat
    chat_ids = ['@koshy_alerts', '1367653901', '5210270840']  # Channel and personal chat IDs
    
    global _TELEGRAM_BOT, _TELEGRAM_BOT_LOCK
    # CRITICAL: Use lock to prevent race condition when multiple alerts trigger in parallel
    async with _TELEGRAM_BOT_LOCK:
        if _TELEGRAM_BOT is None:
            try:
                _TELEGRAM_BOT = Bot(token=bot_token)
            except Exception as e:
                logger.error(f"Error initializing Telegram bot: {e}")
                return False
    
    # Send to all chat IDs in PARALLEL, return True if at least one succeeds
    async def _send_to_chat(chat_id):
        try:
            await _TELEGRAM_BOT.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN
            )
            return True
        except Exception as e:
            logger.error(f"Error sending Telegram message to {chat_id}: {e}")
            return False

    results = await asyncio.gather(*[_send_to_chat(cid) for cid in chat_ids])
    return any(results)

    
async def save_ticker_candle_to_database(ticker, row_data, columns, db, table_name="one_min_ohlc"):
    """
    Save ticker candle data to MySQL database - ONLY for 1-minute candles.
    """
    try:
        # Validate row_data
        if not row_data or not columns or len(row_data) != len(columns):
            logger.warning(f"Invalid row data or column mismatch for {ticker}")
            return False

        # Only save 1-minute candles to database
        if table_name != "one_min_ohlc":
            return True

        # Create DataFrame for the row
        df_row = pd.DataFrame([row_data], columns=columns)

        # Extract OHLC data
        if all(col in df_row.columns for col in ['timestamp', 'open', 'high', 'low', 'close']):
            timestamp = pd.to_datetime(df_row['timestamp'].iloc[0])
            open_price = float(df_row['open'].iloc[0])
            high_price = float(df_row['high'].iloc[0])
            low_price = float(df_row['low'].iloc[0])
            close_price = float(df_row['close'].iloc[0])

            # Save 1-minute data to database (without volume)
            await db.insert_one_min_ohlc(ticker, timestamp, open_price, high_price, low_price, close_price)
            return True
        else:
            logger.warning(f"Missing required OHLC columns for {ticker}")
            return False

    except Exception as e:
        logger.error(f"Error saving to database for {ticker}: {e}")
        return False


def create_debug_csv_directory():
    """Create debug CSV output directory if it doesn't exist"""
    try:
        os.makedirs(DEBUG_CSV_OUTPUT_DIR, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Error creating debug CSV directory: {e}")
        return False


def get_debug_csv_filename(symbol, interval):
    """Generate debug CSV filename for symbol and timeframe"""
    try:
        # Clean symbol name for filename (remove special characters)
        clean_symbol = "".join(c for c in symbol if c.isalnum() or c in ('-', '_')).rstrip()
        
        # Create filename with timeframe
        if interval == '1minute':
            filename = f"{clean_symbol}_debug.csv"
        else:
            timeframe = interval.replace('minute', 'min')
            filename = f"{clean_symbol}_{timeframe}_debug.csv"
        
        return os.path.join(DEBUG_CSV_OUTPUT_DIR, filename)
    except Exception as e:
        logger.error(f"Error generating debug CSV filename for {symbol} {interval}: {e}")
        return None


def _write_csv_sync(csv_filename, row_data, file_exists):
    """Synchronous CSV write operation (runs in thread pool)"""
    try:
        # Create DataFrame for this row
        df_row = pd.DataFrame([row_data])
        
        # Append to CSV file
        if file_exists:
            df_row.to_csv(csv_filename, mode='a', header=False, index=False)
        else:
            df_row.to_csv(csv_filename, mode='w', header=True, index=False)
        
        return True
    except Exception as e:
        logger.error(f"Error writing CSV file {csv_filename}: {e}")
        return False


async def log_debug_data_to_csv(symbol, interval, debug_data):
    """
    Async: Log debug information to CSV file for each ticker and timeframe
    
    Args:
        symbol: Stock symbol
        interval: Timeframe (e.g., '1minute', '5minute')
        debug_data: Dictionary containing all debug information
    """
    try:
        # Create directory if it doesn't exist (synchronous, but fast)
        if not create_debug_csv_directory():
            return False
        
        # Get filename for this symbol and timeframe
        csv_filename = get_debug_csv_filename(symbol, interval)
        if not csv_filename:
            return False
        
        # Check if file exists to determine if we need to write headers
        file_exists = os.path.exists(csv_filename)
        
        # Prepare row data
        row_data = {
            'timestamp': debug_data.get('timestamp', ''),
            'symbol': symbol,
            'timeframe': interval,
            'open': debug_data.get('open', 0),
            'high': debug_data.get('high', 0),
            'low': debug_data.get('low', 0),
            'close': debug_data.get('close', 0),
            'volume': debug_data.get('volume', 0),
            'ha_open': debug_data.get('ha_open', 0),
            'ha_high': debug_data.get('ha_high', 0),
            'ha_low': debug_data.get('ha_low', 0),
            'ha_close': debug_data.get('ha_close', 0),
            'psar_value': debug_data.get('psar_value', 0),
            'psar_signal': debug_data.get('psar_signal', 0),
            'stochastic_k': debug_data.get('stochastic_k', 0),
            'stochastic_d': debug_data.get('stochastic_d', 0),
            'k_in_range': debug_data.get('k_in_range', False),
            'psar_signal_match': debug_data.get('psar_signal_match', False),
            'candle_color': debug_data.get('candle_color', ''),
            'candle_type': debug_data.get('candle_type', 0),
            'candle_conditions_met': debug_data.get('candle_conditions_met', False),
            'alert_triggered': debug_data.get('alert_triggered', False),
            'scan_id': debug_data.get('scan_id', 0),
            'scan_name': debug_data.get('scan_name', ''),
            'condition_id': debug_data.get('condition_id', 0),
            'kline_start': debug_data.get('kline_start', 0),
            'kline_end': debug_data.get('kline_end', 0),
            'signaldirection': debug_data.get('signaldirection', 0),
            'processing_time': debug_data.get('processing_time', ''),
            'error_message': debug_data.get('error_message', '')
        }
        
        # Run CSV write in thread pool executor (non-blocking)
        result = await asyncio.to_thread(_write_csv_sync, csv_filename, row_data, file_exists)
        return result
        
    except Exception as e:
        logger.error(f"Error logging debug data to CSV for {symbol} {interval}: {e}")
        return False


def heikin_ashi_numpy(open_, high, low, close):
    """Calculate Heikin Ashi on entire dataset (full recalculation)"""
    # Removed verbose HA calculation logs (high frequency, low value)
    ha_close = (open_ + high + low + close) / 4
    ha_open = np.zeros_like(open_)
    ha_open[0] = (open_[0] + close[0]) / 2  # Standard HA formula
    for i in range(1, len(open_)):
        ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2
    ha_high = np.maximum.reduce([high, ha_open, ha_close])
    ha_low = np.minimum.reduce([low, ha_open, ha_close])
    return ha_open, ha_high, ha_low, ha_close


def heikin_ashi_incremental(open_, high, low, close, last_ha_open=None, last_ha_close=None):
    """Calculate Heikin Ashi incrementally for new candles using previous HA values.
    
    Args:
        open_, high, low, close: Arrays of new candles to calculate HA for
        last_ha_open: Last HA open value from previous calculation (None for first candle)
        last_ha_close: Last HA close value from previous calculation (None for first candle)
    
    Returns:
        ha_open, ha_high, ha_low, ha_close: Heikin Ashi values for the new candles
    """
    if len(open_) == 0:
        return np.array([]), np.array([]), np.array([]), np.array([])
    
    ha_close = (open_ + high + low + close) / 4
    ha_open = np.zeros_like(open_)
    
    # Initialize first HA open
    if last_ha_open is not None and last_ha_close is not None:
        # Continue from previous HA values
        ha_open[0] = (last_ha_open + last_ha_close) / 2
    else:
        # First candle: use standard HA formula
        ha_open[0] = (open_[0] + close[0]) / 2
    
    # Calculate subsequent HA opens
    for i in range(1, len(open_)):
        ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2
    
    ha_high = np.maximum.reduce([high, ha_open, ha_close])
    ha_low = np.minimum.reduce([low, ha_open, ha_close])
    
    return ha_open, ha_high, ha_low, ha_close


def psar(high, low, close, af0=0.02, af=None, max_af=0.2):
    """
    Calculate Parabolic SAR using TA-Lib (SYNCHRONOUS - runs in thread pool).
    
    Args:
        high: array of high prices
        low: array of low prices
        close: array of close prices (not used by TA-Lib SAR, but kept for compatibility)
        af0: acceleration factor (default 0.02)
        af: initial acceleration factor (if None, uses af0) - not used by TA-Lib, kept for compatibility
        max_af: maximum acceleration factor (default 0.2)
    
    Returns:
        Array of PSAR values
    """
    # Convert to numpy arrays if needed
    high = np.asarray(high, dtype=np.float64)
    low = np.asarray(low, dtype=np.float64)
    
    # TA-Lib SAR signature: SAR(high, low, acceleration=0.02, maximum=0.2)
    # Use af0 as acceleration, max_af as maximum
    psar_values = talib.SAR(high, low, acceleration=float(af0), maximum=float(max_af))
    
    # Replace NaN values with 0 (for compatibility with existing code)
    psar_values = np.nan_to_num(psar_values, nan=0.0)
    
    return psar_values

async def psar_async(high, low, close, af0=0.02, af=None, max_af=0.2):
    """
    Calculate Parabolic SAR asynchronously (offloads CPU work to thread pool).
    This prevents blocking the event loop during TA-Lib calculations.
    
    Returns:
        Array of PSAR values
    """
    return await asyncio.to_thread(psar, high, low, close, af0, af, max_af)

def get_psar_signals(close, psar_values):
    signals = np.zeros(len(close))   
    for i in range(1, len(close)):
        if close[i] > psar_values[i] and close[i-1] <= psar_values[i-1]:
            signals[i] = 1  # Long signal
        elif close[i] < psar_values[i] and close[i-1] >= psar_values[i-1]:
            signals[i] = -1  # Short signal
    
    return signals

def calc_fastStochastics(low, high, close, lookback_period=14, d_period=3, k_smoothing_period=3):
    """
    Calculate Fast Stochastic Oscillator using TA-Lib (SYNCHRONOUS - runs in thread pool).
    
    Args:
        low: array of low prices
        high: array of high prices
        close: array of close prices
        lookback_period: period for %K calculation (default 14)
        d_period: period for %D calculation (default 3)
        k_smoothing_period: smoothing period for %K (default 3)
    
    Returns:
        Tuple of (K values, D values) as numpy arrays
    """
    # Coerce window arguments to safe integers
    try:
        lookback_period = max(1, int(lookback_period))
    except Exception:
        lookback_period = 14
    try:
        d_period = max(1, int(d_period))
    except Exception:
        d_period = 3
    try:
        k_smoothing_period = max(1, int(k_smoothing_period))
    except Exception:
        k_smoothing_period = 3
    
    # Convert to numpy arrays if needed
    high = np.asarray(high, dtype=np.float64)
    low = np.asarray(low, dtype=np.float64)
    close = np.asarray(close, dtype=np.float64)
    
    # TA-Lib STOCH signature: STOCH(high, low, close, 
    #   fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    # Returns: (slowk, slowd)
    # For fast stochastic: fastk_period = lookback_period, slowk_period = k_smoothing_period, slowd_period = d_period
    slowk, slowd = talib.STOCH(
        high, low, close,
        fastk_period=lookback_period,
        slowk_period=k_smoothing_period,
        slowk_matype=0,  # Simple Moving Average
        slowd_period=d_period,
        slowd_matype=0   # Simple Moving Average
    )
    
    # Replace NaN values with 0 (for compatibility with existing code)
    slowk = np.nan_to_num(slowk, nan=0.0)
    slowd = np.nan_to_num(slowd, nan=0.0)
    
    # Convert to lists for compatibility (if needed)
    # But return as numpy arrays for better performance
    return slowk, slowd

async def calc_fastStochastics_async(low, high, close, lookback_period=14, d_period=3, k_smoothing_period=3):
    """
    Calculate Fast Stochastic Oscillator asynchronously (offloads CPU work to thread pool).
    This prevents blocking the event loop during TA-Lib calculations.
    
    Returns:
        Tuple of (K values, D values) as numpy arrays
    """
    return await asyncio.to_thread(calc_fastStochastics, low, high, close, lookback_period, d_period, k_smoothing_period)

async def calc_lrc_async(close, period, std_multiplier):
    """
    Calculate Linear Regression Channel asynchronously (offloads CPU work to thread pool).
    This prevents blocking the event loop during LRC calculations.
    
    Returns:
        Tuple of (LRL values, UCL values, LCL values, angles) as numpy arrays
    """
    return await asyncio.to_thread(linear_regression_channel_numba_sliding, close, period, std_multiplier)

# --- Main Alert Engine ---

class RedisAlertEngine:
    def __init__(self, config_tables, db=None, loglevel=1):
        """
        config_tables: dict with keys:
            - df_scan_items
            - df_scan_names
            - df_custom_indicators
            - df_conditions
            - df_HLFP
        db: database connection object (now properly handled)
        loglevel: logging level (default 1)
        """
        self.df_scan_items = config_tables['df_scan_items']
        self.df_scan_names = config_tables['df_scan_names']
        self.df_custom_indicators = config_tables['df_custom_indicators']
        self.df_conditions = config_tables['df_conditions']
        self.df_HLFP = config_tables.get('df_HLFP', pd.DataFrame())  # Safe access
        
        self.interval_to_digit = {
            '1minute': '1', '2minute': '2', '5minute': '5', '3minute': '3',
            '10minute': '10', '15minute': '15', '30minute': '30', '60minute': '60'
        }
        
        self.db = db  # Now properly assigned
        self.loglevel = loglevel
        self.today = datetime.now().strftime('%Y-%m-%d')
        
        # Validate required tables
        self._validate_config_tables()
    
    def _validate_config_tables(self):
        """Validate that required configuration tables are present and not empty"""
        required_tables = ['df_scan_items', 'df_scan_names', 'df_custom_indicators', 'df_conditions']
        for table_name in required_tables:
            table = getattr(self, table_name)
            if table.empty:
                logger.warning(f"Warning: {table_name} is empty")

    async def _fetch_1min_ohlc_data(self, redis_client, token, last_n_candles=500, fetch_all=False):
        """Fetch 1-minute OHLC data from Redis for a token.
        If fetch_all=True, returns the full history once (used for startup initialization).
        Otherwise returns the last_n_candles (default 500) for incremental updates.
        """
        try:
            sorted_set_key = f"ohlc_sorted:{token}"
            if fetch_all:
                ohlc_entries = await redis_client.zrange(sorted_set_key, 0, -1)
            else:
                ohlc_entries = await redis_client.zrange(sorted_set_key, -last_n_candles, -1)
            
            if not ohlc_entries:
                return pd.DataFrame()
            
            ohlc_list = [json.loads(entry) for entry in ohlc_entries]
            df = pd.DataFrame(ohlc_list)
            
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                logger.warning(f"Missing required columns in data for token {token}")
                return pd.DataFrame()
            
            df = df.dropna(subset=['open', 'high', 'low', 'close'])
            df = df.drop_duplicates(subset=['timestamp'], keep='last')
            
            if df.empty:
                return pd.DataFrame()
            
            # Ensure timestamps are in pandas datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
            
            # Reset index and return
            df = df.reset_index(drop=True)
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            return df
            
        except Exception as e:
            logger.error(f"[FETCH][ERROR] Failed to fetch 1-min data for token {token}: {e}")
            return pd.DataFrame()

    async def _fetch_resampled_ohlc_data(self, redis_client, symbol, interval, last_n_candles=500, fetch_all=False):
        """Fetch resampled OHLC data from Redis for a symbol/interval.
        If fetch_all=True, returns the full history once (used for startup initialization).
        Otherwise returns the last_n_candles (default 500) for incremental updates.
        """
        try:
            zset_key = f"resampled_ohlc_sorted:{symbol}:{interval}"
            # Fetch full or last-N depending on mode
            if fetch_all:
                entries = await redis_client.zrange(zset_key, 0, -1)
            else:
                entries = await redis_client.zrange(zset_key, -last_n_candles, -1)
            
            if not entries:
                return pd.DataFrame()
            
            # Parse individual entries from sorted set
            arr = [json.loads(x) for x in entries]
            if not arr:
                return pd.DataFrame()
            
            df = pd.DataFrame(arr)
            # Normalize types and column names
            if 'datetime' in df.columns:
                df['timestamp'] = pd.to_datetime(df['datetime'])
                df = df.drop(columns=['datetime'])
            elif 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Ensure required columns exist
            required_cols = ['timestamp', 'open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_cols):
                logger.warning(f"Missing required columns in resampled data for {symbol} {interval}")
                return pd.DataFrame()
            
            df = df.sort_values('timestamp')
            df = df.reset_index(drop=True)
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Add volume if missing
            if 'volume' not in df.columns:
                df['volume'] = 0
            
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            
        except Exception as e:
            logger.error(f"[FETCH][ERROR] Failed to fetch resampled data for {symbol} {interval}: {e}")
            return pd.DataFrame()

    async def process_alert(self, exchange_code, scanID, alert_timestamp, LRL_value, lrcangletype, lrcanglestart, lrcangleend, angle_degrees, crossover_index, psar_signal, candle_color, high_ha, digit_name, conditionID, redis_client,close_ha,scan_name):
        """
        Process and handle alerts based on angle type and conditions
        Enhanced with better error handling and validation
        """
        try:
            # Parse and format alert timestamp with better error handling
            alert_timestamp_str = str(alert_timestamp)
            alert_timestamp_str = alert_timestamp_str[:26]  
            
            try:
                if 'T' in alert_timestamp_str:
                    alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
                else:
                    alert_timestamp_dt = datetime.strptime(alert_timestamp_str, "%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                logger.error(f"Error parsing timestamp {alert_timestamp_str}: {e}")
                alert_timestamp_dt = datetime.now()
            
            # Determine if alert should be generated
            # NOTE: LRC angle filtering has been disabled as per latest requirements.
            # Any condition that reaches this point (RESULT: PASSED) should generate an alert.
            should_generate_alert = True
            # We intentionally ignore lrcangletype / lrcanglestart / lrcangleend here.
            
            if should_generate_alert:
                info = f"Alert {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal} color: {candle_color} high_ha: {high_ha} < LRL:{LRL_value}"
                
                logger.info(f"[process_alert] Alert should be generated for {exchange_code}. Database connection available: {self.db is not None}")
                
                if self.db:
                    module_name = 'alert custom angle' if lrcangletype == 'custom' else 'alert normal angle'

                    # ✅ FIX 2: Push alert delivery to async queue instead of blocking inline
                    # This decouples DB insert + Telegram + Redis publish from the main processing loop
                    queue = await _ensure_alert_queue()
                    await queue.put({
                        'db': self.db,
                        'redis_client': redis_client,
                        'exchange_code': exchange_code,
                        'alert_timestamp': alert_timestamp,
                        'alert_timestamp_dt': alert_timestamp_dt,
                        'scanID': scanID,
                        'digit_name': digit_name,
                        'conditionID': conditionID,
                        'close_ha': close_ha,
                        'scan_name': scan_name,
                        'info': info,
                        'module_name': module_name,
                        'today': self.today,
                    })
                    logger.info(f"[process_alert] ✅ Alert queued for async delivery: {exchange_code} {digit_name} (queue size: {queue.qsize()})")
                    db_error_message = ""
                else:
                    logger.warning(f"WARNING: No database connection available for {exchange_code}")
                    db_error_message = "No database connection available"
                
                return db_error_message if 'db_error_message' in locals() else ""
                
            else:
                # Log when alert conditions are not met
                if self.loglevel >= 1:
                    info = f"NOT {exchange_code} {alert_timestamp} K crossover: {crossover_index} psar: {psar_signal} color: {candle_color} high_ha: {high_ha} < LRL:{LRL_value}"
                    if self.db:
                        try:
                            await self.db.insert_trade_log(
                                date_log=self.today, 
                                module='checkAlerts_interval', 
                                activity='no angle', 
                                important_data=info, 
                                priority=2, 
                                strategy_trade_id='', 
                                timestamp=datetime.now()
                            )
                        except Exception as e:
                            logger.error(f"Error inserting no-alert log: {e}")
                return ""
                
        except Exception as e:
            logger.error(f"Error in process_alert: {e}")
            return f"Error in process_alert: {e}"

    async def _store_indicator_data(self, redis_client, indicator_key, timestamps, values, signals=None, values2=None, values3=None):
        """Store indicator data in Redis sorted set.
        
        Args:
            redis_client: Redis client
            indicator_key: Redis key (e.g., 'psar:RELIANCE:5minute:0.02:0.2' or 'stoch_data:RELIANCE:5minute:14:3:3')
            timestamps: Array of timestamps (pandas datetime or string)
            values: Array of indicator values (PSAR values or K for Stochastic)
            signals: Optional array of signals (for PSAR signals)
            values2: Optional second array of values (for Stochastic D values)
        
        Returns:
            Number of new entries stored
        """
        try:
            if len(timestamps) == 0 or len(values) == 0:
                return 0
            
            # Get the last (highest) score from Redis - much faster than checking each score
            last_entry = await redis_client.zrange(indicator_key, -1, -1, withscores=True)
            last_score = None
            if last_entry:
                # last_entry is a list of tuples: [(entry_json, score), ...]
                # Get the score from the last entry
                last_score = int(last_entry[0][1]) if last_entry else None
            
            # Pre-filter entries: only keep those with scores > last_score (batch filtering)
            new_entries = []
            for i, ts in enumerate(timestamps):
                try:
                    # Convert timestamp to score
                    if isinstance(ts, pd.Timestamp):
                        ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
                        ts_score = int(ts.timestamp())
                    else:
                        ts_str = str(ts)
                        ts_dt = pd.to_datetime(ts_str)
                        ts_score = int(ts_dt.timestamp())
                    
                    # Only keep entries with scores greater than the last score (or all if no last score exists)
                    if last_score is not None and ts_score <= last_score:
                        continue
                    
                    # Prepare indicator data
                    # OPTIMIZED: Add stored_at timestamp when creating data structure (no extra latency)
                    current_storage_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
                    indicator_data = {
                        'timestamp': ts_str,
                        'stored_at': current_storage_time  # Track when this entry was stored in Redis
                    }
                    
                    # Determine indicator type based on parameters
                    if signals is not None and i < len(signals):
                        # PSAR indicator: use psar_value and signal
                        indicator_data['psar_value'] = float(values[i]) if not np.isnan(values[i]) else None
                        indicator_data['signal'] = int(signals[i])
                    elif values2 is not None and i < len(values2):
                        # Stochastic indicator: use k_value and d_value
                        indicator_data['k_value'] = float(values[i]) if not np.isnan(values[i]) else None
                        indicator_data['d_value'] = float(values2[i]) if not np.isnan(values2[i]) else None
                    elif values3 is not None and i < len(values3):
                        # LRC indicator: use lrl_value, ucl_value, lcl_value
                        indicator_data['lrl_value'] = float(values[i]) if not np.isnan(values[i]) else None
                        indicator_data['ucl_value'] = float(values2[i]) if values2 is not None and i < len(values2) and not np.isnan(values2[i]) else None
                        indicator_data['lcl_value'] = float(values3[i]) if i < len(values3) and not np.isnan(values3[i]) else None
                    else:
                        # Fallback: use generic value (shouldn't happen in normal flow)
                        indicator_data['value'] = float(values[i]) if not np.isnan(values[i]) else None
                    
                    # Store entry data for batch processing
                    new_entries.append((ts_score, indicator_data))
                except Exception as e:
                    logger.error(f"[STORE_IND][ERROR] Failed to prepare indicator data for {indicator_key} at {ts}: {e}")
                    continue
            
            # Batch add all new entries at once (only if there are entries to add)
            stored_count = 0
            if new_entries:
                # First, batch check which scores already exist using zcount
                check_pipe = redis_client.pipeline()
                for ts_score, indicator_data in new_entries:
                    check_pipe.zcount(indicator_key, ts_score, ts_score)
                
                # Execute checks
                try:
                    existing_counts = await check_pipe.execute()
                except Exception as e:
                    logger.error(f"[STORE_IND][ERROR] Failed to check existing scores for {indicator_key}: {e}")
                    existing_counts = [0] * len(new_entries)  # Default to all new if check fails
                
                # Filter entries that don't exist (count == 0)
                entries_to_add = []
                for (ts_score, indicator_data), count in zip(new_entries, existing_counts):
                    if count == 0:
                        entries_to_add.append((ts_score, indicator_data))
                
                # Only add entries that don't exist
                if entries_to_add:
                    pipe = redis_client.pipeline()
                    for ts_score, indicator_data in entries_to_add:
                        pipe.zadd(indicator_key, {json.dumps(indicator_data): ts_score})
                        stored_count += 1
                    
                    # OPTIMIZED: Set TTL in same pipeline (combine operations)
                    pipe.expire(indicator_key, 30 * 24 * 60 * 60)  # 30 days in seconds
                    
                    # Execute all operations in single batch
                    await pipe.execute()
                # Removed indicator store log (high frequency)
            
            return stored_count
            
        except Exception as e:
            logger.error(f"[STORE_IND][ERROR] Failed to store indicator data for {indicator_key}: {e}")
            return 0

    async def _get_indicator_data(self, redis_client, indicator_key, timestamps=None, last_n=None):
        """Get indicator data from Redis.
        
        Args:
            redis_client: Redis client
            indicator_key: Redis key
            timestamps: Optional list of timestamps to fetch (if None, fetches all or last_n)
            last_n: Optional number of last entries to fetch (more efficient than fetching all)
        
        Returns:
            Dictionary mapping timestamp strings to indicator values
            For PSAR: {'psar_value': psar_value, 'signal': signal}
            For Stochastic: {'k_value': k_value, 'd_value': d_value}
        """
        try:
            if last_n is not None:
                # Optimized: Fetch only last N entries (much faster than individual queries)
                entries = await redis_client.zrange(indicator_key, -last_n, -1)
            elif timestamps is None:
                # Fetch all
                entries = await redis_client.zrange(indicator_key, 0, -1)
            else:
                # Optimized: Use single range query instead of N individual queries
                # Convert all timestamps to scores first
                ts_scores = []
                for ts in timestamps:
                    try:
                        if isinstance(ts, pd.Timestamp):
                            ts_score = int(ts.timestamp())
                        else:
                            ts_dt = pd.to_datetime(ts)
                            ts_score = int(ts_dt.timestamp())
                        ts_scores.append(ts_score)
                    except Exception:
                        continue
                
                if ts_scores:
                    # Get min/max score range and fetch all entries in that range in one query
                    min_score = min(ts_scores)
                    max_score = max(ts_scores)
                    entries = await redis_client.zrangebyscore(indicator_key, min_score, max_score)
                else:
                    entries = []
            
            indicator_dict = {}
            for entry in entries:
                try:
                    data = json.loads(entry)
                    ts_str = data.get('timestamp')
                    if ts_str:
                        # Support both old format (value/value2) and new format (psar_value/k_value/d_value)
                        # for backward compatibility during migration
                        # Use 'in' check to properly handle None/0 values
                        indicator_dict[ts_str] = {
                            'psar_value': data.get('psar_value') if 'psar_value' in data else data.get('value'),
                            'k_value': data.get('k_value') if 'k_value' in data else data.get('value'),
                            'd_value': data.get('d_value') if 'd_value' in data else data.get('value2'),
                            'signal': data.get('signal')  # May be None for Stochastic
                        }
                except Exception:
                    continue
            
            return indicator_dict
            
        except Exception as e:
            logger.error(f"[GET_IND][ERROR] Failed to get indicator data for {indicator_key}: {e}")
            return {}

    async def process_symbol(self, symbol, token, interval, basket_id, redis_client):
        """
        Process a symbol and check for alerts with distributed lock protection.
        Uses a single lock per (symbol, interval) to prevent race conditions in parallel processing.
        
        Args:
            symbol: Stock symbol
            token: Instrument token
            interval: Timeframe interval
            basket_id: Basket ID
            redis_client: Redis client instance
        """
        lock_key = f"lock:process:{symbol}:{interval}"
        
        try:
            # Single distributed lock protects entire process_symbol execution
            # Prevents race conditions when multiple parallel tasks process same symbol/interval
            # Increased timeout to 120 seconds to handle longer processing times (startup, full rebuilds, etc.)
            # blocking_timeout of 30 seconds allows waiting for lock if another process is using it
            async with Lock(redis_client, lock_key, timeout=120, blocking_timeout=30):
                return await self._process_symbol_unlocked(symbol, token, interval, basket_id, redis_client)
        except Exception as e:
            # Handle lock errors gracefully - distinguish between acquisition and release errors
            error_msg = str(e)
            if "no longer owned" in error_msg or "Cannot release" in error_msg:
                # Lock expired during processing - this is expected if processing takes > timeout
                # Log as warning instead of error since the processing may have completed successfully
                logger.warning(f"[LOCK][WARN] Lock expired for {symbol} {interval} (processing took >120s): {e}")
            else:
                # Actual lock acquisition failure
                logger.error(f"[LOCK][ERROR] Failed to acquire lock for {symbol} {interval}: {e}")
            return None
    
    async def _process_symbol_unlocked(self, symbol, token, interval, basket_id, redis_client):
        """
        Internal method: Process a symbol and check for alerts (without lock - lock is acquired in wrapper).
        Enhanced with better validation and error handling.
        Fetches OHLC data directly from Redis (self-contained).
        """
        try:
            logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 1: Starting process_symbol")
            
            # Validate input parameters
            if not symbol or not token or not interval:
                logger.warning(f"Invalid parameters: symbol={symbol}, token={token}, interval={interval}")
                return
                
            # Decide fetch window based on whether HA initial check has completed
            ha_state_key = f"ha_state:{symbol}:{interval}"
            ha_state = await redis_client.hgetall(ha_state_key)
            initial_check_done = ha_state.get('initial_check_done') == '1'

            fetch_all = not initial_check_done  # Full history once on startup

            # Fetch OHLC data directly from Redis (self-contained approach)
            logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 2: Fetching OHLC data (fetch_all={fetch_all})")
            if interval == '1minute':
                ohlc_df = await self._fetch_1min_ohlc_data(
                    redis_client, token,
                    last_n_candles=500,
                    fetch_all=fetch_all
                )
            else:
                ohlc_df = await self._fetch_resampled_ohlc_data(
                    redis_client, symbol, interval,
                    last_n_candles=500,
                    fetch_all=fetch_all
                )
                
            if ohlc_df is None or ohlc_df.empty:
                logger.warning(f"No data fetched for {symbol} {interval}")
                return
                
            if len(ohlc_df) < 2:
                logger.warning(f"Insufficient data for {symbol} {interval}: {len(ohlc_df)} candles (minimum 2 required)")
                return

            logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 2: Fetched {len(ohlc_df)} candles")

            # Validate required columns
            required_columns = ['open', 'high', 'low', 'close', 'timestamp']
            missing_columns = [col for col in required_columns if col not in ohlc_df.columns]
            if missing_columns:
                logger.warning(f"Missing required columns for {symbol}: {missing_columns}")
                return

            # Extract and validate OHLC data
            try:
                data_combined = ohlc_df[['open', 'high', 'low', 'close']].values.astype(float)
                dates_combined = pd.to_datetime(ohlc_df['timestamp']).values
            except Exception as e:
                logger.error(f"Error processing OHLC data for {symbol}: {e}")
                return

            # Calculate Heikin Ashi - optimized: full gap check only on startup, then incremental
            logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 3: Calculating Heikin Ashi (initial_check_done={initial_check_done})")
            # Strategy: Use incremental updates after initial check is done
            try:
                candle_data_key = f"candle_data:{symbol}:{interval}"
                last_ha_open = float(ha_state.get('last_ha_open', 0)) if ha_state.get('last_ha_open') else None
                last_ha_close = float(ha_state.get('last_ha_close', 0)) if ha_state.get('last_ha_close') else None
                last_processed_ts = ha_state.get('last_ts')
                
                # Track if we complete initial check in this execution
                initial_check_completed_this_run = False
                
                # OPTIMIZED: Check if candle_data exists - if it does, preserve it and only add missing candles
                candle_data_exists = await redis_client.exists(candle_data_key)
                
                if not initial_check_done:
                    if candle_data_exists:
                        # Candle data exists but HA state missing - preserve data, only add missing candles
                        # Get last candle to determine what's missing
                        last_candle_entry = await redis_client.zrange(candle_data_key, -1, -1)
                        if last_candle_entry:
                            try:
                                last_candle_data = json.loads(last_candle_entry[0])
                                last_existing_ts = pd.to_datetime(last_candle_data.get('timestamp'))
                                # Calculate HA for all data (needed for new candles)
                                ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(
                                    data_combined[:, 0], data_combined[:, 1], data_combined[:, 2], data_combined[:, 3]
                                )
                                ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
                                # Set HA state to enable incremental mode going forward
                                pipe = redis_client.pipeline()
                                # OPTIMIZED: Add stored_at timestamp to HA state (no extra latency)
                                current_storage_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
                                pipe.hset(ha_state_key, mapping={
                                    'initial_check_done': '1',
                                    'stored_at': current_storage_time
                                })
                                pipe.expire(ha_state_key, 30 * 24 * 60 * 60)  # 30 days in seconds
                                await pipe.execute()
                                initial_check_completed_this_run = True
                            except Exception as e:
                                logger.warning(f"[HA][INIT][WARN] Could not read last candle, will rebuild: {e}")
                                # Fall through to full rebuild
                                candle_data_exists = False
                        else:
                            candle_data_exists = False
                    
                    if not candle_data_exists:
                        # Candle data doesn't exist - full rebuild
                        try:
                            # Remove any prior candle_data for a clean rebuild
                            await redis_client.delete(candle_data_key)
                            # Clear HA state fields to avoid mixing with old state
                            try:
                                await redis_client.hdel(ha_state_key, 'last_ha_open', 'last_ha_close', 'last_ts')
                            except Exception:
                                pass
                        except Exception as _clr_err:
                            logger.warning(f"[HA][INIT] warn: could not clear previous keys for {symbol} {interval}: {_clr_err}")

                        # Calculate HA from the beginning of dataset
                        ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(
                            data_combined[:, 0], data_combined[:, 1], data_combined[:, 2], data_combined[:, 3]
                        )
                        ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
                        
                        # OPTIMIZED: Mark initial check as done and set TTL in single pipeline
                        # OPTIMIZED: Add stored_at timestamp to HA state (no extra latency)
                        current_storage_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
                        pipe = redis_client.pipeline()
                        pipe.hset(ha_state_key, mapping={
                            'initial_check_done': '1',
                            'stored_at': current_storage_time
                        })
                        pipe.expire(ha_state_key, 30 * 24 * 60 * 60)  # 30 days in seconds
                        await pipe.execute()
                        initial_check_completed_this_run = True
                else:
                    # Initial check already done - use incremental updates only (optimized)
                    
                    # Load last processed timestamp and HA values
                    if last_processed_ts and last_ha_open is not None and last_ha_close is not None:
                        last_ts = pd.to_datetime(last_processed_ts)
                        dates_pd = pd.to_datetime(dates_combined)
                        new_mask = dates_pd > last_ts
                        new_count = new_mask.sum()
                        
                        if new_count > 0 and new_count < len(data_combined):
                            # Calculate HA only for new candles incrementally
                            new_data = data_combined[new_mask]
                            new_ha_open, new_ha_high, new_ha_low, new_ha_close = heikin_ashi_incremental(
                                new_data[:, 0], new_data[:, 1], new_data[:, 2], new_data[:, 3],
                                last_ha_open, last_ha_close
                            )
                            
                            # Load existing HA from candle_data for old candles (last 500 for indicator windows)
                            existing_ha_entries = await redis_client.zrange(candle_data_key, -500, -1)
                            existing_ha_dict = {}
                            for entry in existing_ha_entries:
                                try:
                                    candle_data = json.loads(entry)
                                    ts_str = candle_data.get('timestamp')
                                    if ts_str and 'ha_open' in candle_data and candle_data['ha_open'] is not None:
                                        existing_ha_dict[ts_str] = {
                                            'ha_open': candle_data['ha_open'],
                                            'ha_high': candle_data['ha_high'],
                                            'ha_low': candle_data['ha_low'],
                                            'ha_close': candle_data['ha_close']
                                        }
                                except Exception:
                                    continue
                            
                            # Build full HA array: existing + new
                            ha_combined_list = []
                            new_idx = 0
                            for i, ts in enumerate(dates_combined):
                                ts_str = pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S')
                                if ts_str in existing_ha_dict:
                                    ha_combined_list.append([
                                        existing_ha_dict[ts_str]['ha_open'],
                                        existing_ha_dict[ts_str]['ha_high'],
                                        existing_ha_dict[ts_str]['ha_low'],
                                        existing_ha_dict[ts_str]['ha_close']
                                    ])
                                elif new_mask[i]:
                                    ha_combined_list.append([
                                        new_ha_open[new_idx],
                                        new_ha_high[new_idx],
                                        new_ha_low[new_idx],
                                        new_ha_close[new_idx]
                                    ])
                                    new_idx += 1
                                else:
                                    # Should not happen after initial check, but calculate full as safety
                                    logger.warning(f"[HA][WARN] symbol={symbol} interval={interval} gap detected at {ts_str} - recalculating full")
                                    ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(
                                        data_combined[:, 0], data_combined[:, 1], data_combined[:, 2], data_combined[:, 3]
                                    )
                                    ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
                                    break
                            else:
                                ha_combined = np.array(ha_combined_list)
                        else:
                            # All new or no state - calculate full dataset
                            ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(
                                data_combined[:, 0], data_combined[:, 1], data_combined[:, 2], data_combined[:, 3]
                            )
                            ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
                    else:
                        # No state - calculate full dataset (shouldn't happen after initial check, but safety)
                        logger.warning(f"[HA][WARN] symbol={symbol} interval={interval} no state found after initial check - calculating full")
                        ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(
                            data_combined[:, 0], data_combined[:, 1], data_combined[:, 2], data_combined[:, 3]
                        )
                        ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
                
                # Store last HA values for next incremental calculation
                last_ts_str = pd.to_datetime(dates_combined[-1]).strftime('%Y-%m-%d %H:%M:%S')
                # Build state update - preserve initial_check_done flag if it was set
                # OPTIMIZED: Add stored_at timestamp to HA state (no extra latency)
                current_storage_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
                state_update = {
                    'last_ha_open': str(ha_combined[-1, 0]),
                    'last_ha_close': str(ha_combined[-1, 3]),
                    'last_ts': last_ts_str,
                    'stored_at': current_storage_time  # Track when this state was updated in Redis
                }
                # Preserve initial_check_done flag if it exists (was set in this run or previously)
                if initial_check_done or initial_check_completed_this_run:
                    state_update['initial_check_done'] = '1'
                
                # OPTIMIZED: Combine hset and expire in single pipeline
                pipe = redis_client.pipeline()
                pipe.hset(ha_state_key, mapping=state_update)
                pipe.expire(ha_state_key, 30 * 24 * 60 * 60)  # 30 days in seconds
                await pipe.execute()
                
            except Exception as e:
                logger.error(f"[HA][ERROR] symbol={symbol} interval={interval} err={e}")
                # Fallback to full calculation
                try:
                    ha_open, ha_high, ha_low, ha_close = heikin_ashi_numpy(
                        data_combined[:, 0], data_combined[:, 1], data_combined[:, 2], data_combined[:, 3]
                    )
                    ha_combined = np.column_stack((ha_open, ha_high, ha_low, ha_close))
                except Exception as e2:
                    logger.error(f"[HA][ERROR] Fallback also failed for {symbol}: {e2}")
                    return

            # Store candle_data (OHLC + HA only) in Redis - do this ONCE per symbol/interval, not per condition
            # NOTE: Indicator values are NOT stored because they are condition-specific.
            # Each condition (cond1, cond2) can have different PSAR/Stochastic configurations.
            # Indicators are calculated on-demand during alert checks.
            try:
                candle_data_key = f"candle_data:{symbol}:{interval}"
                
                # Get last stored timestamp to only store new candles
                # OPTIMIZED: Check if candle_data exists - if it does, only add missing candles
                last_stored_entry = await redis_client.zrange(candle_data_key, -1, -1)
                last_stored_ts = None
                if last_stored_entry:
                    try:
                        last_data = json.loads(last_stored_entry[0])
                        last_stored_ts = pd.to_datetime(last_data['timestamp'])
                    except Exception:
                        pass
                elif not initial_check_done:
                    # No candle_data exists and initial_check_done=False - full storage
                    last_stored_ts = None
                
                # Batch Redis operations using pipeline for better performance
                # Collect all candles to store, then execute in single pipeline
                candles_to_store = []
                
                for idx in range(len(data_combined)):
                    try:
                        ts = pd.to_datetime(dates_combined[idx])
                        ts_score = int(ts.timestamp())
                        ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
                        
                        # Skip if already stored (unless it's the last candle which might need update)
                        # On startup, last_stored_ts is None, so all candles will be stored
                        if last_stored_ts and ts < last_stored_ts and idx < len(data_combined) - 1:
                            continue
                        
                        # Prepare candle data (OHLC + HA only - indicators are condition-specific and not stored)
                        # OPTIMIZED: Add stored_at timestamp when creating data structure (no extra latency)
                        current_storage_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
                        candle_entry = {
                            'timestamp': ts_str,
                            'symbol': symbol,
                            'interval': interval,
                            'stored_at': current_storage_time,  # Track when this entry was stored in Redis
                            # Regular OHLC
                            'open': float(data_combined[idx, 0]),
                            'high': float(data_combined[idx, 1]),
                            'low': float(data_combined[idx, 2]),
                            'close': float(data_combined[idx, 3]),
                            # Heikin Ashi OHLC (universal - same for all conditions)
                            'ha_open': float(ha_combined[idx, 0]) if idx < len(ha_combined) else 0.0,
                            'ha_high': float(ha_combined[idx, 1]) if idx < len(ha_combined) else 0.0,
                            'ha_low': float(ha_combined[idx, 2]) if idx < len(ha_combined) else 0.0,
                            'ha_close': float(ha_combined[idx, 3]) if idx < len(ha_combined) else 0.0,
                            # NOTE: Indicator values (PSAR, Stochastic) are NOT stored here
                            # because each condition (cond1, cond2) can have different indicator configurations.
                            # Indicators are calculated on-demand during alert checks.
                        }
                        
                        candles_to_store.append((ts_score, candle_entry))
                    except Exception as e:
                        logger.error(f"[STORE][ERROR] Failed to prepare candle {idx} for {symbol} {interval}: {e}")
                        continue
                
                # Batch execute all Redis operations in a single pipeline
                if candles_to_store:
                    try:
                        # First, batch check which scores already exist using zcount
                        check_pipe = redis_client.pipeline()
                        for ts_score, candle_entry in candles_to_store:
                            check_pipe.zcount(candle_data_key, ts_score, ts_score)
                        
                        # Execute checks
                        try:
                            existing_counts = await check_pipe.execute()
                        except Exception as e:
                            logger.error(f"[STORE][ERROR] Failed to check existing scores for {candle_data_key}: {e}")
                            existing_counts = [0] * len(candles_to_store)  # Default to all new if check fails
                        
                        # Filter entries that don't exist (count == 0)
                        entries_to_add = []
                        for (ts_score, candle_entry), count in zip(candles_to_store, existing_counts):
                            if count == 0:
                                entries_to_add.append((ts_score, candle_entry))
                        
                        # Only add entries that don't exist
                        if entries_to_add:
                            pipe = redis_client.pipeline()
                            for ts_score, candle_entry in entries_to_add:
                                pipe.zadd(candle_data_key, {json.dumps(candle_entry): ts_score})
                            
                            # OPTIMIZED: Set TTL in same pipeline (combine operations)
                            pipe.expire(candle_data_key, 30 * 24 * 60 * 60)  # 30 days in seconds
                            
                            # Execute all operations in single round trip
                            await pipe.execute()
          
                        
                    except Exception as e:
                        logger.error(f"[STORE][ERROR] Failed to batch store candles for {symbol} {interval}: {e}")
                        import traceback
                        logger.error(f"[STORE][ERROR] Traceback: {traceback.format_exc()}")
             
                else:
                    # Log why candles_to_store is empty (important for debugging)
                    logger.warning(f"[STORE][WARN] symbol={symbol} interval={interval} candles_to_store is empty!")
                    logger.warning(f"[STORE][WARN] last_stored_ts={last_stored_ts}, data_combined length={len(data_combined)}")

                
                # OPTIMIZED: Set TTL in same pipeline as zadd operations (already done above)
                if len(candles_to_store) == 0:
                    # Explicitly log when no candles were stored (this shouldn't happen on startup)
                    logger.warning(f"[STORE][WARN] symbol={symbol} interval={interval} NO CANDLES STORED! candles_to_store was empty")
                
            except Exception as store_err:
                logger.error(f"[STORE][ERROR] symbol={symbol} interval={interval} err={store_err}")

            logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 4: Stored candle data")

            # Get digit name and validate
            digit_name = self.interval_to_digit.get(interval, None)
            if digit_name is None:
                logger.error(f"[ALERT][ERROR] Unknown interval: {symbol} {interval}")
                return
                
            column_name = str(digit_name) + 'min'
            
            # Filter scan items
            logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 5: Filtering scan items (column={column_name})")
            try:
                # Check if column exists in DataFrame
                if column_name not in self.df_scan_items.columns:
                    logger.error(f"[ALERT][ERROR] Column '{column_name}' not found in scan_items for {symbol} {interval}. Available columns: {list(self.df_scan_items.columns)}")
                    return
                
                df_items = self.df_scan_items[self.df_scan_items[column_name] == 1]
                if basket_id is not None:
                    df_items = df_items[df_items['basket_id'] == basket_id]
                    
                if df_items.empty:
                    logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 5: No scan items enabled, skipping")
                    return
                    
                logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 5: Found {len(df_items)} scan items")
            except KeyError as e:
                logger.error(f"[ALERT][ERROR] KeyError filtering scan items for {symbol} {interval}: {e}. Column name: {column_name}")
                logger.error(f"[ALERT][ERROR] Available columns in df_scan_items: {list(self.df_scan_items.columns)}")
                return
            except Exception as e:
                logger.error(f"[ALERT][ERROR] Error filtering scan items for {symbol} {interval}: {e}")
                import traceback
                logger.error(f"[ALERT][ERROR] Traceback: {traceback.format_exc()}")
                return

            # Get conditions
            try:
                conditions = df_items.conditionID.unique()
                if len(conditions) == 0:
                    logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 6: No conditions found, skipping")
                    return
                    
                logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 6: Processing {len(conditions)} condition(s)")
            except Exception as e:
                logger.error(f"Error getting conditions for {symbol}: {e}")
                return
            
            # Process each condition
            for conditionID in conditions:
                logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 7: Processing conditionID={conditionID}")
                try:
                    condition_filtered = self.df_conditions[self.df_conditions['id'] == conditionID]
                    if len(condition_filtered) == 0:
                        continue
                    # Removed condition found log (high frequency)
                    # Process condition 1 and 2
                    for cond in [1, 2]:
                        try:
                            if cond == 1:
                                condition = condition_filtered['condition1'].iloc[0]
                                cand_type = condition_filtered['candle1'].iloc[0]
                                psarid = condition_filtered['psar1'].iloc[0]
                            elif cond == 2:
                                condition = condition_filtered['condition2'].iloc[0]
                                cand_type = condition_filtered['candle2'].iloc[0]
                                psarid = condition_filtered['psar2'].iloc[0]
                            else:
                                break

                            if condition != 1:
                                break

                            # Get condition parameters
                            kline_start = condition_filtered['kline_start'].iloc[0]
                            kline_end = condition_filtered['kline_end'].iloc[0]

                            # Get scan ID and name
                            scan_items_filtered = df_items[
                                (df_items[column_name] == 1) & 
                                (df_items['conditionID'] == conditionID)
                            ]
                            
                            if scan_items_filtered.empty:
                                continue
                            
                            # Removed scan items log (high frequency)

                            scanID = scan_items_filtered['scanID'].iloc[0]
                            scan_name_filtered = self.df_scan_names[self.df_scan_names['id'] == scanID]
                            
                            if scan_name_filtered.empty:
                                scan_name = f"Scan_{scanID}"
                            else:
                                scan_name = scan_name_filtered['name'].iloc[0]

                            # Get PSAR parameters
                            psar_filtered = self.df_custom_indicators[self.df_custom_indicators.id == psarid]
                            if psar_filtered.empty:
                                logger.warning(f"No PSAR config found for ID {psarid}")
                                continue
                                
                            psar_values = psar_filtered['value'].iloc[0]
                            try:
                                acceleration_str, max_acceleration_str = psar_values.split(',')
                                PSAR_acceleration = float(acceleration_str.strip())
                                PSAR_max_acceleration = float(max_acceleration_str)
                            except ValueError:
                                logger.warning(f"Invalid PSAR values: {psar_values}")
                                continue

                            # Get Stochastic parameters
                            stochid = condition_filtered['stochid'].iloc[0]
                            stoch_filtered = self.df_custom_indicators[self.df_custom_indicators.id == stochid]
                            if stoch_filtered.empty:
                                logger.warning(f"No Stochastic config found for ID {stochid}")
                                continue
                                
                            stoch_values = stoch_filtered['value'].iloc[0]
                            try:
                                period_str, k_avg_str, d_avg_str = stoch_values.split(',')
                                stoch_period = float(period_str)
                                k_avg = float(k_avg_str)
                                d_avg = float(d_avg_str)
                            except ValueError:
                                logger.warning(f"Invalid Stochastic values: {stoch_values}")
                                continue

                            # Get LRC parameters for filter
                            lrcid = condition_filtered['lrcid'].iloc[0] if 'lrcid' in condition_filtered.columns else None
                            lrc_filter_enabled = condition_filtered['lrc_filter_enabled'].iloc[0] if 'lrc_filter_enabled' in condition_filtered.columns else 0
                            lrc_filter_type = condition_filtered['lrc_filter_type'].iloc[0] if 'lrc_filter_type' in condition_filtered.columns else None
                            
                            # Get time filter parameters
                            time_filter_enabled = condition_filtered['time_filter_enabled'].iloc[0] if 'time_filter_enabled' in condition_filtered.columns else 0
                            time_filter_start = condition_filtered['time_filter_start'].iloc[0] if 'time_filter_start' in condition_filtered.columns else None
                            time_filter_end = condition_filtered['time_filter_end'].iloc[0] if 'time_filter_end' in condition_filtered.columns else None
                            
                            # Get angle parameters
                            lrcangletype = condition_filtered['lrcangletype'].iloc[0] if 'lrcangletype' in condition_filtered.columns else 'None'
                            lrcanglestart = condition_filtered['lrcanglestart'].iloc[0] if 'lrcanglestart' in condition_filtered.columns else 0
                            lrcangleend = condition_filtered['lrcangleend'].iloc[0] if 'lrcangleend' in condition_filtered.columns else 0
                            signaldirection = condition_filtered['signaldirection'].iloc[0]

                            # Indicator calculation strategy:
                            # OPTIMIZED: Reduced from 500 to 150 candles (3x faster, still enough for indicator warm-up)
                            # Since indicators are only used for alert checks (not stored), we don't need full history
                            # This significantly improves performance, especially at startup
                            window_size = min(250, len(data_combined))  # Optimized from 500 to 250 for better balance
                            
                            logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: Calculating indicators (window={window_size})")
                            
                            if window_size < len(data_combined):
                                # Use only last 500 candles for indicator calculation
                                data_window = data_combined[-window_size:]
                                low_window = data_window[:, 2]
                                high_window = data_window[:, 1]
                                close_window = data_window[:, 3]
                            else:
                                # Use all data if dataset is smaller than 500
                                low_window = data_combined[:, 2]
                                high_window = data_combined[:, 1]
                                close_window = data_combined[:, 3]

                            try:
                                # PSAR indicator key (using underscore for config values to avoid creating new folders)
                                psar_key = f"psar:{symbol}:{interval}:{PSAR_max_acceleration}_{PSAR_acceleration}"
                                
                                # Stochastic indicator key (single key for both K and D, using underscore for config values)
                                stoch_key = f"stoch_data:{symbol}:{interval}:{stoch_period}_{k_avg}_{d_avg}"
                                
                                # LRC indicator key (if LRC filter is enabled)
                                lrc_key = None
                                lrc_period = None
                                lrc_stdev = None
                                lrc_key_exists = False
                                
                                if lrc_filter_enabled and lrcid:
                                    lrc_filtered = self.df_custom_indicators[self.df_custom_indicators.id == lrcid]
                                    if not lrc_filtered.empty:
                                        lrc_values = lrc_filtered['value'].iloc[0]
                                        try:
                                            period_str, std_dev_str = lrc_values.split(',')
                                            lrc_period = int(float(period_str.strip()))
                                            lrc_stdev = float(std_dev_str.strip())
                                            lrc_key = f"lrc:{symbol}:{interval}:{lrc_period}_{lrc_stdev}"
                                        except (ValueError, IndexError):
                                            logger.warning(f"Invalid LRC values: {lrc_values}")
                                
                                # OPTIMIZED: Check indicator keys in single pipeline call
                                check_pipe = redis_client.pipeline()
                                check_pipe.exists(psar_key)
                                check_pipe.exists(stoch_key)
                                if lrc_key:
                                    check_pipe.exists(lrc_key)
                                check_results = await check_pipe.execute()
                                psar_key_exists = check_results[0]
                                stoch_exists = check_results[1]
                                if lrc_key:
                                    lrc_key_exists = check_results[2]
                                
                                # Handle indicators - restructured to avoid redundant fetches
                                # Initialize variables
                                psar_data = None
                                signals = None
                                K = None
                                D = None
                                LRL = None
                                UCL = None
                                LCL = None
                                dates_str = [pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in dates_combined]
                                
                                # OPTIMIZED: Handle both keys together to avoid redundant fetches
                                if psar_key_exists and stoch_exists:
                                    # Both keys exist - fetch stored data, calculate and store new indicators, then use ONLY stored data for alerts
                                    
                                    # OPTIMIZED: Fetch stored data (enough for alert checks)
                                    # Fetch enough entries to cover all timestamps we're checking (reduced from 500 to 150)
                                    last_n_indicators = min(250, len(data_combined))
                                    stored_psar = await self._get_indicator_data(redis_client, psar_key, last_n=last_n_indicators)
                                    stored_stoch = await self._get_indicator_data(redis_client, stoch_key, last_n=last_n_indicators)
                                    stored_lrc = {}
                                    if lrc_key and lrc_key_exists:
                                        stored_lrc = await self._get_indicator_data(redis_client, lrc_key, last_n=last_n_indicators)
                                    
                                    ohlc_for_ind = ohlc_df.copy()
                                    
                                    if ohlc_for_ind is None or ohlc_for_ind.empty:
                                        logger.error(f"[IND][ERROR] No OHLC data available for incremental indicator calculation")
                                    else:
                                        # Convert timestamps
                                        ohlc_for_ind['timestamp_dt'] = pd.to_datetime(ohlc_for_ind['timestamp'])
                                        timestamps_inc = ohlc_for_ind['timestamp_dt'].values
                                        
                                        # Calculate indicators on all data (store_indicator_data will skip existing scores)
                                        high_inc = ohlc_for_ind['high'].values.astype(float)
                                        low_inc = ohlc_for_ind['low'].values.astype(float)
                                        close_inc = ohlc_for_ind['close'].values.astype(float)
                                        
                                        # CRITICAL OPTIMIZATION: Calculate PSAR, Stochastic, and LRC in PARALLEL using thread pool
                                        # This prevents CPU-bound TA-Lib calculations from blocking the event loop
                                        # Expected speedup: 5-10x (CPU work runs in parallel, doesn't block other tasks)
                                        psar_task = psar_async(high_inc, low_inc, close_inc,
                                                              af0=float(PSAR_acceleration), 
                                                              af=float(PSAR_acceleration), 
                                                              max_af=float(PSAR_max_acceleration))
                                        stoch_task = calc_fastStochastics_async(low_inc, high_inc, close_inc,
                                                                               lookback_period=stoch_period, 
                                                                               d_period=d_avg, 
                                                                               k_smoothing_period=k_avg)
                                        
                                        # Calculate LRC if filter is enabled
                                        lrc_task = None
                                        if lrc_filter_enabled and lrc_period and lrc_stdev:
                                            lrc_task = calc_lrc_async(close_inc, lrc_period, lrc_stdev)
                                        
                                        # Run calculations in parallel (non-blocking)
                                        if lrc_task:
                                            psar_values_inc, (K_inc, D_inc), (LRL_inc, UCL_inc, LCL_inc, _) = await asyncio.gather(
                                                psar_task, stoch_task, lrc_task
                                            )
                                            # ✅ Directly align LRC arrays with the current OHLC window
                                            LRL = LRL_inc
                                            UCL = UCL_inc
                                            LCL = LCL_inc
                                        else:
                                            psar_values_inc, (K_inc, D_inc) = await asyncio.gather(psar_task, stoch_task)
                                        
                                        # Calculate PSAR signals (lightweight, can run in event loop)
                                        psar_signals_inc = get_psar_signals(close_inc, psar_values_inc)
                                        
                                        # Store all PSAR values (only new ones will be added, existing scores won't be overwritten)
                                        await self._store_indicator_data(redis_client, psar_key, timestamps_inc,
                                                                        psar_values_inc, signals=psar_signals_inc)
                                        
                                        # Store all Stochastic values (only new ones will be added, existing scores won't be overwritten)
                                        await self._store_indicator_data(redis_client, stoch_key, timestamps_inc, 
                                                                        K_inc, values2=D_inc)
                                        
                                        # Store LRC values if calculated
                                        if lrc_task and lrc_key:
                                            await self._store_indicator_data(redis_client, lrc_key, timestamps_inc,
                                                                            LRL_inc, values2=UCL_inc, values3=LCL_inc)
                                            stored_lrc = await self._get_indicator_data(redis_client, lrc_key, last_n=last_n_indicators)
                                            logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: LRC stored, fetched {len(stored_lrc)} entries")
                                        
                                        # CRITICAL: Re-fetch stored data after storing new values to get complete stored dataset
                                        # This ensures we use ONLY stored data (including newly stored) for alert checks
                                        stored_psar = await self._get_indicator_data(redis_client, psar_key, last_n=last_n_indicators)
                                        stored_stoch = await self._get_indicator_data(redis_client, stoch_key, last_n=last_n_indicators)
                                        
                                        # Build arrays: Use ONLY stored Redis values for alert checks (ensures consistency)
                                        # Normalize timestamps for lookup (handle format variations: '2025-11-17 12:45:00' vs '2025-11-13T13:35:00.000000000')
                                        def normalize_timestamp(ts):
                                            """Normalize timestamp to standard format for lookup"""
                                            try:
                                                # Parse and reformat to handle any input format
                                                dt = pd.to_datetime(ts)
                                                return dt.strftime('%Y-%m-%d %H:%M:%S')
                                            except Exception:
                                                return str(ts)
                                        
                                        # Create normalized lookup dictionary for stored_psar
                                        stored_psar_normalized = {}
                                        for ts_key, value in stored_psar.items():
                                            normalized_key = normalize_timestamp(ts_key)
                                            stored_psar_normalized[normalized_key] = value
                                        
                                        psar_data_list = []
                                        signals_list = []
                                        for ts in dates_str:
                                            normalized_ts = normalize_timestamp(ts)
                                            if normalized_ts in stored_psar_normalized and stored_psar_normalized[normalized_ts].get('psar_value') is not None:
                                                # Use stored value from Redis (ensures consistency with stored signal)
                                                psar_data_list.append(stored_psar_normalized[normalized_ts].get('psar_value'))
                                                signals_list.append(stored_psar_normalized[normalized_ts].get('signal', 0))
                                            else:
                                                # Timestamp not in stored data (shouldn't happen after storing, but handle gracefully)
                                                psar_data_list.append(np.nan)
                                                signals_list.append(0)
                                        
                                        psar_data = np.array(psar_data_list)
                                        signals = np.array(signals_list)
                                        
                                        # For Stochastic: Use ONLY stored Redis values for alert checks
                                        # Normalize timestamps for lookup (reuse same normalization function)
                                        stored_stoch_normalized = {}
                                        for ts_key, value in stored_stoch.items():
                                            normalized_key = normalize_timestamp(ts_key)
                                            stored_stoch_normalized[normalized_key] = value
                                        
                                        K_list = []
                                        D_list = []
                                        for ts in dates_str:
                                            normalized_ts = normalize_timestamp(ts)
                                            if normalized_ts in stored_stoch_normalized and stored_stoch_normalized[normalized_ts].get('k_value') is not None:
                                                # Use stored value from Redis (ensures consistency)
                                                K_list.append(stored_stoch_normalized[normalized_ts].get('k_value'))
                                                D_list.append(stored_stoch_normalized[normalized_ts].get('d_value'))
                                            else:
                                                # Timestamp not in stored data (shouldn't happen after storing, but handle gracefully)
                                                K_list.append(np.nan)
                                                D_list.append(np.nan)
                                        
                                        K = np.array(K_list)
                                        D = np.array(D_list)
                                        
                                        # For LRC: we now use the directly computed LRL/UCL/LCL arrays above for alerts.
                                        # Keep Redis storage only for charts/debugging; do NOT rebuild LRC from stored timestamps.
                                        if False and lrc_filter_enabled and lrc_key and stored_lrc:
                                            logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: Building LRC arrays from {len(stored_lrc)} stored entries")
                                            stored_lrc_normalized = {}
                                            for ts_key, value in stored_lrc.items():
                                                normalized_key = normalize_timestamp(ts_key)
                                                stored_lrc_normalized[normalized_key] = value
                                            
                                            LRL_list = []
                                            UCL_list = []
                                            LCL_list = []
                                            missing_timestamps = []
                                            for ts in dates_str:
                                                normalized_ts = normalize_timestamp(ts)
                                                if normalized_ts in stored_lrc_normalized:
                                                    lrc_data = stored_lrc_normalized[normalized_ts]
                                                    LRL_list.append(lrc_data.get('lrl_value') if lrc_data.get('lrl_value') is not None else np.nan)
                                                    UCL_list.append(lrc_data.get('ucl_value') if lrc_data.get('ucl_value') is not None else np.nan)
                                                    LCL_list.append(lrc_data.get('lcl_value') if lrc_data.get('lcl_value') is not None else np.nan)
                                                else:
                                                    LRL_list.append(np.nan)
                                                    UCL_list.append(np.nan)
                                                    LCL_list.append(np.nan)
                                                    missing_timestamps.append(normalized_ts)
                                            
                                            # ✅ FIX: If we have missing timestamps, recalculate LRC on-the-fly for those candles
                                            # This handles cases where new candles were added after LRC was stored
                                            if missing_timestamps and lrc_period and lrc_stdev:
                                                logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} | Step 8: {len(missing_timestamps)} timestamps missing from LRC, recalculating on-the-fly")
                                                try:
                                                    # Recalculate LRC on the full close array to get values for missing timestamps
                                                    close_for_lrc = ohlc_for_ind['close'].values.astype(float)
                                                    LRL_recalc, UCL_recalc, LCL_recalc, _ = await calc_lrc_async(close_for_lrc, lrc_period, lrc_stdev)
                                                    
                                                    # Map recalculated values back to the arrays
                                                    ohlc_timestamps_str = [pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in ohlc_for_ind['timestamp_dt'].values]
                                                    for idx, ts in enumerate(dates_str):
                                                        normalized_ts = normalize_timestamp(ts)
                                                        if normalized_ts in missing_timestamps:
                                                            # Find the index in ohlc_timestamps_str that matches
                                                            try:
                                                                ohlc_idx = ohlc_timestamps_str.index(normalized_ts)
                                                                if ohlc_idx < len(LRL_recalc) and not np.isnan(LRL_recalc[ohlc_idx]):
                                                                    LRL_list[idx] = LRL_recalc[ohlc_idx]
                                                                    UCL_list[idx] = UCL_recalc[ohlc_idx]
                                                                    LCL_list[idx] = LCL_recalc[ohlc_idx]
                                                                    logger.debug(f"[PROCESS] symbol={symbol} interval={interval} | Filled missing LRC for {normalized_ts} from recalculation")
                                                            except (ValueError, IndexError):
                                                                pass  # Timestamp not found in OHLC data
                                                except Exception as e:
                                                    logger.warning(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} | Error recalculating LRC: {e}")
                                            
                                            # Log missing timestamps for debugging (after recalculation attempt)
                                            if missing_timestamps:
                                                still_missing = [ts for idx, ts in enumerate(dates_str) if np.isnan(LRL_list[idx])]
                                                if still_missing:
                                                    logger.warning(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} | Step 8: {len(still_missing)} timestamps still missing from LRC after recalculation (first 5: {still_missing[:5]})")
                                                    # Log sample of available timestamps for comparison
                                                    available_samples = list(stored_lrc_normalized.keys())[:5]
                                                    logger.warning(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} | Step 8: Sample available LRC timestamps (first 5): {available_samples}")
                                            
                                            LRL = np.array(LRL_list)
                                            UCL = np.array(UCL_list)
                                            LCL = np.array(LCL_list)
                                        elif lrc_filter_enabled and lrc_filter_type:
                                            # LRC filter is enabled but no stored LRC data — check if already calculated in primary path
                                            if LRL is not None and UCL is not None and LCL is not None:
                                                logger.info(f"[PROCESS] LRC already calculated in primary path, skipping stored data rebuild (LRL len={len(LRL)})")
                                            else:
                                                logger.warning(f"LRC filter enabled but no LRC data available (lrc_key={lrc_key}, stored_lrc exists={stored_lrc is not None})")
                                                # Set to None so the fallback below will recalculate
                                                LRL = None
                                                UCL = None
                                                LCL = None
                                
                                else:
                                    # One or both keys don't exist - handle independently
                                    
                                    # Process PSAR indicator
                                    if not psar_key_exists:
                                        # PSAR key doesn't exist - calculate and store
                                        ohlc_for_ind = ohlc_df.copy()
                                        
                                        if ohlc_for_ind is None or ohlc_for_ind.empty:
                                            logger.error(f"[IND][ERROR] No OHLC data available for PSAR calculation")
                                        else:
                                            ohlc_for_ind['timestamp_dt'] = pd.to_datetime(ohlc_for_ind['timestamp'])
                                            timestamps_all = ohlc_for_ind['timestamp_dt'].values
                                            high_all = ohlc_for_ind['high'].values.astype(float)
                                            low_all = ohlc_for_ind['low'].values.astype(float)
                                            close_all = ohlc_for_ind['close'].values.astype(float)
                                            
                                            # OPTIMIZED: Use async version to offload CPU work
                                            psar_values_all = await psar_async(high_all, low_all, close_all, 
                                                              af0=float(PSAR_acceleration),
                                                              af=float(PSAR_acceleration),
                                                              max_af=float(PSAR_max_acceleration))
                                            psar_signals_all = get_psar_signals(close_all, psar_values_all)
                                            
                                            # Store PSAR values
                                            await self._store_indicator_data(redis_client, psar_key, timestamps_all, 
                                                                            psar_values_all, signals=psar_signals_all)
                                            
                                            # Use calculated values directly
                                            timestamps_all_str = [pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps_all]
                                            psar_dict = {ts: val for ts, val in zip(timestamps_all_str, psar_values_all)}
                                            signals_dict = {ts: sig for ts, sig in zip(timestamps_all_str, psar_signals_all)}
                                            psar_data = np.array([psar_dict.get(ts, np.nan) for ts in dates_str])
                                            signals = np.array([signals_dict.get(ts, 0) for ts in dates_str])
                                    else:
                                        # PSAR key exists - fetch stored data, calculate and store new, then use ONLY stored data
                                        ohlc_for_ind = ohlc_df.copy()
                                        
                                        if ohlc_for_ind is None or ohlc_for_ind.empty:
                                            logger.error(f"[IND][ERROR] No OHLC data available for PSAR calculation")
                                        else:
                                            ohlc_for_ind['timestamp_dt'] = pd.to_datetime(ohlc_for_ind['timestamp'])
                                            timestamps_inc = ohlc_for_ind['timestamp_dt'].values
                                            high_inc = ohlc_for_ind['high'].values.astype(float)
                                            low_inc = ohlc_for_ind['low'].values.astype(float)
                                            close_inc = ohlc_for_ind['close'].values.astype(float)
                                            
                                            # OPTIMIZED: Use async version to offload CPU work
                                            psar_values_inc = await psar_async(high_inc, low_inc, close_inc,
                                                                  af0=float(PSAR_acceleration),
                                                                  af=float(PSAR_acceleration),
                                                                  max_af=float(PSAR_max_acceleration))
                                            psar_signals_inc = get_psar_signals(close_inc, psar_values_inc)
                                            await self._store_indicator_data(redis_client, psar_key, timestamps_inc,
                                                                            psar_values_inc, signals=psar_signals_inc)
                                        
                                        # CRITICAL: Fetch stored data after storing to use ONLY stored values for alerts (reduced from 500 to 150)
                                        last_n_indicators = min(250, len(data_combined))
                                        stored_psar = await self._get_indicator_data(redis_client, psar_key, last_n=last_n_indicators)
                                        
                                        # Normalize timestamps for lookup (handle format variations)
                                        def normalize_timestamp(ts):
                                            """Normalize timestamp to standard format for lookup"""
                                            try:
                                                dt = pd.to_datetime(ts)
                                                return dt.strftime('%Y-%m-%d %H:%M:%S')
                                            except Exception:
                                                return str(ts)
                                        
                                        # Create normalized lookup dictionary
                                        stored_psar_normalized = {}
                                        for ts_key, value in stored_psar.items():
                                            normalized_key = normalize_timestamp(ts_key)
                                            stored_psar_normalized[normalized_key] = value
                                        
                                        psar_data = np.array([stored_psar_normalized.get(normalize_timestamp(ts), {}).get('psar_value', np.nan) for ts in dates_str])
                                        signals = np.array([stored_psar_normalized.get(normalize_timestamp(ts), {}).get('signal', 0) for ts in dates_str])
                                    
                                    # Process Stochastic indicator
                                    if not stoch_exists:
                                        # Stochastic key doesn't exist - calculate and store
                                        ohlc_for_ind = ohlc_df.copy()
                                        
                                        if ohlc_for_ind is None or ohlc_for_ind.empty:
                                            logger.error(f"[IND][ERROR] No OHLC data available for Stochastic calculation")
                                        else:
                                            ohlc_for_ind['timestamp_dt'] = pd.to_datetime(ohlc_for_ind['timestamp'])
                                            timestamps_all = ohlc_for_ind['timestamp_dt'].values
                                            high_all = ohlc_for_ind['high'].values.astype(float)
                                            low_all = ohlc_for_ind['low'].values.astype(float)
                                            close_all = ohlc_for_ind['close'].values.astype(float)
                                            
                                            # OPTIMIZED: Use async version to offload CPU work
                                            K_all, D_all = await calc_fastStochastics_async(low_all, high_all, close_all, 
                                                                           lookback_period=stoch_period,
                                                                           d_period=d_avg,
                                                                           k_smoothing_period=k_avg)
                                            
                                            # Store both K and D values in single key
                                            await self._store_indicator_data(redis_client, stoch_key, timestamps_all, K_all, values2=D_all)
                                            
                                            # Use calculated values directly
                                            timestamps_all_str = [pd.to_datetime(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps_all]
                                            k_dict = {ts: val for ts, val in zip(timestamps_all_str, K_all)}
                                            d_dict = {ts: val for ts, val in zip(timestamps_all_str, D_all)}
                                            K = np.array([k_dict.get(ts, np.nan) for ts in dates_str])
                                            D = np.array([d_dict.get(ts, np.nan) for ts in dates_str])
                                    else:
                                        # Stochastic key exists - fetch stored data, calculate and store new, then use ONLY stored data
                                        ohlc_for_ind = ohlc_df.copy()
                                        
                                        if ohlc_for_ind is None or ohlc_for_ind.empty:
                                            logger.error(f"[IND][ERROR] No OHLC data available for Stochastic calculation")
                                        else:
                                            ohlc_for_ind['timestamp_dt'] = pd.to_datetime(ohlc_for_ind['timestamp'])
                                            timestamps_inc = ohlc_for_ind['timestamp_dt'].values
                                            high_inc = ohlc_for_ind['high'].values.astype(float)
                                            low_inc = ohlc_for_ind['low'].values.astype(float)
                                            close_inc = ohlc_for_ind['close'].values.astype(float)
                                            
                                            # OPTIMIZED: Use async version to offload CPU work
                                            K_inc, D_inc = await calc_fastStochastics_async(low_inc, high_inc, close_inc,
                                                                               lookback_period=stoch_period,
                                                                               d_period=d_avg,
                                                                               k_smoothing_period=k_avg)
                                            await self._store_indicator_data(redis_client, stoch_key, timestamps_inc, 
                                                                            K_inc, values2=D_inc)
                                        
                                        # CRITICAL: Fetch stored data after storing to use ONLY stored values for alerts (reduced from 500 to 150)
                                        last_n_indicators = min(250, len(data_combined))
                                        stored_stoch = await self._get_indicator_data(redis_client, stoch_key, last_n=last_n_indicators)
                                        
                                        # Normalize timestamps for lookup (handle format variations)
                                        def normalize_timestamp(ts):
                                            """Normalize timestamp to standard format for lookup"""
                                            try:
                                                dt = pd.to_datetime(ts)
                                                return dt.strftime('%Y-%m-%d %H:%M:%S')
                                            except Exception:
                                                return str(ts)
                                        
                                        # Create normalized lookup dictionary
                                        stored_stoch_normalized = {}
                                        for ts_key, value in stored_stoch.items():
                                            normalized_key = normalize_timestamp(ts_key)
                                            stored_stoch_normalized[normalized_key] = value
                                        
                                        K = np.array([stored_stoch_normalized.get(normalize_timestamp(ts), {}).get('k_value', np.nan) for ts in dates_str])
                                        D = np.array([stored_stoch_normalized.get(normalize_timestamp(ts), {}).get('d_value', np.nan) for ts in dates_str])
                                
                                # Calculate PSAR on optimized window (fallback if keys don't exist but we need values)
                                # OPTIMIZED: Use async version and calculate in parallel with Stochastic
                                if psar_data is None:
                                    psar_task = psar_async(high_window, low_window, close_window, af0=float(PSAR_acceleration), af=float(PSAR_acceleration), max_af=float(PSAR_max_acceleration))
                                    
                                    # Calculate Stochastic in parallel if needed
                                    if K is None or D is None:
                                        stoch_task = calc_fastStochastics_async(low_window, high_window, close_window, lookback_period=stoch_period, d_period=d_avg, k_smoothing_period=k_avg)
                                        psar_data_window, (K_window, D_window) = await asyncio.gather(psar_task, stoch_task)
                                    else:
                                        psar_data_window = await psar_task
                                    
                                    signals_window = get_psar_signals(close_window, psar_data_window)
                                    
                                    # Pad arrays to match full dataset length
                                    if window_size < len(data_combined):
                                        padding_size = len(data_combined) - window_size
                                        psar_data = np.concatenate([np.full(padding_size, np.nan), psar_data_window])
                                        # ✅ FIX: Recalculate signals on full dataset instead of padding with zeros
                                        # Get full close prices for signal calculation
                                        close_full = data_combined[:, 3]  # Close prices from full dataset
                                        signals = get_psar_signals(close_full, psar_data)
                                    else:
                                        psar_data = psar_data_window
                                        signals = signals_window
                                
                                # Calculate Stochastic on optimized window (fallback if keys don't exist)
                                if K is None or D is None:
                                    # Already calculated above in parallel, or calculate now if PSAR was already done
                                    if 'K_window' not in locals():
                                        K_window, D_window = await calc_fastStochastics_async(low_window, high_window, close_window, lookback_period=stoch_period, d_period=d_avg, k_smoothing_period=k_avg)
                                    
                                    # Pad arrays to match full dataset length
                                    if window_size < len(data_combined):
                                        padding_size = len(data_combined) - window_size
                                        K = np.concatenate([np.full(padding_size, np.nan), K_window])
                                        D = np.concatenate([np.full(padding_size, np.nan), D_window])
                                    else:
                                        K = K_window
                                        D = D_window
                                
                                # Calculate LRC if filter is enabled but LRC data is not available (fallback path)
                                if lrc_filter_enabled and lrc_filter_type and (LRL is None or UCL is None or LCL is None):
                                    if lrc_period and lrc_stdev and lrc_key:
                                        try:
                                            logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: Calculating LRC (fallback path) period={lrc_period}, stdev={lrc_stdev}")
                                            
                                            # ✅ FIX: Use direct calculation approach (same as Path 1) to avoid timestamp mismatches
                                            ohlc_for_lrc = ohlc_df.copy()
                                            if ohlc_for_lrc is not None and not ohlc_for_lrc.empty:
                                                ohlc_for_lrc['timestamp_dt'] = pd.to_datetime(ohlc_for_lrc['timestamp'])
                                                timestamps_lrc = ohlc_for_lrc['timestamp_dt'].values
                                                close_lrc = ohlc_for_lrc['close'].values.astype(float)
                                                
                                                # Calculate LRC on full dataset for both storage AND alerts
                                                LRL_full, UCL_full, LCL_full, _ = await calc_lrc_async(close_lrc, lrc_period, lrc_stdev)
                                                
                                                # Store LRC values in Redis (for charts/debugging)
                                                await self._store_indicator_data(redis_client, lrc_key, timestamps_lrc,
                                                                                LRL_full, values2=UCL_full, values3=LCL_full)
                                                
                                                # ✅ CRITICAL FIX: Use calculated arrays directly for alerts (no Redis lookup)
                                                # This ensures 1:1 alignment with OHLC data and eliminates timestamp mismatch NaN issues
                                                LRL = LRL_full
                                                UCL = UCL_full
                                                LCL = LCL_full
                                                
                                                logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: LRC calculated and stored (fallback path - direct assignment)")
                                            else:
                                                # Calculate LRC on window only
                                                LRL_window, UCL_window, LCL_window, _ = await calc_lrc_async(close_window, lrc_period, lrc_stdev)
                                                
                                                # Pad arrays to match full dataset length
                                                if window_size < len(data_combined):
                                                    padding_size = len(data_combined) - window_size
                                                    LRL = np.concatenate([np.full(padding_size, np.nan), LRL_window])
                                                    UCL = np.concatenate([np.full(padding_size, np.nan), UCL_window])
                                                    LCL = np.concatenate([np.full(padding_size, np.nan), LCL_window])
                                                else:
                                                    LRL = LRL_window
                                                    UCL = UCL_window
                                                    LCL = LCL_window
                                        except Exception as e:
                                            logger.error(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: Error calculating LRC (fallback): {e}")
                                            import traceback
                                            logger.error(f"[PROCESS] LRC fallback traceback: {traceback.format_exc()}")
                                    else:
                                        logger.warning(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: LRC filter enabled but lrc_period={lrc_period}, lrc_stdev={lrc_stdev}, lrc_key={lrc_key}")
                                
                                # Align indicator arrays with data_combined for alert checks
                                # If we have full data from Redis, we need to align with the window we're checking
                                if psar_data is not None and len(psar_data) < len(data_combined):
                                    # Need to pad to match data_combined length
                                    logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: Padding PSAR data to match data_combined length")
                                    padding_size = len(data_combined) - len(psar_data)
                                    psar_data = np.concatenate([np.full(padding_size, np.nan), psar_data])
                                    # ✅ FIX: Recalculate signals on full dataset instead of padding with zeros
                                    close_full = data_combined[:, 3]  # Close prices from full dataset
                                    signals = get_psar_signals(close_full, psar_data)
                                
                                if K is not None and len(K) < len(data_combined):
                                    logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: Padding K data to match data_combined length")
                                    padding_size = len(data_combined) - len(K)
                                    K = np.concatenate([np.full(padding_size, np.nan), K])
                                    D = np.concatenate([np.full(padding_size, np.nan), D])
                                
                                # Log successful completion of Step 8
                                logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8: Indicators calculated successfully (PSAR={psar_data is not None}, K={K is not None}, D={D is not None})")
                                
                            except Exception as e:
                                logger.error(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 8 ERROR: {e}")
                                import traceback
                                logger.error(f"[PROCESS] Step 8 Traceback: {traceback.format_exc()}")
                                continue

                            # Initialize values
                            LRL_value = angle_degrees = crossover_index = 0

                            # CRITICAL FIX: Track which candles have been checked for alerts to prevent missing alerts
                            # Use Redis to track checked candle timestamps per (symbol, interval, conditionID, cond)
                            alert_check_key = f"alert_checked:{symbol}:{interval}:{conditionID}:{cond}"
                            
                            # Get the last checked timestamp from Redis
                            last_checked_ts_str = await redis_client.hget(alert_check_key, 'last_checked_ts')
                            last_checked_ts = None
                            if last_checked_ts_str:
                                try:
                                    last_checked_ts = pd.to_datetime(last_checked_ts_str)
                                except Exception:
                                    pass
                            
                            # Determine which candles need to be checked (all candles after last_checked_ts)
                            # CRITICAL: Only check candles within reasonable time window to avoid generating alerts for very old candles
                            # Only check candles from the last 1 hour (configurable)
                            MAX_ALERT_AGE_HOURS = 1  # Don't generate alerts for candles older than 1 hour
                            
                            # Helper function to normalize timestamps to timezone-naive for safe comparison
                            def normalize_to_naive(ts):
                                """Convert timestamp to timezone-naive for safe comparison"""
                                if ts is None:
                                    return None
                                if isinstance(ts, pd.Timestamp):
                                    return ts.tz_localize(None) if ts.tz is not None else ts
                                elif hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
                                    return ts.replace(tzinfo=None)
                                return ts
                            
                            # Get current time and create cutoff (ensure both are timezone-naive)
                            current_time = datetime.now(IST)
                            current_time_naive = current_time.replace(tzinfo=None)
                            max_age_cutoff = normalize_to_naive(current_time_naive - pd.Timedelta(hours=MAX_ALERT_AGE_HOURS))
                            
                            candles_to_check_indices = []
                            dates_pd = pd.to_datetime(dates_combined)
                            # Ensure dates_pd is timezone-naive for comparison (data timestamps are timezone-naive)
                            if isinstance(dates_pd, pd.DatetimeIndex):
                                if dates_pd.tz is not None:
                                    dates_pd = dates_pd.tz_localize(None)
                            elif hasattr(dates_pd, 'tz') and dates_pd.tz is not None:
                                dates_pd = dates_pd.tz_localize(None)
                            
                            if last_checked_ts is None:
                                # First time checking - only check recent candles (last 1 hour, max 100 candles)
                                # This prevents generating alerts for very old historical data
                                for i in range(len(dates_pd)):
                                    # DatetimeIndex is directly indexable, no need for .iloc
                                    candle_ts_raw = dates_pd[i]
                                    candle_ts = normalize_to_naive(candle_ts_raw)
                                    
                                    # Safe comparison (both are now timezone-naive)
                                    if candle_ts >= max_age_cutoff:
                                        candles_to_check_indices.append(i)
                                    
                                    # Limit to last 100 candles even if within time window (performance)
                                    if len(candles_to_check_indices) >= 100:
                                        break
                                
                                if candles_to_check_indices:
                                    logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 9: First check - checking {len(candles_to_check_indices)} recent candles (within last {MAX_ALERT_AGE_HOURS} hours)")
                                else:
                                    logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 9: First check - no recent candles to check (all candles older than {MAX_ALERT_AGE_HOURS} hours)")
                            else:
                                # Check candles after last_checked_ts, but only if they're within the time window
                                # Normalize last_checked_ts to timezone-naive
                                last_checked_ts = normalize_to_naive(last_checked_ts)
                                
                                # ✅ CRITICAL FIX: Always check the most recent closed candle to avoid delays
                                # Find the most recent candle first
                                most_recent_candle_ts = None
                                most_recent_candle_idx = None
                                
                                for i in range(len(dates_pd)):
                                    candle_ts_raw = dates_pd[i]
                                    candle_ts = normalize_to_naive(candle_ts_raw)
                                    
                                    # Track the most recent candle
                                    if most_recent_candle_ts is None or candle_ts > most_recent_candle_ts:
                                        most_recent_candle_ts = candle_ts
                                        most_recent_candle_idx = i
                                
                                # ✅ ALWAYS check the most recent candle if it's within the last 10 minutes
                                # This ensures immediate alert delivery regardless of last_checked_ts
                                # We use 10 minutes to cover all timeframes (1min, 5min, 15min, etc.)
                                recent_candle_window_minutes = 10  # Check most recent candle if within last 10 minutes
                                if most_recent_candle_idx is not None and most_recent_candle_ts is not None:
                                    time_since_most_recent = (current_time_naive - most_recent_candle_ts).total_seconds() / 60  # minutes
                                    
                                    if time_since_most_recent <= recent_candle_window_minutes and most_recent_candle_ts >= max_age_cutoff:
                                        candles_to_check_indices.append(most_recent_candle_idx)
                                        logger.info(f"[PROCESS] symbol={symbol} interval={interval} | ✅ Always checking most recent candle {most_recent_candle_ts} (age: {time_since_most_recent:.1f}min, last_checked: {last_checked_ts})")
                                
                                # Also check any candles > last_checked_ts (to catch any we might have missed)
                                # Note: We use > (not >=) to avoid re-checking the same candle unnecessarily
                                # But the most recent candle is always checked above regardless
                                for i in range(len(dates_pd)):
                                    if i in candles_to_check_indices:
                                        continue  # Skip if already added
                                        
                                    candle_ts_raw = dates_pd[i]
                                    candle_ts = normalize_to_naive(candle_ts_raw)
                                    
                                    # Check if: 1) > last_checked_ts AND 2) within time window
                                    if candle_ts > last_checked_ts and candle_ts >= max_age_cutoff:
                                        candles_to_check_indices.append(i)
                                
                                # Sort indices to process in chronological order
                                candles_to_check_indices = sorted(set(candles_to_check_indices))
                                
                                if candles_to_check_indices:
                                    oldest_ts = dates_pd[candles_to_check_indices[0]] if candles_to_check_indices else None
                                    newest_ts = dates_pd[candles_to_check_indices[-1]] if candles_to_check_indices else None
                                    logger.info(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 9: Checking {len(candles_to_check_indices)} new candles (last checked: {last_checked_ts}, range: {oldest_ts} to {newest_ts})")
                                else:
                                    logger.debug(f"[PROCESS] symbol={symbol} interval={interval} conditionID={conditionID} cond={cond} | Step 9: No new candles to check (last checked: {last_checked_ts})")
                            
                            # Track the latest timestamp we'll check (for updating last_checked_ts)
                            latest_checked_ts = last_checked_ts
                            candles_processed = 0
                            
                            # Find the most recent candle timestamp (for buffer calculation)
                            most_recent_candle_in_data = None
                            if len(dates_combined) > 0:
                                most_recent_candle_in_data = pd.to_datetime(dates_combined[-1])
                            
                            # Check each unprocessed candle
                            for i in candles_to_check_indices:
                                try:
                                    # Bounds check
                                    if i >= len(ha_combined) or i < 0:
                                        logger.warning(f"[ALERT_CHECK] Index {i} out of bounds for {symbol} {interval} (len={len(ha_combined)})")
                                        continue
                                    
                                    # ✅ FIX 3: Verify signals array alignment before using
                                    if signals is None or len(signals) == 0:
                                        logger.warning(f"[ALERT_CHECK] Signals array is empty for {symbol} {interval}")
                                        continue
                                    
                                    if i >= len(signals) or i < 0:
                                        logger.warning(f"[ALERT_CHECK] Signal index {i} out of bounds for {symbol} {interval} (len={len(signals)})")
                                        continue
                                    
                                    # Verify that signals array matches data_combined length
                                    if len(signals) != len(data_combined):
                                        logger.warning(
                                            f"[ALERT_CHECK] Signal array length mismatch: signals={len(signals)}, "
                                            f"data_combined={len(data_combined)} for {symbol} {interval}"
                                        )
                                        # Recalculate signals on full dataset as fallback
                                        close_full = data_combined[:, 3]
                                        signals = get_psar_signals(close_full, psar_data)
                                        
                                    open_ha = ha_combined[i, 0]
                                    high_ha = ha_combined[i, 1]
                                    low_ha = ha_combined[i, 2]
                                    close_ha = ha_combined[i, 3]

                                    psar_signal = signals[i]
                                    
                                    # Update latest_checked_ts to track progress
                                    current_candle_ts = pd.to_datetime(dates_combined[i])
                                    if latest_checked_ts is None or current_candle_ts > latest_checked_ts:
                                        latest_checked_ts = current_candle_ts
                                    
                                    candles_processed += 1

                                    # ---------------------------------------------------------------------------
                                    # Prepare data for this row
                                    row_data = [
                                        pd.to_datetime(dates_combined[i]),         # timestamp
                                        data_combined[i,0], data_combined[i,1], data_combined[i,2], data_combined[i,3],    # open, high, low, close
                                        ha_combined[i,0], ha_combined[i,1], ha_combined[i,2], ha_combined[i,3],            # ha_open, ha_high, ha_low, ha_close
                                        psar_data[i], psar_signal, K[i]            # psar value, psar signal, stochastic K
                                    ]
                                    columns = [
                                        "timestamp", "open", "high", "low", "close",
                                        "ha_open", "ha_high", "ha_low", "ha_close",
                                        "psar", "psar_signal", "stochastic_k"
                                    ]

                                    # Database saving moved to main-consumer.py for better architecture
                                    # ---------------------------------------------------------------------------
    
                                    # Removed K value log (high frequency - called for every candle check)
                                    
                                    # Prepare alert timestamp
                                    alert_timestamp = pd.to_datetime(dates_combined[i])
                                    alert_timestamp_str = alert_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                                    
                                    # Extract indicator values with proper NaN/None handling
                                    def safe_extract(value):
                                        """Safely extract float value, returning None for NaN/None/invalid"""
                                        if value is None:
                                            return None
                                        try:
                                            val_float = float(value)
                                            if np.isnan(val_float):
                                                return None
                                            return val_float
                                        except (TypeError, ValueError, IndexError):
                                            return None
                                    
                                    psar_value = safe_extract(psar_data[i])
                                    stoch_k = safe_extract(K[i])
                                    stoch_d = safe_extract(D[i])
                                    
                                    # Helper function to safely format values
                                    def safe_format(value, default='N/A'):
                                        if value is None:
                                            return default
                                        try:
                                            if isinstance(value, (float, np.floating)) and np.isnan(value):
                                                return default
                                            return f"{float(value):.2f}"
                                        except (TypeError, ValueError):
                                            return default
                                    
                                    # Detailed alert check logging with all values
                                    failure_reasons = []
                                    
                                    # Check time filter (exclude alerts in specified time range)
                                    if time_filter_enabled and time_filter_start and time_filter_end:
                                        try:
                                            alert_time = alert_timestamp.time()
                                            # Parse time strings (format: "HH:MM" or "HH:MM:SS")
                                            # Handle multiple input types: string, time, Timedelta, pandas Timedelta
                                            def parse_time(t_val):
                                                if isinstance(t_val, str):
                                                    parts = t_val.split(':')
                                                    return dt_time(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)
                                                elif isinstance(t_val, dt_time):
                                                    return t_val
                                                elif hasattr(t_val, 'total_seconds'):  # Timedelta object
                                                    # Convert Timedelta to time (assumes it's a time offset)
                                                    total_seconds = int(t_val.total_seconds())
                                                    hours = (total_seconds // 3600) % 24
                                                    minutes = (total_seconds % 3600) // 60
                                                    seconds = total_seconds % 60
                                                    return dt_time(hours, minutes, seconds)
                                                elif pd.notna(t_val) and t_val is not None:
                                                    # Try to convert to string first, then parse
                                                    return parse_time(str(t_val))
                                                return None
                                            
                                            start_time = parse_time(time_filter_start)
                                            end_time = parse_time(time_filter_end)
                                            
                                            # Skip if parsing failed
                                            if start_time is None or end_time is None:
                                                logger.warning(f"Could not parse time filter values: start={time_filter_start}, end={time_filter_end}")
                                                # Continue without time filter check
                                            else:
                                                # Check if alert time falls within excluded range
                                                if start_time == end_time:
                                                    # Same time: exclude alerts at exact time
                                                    if alert_time == start_time:
                                                        failure_reasons.append(f"Alert time {alert_time} matches excluded time {start_time}")
                                                        result_status = "REJECTED"
                                                        result_reason = failure_reasons[-1]
                                                        logger.info(
                                                            f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                            f"RESULT: {result_status} | REASON: {result_reason}"
                                                        )
                                                        continue
                                                else:
                                                    # Time range: exclude alerts within range
                                                    if start_time <= end_time:
                                                        # Normal range (e.g., 10:00 to 11:30)
                                                        if start_time <= alert_time <= end_time:
                                                            failure_reasons.append(f"Alert time {alert_time} falls within excluded range {start_time}-{end_time}")
                                                            result_status = "REJECTED"
                                                            result_reason = failure_reasons[-1]
                                                            logger.info(
                                                                f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                                f"RESULT: {result_status} | REASON: {result_reason}"
                                                            )
                                                            continue
                                                    else:
                                                        # Wraps around midnight (shouldn't happen for market hours 9:15-15:30)
                                                        if alert_time >= start_time or alert_time <= end_time:
                                                            failure_reasons.append(f"Alert time {alert_time} falls within excluded range {start_time}-{end_time}")
                                                            result_status = "REJECTED"
                                                            result_reason = failure_reasons[-1]
                                                            logger.info(
                                                                f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                                f"RESULT: {result_status} | REASON: {result_reason}"
                                                            )
                                                            continue
                                        except Exception as e:
                                            logger.warning(f"Error checking time filter: {e}")
                                            import traceback
                                            logger.warning(f"Time filter error traceback: {traceback.format_exc()}")
                                    
                                    # Check LRC filter (high must be below middle_LRC or lower_LRC)
                                    if lrc_filter_enabled and lrc_filter_type:
                                        # Only check if LRC arrays are available and not None
                                        if LRL is not None and UCL is not None and LCL is not None:
                                            if i < len(LRL) and i < len(UCL) and i < len(LCL):
                                                lrl_value = LRL[i]
                                                ucl_value = UCL[i]
                                                lcl_value = LCL[i]
                                                
                                                # ✅ FIX: Check if we have enough candles for LRC calculation
                                                # LRC needs at least 'lrc_period' candles, so first (lrc_period - 1) values will be NaN
                                                # Only reject if NaN occurs when we should have valid data
                                                min_required_index = (lrc_period - 1) if (lrc_period and lrc_period > 0) else 0
                                                
                                                if i < min_required_index:
                                                    # This is expected - not enough candles yet for LRC calculation
                                                    logger.debug(
                                                        f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                        f"LRC not available yet (index {i} < required {min_required_index}, need {lrc_period} candles) - skipping LRC filter check"
                                                    )
                                                    # Continue without LRC filter check (allow alert to proceed)
                                                elif np.isnan(lrl_value) or np.isnan(ucl_value) or np.isnan(lcl_value):
                                                    # NaN when we should have valid data - REJECT ALERT (strict enforcement)
                                                    # This means LRC data is unavailable even after recalculation attempts
                                                    failure_reasons.append("LRC values are NaN (data unavailable or calculation failed)")
                                                    result_status = "REJECTED"
                                                    result_reason = failure_reasons[-1]
                                                    logger.error(
                                                        f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                        f"LRC values are NaN at index {i} (array length={len(LRL)}, min_required_index={min_required_index}, lrc_period={lrc_period}) | "
                                                        f"Timestamp lookup: alert_ts='{alert_timestamp_str}', dates_str[{i}]='{dates_str[i] if i < len(dates_str) else 'OUT_OF_RANGE'}' | "
                                                        f"RESULT: {result_status} | REASON: {result_reason}"
                                                    )
                                                    continue  # Reject alert - LRC must be precise
                                                else:
                                                    # Valid LRC values - perform filter check
                                                    middle_lrc = (ucl_value + lcl_value) / 2  # Middle LRC line
                                                    
                                                    if lrc_filter_type == 'middle' or lrc_filter_type == 1:
                                                        # Check: high < middle_LRC
                                                        if not (high_ha < middle_lrc):
                                                            failure_reasons.append(f"High {high_ha:.2f} not below middle LRC {middle_lrc:.2f}")
                                                            result_status = "REJECTED"
                                                            result_reason = failure_reasons[-1]
                                                            logger.info(
                                                                f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                                f"High_HA={high_ha:.2f} Middle_LRC={middle_lrc:.2f} | "
                                                                f"RESULT: {result_status} | REASON: {result_reason}"
                                                            )
                                                            continue
                                                    elif lrc_filter_type == 'lower' or lrc_filter_type == 2:
                                                        # Check: high < lower_LRC
                                                        if not (high_ha < lcl_value):
                                                            failure_reasons.append(f"High {high_ha:.2f} not below lower LRC {lcl_value:.2f}")
                                                            result_status = "REJECTED"
                                                            result_reason = failure_reasons[-1]
                                                            logger.info(
                                                                f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                                f"High_HA={high_ha:.2f} Lower_LRC={lcl_value:.2f} | "
                                                                f"RESULT: {result_status} | REASON: {result_reason}"
                                                            )
                                                            continue
                                            else:
                                                failure_reasons.append("LRC data index out of range")
                                                result_status = "REJECTED"
                                                result_reason = failure_reasons[-1]
                                                logger.warning(
                                                    f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                    f"RESULT: {result_status} | REASON: {result_reason}"
                                                )
                                                continue
                                        else:
                                            # LRC filter enabled but no LRC data available - skip filter check (don't reject alert)
                                            logger.warning(
                                                f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                f"LRC filter enabled but LRC data not available (LRL={LRL is not None}, UCL={UCL is not None}, LCL={LCL is not None}) - skipping LRC filter check"
                                            )
                                            # Continue without LRC filter check (allow alert to proceed)
                                    
                                    # Check if K is within range (skip if K is NaN/None)
                                    if stoch_k is None:
                                        failure_reasons.append(f"K value is NaN/None (cannot check range)")
                                        result_status = "REJECTED"
                                        result_reason = failure_reasons[-1]
                                        logger.info(
                                            f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                            f"PSAR_value={safe_format(psar_value)} PSAR_signal={psar_signal} | "
                                            f"K={safe_format(stoch_k)} D={safe_format(stoch_d)} | "
                                            f"RESULT: {result_status} | "
                                            f"REASON: {result_reason}"
                                        )
                                        continue
                                    
                                    # Now stoch_k is guaranteed to be a valid float (not None, not NaN)
                                    k_in_range = kline_start < stoch_k < kline_end
                                    if not k_in_range:
                                        failure_reasons.append(f"K value {safe_format(stoch_k)} not in range [{kline_start}, {kline_end}]")
                                        result_status = "REJECTED"
                                        result_reason = failure_reasons[-1]
                                        logger.info(
                                            f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                            f"PSAR_value={safe_format(psar_value)} PSAR_signal={psar_signal} | "
                                            f"K={safe_format(stoch_k)} D={safe_format(stoch_d)} | "
                                            f"RESULT: {result_status} | "
                                            f"REASON: {result_reason}"
                                        )
                                        continue

                                    # Check PSAR signal direction with defensive checks
                                    # Coerce to int to avoid numpy type comparison issues
                                    psar_signal_int = int(psar_signal) if not np.isnan(psar_signal) else 0
                                    signaldirection_int = int(signaldirection)

                                    # ✅ FIX 1: Defensive check - reject if signal is 0 when direction requires crossover
                                    if psar_signal_int == 0 and signaldirection_int != 0:
                                        failure_reasons.append(f"PSAR signal is 0 (no crossover) but required direction is {signaldirection_int}")
                                        result_status = "REJECTED"
                                        result_reason = failure_reasons[-1]
                                        logger.info(
                                            f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                            f"PSAR_value={safe_format(psar_value)} PSAR_signal={psar_signal_int} (coerced) | "
                                            f"K={safe_format(stoch_k)} D={safe_format(stoch_d)} | "
                                            f"RESULT: {result_status} | "
                                            f"REASON: {result_reason}"
                                        )
                                        continue

                                    # ✅ FIX 2: Explicit type-coerced comparison
                                    psar_signal_match = psar_signal_int == signaldirection_int
                                    if not psar_signal_match:
                                        failure_reasons.append(f"PSAR signal {psar_signal_int} does not match required direction {signaldirection_int}")
                                        result_status = "REJECTED"
                                        result_reason = failure_reasons[-1]
                                        logger.info(
                                            f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                            f"PSAR_value={safe_format(psar_value)} PSAR_signal={psar_signal_int} (coerced) | "
                                            f"K={safe_format(stoch_k)} D={safe_format(stoch_d)} | "
                                            f"RESULT: {result_status} | "
                                            f"REASON: {result_reason}"
                                        )
                                        continue

                                    candle_color = 'green' if close_ha > open_ha else 'red'

                                    # Check candle type conditions
                                    alert_triggered = False
                                    
                                    if cand_type == 1:
                                        if candle_color != 'green':
                                            failure_reasons.append(f"Candle type 1 requires green candle, got {candle_color}")
                                        else:
                                            alert_triggered = True
                                    elif cand_type == 2:
                                        no_lower_wick = abs(low_ha - open_ha) <= 0.001  # No tolerance - exact match required
                                        upper_wick = high_ha > close_ha
                                        if candle_color != 'green':
                                            failure_reasons.append(f"Candle type 2 requires green candle, got {candle_color}")
                                        elif not no_lower_wick:
                                            failure_reasons.append(f"Candle type 2 requires no lower wick, got lower_wick={abs(low_ha - open_ha):.4f}")
                                        elif not upper_wick:
                                            failure_reasons.append(f"Candle type 2 requires upper wick, got high_ha={high_ha:.2f} <= close_ha={close_ha:.2f}")
                                        else:
                                            alert_triggered = True
                                    elif cand_type == 3:
                                        no_upper_wick = abs(high_ha - close_ha) <= 0.001  # Minimal tolerance added
                                        no_lower_wick = abs(low_ha - open_ha) <= 0.001  # Minimal tolerance added
                                        if candle_color != 'green':
                                            failure_reasons.append(f"Candle type 3 requires green candle, got {candle_color}")
                                        elif not no_lower_wick:
                                            failure_reasons.append(f"Candle type 3 requires no lower wick, got lower_wick={abs(low_ha - open_ha):.4f}")
                                        elif not no_upper_wick:
                                            failure_reasons.append(f"Candle type 3 requires no upper wick, got upper_wick={abs(high_ha - close_ha):.4f}")
                                        else:
                                            alert_triggered = True
                                    
                                    # Determine final result and reason
                                    if alert_triggered:
                                        # ✅ FIX 4: Final defensive assertion before processing alert
                                        # Verify PSAR signal check passed (defensive programming)
                                        psar_signal_int = int(psar_signal) if not np.isnan(psar_signal) else 0
                                        signaldirection_int = int(signaldirection)
                                        
                                        if psar_signal_int != signaldirection_int:
                                            logger.error(
                                                f"[ALERT_CHECK][CRITICAL] Alert triggered but PSAR signal mismatch: "
                                                f"signal={psar_signal_int}, direction={signaldirection_int} for {symbol} {interval}"
                                            )
                                            # Don't process alert if check failed
                                            alert_triggered = False
                                            result_status = "REJECTED"
                                            result_reason = f"CRITICAL: PSAR signal check failed: {psar_signal_int} != {signaldirection_int}"
                                            logger.info(
                                                f"[ALERT_CHECK] symbol={symbol} timeframe={interval} timestamp={alert_timestamp_str} | "
                                                f"PSAR_value={safe_format(psar_value)} PSAR_signal={psar_signal_int} | "
                                                f"K={safe_format(stoch_k)} D={safe_format(stoch_d)} | "
                                                f"RESULT: {result_status} | "
                                                f"REASON: {result_reason}"
                                            )
                                            db_error_message = ""
                                        else:
                                            result_status = "PASSED"
                                            result_reason = "All conditions met - alert triggered"
                                            
                                            # Process alert and capture database error
                                            db_error_message = await self.process_alert(
                                                symbol, scanID, alert_timestamp, LRL_value, 
                                                lrcangletype, lrcanglestart, lrcangleend, 
                                                angle_degrees, crossover_index, psar_signal, 
                                                candle_color, high_ha, digit_name, conditionID, 
                                                redis_client,close_ha,scan_name
                                            )
                                    else:
                                        # Log failure with all details
                                        failure_reason = "; ".join(failure_reasons) if failure_reasons else "Unknown reason"
                                        result_status = "REJECTED"
                                        result_reason = failure_reason
                                        
                                        # No alert triggered, set empty error message
                                        db_error_message = ""
                                    
                                    # Single consolidated log entry with all key information
                                    logger.info(
                                        f"[ALERT_CHECK] symbol={symbol} timeframe={interval} | "
                                        f"PSAR_value={safe_format(psar_value)} PSAR_signal={psar_signal} | "
                                        f"K={safe_format(stoch_k)} D={safe_format(stoch_d)} | "
                                        f"RESULT: {result_status} | "
                                        f"REASON: {result_reason}"
                                    )
                                    
                                    # Log debug data to CSV ONLY when alert triggers (reduces I/O by 90%+)
                                    if alert_triggered:
                                        try:
                                            debug_data = {
                                                'timestamp': pd.to_datetime(dates_combined[i]).strftime('%Y-%m-%d %H:%M:%S'),
                                                'open': round(data_combined[i, 0], 2),
                                                'high': round(data_combined[i, 1], 2),
                                                'low': round(data_combined[i, 2], 2),
                                                'close': round(data_combined[i, 3], 2),
                                                'volume': 0,  # Volume not available in current data structure
                                                'ha_open': round(ha_combined[i, 0], 2),
                                                'ha_high': round(ha_combined[i, 1], 2),
                                                'ha_low': round(ha_combined[i, 2], 2),
                                                'ha_close': round(ha_combined[i, 3], 2),
                                                'psar_value': round(psar_data[i], 2),
                                                'psar_signal': int(psar_signal),
                                                'stochastic_k': round(K[i], 2) if not np.isnan(K[i]) else 0,
                                                'stochastic_d': round(D[i], 2) if not np.isnan(D[i]) else 0,
                                                'k_in_range': kline_start < K[i] < kline_end,
                                                'psar_signal_match': psar_signal == signaldirection,
                                                'candle_color': candle_color,
                                                'candle_type': int(cand_type),
                                                'candle_conditions_met': alert_triggered,
                                                'alert_triggered': alert_triggered,
                                                'scan_id': int(scanID),
                                                'scan_name': scan_name,
                                                'condition_id': int(conditionID),
                                                'kline_start': float(kline_start),
                                                'kline_end': float(kline_end),
                                                'signaldirection': int(signaldirection),
                                                'processing_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'error_message': db_error_message if 'db_error_message' in locals() else ''
                                            }
                                            
                                            # Log to CSV file (only when alert triggers) - async, non-blocking
                                            # asyncio.create_task(log_debug_data_to_csv(symbol, interval, debug_data))
                                        except Exception as debug_error:
                                            logger.error(f"Error logging debug data for {symbol}: {debug_error}")
                                            # Log error case to CSV as well
                                            try:
                                                error_debug_data = {
                                                    'timestamp': pd.to_datetime(dates_combined[i]).strftime('%Y-%m-%d %H:%M:%S'),
                                                    'symbol': symbol,
                                                    'timeframe': interval,
                                                    'error_message': str(debug_error),
                                                    'processing_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                                }
                                                # Async CSV logging for error case
                                                # asyncio.create_task(log_debug_data_to_csv(symbol, interval, error_debug_data))
                                            except Exception:
                                                pass  # Avoid nested error loops
                                except Exception as e:
                                    logger.error(f"Error processing candle {i} for {symbol}: {e}")
                                    continue
                            
                            # CRITICAL: Update last_checked_ts in Redis after processing all candles
                            # This ensures we don't miss any candles even if processing is delayed
                            # Only update if we actually processed candles (prevents unnecessary Redis writes)
                            if candles_processed > 0 and latest_checked_ts is not None:
                                try:
                                    # ✅ FIX: If we checked the most recent candle, subtract 1 minute buffer
                                    # This ensures we always re-check the most recent candle on the next run
                                    # (since we always check candles within 10 minutes, this buffer ensures re-checking)
                                    ts_to_save = latest_checked_ts
                                    if most_recent_candle_in_data is not None:
                                        time_diff = (most_recent_candle_in_data - latest_checked_ts).total_seconds()
                                        # If the checked candle is the most recent (or very close), subtract 1 minute buffer
                                        if abs(time_diff) < 120:  # Within 2 minutes of most recent
                                            ts_to_save = latest_checked_ts - pd.Timedelta(minutes=1)
                                            logger.debug(f"[ALERT_CHECK] Adjusted last_checked_ts by -1min buffer for {symbol} {interval} (most recent candle: {most_recent_candle_in_data})")
                                    
                                    latest_checked_ts_str = ts_to_save.strftime('%Y-%m-%d %H:%M:%S')
                                    await redis_client.hset(alert_check_key, mapping={
                                        'last_checked_ts': latest_checked_ts_str,
                                        'last_check_time': datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
                                        'candles_checked': str(candles_processed)
                                    })
                                    await set_ttl_safe(redis_client, alert_check_key, 30 * 24 * 60 * 60)  # 30 days
                                    logger.debug(f"[ALERT_CHECK] Updated last_checked_ts for {symbol} {interval} conditionID={conditionID} cond={cond} to {latest_checked_ts_str} (checked {candles_processed} candles)")
                                except Exception as update_err:
                                    logger.error(f"[ALERT_CHECK][ERROR] Failed to update last_checked_ts for {symbol} {interval}: {update_err}")
                        except Exception as e:
                            logger.error(f"Error processing condition {cond} for {symbol}: {e}")
                            continue
                except Exception as e:
                    logger.error(f"Error processing condition {conditionID} for {symbol}: {e}")
                    continue
                    
            logger.info(f"[PROCESS] symbol={symbol} interval={interval} | Step 10: Completed process_symbol")
        except Exception as e:
            logger.error(f"Error processing symbol {symbol}: {e}")
            logger.error(f"[PROCESS] symbol={symbol} interval={interval} | ERROR: Failed at step with error: {e}")
            return