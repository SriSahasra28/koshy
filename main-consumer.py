# python/alert_worker.py

from   background.async_db import dbconnection
import sys
import redis.asyncio as redis
import json
import pandas as pd
import time
import asyncio
from datetime import datetime, timedelta
import pytz
import os
from redis_alert_engine import RedisAlertEngine
from loguru import logger

IST = pytz.timezone("Asia/Kolkata")

# Create CSV output directory
CSV_OUTPUT_DIR = "C:/Users/Administrator/Desktop/React/koshy-trading-app-server/data"
os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)

# Helper function for safe TTL setting
async def set_ttl_safe(redis_client, key, ttl=30*24*60*60):
    """Set TTL on key, silently ignore errors (non-critical operation)"""
    try:
        await redis_client.expire(key, ttl)
    except Exception:
        pass  # TTL failure is non-critical

# --- Logging setup using loguru (Console + File for scheduled runs) ---
def _setup_logging():
    """Setup loguru logger - console + file logging for scheduled runs"""
    try:
        # Remove default handler
        logger.remove()
        
        # Add console handler
        logger.add(
            sys.stdout,
            level="INFO",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
            colorize=True
        )
        
        # Add file handler — absolute path so logs land in the same place
        # regardless of how the process is started (Task Scheduler, CLI, etc.)
        log_dir = os.path.dirname(os.path.abspath(__file__))
        log_file = os.path.join(log_dir, "main_consumer.log")
        logger.add(
            log_file,
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            rotation="00:00",  # Rotate at midnight
            retention="30 days",  # Keep 30 days of logs
            compression="zip",  # Compress old logs
            enqueue=True,  # Thread-safe logging
            backtrace=False,  # Disable backtrace for performance
            diagnose=False  # Disable diagnose for performance
        )
        
        logger.info(f"main-consumer logging initialized (console + file: {log_file})")
    except Exception as e:
        # Fallback to basic console logging if setup fails
        logger.add(sys.stdout, level="INFO")
        logger.error(f"Failed to setup logging: {e}")

_setup_logging()

async def load_data():
    """Load all data asynchronously with error handling"""
    db = None
    try:
        logger.info("[STARTUP] Initializing database connection pool")
        db = dbconnection()
        loop = asyncio.get_event_loop()
        await db.create_pool(loop=loop)
        
        # Verify pool was created successfully
        if not hasattr(db, 'pool') or db.pool is None:
            raise Exception("Database pool creation failed - pool attribute not set")

        logger.info("[STARTUP] Loading configuration data from database")
        # Load data the same way as main_redis_may.py
        df_scan_items = await db.get_scan_items()
        df_scan_names = await db.get_scan_names()
        df_custom_indicators = await db.get_custom_indicators()
        df_conditions = await db.get_conditions()
        df_HLFP = await db.get_hlfp()
        
        # Load priority stocks and timeframes like main_redis_may.py
        priority_stocks_tpl = await db.get_priority_instruments_to_trade()
        df_priority_stocks = pd.DataFrame(priority_stocks_tpl, columns=['instrument_token', 'symbol', 'basket_id'])
        df_basket_timeframes = await db.get_timeframes()
        
        logger.info(f"[STARTUP] Data loaded: {len(df_scan_items)} scan items, {len(df_conditions)} conditions, {len(df_priority_stocks)} priority stocks")
        return db, df_scan_items, df_scan_names, df_custom_indicators, df_conditions, df_HLFP, df_priority_stocks, df_basket_timeframes
        
    except Exception as e:
        logger.error(f"[STARTUP][ERROR] Error loading data from database: {e}")
        logger.warning("[STARTUP] Continuing without database connection - using empty DataFrames")
        
        # Return empty DataFrames and None for db if database fails
        return None, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def create_token_map(df_priority_stocks, df_basket_timeframes):
    """Create token mapping from priority stocks - Fixed version"""
    token_map = {}
    
    for _, row in df_priority_stocks.iterrows():
        token = row['instrument_token']
        symbol = row['symbol']
        basket_id = row['basket_id']
        
        # Get timeframes for this basket
        filtered_timeframes = df_basket_timeframes[df_basket_timeframes.basket_id == basket_id]
        
        # Check which timeframes are enabled for this basket
        timeframe_configs = {
            'minute': ('1min', '1minute'),
            '2min': ('2min', '2minute'),
            '3min': ('3min', '3minute'),
            '5min': ('5min', '5minute'),
            '10min': ('10min', '10minute'),
            '15min': ('15min', '15minute'),
            '30min': ('30min', '30minute'),
            '60min': ('60min', '60minute')
        }
        
        for tf_key, (col_name, interval_name) in timeframe_configs.items():
            if col_name in filtered_timeframes.columns:
                is_enabled = filtered_timeframes[col_name].any()
                if is_enabled:
                    # Create unique token key for each timeframe
                    if tf_key == 'minute':
                        token_key = str(token)
                    else:
                        token_key = f"{token}_{tf_key}"
                    
                    token_map[token_key] = {
                        'symbol': symbol,
                        'interval': interval_name,
                        'basket_id': basket_id,
                        'base_token': token,
                        'timeframe_key': tf_key
                    }
    
    return token_map

def _interval_minutes(interval_name: str) -> int:
    """Convert interval name to minutes"""
    return {
        '1minute': 1, '2minute': 2, '3minute': 3, '5minute': 5,
        '10minute': 10, '15minute': 15, '30minute': 30, '60minute': 60,
    }.get(interval_name, 1)


async def save_1min_candle_to_database(symbol, df, db):
    """Save 1-minute candle data to database"""
    try:
        if df is None or df.empty:
            return False
            
        # Get the last candle (most recent)
        last_candle = df.iloc[-1]
        
        # Extract OHLC data
        timestamp = pd.to_datetime(last_candle['timestamp'])
        open_price = float(last_candle['open'])
        high_price = float(last_candle['high'])
        low_price = float(last_candle['low'])
        close_price = float(last_candle['close'])
        
        # Save to database
        await db.insert_one_min_ohlc(symbol, timestamp, open_price, high_price, low_price, close_price)
        logger.debug(f"[DB] Saved 1-minute candle to database for {symbol} at {timestamp}")
        return True
        
    except Exception as e:
        logger.error(f"[DB][ERROR] Error saving 1-minute candle to database for {symbol}: {e}")
        return False

async def fetch_candles_from_date(redis_client, token, start_date=None, last_n_candles=None, fetch_all=False):
    """Fetch candles from a specific start date OR last N candles, forward-fill gaps using prev-close logic.
    
    Args:
        redis_client: Redis client
        token: Token ID
        start_date: Optional start date (if None, uses last_n_candles)
        last_n_candles: Optional number of last candles to fetch (default: 500 for indicators)
    
    Returns cleaned DataFrame with forward-filled gaps.
    """
    try:
        sorted_set_key = f"ohlc_sorted:{token}"
        
        # Optimize: Fetch only last N candles if no start_date specified
        if fetch_all:
            # Fetch the entire history for this token
            ohlc_entries = await redis_client.zrange(sorted_set_key, 0, -1)
            logger.info(f"[FETCH] Fetched ALL {len(ohlc_entries)} candles for token {token} from Redis (startup full-load)")
        elif start_date is None:
            if last_n_candles is None:
                last_n_candles = 500  # Default: enough for indicator windows (PSAR needs 500)
            ohlc_entries = await redis_client.zrange(sorted_set_key, -last_n_candles, -1)
            logger.debug(f"[FETCH] Fetched last {len(ohlc_entries)} candles for token {token} from Redis")
        else:
            start_timestamp = int(start_date.timestamp())
            ohlc_entries = await redis_client.zrangebyscore(sorted_set_key, start_timestamp, '+inf')
            logger.debug(f"[FETCH] Fetched {len(ohlc_entries)} entries for token {token} from Redis (from {start_date})")
        
        if not ohlc_entries:
            return None
        
        ohlc_list = [json.loads(entry) for entry in ohlc_entries]
        df = pd.DataFrame(ohlc_list)

        required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_columns):
            logger.warning(f"[FETCH][WARN] Missing required columns in data for token {token}")
            return None

        df = df.dropna(subset=['open', 'high', 'low', 'close'])
        df = df.drop_duplicates(subset=['timestamp'], keep='last')

        if df.empty:
            return None

        # Ensure timestamps are in pandas datetime (assume in IST already)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        df = df.set_index('timestamp')

        # Build expected trading minutes between min and max timestamps per day (no strict 09:15 presence check)
        days = pd.to_datetime(df.index.date).unique()
        trading_minutes = []
        for day in days:
            market_open = pd.Timestamp(day.year, day.month, day.day, 9, 15)
            market_close = pd.Timestamp(day.year, day.month, day.day, 15, 29)
            start = max(market_open, df.index.min())
            end = min(market_close, df.index.max())
            if start <= end:
                trading_minutes += list(pd.date_range(start, end, freq='min'))
        if not trading_minutes:
            return None
        trading_minutes = pd.DatetimeIndex(trading_minutes)

        # Reindex and apply your flat-fill logic
        df_full = df.reindex(trading_minutes)
        df_full['volume'] = df_full['volume'].fillna(0)

        # Replace missing OHLC with previous close
        previous_close = None
        for i in range(len(df_full)):
            row = df_full.iloc[i]
            if pd.isna(row['close']):
                if previous_close is not None:
                    for col in ['open', 'high', 'low', 'close']:
                        df_full.iat[i, df_full.columns.get_loc(col)] = previous_close
                    df_full.iat[i, df_full.columns.get_loc('volume')] = 0
            else:
                previous_close = row['close']

        # Reset index and return all data (no limit needed for date-based fetching)
        df_full = df_full.reset_index().rename(columns={'index': 'timestamp'})

        return df_full if not df_full.empty else None

    except Exception as e:
        logger.error(f"[FETCH][ERROR] Error fetching candles for token {token}: {e}")
        return None

async def store_resampled_data_in_redis(redis_client, symbol, interval, df, only_latest=False):
    """Store resampled data in Redis - each candle as individual entry in sorted set.
    
    Args:
        redis_client: Redis client
        symbol: Symbol name
        interval: Timeframe interval
        df: DataFrame with candles to store
        only_latest: If True, only store candles newer than what's already in Redis (for incremental updates)
    """
    try:
        if df is None or df.empty:
            return False
        
        # Use sorted set for individual candle storage (same pattern as 1-minute data)
        sorted_set_key = f"resampled_ohlc_sorted:{symbol}:{interval}"
        
        # Get latest timestamp already in Redis (if only_latest=True)
        latest_stored_ts = None
        if only_latest:
            try:
                latest_entry = await redis_client.zrange(sorted_set_key, -1, -1)
                if latest_entry:
                    latest_data = json.loads(latest_entry[0])
                    latest_stored_ts = pd.to_datetime(latest_data['datetime'])
            except Exception:
                pass
        
        # Store each candle individually with timestamp as score
        stored_count = 0
        new_count = 0
        for _, row in df.iterrows():
            try:
                # Parse timestamp
                ts = pd.to_datetime(row['timestamp'])
                
                # Skip if only_latest and this candle is not newer
                if only_latest and latest_stored_ts is not None and ts <= latest_stored_ts:
                    continue
                
                timestamp_score = int(ts.timestamp())
                
                # Create candle data
                # OPTIMIZED: Add stored_at timestamp when creating data structure (no extra latency)
                current_storage_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
                candle_data = {
                    'datetime': ts.strftime('%Y-%m-%d %H:%M:%S'),
                    'stored_at': current_storage_time,  # Track when this entry was stored in Redis
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': int(row['volume']) if 'volume' in row else 0
                }
                
                # Check if score already exists using zcount (efficient check)
                existing_count = await redis_client.zcount(sorted_set_key, timestamp_score, timestamp_score)
                is_new = existing_count == 0
                
                # Only add if score doesn't exist
                if is_new:
                    await redis_client.zadd(sorted_set_key, {json.dumps(candle_data): timestamp_score})
                # Set 30-day TTL on ohlc_sorted key (refresh TTL on each update)
                await set_ttl_safe(redis_client, sorted_set_key)
                stored_count += 1
                if is_new:
                    new_count += 1
            except Exception as e:
                logger.error(f"[STORE][ERROR] Failed to store candle for {symbol} {interval}: {e}")
                continue
        
        if only_latest:
            logger.debug(f"[STORE] symbol={symbol} interval={interval} stored={new_count} new candles (total processed={stored_count})")
        else:
            logger.debug(f"[STORE] symbol={symbol} interval={interval} stored={stored_count} candles")
        return True
        
    except Exception as e:
        logger.error(f"[STORE][ERROR] symbol={symbol} interval={interval} err={e}")
        return False


def should_process_timeframe(current_time, interval):
    """Decide whether to process a timeframe for the just-finalized 1-min candle.

    Timeline:
      tick_zerodha finalizes the 1-min candle at minute T (e.g. 15:14) and
      immediately pushes its token to ohlc_ready.  main-consumer receives the
      push when current_time ≈ T+1min (e.g. 15:15:0x).

      last_closed  = current_time floored to minute - 1min  → T  (15:14)
      minutes_since_start = (T - 09:15) in minutes           → 359  (for T=15:14)

    For an N-min candle the last 1-min bar lands at offset (k*N - 1) from 09:15.
    So the correct trigger condition is:
        (minutes_since_start + 1) % interval_minutes == 0

    Example (5min): 359 + 1 = 360, 360 % 5 = 0  ✅  triggers immediately
    Old (buggy) check: 359 % 5 = 4  ✗  delayed until T+2 (15:16)
    """
    # Market hours (inclusive) 09:16–15:30 for completed candles
    if current_time.hour < 9 or (current_time.hour == 9 and current_time.minute < 16):
        return False
    if current_time.hour > 15 or (current_time.hour == 15 and current_time.minute > 30):
        return False

    interval_minutes = {
        '1minute': 1,
        '2minute': 2,
        '3minute': 3,
        '5minute': 5,
        '10minute': 10,
        '15minute': 15,
        '30minute': 30,
        '60minute': 60,
    }.get(interval)
    if not interval_minutes:
        return False

    # last_closed = the candle that was JUST finalized and pushed to ohlc_ready
    last_closed = current_time.replace(second=0, microsecond=0) - pd.Timedelta(minutes=1)
    day_start = last_closed.replace(hour=9, minute=15, second=0, microsecond=0)
    if last_closed < day_start:
        return False
    minutes_since_start = int((last_closed - day_start).total_seconds() // 60)
    # +1 because last_closed is the LAST bar of the N-min block, not the first.
    # (k*N - 1) offset from start → (minutes_since_start + 1) is divisible by N.
    return (minutes_since_start + 1) % interval_minutes == 0



def resample_ohlc_data(df, interval_minutes):
    """Resample 1-minute OHLC data to higher timeframes with validation and stable alignment."""
    if interval_minutes == 1:
        return df
    
    try:
        # Validate input data
        if df is None or df.empty or len(df) < interval_minutes:
            logger.warning(f"[RESAMPLE][WARN] Insufficient data for {interval_minutes}-minute resampling")
            return pd.DataFrame()
        
        # Diagnostics: input length and time bounds
        try:
            ts_series = pd.to_datetime(df['timestamp'])
            input_len = len(df)
            start_ts = ts_series.min()
            end_ts = ts_series.max()
            logger.debug(f"[RESAMPLE] interval={interval_minutes} | len={input_len} | start={start_ts} | end={end_ts}")
        except Exception as diag_err:
            logger.warning(f"[RESAMPLE][WARN] Diagnostics error: {diag_err}")
        
        # Set timestamp as index for resampling
        df_resample = df.copy()
        df_resample['timestamp'] = pd.to_datetime(df_resample['timestamp'])
        df_resample.set_index('timestamp', inplace=True)
        
        # Resample aligned to 09:15 using origin/offset; completed candles only
        # closed='right': candle labeled 09:15 contains {09:16, 09:17, ..., 09:20} (proven matching on 1/2/5min)
        resampled = df_resample.resample(
            f'{interval_minutes}min', origin='start_day', offset='15min', label='left', closed='right'
        ).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # Reset index to get timestamp back as column
        resampled.reset_index(inplace=True)
        resampled['timestamp'] = resampled['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return resampled
        
    except Exception as e:
        logger.error(f"[RESAMPLE][ERROR] Error resampling data to {interval_minutes}min: {e}")
        return pd.DataFrame()

async def main():
    """Main async function with improved error handling"""
    redis_client = None
    
    try:
        # Load data asynchronously with database connection
        db, df_scan_items, df_scan_names, df_custom_indicators, df_conditions, df_HLFP, df_priority_stocks, df_basket_timeframes = await load_data()
        
        # Check if data was loaded successfully
        if df_priority_stocks.empty:
            logger.warning("[STARTUP] No priority stocks loaded. System will continue but no alerts will be processed.")
            logger.warning("[STARTUP] Check database connection or ensure priority stocks are configured.")
            # Continue with empty data - system can still run but won't process alerts
            df_priority_stocks = pd.DataFrame(columns=['instrument_token', 'symbol', 'basket_id'])
            df_basket_timeframes = pd.DataFrame()
        else:
            logger.info(f"[STARTUP] Data loaded successfully. Loaded {len(df_priority_stocks)} priority stocks and {len(df_basket_timeframes)} timeframes.")
        
        # Create token mapping
        logger.info("[STARTUP] Creating token mapping from priority stocks and timeframes")
        token_map = create_token_map(df_priority_stocks, df_basket_timeframes)
        
        if not token_map:
            logger.warning("[STARTUP] No token mappings created. Check your data configuration.")
            logger.warning("[STARTUP] System will continue but no alerts will be processed.")
            logger.warning(f"[STARTUP] REASON: Either df_priority_stocks is empty OR df_basket_timeframes is empty OR no timeframes are enabled")
            logger.warning(f"[STARTUP] df_priority_stocks length: {len(df_priority_stocks)}, df_basket_timeframes length: {len(df_basket_timeframes)}")
            # Continue with empty token_map - system can still run but won't process anything
        else:
            logger.info(f"[STARTUP] Created {len(token_map)} token mappings")
            # --- DIAGNOSTIC: show which basket_ids are in filter_options vs timeframes ---
            filter_basket_ids = set(df_priority_stocks['basket_id'].unique())
            timeframe_basket_ids = set(df_basket_timeframes['basket_id'].unique()) if not df_basket_timeframes.empty else set()
            missing_basket_ids = filter_basket_ids - timeframe_basket_ids
            logger.info(f"[STARTUP][DIAG] filter_options basket_ids: {filter_basket_ids}")
            logger.info(f"[STARTUP][DIAG] timeframes (active scans) basket_ids: {timeframe_basket_ids}")
            if missing_basket_ids:
                missing_symbols = df_priority_stocks[df_priority_stocks['basket_id'].isin(missing_basket_ids)]['symbol'].tolist()
                logger.warning(f"[STARTUP][DIAG] *** BASKET_ID MISMATCH *** These basket_ids have symbols in filter_options but NO active scan configured: {missing_basket_ids}")
                logger.warning(f"[STARTUP][DIAG] Affected symbols (will NOT be scanned): {missing_symbols}")
                logger.warning(f"[STARTUP][DIAG] FIX: Go to /scan page, ensure the scan for these groups is active=1 and has active ScanItems with timeframes enabled.")
        
        config_tables = {
            'df_scan_items': df_scan_items,
            'df_scan_names': df_scan_names,
            'df_custom_indicators': df_custom_indicators,
            'df_conditions': df_conditions,
            'df_HLFP': df_HLFP,
        }
        
        # Test database connection only if db was created successfully
        if db is not None:
            logger.info("[STARTUP] Testing database connection")
            try:
                db_test_result = await db.test_connection()
                if not db_test_result:
                    logger.error("[STARTUP] Database connection test failed. Alert engine will run without database.")
                    db = None
                else:
                    logger.info("[STARTUP] Database connection test passed")
            except Exception as db_test_err:
                logger.error(f"[STARTUP] Database connection test error: {db_test_err}. Alert engine will run without database.")
                db = None
        else:
            logger.warning("[STARTUP] No database connection available. Alert engine will run without database.")
        
        # Create alert engine with database connection
        logger.info("[STARTUP] Initializing Redis Alert Engine")
        alert_engine = RedisAlertEngine(config_tables, db=db)
        
        # Initialize Redis connection
        logger.info("[STARTUP] Connecting to Redis")
        redis_client = redis.from_url('redis://localhost', decode_responses=True)
        
        # Test Redis connection
        await redis_client.ping()
        logger.info("[STARTUP] Redis connection established")
        
        # Store symbol-to-token mapping in Redis for frontend API access
        logger.info("[STARTUP] Storing symbol-to-token mappings in Redis")
        for _, row in df_priority_stocks.iterrows():
            symbol = row['symbol']
            token = row['instrument_token']
            await redis_client.hset('symbol_to_token', symbol, token)
            await redis_client.hset('token_to_symbol', str(token), symbol)
        
        # Set TTL on symbol/token mapping keys (30 days - reference data, rarely changes)
        await set_ttl_safe(redis_client, 'symbol_to_token')
        await set_ttl_safe(redis_client, 'token_to_symbol')
        
        logger.info(f"[STARTUP] Alert worker initialized. token_timeframes={len(token_map)}")
        
        # Track last processed closed-candle timestamp per (token, interval)
        last_processed = {}
        last_minute_marker = None

        # --- One-time startup full-load: fetch all 1-min candles and initialize higher TF + indicators ---
        try:
            # Group timeframe configs by base token for single fetch per token
            tokens_to_process = {}
            for _tk, cfg in token_map.items():
                base_token = cfg['base_token']
                tokens_to_process.setdefault(base_token, []).append((_tk, cfg))

            logger.info(f"[STARTUP] Full initialization for {len(tokens_to_process)} tokens")
            if len(tokens_to_process) == 0:
                logger.warning("[STARTUP] WARNING: tokens_to_process is empty! No resampled_ohlc_sorted keys will be created.")
                logger.warning("[STARTUP] Check: 1) Are priority stocks configured in database? 2) Are timeframes enabled for baskets?")
            
            # ✅ Step 1: Fetch all 1-min data for all tokens (parallelized)
            token_data = {}
            
            # Parallelize token fetching to speed up startup
            async def process_token_fetch(base_token, timeframe_configs):
                """Process a single token's data fetching"""
                try:
                    logger.info(f"[STARTUP] Fetching data for token {base_token} with {len(timeframe_configs)} timeframes")
                    # Fetch ALL candles at startup for complete resampling/indicator seed
                    df_1min_all = await fetch_candles_from_date(redis_client, base_token, fetch_all=True)
                    if df_1min_all is None or df_1min_all.empty:
                        logger.warning(f"[STARTUP] No data for token {base_token} in Redis (ohlc_sorted:{base_token} is empty or missing)")
                        logger.warning(f"[STARTUP] Ensure tick_zerodha.py has run and loaded data into Redis first")
                        return None
                    
                    logger.info(f"[STARTUP] Token {base_token}: Fetched {len(df_1min_all)} candles from Redis")
                    return (base_token, df_1min_all, timeframe_configs)
                except Exception as e:
                    logger.error(f"[STARTUP] Error fetching data for token {base_token}: {e}")
                    return None
            
            # Process tokens in parallel batches (15 at a time to avoid overwhelming Redis)
            TOKEN_FETCH_BATCH_SIZE = 15
            token_items = list(tokens_to_process.items())
            
            for i in range(0, len(token_items), TOKEN_FETCH_BATCH_SIZE):
                batch = token_items[i:i + TOKEN_FETCH_BATCH_SIZE]
                batch_tasks = [process_token_fetch(base_token, configs) for base_token, configs in batch]
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for result in batch_results:
                    if result and not isinstance(result, Exception):
                        base_token, df_1min_all, timeframe_configs = result
                        token_data[base_token] = (df_1min_all, timeframe_configs)
            
            logger.info(f"[STARTUP] Successfully fetched 1-min data for {len(token_data)} tokens")
            
            # ✅ Step 2: Collect ALL symbol-timeframe combinations and process resampling in batches (like main loop)
            all_resample_tasks = []
            all_resample_info = []
            
            # Collect all symbol-timeframe combinations
            for base_token, (df_1min_all, timeframe_configs) in token_data.items():
                for _tk, cfg in timeframe_configs:
                    all_resample_tasks.append((base_token, df_1min_all, cfg))
                    all_resample_info.append(f"{cfg['symbol']}:{cfg['interval']}")
            
            logger.info(f"[STARTUP] Collected {len(all_resample_tasks)} symbol-timeframe combinations for resampling")
            
            # Process resampling in batches (same as main loop - 30 at a time)
            async def process_single_resample(base_token, df_1min_all, cfg):
                """Process resampling for a single symbol-timeframe combination"""
                try:
                    symbol = cfg['symbol']
                    interval = cfg['interval']
                    interval_mins = _interval_minutes(interval)
                    zset_key = f"resampled_ohlc_sorted:{symbol}:{interval}"
                    
                    # Check if key exists and has data
                    existing_entries = await redis_client.zrange(zset_key, -1, -1)
                    key_exists = len(existing_entries) > 0
                    
                    if key_exists:
                        # Key exists - proceed with incremental load
                        try:
                            last_entry = json.loads(existing_entries[0])
                            last_timestamp_str = last_entry.get('datetime') or last_entry.get('timestamp')
                            last_timestamp = pd.to_datetime(last_timestamp_str)
                            
                            logger.info(f"[STARTUP][INCREMENTAL] symbol={symbol} interval={interval} key exists, last candle: {last_timestamp}, proceeding with incremental load")
                            
                            # Fetch only 1-min candles after the last resampled candle timestamp
                            # Add 1 minute to avoid duplicate (last candle already processed)
                            start_date = last_timestamp + pd.Timedelta(minutes=1)
                            
                            # Fetch new 1-min candles from start_date onwards
                            df_1min_new = await fetch_candles_from_date(
                                redis_client, base_token, 
                                start_date=start_date, 
                                fetch_all=False
                            )
                            
                            if df_1min_new is None or df_1min_new.empty:
                                logger.info(f"[STARTUP][INCREMENTAL] symbol={symbol} interval={interval} no new data after {last_timestamp}")
                                return True
                            
                            logger.info(f"[STARTUP][INCREMENTAL] symbol={symbol} interval={interval} found {len(df_1min_new)} new 1-min candles after {last_timestamp}")
                            
                            if interval_mins == 1:
                                # For 1-minute: just append new candles
                                await store_resampled_data_in_redis(redis_client, symbol, interval, df_1min_new, only_latest=True)
                                logger.info(f"[STARTUP][INCREMENTAL] symbol={symbol} interval={interval} appended {len(df_1min_new)} new 1-min candles")
                            else:
                                # For higher timeframes: need to resample from last complete candle boundary
                                lookback_minutes = max(interval_mins * 2, 100)  # At least 2 intervals worth
                                lookback_date = start_date - pd.Timedelta(minutes=lookback_minutes)
                                
                                # Fetch from lookback_date to get complete resampling context
                                df_1min_for_resample = await fetch_candles_from_date(
                                    redis_client, base_token,
                                    start_date=lookback_date,
                                    fetch_all=False
                                )
                                
                                if df_1min_for_resample is not None and not df_1min_for_resample.empty:
                                    # Resample the extended window
                                    resampled_df = resample_ohlc_data(df_1min_for_resample, interval_mins)
                                    
                                    if resampled_df is not None and not resampled_df.empty:
                                        # Filter to only candles after last_timestamp
                                        resampled_df['timestamp_dt'] = pd.to_datetime(resampled_df['timestamp'])
                                        new_resampled = resampled_df[resampled_df['timestamp_dt'] > last_timestamp]
                                        new_resampled = new_resampled.drop(columns=['timestamp_dt'])
                                        
                                        if not new_resampled.empty:
                                            await store_resampled_data_in_redis(redis_client, symbol, interval, new_resampled, only_latest=True)
                                            logger.info(f"[STARTUP][INCREMENTAL] symbol={symbol} interval={interval} appended {len(new_resampled)} new resampled candles")
                                        else:
                                            logger.info(f"[STARTUP][INCREMENTAL] symbol={symbol} interval={interval} no new resampled candles after {last_timestamp}")
                                    else:
                                        logger.warning(f"[STARTUP][INCREMENTAL] symbol={symbol} interval={interval} resample produced empty result")
                                else:
                                    logger.warning(f"[STARTUP][INCREMENTAL] symbol={symbol} interval={interval} could not fetch lookback data for resampling")
                            
                        except Exception as inc_err:
                            logger.error(f"[STARTUP][INCREMENTAL][ERROR] symbol={symbol} interval={interval} error: {inc_err}")
                            import traceback
                            logger.error(f"[STARTUP][INCREMENTAL][ERROR] Traceback: {traceback.format_exc()}")
                            # Fallback to full rebuild on error
                            logger.warning(f"[STARTUP][FALLBACK] Falling back to full rebuild for {symbol} {interval}")
                            key_exists = False  # Force full rebuild
                    
                    if not key_exists:
                        # Key doesn't exist or was cleared - full load
                        logger.info(f"[STARTUP][FULL] symbol={symbol} interval={interval} key missing or empty, performing full load")
                        
                        if interval_mins == 1:
                            # Store full 1-minute as resampled store for frontend consistency
                            df_1 = df_1min_all.copy()
                            await store_resampled_data_in_redis(redis_client, symbol, interval, df_1, only_latest=False)
                            logger.info(f"[STARTUP][FULL] symbol={symbol} interval={interval} stored {len(df_1)} 1-min candles")
                        else:
                            resampled_df = resample_ohlc_data(df_1min_all, interval_mins)
                            if resampled_df is not None and not resampled_df.empty:
                                await store_resampled_data_in_redis(redis_client, symbol, interval, resampled_df, only_latest=False)
                                logger.info(f"[STARTUP][FULL] symbol={symbol} interval={interval} stored {len(resampled_df)} resampled candles")
                            else:
                                logger.warning(f"[STARTUP][FULL] symbol={symbol} interval={interval} resample produced empty result")
                    
                    return True
                    
                except Exception as e:
                    logger.error(f"[STARTUP][RESAMPLE][ERROR] token={base_token} {cfg['symbol']} {cfg['interval']}: {e}")
                    import traceback
                    logger.error(f"[STARTUP][RESAMPLE][ERROR] Traceback: {traceback.format_exc()}")
                    return False
            
            # Process resampling in batches (same batch size as main loop)
            RESAMPLE_BATCH_SIZE = 30
            total_resample_batches = (len(all_resample_tasks) + RESAMPLE_BATCH_SIZE - 1) // RESAMPLE_BATCH_SIZE
            
            if all_resample_tasks:
                logger.info(f"[STARTUP][RESAMPLE] Processing {len(all_resample_tasks)} symbol-timeframe combinations in {total_resample_batches} batches")
                
                resample_batch_num = 0
                for i in range(0, len(all_resample_tasks), RESAMPLE_BATCH_SIZE):
                    batch = all_resample_tasks[i:i + RESAMPLE_BATCH_SIZE]
                    batch_info = all_resample_info[i:i + RESAMPLE_BATCH_SIZE]
                    resample_batch_num += 1
                    
                    logger.info(f"[STARTUP][RESAMPLE] Executing batch {resample_batch_num}/{total_resample_batches} | Tasks: {len(batch)}")
                    logger.debug(f"[STARTUP][RESAMPLE] Batch {resample_batch_num} symbols: {', '.join(batch_info[:5])}{'...' if len(batch_info) > 5 else ''}")
                    
                    # Execute batch in parallel
                    batch_start_time = time.time()
                    batch_tasks = [process_single_resample(base_token, df_1min_all, cfg) for base_token, df_1min_all, cfg in batch]
                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    batch_duration = time.time() - batch_start_time
                    
                    # Count successes and failures
                    success_count = sum(1 for r in results if not isinstance(r, Exception) and r is not None)
                    error_count = sum(1 for r in results if isinstance(r, Exception))
                    
                    logger.info(f"[STARTUP][RESAMPLE] Completed batch {resample_batch_num}/{total_resample_batches} | Duration: {batch_duration:.2f}s | Success: {success_count}/{len(batch)} | Errors: {error_count}")
                    
                    # Log any errors from the batch
                    for idx, result in enumerate(results):
                        if isinstance(result, Exception):
                            task_info = batch_info[idx] if idx < len(batch_info) else f"task_{idx}"
                            logger.error(f"[STARTUP][RESAMPLE][ERROR] Batch {resample_batch_num} task {task_info}: {result}")
            
            logger.info(f"[STARTUP] Successfully processed resampling for {len(token_data)} tokens")
            
            # ✅ Step 3: Collect ALL symbol-timeframe combinations for alert processing and process in batches (like main loop)
            all_startup_tasks = []
            all_startup_info = []
            
            # Collect all symbol-timeframe combinations (same pattern as resampling)
            for base_token, (df_1min_all, timeframe_configs) in token_data.items():
                for _tk, cfg in timeframe_configs:
                    symbol = cfg['symbol']
                    interval = cfg['interval']
                    basket_id = cfg['basket_id']
                    
                    # Create task for parallel processing (same pattern as main loop)
                    async def process_one_symbol_startup(sym, tok, inter, bas_id):
                        """Process a single symbol during startup - wrapped for parallel execution"""
                        try:
                            # Quick Redis check for candle_data (non-blocking, done in parallel)
                            candle_key = f"candle_data:{sym}:{inter}"
                            ha_state_key = f"ha_state:{sym}:{inter}"
                            
                            candle_data_exists = await redis_client.exists(candle_key)
                            
                            if candle_data_exists:
                                # Data exists - ensure HA state is set for incremental updates
                                ha_state = await redis_client.hgetall(ha_state_key)
                                if not ha_state or ha_state.get('initial_check_done') != '1':
                                    # HA state missing or incomplete - set it to enable incremental updates
                                    try:
                                        last_candle_entry = await redis_client.zrange(candle_key, -1, -1)
                                        if last_candle_entry:
                                            last_candle_data = json.loads(last_candle_entry[0])
                                            last_ts = last_candle_data.get('timestamp')
                                            current_storage_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
                                            await redis_client.hset(ha_state_key, mapping={
                                                'initial_check_done': '1',
                                                'last_ts': last_ts if last_ts else '',
                                                'stored_at': current_storage_time
                                            })
                                            await set_ttl_safe(redis_client, ha_state_key)
                                    except Exception as ha_err:
                                        logger.warning(f"[STARTUP][CANDLE_DATA][WARN] symbol={sym} interval={inter} could not set HA state: {ha_err}")
                            else:
                                # Data doesn't exist - alert engine will do full rebuild
                                try:
                                    await redis_client.hdel(ha_state_key, 'initial_check_done', 'last_ha_open', 'last_ha_close', 'last_ts')
                                except Exception:
                                    pass
                            
                            # Process symbol (alert engine will handle full rebuild if needed)
                            await alert_engine.process_symbol(sym, tok, inter, bas_id, redis_client=redis_client)
                            return True
                        except Exception as e:
                            logger.error(f"[STARTUP][ERROR] Error processing {sym} {inter}: {str(e)}")
                            return None
                    
                    # Add task to global list (across all tokens/timeframes)
                    task = process_one_symbol_startup(symbol, base_token, interval, basket_id)
                    all_startup_tasks.append(task)
                    all_startup_info.append(f"{symbol}:{interval}")
            
            logger.info(f"[STARTUP] Collected {len(all_startup_tasks)} symbol-timeframe combinations for alert processing")
            
            # ✅ Step 3: Batch process ALL startup tasks in parallel (across all tokens)
            if all_startup_tasks:
                STARTUP_BATCH_SIZE = 30
                total_startup_batches = (len(all_startup_tasks) + STARTUP_BATCH_SIZE - 1) // STARTUP_BATCH_SIZE
                logger.info(f"[STARTUP][BATCH] Processing {len(all_startup_tasks)} tasks across {len(token_data)} tokens in {total_startup_batches} batches")
                
                startup_batch_num = 0
                for i in range(0, len(all_startup_tasks), STARTUP_BATCH_SIZE):
                    batch = all_startup_tasks[i:i + STARTUP_BATCH_SIZE]
                    batch_info = all_startup_info[i:i + STARTUP_BATCH_SIZE]
                    if batch:
                        startup_batch_num += 1
                        logger.info(f"[STARTUP][BATCH] Executing batch {startup_batch_num}/{total_startup_batches} | Tasks: {len(batch)}")
                        logger.debug(f"[STARTUP][BATCH] Batch {startup_batch_num} symbols: {', '.join(batch_info[:5])}{'...' if len(batch_info) > 5 else ''}")
                        
                        # Execute batch in parallel with error handling
                        batch_start_time = time.time()
                        results = await asyncio.gather(*batch, return_exceptions=True)
                        batch_duration = time.time() - batch_start_time
                        
                        # Count successes and failures
                        success_count = sum(1 for r in results if not isinstance(r, Exception) and r is not None)
                        error_count = sum(1 for r in results if isinstance(r, Exception))
                        
                        logger.info(f"[STARTUP][BATCH] Completed batch {startup_batch_num}/{total_startup_batches} | Duration: {batch_duration:.2f}s | Success: {success_count}/{len(batch)} | Errors: {error_count}")
                        
                        # Log any errors from the batch (for debugging)
                        for idx, result in enumerate(results):
                            if isinstance(result, Exception):
                                task_info = batch_info[idx] if idx < len(batch_info) else f"task_{idx}"
                                logger.error(f"[STARTUP][BATCH][ERROR] Batch {startup_batch_num} task {task_info}: {result}")
            else:
                logger.warning("[STARTUP] No startup tasks to process")
            logger.info("[STARTUP] Full initialization complete; switching to incremental updates")
        except Exception as e:
            logger.error(f"[STARTUP] Initialization pass failed: {e}")

        # Helper: map base token to its enabled intervals for quick lookup
        base_token_to_intervals = {}
        for _tk, cfg in token_map.items():
            base_token_to_intervals.setdefault(cfg['base_token'], set()).add(cfg['interval'])

        def _align_start(ts: datetime, interval_mins: int) -> datetime:
            # Align to day 09:15 start with given interval
            day_start = ts.replace(hour=9, minute=15, second=0, microsecond=0)
            if ts < day_start:
                return day_start
            total_minutes = int((ts.replace(second=0, microsecond=0) - day_start).total_seconds() // 60)
            bucket = (total_minutes // interval_mins) * interval_mins
            return day_start + timedelta(minutes=bucket)



        async def update_incremental_resample(symbol: str, interval: str, candle: dict, df_1min_all: pd.DataFrame = None):
            """Incrementally update resampled data for a symbol/interval using the provided 1-min candle.
            Stores state in Redis hash and finalized candles as individual entries in sorted set.
            candle keys: timestamp(str or dt), open, high, low, close, volume(int)
            If resampled_ohlc_sorted doesn't exist, performs full resample from beginning using df_1min_all.
            """
            try:
                interval_mins = _interval_minutes(interval)
                if interval_mins == 1:
                    return

                # Normalize timestamp
                ts = candle.get('timestamp')
                if not isinstance(ts, datetime):
                    ts = pd.to_datetime(ts)
                start_ts = _align_start(ts, interval_mins)

                state_key = f"resample_state:{symbol}:{interval}"
                zset_key = f"resampled_ohlc_sorted:{symbol}:{interval}"

                # Try to load existing data - if empty, perform full resample (no EXISTS check needed)
                try:
                    existing_entries = await redis_client.zrange(zset_key, 0, 0)  # Just check if any entries exist
                    if not existing_entries:
                        # No resampled data exists - perform full resample from beginning
                        if df_1min_all is not None and not df_1min_all.empty:
                            logger.info(f"[RESAMPLE][FULL] symbol={symbol} interval={interval} performing full resample from beginning")
                            try:
                                # Perform full resample
                                resampled_df = resample_ohlc_data(df_1min_all, interval_mins)
                                if resampled_df is not None and not resampled_df.empty:
                                    # Store all resampled candles
                                    await store_resampled_data_in_redis(redis_client, symbol, interval, resampled_df, only_latest=False)
                                    logger.info(f"[RESAMPLE][FULL] symbol={symbol} interval={interval} stored {len(resampled_df)} candles")
                                else:
                                    logger.warning(f"[RESAMPLE][FULL] symbol={symbol} interval={interval} resample produced empty result")
                            except Exception as e:
                                logger.error(f"[RESAMPLE][FULL][ERROR] symbol={symbol} interval={interval} err={e}")
                        else:
                            logger.warning(f"[RESAMPLE][FULL][WARN] symbol={symbol} interval={interval} sorted set missing but no 1-min data provided for full resample")
                        # After full resample (or if no data), continue with incremental update
                        # Initialize state with current candle
                        mapping = {
                            'start': start_ts.strftime('%Y-%m-%d %H:%M:%S'),
                            'open': candle['open'],
                            'high': candle['high'],
                            'low': candle['low'],
                            'close': candle['close'],
                            'volume': candle.get('volume', 0)
                        }
                        await redis_client.hset(state_key, mapping=mapping)
                        # Set TTL on resample_state key (30 days - state data)
                        await set_ttl_safe(redis_client, state_key)
                        return
                except Exception as e:
                    # Redis error or key doesn't exist - perform full resample
                    logger.warning(f"[RESAMPLE][FULL] symbol={symbol} interval={interval} Redis error or missing key: {e}")
                    if df_1min_all is not None and not df_1min_all.empty:
                        try:
                            resampled_df = resample_ohlc_data(df_1min_all, interval_mins)
                            if resampled_df is not None and not resampled_df.empty:
                                await store_resampled_data_in_redis(redis_client, symbol, interval, resampled_df, only_latest=False)
                                logger.info(f"[RESAMPLE][FULL] symbol={symbol} interval={interval} stored {len(resampled_df)} candles")
                        except Exception as resample_err:
                            logger.error(f"[RESAMPLE][FULL][ERROR] symbol={symbol} interval={interval} err={resample_err}")
                    # Initialize state
                    mapping = {
                        'start': start_ts.strftime('%Y-%m-%d %H:%M:%S'),
                        'open': candle['open'],
                        'high': candle['high'],
                        'low': candle['low'],
                        'close': candle['close'],
                        'volume': candle.get('volume', 0)
                    }
                    await redis_client.hset(state_key, mapping=mapping)
                    # Set TTL on resample_state key (30 days - state data)
                    await set_ttl_safe(redis_client, state_key)
                    return

                # Load state
                state = await redis_client.hgetall(state_key)
                def _finalize_and_store(st):
                    # Write finalized candle as individual entry in sorted set
                    # OPTIMIZED: Add stored_at timestamp when creating data structure (no extra latency)
                    current_storage_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
                    dt_start = pd.to_datetime(st['start'])
                    out = {
                        'datetime': dt_start.strftime('%Y-%m-%d %H:%M:%S'),
                        'stored_at': current_storage_time,  # Track when this entry was stored in Redis
                        'open': float(st['open']),
                        'high': float(st['high']),
                        'low': float(st['low']),
                        'close': float(st['close']),
                        'volume': int(float(st.get('volume', 0)))
                    }
                    return out, int(dt_start.timestamp())

                # If no state or state belongs to older bucket, finalize the previous state (if exists) and create new
                if not state or ('start' not in state):
                    # Initialize new state with current candle
                    mapping = {
                        'start': start_ts.strftime('%Y-%m-%d %H:%M:%S'),
                        'open': candle['open'],
                        'high': candle['high'],
                        'low': candle['low'],
                        'close': candle['close'],
                        'volume': candle.get('volume', 0)
                    }
                    await redis_client.hset(state_key, mapping=mapping)
                    # Set TTL on resample_state key (30 days - state data)
                    await set_ttl_safe(redis_client, state_key)
                    return

                # Parse existing state times
                cur_start = pd.to_datetime(state['start'])

                if start_ts > cur_start:
                    # Finalize existing state - store as individual entry in sorted set
                    finalized, score = _finalize_and_store(state)
                    # Check if score already exists using zcount (efficient check)
                    existing_count = await redis_client.zcount(zset_key, score, score)
                    # Only add if score doesn't exist
                    if existing_count == 0:
                        await redis_client.zadd(zset_key, {json.dumps(finalized): score})
                    # Set 30-day TTL on resampled_ohlc_sorted key (refresh TTL on each update)
                    await set_ttl_safe(redis_client, zset_key)

                    # Start new state with current candle
                    mapping = {
                        'start': start_ts.strftime('%Y-%m-%d %H:%M:%S'),
                        'open': candle['open'],
                        'high': candle['high'],
                        'low': candle['low'],
                        'close': candle['close'],
                        'volume': candle.get('volume', 0)
                    }
                    await redis_client.hset(state_key, mapping=mapping)
                    # Set TTL on resample_state key (30 days - state data)
                    await set_ttl_safe(redis_client, state_key)
                    return

                # Same bucket: update running state
                new_high = max(float(state['high']), float(candle['high']))
                new_low = min(float(state['low']), float(candle['low']))
                new_close = float(candle['close'])
                new_volume = int(float(state.get('volume', 0))) + int(candle.get('volume', 0))
                await redis_client.hset(state_key, mapping={
                    'high': new_high,
                    'low': new_low,
                    'close': new_close,
                    'volume': new_volume
                })
                # Set TTL on resample_state key (30 days - state data, refresh on each update)
                await set_ttl_safe(redis_client, state_key)
            except Exception as e:
                logger.error(f"[INC-RESAMPLE][ERROR] symbol={symbol} interval={interval} err={e}")


        # ── Hot-reload: listen for new symbols added via the Filter page ─────────
        # When the Node server saves a symbol to filter_options it publishes a
        # { instrument_token, symbol, basket_id } event on the 'new_symbol' channel.
        # We insert it directly into token_map so the main loop picks it up
        # without requiring a restart of main-consumer.py.
        async def hot_reload_new_symbols():
            import redis.asyncio as _aioredis
            hot_client = _aioredis.from_url('redis://localhost', decode_responses=True)
            pubsub = hot_client.pubsub()
            await pubsub.subscribe('new_symbol')
            logger.info('[HOT-RELOAD][CONSUMER] Subscribed to new_symbol channel')
            async for message in pubsub.listen():
                if message['type'] != 'message':
                    continue
                try:
                    data = json.loads(message['data'])
                    token = int(data.get('instrument_token', 0))
                    symbol = str(data.get('option_name', ''))
                    basket_id = int(data.get('basket_id', 0))
                    if not token or not symbol or not basket_id:
                        continue
                    # Check if already in token_map
                    existing = any(
                        cfg['base_token'] == token for cfg in token_map.values()
                    )
                    if existing:
                        logger.info(f'[HOT-RELOAD][CONSUMER] Token {token} ({symbol}) already in token_map')
                        continue
                    # Get timeframes for this basket
                    filtered_tf = df_basket_timeframes[df_basket_timeframes.basket_id == basket_id] \
                        if not df_basket_timeframes.empty else pd.DataFrame()
                    timeframe_configs = {
                        'minute': ('1min', '1minute'),
                        '2min': ('2min', '2minute'),
                        '3min': ('3min', '3minute'),
                        '5min': ('5min', '5minute'),
                        '10min': ('10min', '10minute'),
                        '15min': ('15min', '15minute'),
                        '30min': ('30min', '30minute'),
                        '60min': ('60min', '60minute'),
                    }
                    added = 0
                    for tf_key, (col_name, interval_name) in timeframe_configs.items():
                        is_enabled = False
                        if not filtered_tf.empty and col_name in filtered_tf.columns:
                            is_enabled = bool(filtered_tf[col_name].any())
                        else:
                            # Default: enable 1min if no timeframe config found
                            is_enabled = (tf_key == 'minute')
                        if is_enabled:
                            map_key = str(token) if tf_key == 'minute' else f'{token}_{tf_key}'
                            token_map[map_key] = {
                                'symbol': symbol,
                                'interval': interval_name,
                                'basket_id': basket_id,
                                'base_token': token,
                                'timeframe_key': tf_key,
                                'interval_mins': _interval_minutes(interval_name),
                            }
                            added += 1
                    # Also update Redis symbol<->token maps
                    await redis_client.hset('symbol_to_token', symbol, token)
                    await redis_client.hset('token_to_symbol', str(token), symbol)
                    logger.info(
                        f'[HOT-RELOAD][CONSUMER] Added {symbol} (token {token}) to token_map '
                        f'with {added} timeframe(s). token_map size: {len(token_map)}'
                    )
                except Exception as hr_err:
                    logger.error(f'[HOT-RELOAD][CONSUMER] Error processing new_symbol event: {hr_err}')

        # ── Hot-reload: listen for scan config changes (enable/disable/add/delete) ──
        # When the Node server modifies scans or scanitems it publishes an event
        # on the 'scan_config_changed' channel.  We reload df_scan_items,
        # df_scan_names, and df_conditions from the database so the alert engine
        # picks up the change immediately without a restart.
        async def hot_reload_scan_config():
            import redis.asyncio as _aioredis
            hot_client = _aioredis.from_url('redis://localhost', decode_responses=True)
            pubsub = hot_client.pubsub()
            await pubsub.subscribe('scan_config_changed')
            logger.info('[HOT-RELOAD][SCAN-CONFIG] Subscribed to scan_config_changed channel')
            async for message in pubsub.listen():
                if message['type'] != 'message':
                    continue
                try:
                    data = json.loads(message['data'])
                    change_type = data.get('type', 'unknown')
                    logger.info(f'[HOT-RELOAD][SCAN-CONFIG] Received {change_type} event — reloading scan config from DB')

                    # Reload scan tables from the database
                    new_scan_items = await db.get_scan_items()
                    new_scan_names = await db.get_scan_names()
                    new_conditions = await db.get_conditions()

                    # Update the alert engine's in-memory DataFrames
                    alert_engine.df_scan_items = new_scan_items
                    alert_engine.df_scan_names = new_scan_names
                    alert_engine.df_conditions = new_conditions

                    logger.info(
                        f'[HOT-RELOAD][SCAN-CONFIG] Reloaded: '
                        f'{len(new_scan_items)} scan items, '
                        f'{len(new_scan_names)} scan names, '
                        f'{len(new_conditions)} conditions'
                    )
                except Exception as hr_err:
                    logger.error(f'[HOT-RELOAD][SCAN-CONFIG] Error reloading scan config: {hr_err}')

        # Launch hot-reload listeners as background asyncio tasks
        asyncio.ensure_future(hot_reload_new_symbols())
        asyncio.ensure_future(hot_reload_scan_config())
        logger.info('[HOT-RELOAD][CONSUMER] Hot-reload listener tasks launched (new_symbol + scan_config)')

        logger.info("[MAIN_LOOP] Starting main processing loop")

        while True:
            try:
                # Block for at least one token, then drain the queue burst
                result = await redis_client.blpop(['ohlc_ready'], timeout=5)
                if not result:
                    continue

                tokens_to_consider = set()
                _, token_bytes = result
                tokens_to_consider.add(int(token_bytes))
                # Drain any burst items immediately
                while True:
                    more = await redis_client.lpop('ohlc_ready')
                    if not more:
                        break
                    try:
                        tokens_to_consider.add(int(more))
                    except Exception:
                        continue
                
                cycle_start = time.time()
                logger.info(f"[MAIN_LOOP] Received {len(tokens_to_consider)} token(s) from ohlc_ready queue: {tokens_to_consider}")

                current_time = datetime.now()
                # Minute boundary separator (once per new minute)
                try:
                    minute_marker = current_time.replace(second=0, microsecond=0)
                    if last_minute_marker is None or minute_marker != last_minute_marker:
                        logger.info(f"----- [MINUTE] {minute_marker.strftime('%Y-%m-%d %H:%M')} -----")
                        last_minute_marker = minute_marker
                except Exception:
                    pass

                # --- DIAGNOSTIC: log token_map keys so we can confirm which tokens are registered ---
                token_map_base_tokens = set(v['base_token'] for v in token_map.values())
                unregistered = tokens_to_consider - token_map_base_tokens
                if unregistered:
                    logger.warning(f"[MAIN_LOOP][DIAG] tokens NOT in token_map (no scan configured?): {unregistered}")
                    logger.warning(f"[MAIN_LOOP][DIAG] token_map has {len(token_map)} entries covering base tokens: {token_map_base_tokens}")

                # Determine matching symbol:timeframe combinations
                matching_combinations = []
                for token_key, config in token_map.items():
                    base_token = config['base_token']
                    if base_token not in tokens_to_consider:
                        continue

                    interval = config['interval']
                    if not should_process_timeframe(current_time, interval):
                        logger.debug(f"[MAIN_LOOP][DIAG] Skipping {config['symbol']} {interval}: outside market hours or not aligned yet (time={current_time.strftime('%H:%M:%S')})")
                        continue

                    # Compute last closed candle timestamp aligned to interval
                    last_closed = current_time.replace(second=0, microsecond=0) - pd.Timedelta(minutes=1)
                    day_start = last_closed.replace(hour=9, minute=15, second=0, microsecond=0)
                    if last_closed < day_start:
                        logger.debug(f"[MAIN_LOOP][DIAG] Skipping {config['symbol']} {interval}: last_closed {last_closed} < day_start {day_start}")
                        continue
                    
                    interval_mins = _interval_minutes(interval)
                    if not interval_mins:
                        continue
                    
                    minutes_since_start = int((last_closed - day_start).total_seconds() // 60)
                    if (minutes_since_start + 1) % interval_mins != 0:
                        logger.debug(f"[MAIN_LOOP][DIAG] Skipping {config['symbol']} {interval}: alignment miss. minutes_since_start={minutes_since_start}, interval_mins={interval_mins}, (mss+1)%N={(minutes_since_start+1)%interval_mins}")
                        continue

                    key = (base_token, interval)
                    if last_processed.get(key) == last_closed:
                        logger.debug(f"[MAIN_LOOP][DIAG] Skipping {config['symbol']} {interval}: already processed at {last_closed}")
                        continue

                    last_processed[key] = last_closed
                    matching_combinations.append({
                        'token_key': token_key,
                        'base_token': base_token,
                        'symbol': config['symbol'],
                        'interval': interval,
                        'basket_id': config['basket_id'],
                        'interval_mins': interval_mins
                    })
                
                if not matching_combinations:
                    if token_map_base_tokens & tokens_to_consider:
                        logger.info(f"[MAIN_LOOP] Tokens {tokens_to_consider & token_map_base_tokens} are in token_map but no timeframes matched (alignment check or already processed). Time={current_time.strftime('%H:%M:%S')}")
                    continue
                
                logger.info(f"[MAIN_LOOP] Found {len(matching_combinations)} symbol:timeframe combinations to process")
                
                # ✅ Step 1: Fetch 1-min data for unique tokens (PARALLEL)
                unique_base_tokens = list(set(m['base_token'] for m in matching_combinations))
                token_data_cache = {}
                
                async def fetch_token_data(base_token):
                    try:
                        df_1min = await fetch_candles_from_date(redis_client, base_token, start_date=None, last_n_candles=500)
                        if df_1min is None or df_1min.empty or len(df_1min) < 2:
                            return None
                        return (base_token, df_1min)
                    except Exception as e:
                        logger.error(f"[FETCH][ERROR] token={base_token}: {e}")
                        return None
                
                # Parallel fetch all tokens
                fetch_tasks = [fetch_token_data(bt) for bt in unique_base_tokens]
                fetch_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
                
                for result in fetch_results:
                    if result and not isinstance(result, Exception):
                        base_token, df_1min = result
                        token_data_cache[base_token] = df_1min
                
                logger.info(f"[MAIN_LOOP] Fetched data for {len(token_data_cache)} tokens")
                
                # Filter to only combinations with data
                matching_combinations = [m for m in matching_combinations if m['base_token'] in token_data_cache]
                
                if not matching_combinations:
                    continue
                
                # ✅ Step 2: Incremental resample for higher timeframes (PARALLEL)
                resample_combinations = [m for m in matching_combinations if m['interval'] != '1minute']
                
                if resample_combinations:
                    async def do_resample(m):
                        try:
                            df_1min = token_data_cache[m['base_token']]
                            last_row = df_1min.iloc[-1]
                            candle = {
                                'timestamp': last_row['timestamp'],
                                'open': float(last_row['open']),
                                'high': float(last_row['high']),
                                'low': float(last_row['low']),
                                'close': float(last_row['close']),
                                'volume': int(last_row['volume']) if 'volume' in last_row else 0
                            }
                            await update_incremental_resample(m['symbol'], m['interval'], candle, df_1min_all=df_1min)
                            return True
                        except Exception as e:
                            logger.error(f"[RESAMPLE][ERROR] {m['symbol']}:{m['interval']}: {e}")
                            return None
                    
                    resample_tasks = [do_resample(m) for m in resample_combinations]
                    await asyncio.gather(*resample_tasks, return_exceptions=True)
                    logger.info(f"[MAIN_LOOP] Completed {len(resample_combinations)} resample updates")
                
                # ✅ Step 3: 1-minute DB save and Redis store (PARALLEL)
                one_min_combinations = [m for m in matching_combinations if m['interval'] == '1minute']
                
                if one_min_combinations:
                    async def do_1min_save(m):
                        try:
                            df_1min = token_data_cache[m['base_token']]
                            if db:
                                await save_1min_candle_to_database(m['symbol'], df_1min, db)
                            await store_resampled_data_in_redis(redis_client, m['symbol'], m['interval'], df_1min, only_latest=True)
                            return True
                        except Exception as e:
                            logger.error(f"[1MIN][ERROR] {m['symbol']}: {e}")
                            return None
                    
                    save_tasks = [do_1min_save(m) for m in one_min_combinations]
                    await asyncio.gather(*save_tasks, return_exceptions=True)
                    logger.info(f"[MAIN_LOOP] Saved {len(one_min_combinations)} 1-minute candles")
                
                # ✅ Step 4: Alert processing for ALL combinations (PARALLEL BATCHES)
                async def process_alert(m):
                    try:
                        await alert_engine.process_symbol(m['symbol'], m['base_token'], m['interval'], m['basket_id'], redis_client=redis_client)
                        return True
                    except Exception as e:
                        logger.error(f"[ALERT][ERROR] {m['symbol']}:{m['interval']}: {e}")
                        return None
                
                # Sort by interval ascending so shorter TFs (1min) are processed first.
                # This ensures time-sensitive alerts aren't delayed behind longer TF work.
                matching_combinations.sort(key=lambda m: m['interval_mins'])
                
                BATCH_SIZE = 30
                total_batches = (len(matching_combinations) + BATCH_SIZE - 1) // BATCH_SIZE
                
                logger.info(f"[BATCH] Processing {len(matching_combinations)} alert tasks in {total_batches} batches")
                
                for i in range(0, len(matching_combinations), BATCH_SIZE):
                    batch = matching_combinations[i:i + BATCH_SIZE]
                    batch_num = i // BATCH_SIZE + 1
                    batch_info = [f"{m['symbol']}:{m['interval']}" for m in batch]
                    
                    logger.info(f"[BATCH] Executing batch {batch_num}/{total_batches} | Tasks: {len(batch)}")
                    logger.debug(f"[BATCH] Batch {batch_num} symbols: {', '.join(batch_info[:5])}{'...' if len(batch_info) > 5 else ''}")
                    
                    batch_start = time.time()
                    batch_tasks = [process_alert(m) for m in batch]
                    results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    batch_duration = time.time() - batch_start
                    
                    success = sum(1 for r in results if r is True)
                    errors = sum(1 for r in results if isinstance(r, Exception))
                    
                    logger.info(f"[BATCH] Batch {batch_num}/{total_batches} | Duration: {batch_duration:.2f}s | Success: {success} | Errors: {errors}")
                    
                    # Log any errors from the batch
                    for idx, result in enumerate(results):
                        if isinstance(result, Exception):
                            task_info = batch_info[idx] if idx < len(batch_info) else f"task_{idx}"
                            logger.error(f"[BATCH][ERROR] Batch {batch_num} task {task_info}: {result}")

                
                # Trim last_processed map to avoid unbounded growth
                if len(last_processed) > 5000:
                    cutoff = current_time - timedelta(days=2)
                    last_processed = {k: v for k, v in last_processed.items() 
                                   if hasattr(v, 'to_pydatetime') and v.to_pydatetime() > cutoff}
                
                cycle_duration = time.time() - cycle_start
                logger.info(f"[MAIN_LOOP] Cycle complete | Duration: {cycle_duration:.2f}s | Combinations: {len(matching_combinations)}")
                await asyncio.sleep(0.05)  # avoid hammering
                
            except Exception as e:
                logger.error(f"[MAIN_LOOP][ERROR] Error in main loop: {e}")
                await asyncio.sleep(1)  # Wait before retrying
                
    except KeyboardInterrupt:
        logger.info("[SHUTDOWN] Shutting down gracefully...")
    except Exception as e:
        logger.error(f"[SHUTDOWN][ERROR] Critical error in main: {e}")
    finally:
        if redis_client:
            await redis_client.close()

if __name__ == "__main__":
    asyncio.run(main())