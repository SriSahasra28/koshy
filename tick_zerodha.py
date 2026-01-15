# Download Realtime Tick Data from Zerodha - Improved Version
import asyncio
import os
import json
import logging
import signal
import sys
import time
import warnings
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import pandas as pd
import redis
from kiteconnect import KiteTicker, KiteConnect
import threading
# Local imports
from background.database import DBHelper
from background.login import login
import mysql.connector
from sqlalchemy import create_engine

from pathlib import Path
# Suppress warnings
warnings.filterwarnings('ignore')

# Configuration
class Config:
    """Configuration management for the application"""
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379
    REDIS_DB = 0
    INSTRUMENTS_FILE = str((Path(__file__).parent / 'scan_instruments.csv').resolve())
    LOG_FILE = 'tick_zerodha_logs.log'
    # Use Windows path: C:\Users\Administrator\Desktop\python\koshy_python\logs
    # Falls back to Linux path if Windows path fails
    LOG_DIR = os.path.join('C:', 'Users', 'Administrator', 'Desktop', 'python', 'koshy_python', 'logs')
    MARKET_CLOSE_HOUR = 15
    MARKET_CLOSE_MINUTE = 31
    OHLC_CLOSE_UPDATE_SECOND = 50
    
    # Database configuration
    DB_CONFIG = {
        'user': 'root',
        'password': 'Airforce*123',
        'host': '103.160.145.141',
        'port': '3306',
        'database': 'algo'
    }

    # Redis key patterns
    OHLC_KEY_PREFIX = "ohlc:"
    OHLC_SORTED_KEY_PREFIX = "ohlc_sorted:"
    OHLC_READY_QUEUE = "ohlc_ready"
    OHLC_PUSH_QUEUE = "ohlc_push"


# Enhanced logging configuration
def setup_logging():
    """Setup comprehensive logging configuration"""
    # Determine logs directory with fallback
    abs_logs_dir = None
    log_file_path = None
    
    try:
        # Primary: Use Windows path
        abs_logs_dir = os.path.join('C:', 'Users', 'Administrator', 'Desktop', 'python', 'koshy_python', 'logs')
        os.makedirs(abs_logs_dir, exist_ok=True)
        log_file_path = os.path.join(abs_logs_dir, Config.LOG_FILE)
        print(f"[LOG] Attempting to create log file at: {log_file_path}")
    except Exception as e:
        print(f"[LOG][ERROR] Failed to create Windows log directory: {e}")
        # Fallback to Linux path if Windows path fails
        try:
            abs_logs_dir = '/home/daksh/Desktop/KOSHY_ALGO_SYSTEM/logs'
            os.makedirs(abs_logs_dir, exist_ok=True)
            log_file_path = os.path.join(abs_logs_dir, Config.LOG_FILE)
            print(f"[LOG] Using Linux fallback path: {log_file_path}")
        except Exception as e2:
            print(f"[LOG][ERROR] Failed to create Linux log directory: {e2}")
            # Final fallback - use Config.LOG_DIR or current directory
            try:
                log_dir = Config.LOG_DIR
                os.makedirs(log_dir, exist_ok=True)
                log_file_path = os.path.join(log_dir, Config.LOG_FILE)
                print(f"[LOG] Using Config.LOG_DIR fallback: {log_file_path}")
            except Exception as e3:
                print(f"[LOG][ERROR] Failed to use Config.LOG_DIR: {e3}")
                log_file_path = Config.LOG_FILE
                print(f"[LOG] Using current directory fallback: {log_file_path}")
    
    # Configure logging
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    # File handler (absolute path) with error handling
    try:
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        print(f"[LOG] Successfully created file handler for: {log_file_path}")
    except Exception as e:
        print(f"[LOG][CRITICAL] Failed to create file handler: {e}")
        # Use a basic file handler in current directory as last resort
        try:
            file_handler = logging.FileHandler(Config.LOG_FILE, encoding='utf-8')
            print(f"[LOG] Using fallback file handler in current directory")
        except Exception as e2:
            print(f"[LOG][CRITICAL] Cannot create any file handler: {e2}")
            raise
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.__stdout__)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"tick_zerodha logging initialized -> {log_file_path}")
    return logger

class DatabaseHandler:
    """Handle database operations for fetching instrument data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.config = Config.DB_CONFIG
    
    def get_sqlalchemy_engine(self):
        config = Config.DB_CONFIG
        url = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        return create_engine(url)

    def fetch_and_store_tables(self, tables: List[str]) -> Dict[str, pd.DataFrame]:
        """Fetch selected tables from MySQL and return DataFrames"""
        self.logger.info("Starting database connection using SQLAlchemy engine")
        engine = self.get_sqlalchemy_engine()

        dataframes = {}

        for table_name in tables:
            try:
                query = f"SELECT * FROM {table_name}"
                self.logger.info(f"Fetching data from table: {table_name}")
                df = pd.read_sql(query, con=engine)
                dataframes[table_name] = df
                self.logger.info(f"Fetched {len(df)} rows from {table_name}")
            except Exception as e:
                self.logger.error(f"Error fetching table {table_name}: {str(e)}", exc_info=True)

        return dataframes
    
    def create_scan_instruments_csv(self, force_refresh: bool = True) -> bool:
        """Create scan_instruments.csv file from database data.

        If force_refresh is True, the CSV is overwritten even if it exists.
        """
        try:
            self.logger.info("Creating scan_instruments.csv file from database...")
            
            # List of tables to fetch
            tables_to_fetch = ['scans', 'filter_options']
            
            # Fetch data from database
            tables_data = self.fetch_and_store_tables(tables_to_fetch)
            
            # Process scans table
            scans_df = tables_data['scans']
            basket_id_at_scans = scans_df['basket_id'].unique().tolist()
            self.logger.info(f"Found {len(basket_id_at_scans)} unique basket IDs from scans table")
            
            # Process filter_options table
            filter_options = tables_data['filter_options']
            filtered_df = filter_options[filter_options['basket_id'].isin(basket_id_at_scans)]
            
            # Ensure parent directory exists (cross-platform)
            csv_path = Path(Config.INSTRUMENTS_FILE)
            csv_path.parent.mkdir(parents=True, exist_ok=True)

            # Optionally remove existing file to guarantee refresh
            if force_refresh and csv_path.exists():
                try:
                    csv_path.unlink()
                except Exception as e:
                    self.logger.warning(f"Could not remove existing {csv_path}: {e}")

            # Save to CSV (overwrite if exists)
            filtered_df.to_csv(str(csv_path), index=False)
            self.logger.info(f"scan_instruments.csv created successfully with {len(filtered_df)} records")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating scan_instruments.csv: {str(e)}", exc_info=True)
            return False

class ZerodhaTickHandler:
    """Enhanced tick data handler with better error handling and logging"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = DBHelper()
        self.login_handler = login(False)
        self.kws: Optional[KiteTicker] = None
        self.kite: Optional[KiteConnect] = None
        self.access_token: Optional[str] = None
        self.tokens: List[int] = []
        self.token_to_symbol: Dict[int, str] = {}
        self.redis_client: Optional[redis.Redis] = None
        self.is_market_open = True
        # Clear Redis queues
        # self.redis_client.delete(Config.OHLC_READY_QUEUE)
        # self.redis_client.delete(Config.OHLC_PUSH_QUEUE)

        # Preload historical data
        
    def initialize_connections(self) -> bool:
        """Initialize all connections and dependencies"""
        try:
            # Set working directory
            current_directory = os.path.dirname(os.path.abspath(__file__))
            os.chdir(current_directory)
            self.logger.info(f"Working directory set to: {os.getcwd()}")
            
            # Initialize Zerodha connection
            self.logger.info("Initializing Zerodha connection...")
            status, self.kite, self.kws, self.access_token = self.login_handler.InitiateZerodha()
            
            if not status:
                self.logger.error("Failed to initialize Zerodha connection")
                return False
                
            self.logger.info(f"Zerodha connection initialized successfully {self.access_token}")
            
            # Initialize Redis connection
            self.logger.info("Initializing Redis connection...")
            self.redis_client = redis.Redis(
                host=Config.REDIS_HOST, 
                port=Config.REDIS_PORT, 
                db=Config.REDIS_DB,
                decode_responses=False  # Keep as bytes for consistency
            )
            
            # Test Redis connection
            self.redis_client.ping()
            self.logger.info("Redis connection established successfully")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during initialization: {str(e)}", exc_info=True)
            return False
    
    def load_instruments(self) -> bool:
        """Load instrument tokens from CSV file"""
        try:
            self.logger.info(f"Loading instruments from: {Config.INSTRUMENTS_FILE}")
            
            if not os.path.exists(Config.INSTRUMENTS_FILE):
                self.logger.error(f"Instruments file not found: {Config.INSTRUMENTS_FILE}")
                return False
            
            df = pd.read_csv(Config.INSTRUMENTS_FILE)
            
            if 'instrument_token' not in df.columns:
                self.logger.error("Column 'instrument_token' not found in CSV file")
                return False
                
            self.tokens = df['instrument_token'].tolist()
            # Build token->symbol map for readable logging
            if 'option_name' in df.columns:
                tmp_map = df[['instrument_token', 'option_name']].dropna()
                self.token_to_symbol = {int(r.instrument_token): str(r.option_name) for _, r in tmp_map.iterrows()}
            else:
                self.token_to_symbol = {int(t): str(t) for t in self.tokens}
            self.logger.info(f"Loaded {len(self.tokens)} instrument tokens: {self.tokens}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading instruments: {str(e)}", exc_info=True)
            return False
    
    def initialize_redis_data(self) -> bool:
        """Initialize Redis data structures for all tokens"""
        try:
            self.logger.info("Initializing Redis data structures...")
            now = datetime.now()
            current_minute = now.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            
            for token in self.tokens:
                redis_key = f"{Config.OHLC_KEY_PREFIX}{token}"
                
                # Check if data already exists for this minute
                if self.redis_client.exists(redis_key):
                    existing_minute = self.redis_client.hget(redis_key, "current_minute")
                    if existing_minute:
                        existing_minute = existing_minute.decode()
                        if existing_minute == current_minute:
                            self.logger.debug(f"Data already exists for token {token} at minute {current_minute}")
                            continue
                
                # Initialize with placeholder values - will be set with first tick
                self.redis_client.hset(redis_key, mapping={
                    "open": 0,
                    "low": 0,
                    "high": 0,
                    "close": 0,
                    "volume": 0,
                    "current_minute": current_minute,
                    "tick_count": 0
                })
                self.logger.debug(f"Initialized Redis data for token: {token}")
            
            self.logger.info(f"Redis data structures initialized for {len(self.tokens)} tokens")
            return True
            
        except Exception as e:
            self.logger.error(f"Error initializing Redis data: {str(e)}", exc_info=True)
            return False
    
    def setup_websocket_handlers(self):
        """Setup WebSocket event handlers"""
        self.logger.info("Setting up WebSocket handlers...")
        
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_reconnect = self.on_reconnect
        self.kws.on_noreconnect = self.on_noreconnect
        
        self.logger.info("WebSocket handlers configured")
    
    def is_market_closed(self) -> bool:
        """Check if market is closed"""
        now = datetime.now()
        return (now.hour == Config.MARKET_CLOSE_HOUR and now.minute >= Config.MARKET_CLOSE_MINUTE) or (now.hour >= 16)
    
    def graceful_shutdown(self, ws):
        """Perform graceful shutdown"""
        self.logger.info("Initiating graceful shutdown...")
        self.is_market_open = False
        
        try:
            if hasattr(ws, 'is_connected') and ws.is_connected():
                ws.stop()
                self.logger.info("WebSocket connection stopped")
        except Exception as e:
            self.logger.error(f"Error stopping WebSocket: {str(e)}")
        finally:
            self.logger.info("Sending SIGTERM to current process")
            os.kill(os.getpid(), signal.SIGTERM)
    
    def process_ohlc_data(self, instrument_code: int, last_price: float, volume: int, now: datetime):
        """Process OHLC data for a given instrument"""
        try:
            redis_key = f"{Config.OHLC_KEY_PREFIX}{instrument_code}"
            
            # Get current minute boundary (rounded down to minute)
            current_minute_dt = now.replace(second=0, microsecond=0)
            current_minute_str = current_minute_dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # Fetch stored minute data
            stored_data = self.redis_client.hgetall(redis_key)
            if not stored_data:
                # First tick for this instrument
                self.logger.info(f"First tick for instrument {instrument_code}, initializing...")
                self.initialize_new_minute(redis_key, last_price, current_minute_str, volume)
                return
            
            # Decode stored data
            stored_minute_str = stored_data.get(b"current_minute", b"").decode()
            
            if not stored_minute_str:
                self.logger.warning(f"No stored minute for instrument {instrument_code}, reinitializing...")
                self.initialize_new_minute(redis_key, last_price, current_minute_str, volume)
                return
            
            stored_minute_dt = datetime.strptime(stored_minute_str, "%Y-%m-%d %H:%M:%S")
            
            # Check if we've moved to a new minute
            if stored_minute_dt < current_minute_dt:
                self.logger.info(f"New minute detected for instrument {instrument_code}: {stored_minute_str} -> {current_minute_str}")
                
                # Finalize previous minute first
                if stored_minute_dt is not None:
                    self.finalize_previous_minute(instrument_code, redis_key, stored_data, stored_minute_dt)
                
                # Initialize new minute
                self.initialize_new_minute(redis_key, last_price, current_minute_str, volume)
            else:
                # Update ongoing minute
                self.update_ongoing_minute(redis_key, last_price, volume, stored_data)
                
        except Exception as e:
            self.logger.error(f"Error processing OHLC data for instrument {instrument_code}: {str(e)}", exc_info=True)
    
    async def preload_historical_data(self):
        """IMPROVED: Preload historical data - DATABASE FIRST, then Redis cache"""
        self.logger.info("Starting Database-First Historical Data Preloading...")

        end_date = datetime.now()
        start_date = end_date - timedelta(days=28)
        
        if not os.path.exists(Config.INSTRUMENTS_FILE):
            self.logger.error(f"Instruments file not found: {Config.INSTRUMENTS_FILE}")
            return False
        
        df = pd.read_csv(Config.INSTRUMENTS_FILE)
        
        if 'instrument_token' not in df.columns:
            self.logger.error("Column 'instrument_token' not found in CSV file")
            return False
            
        self.tokens = df['instrument_token'].tolist()
        
        # Initialize async database connection
        from background.async_db import dbconnection
        db = dbconnection()
        loop = asyncio.get_event_loop()
        await db.create_pool(loop=loop)
        
        try:
            # STEP 1: Fill database with historical data from Zerodha API
            self.logger.info("Step 1: Filling database with historical data from Zerodha API...")
            await self._fill_database_with_historical_data(df, start_date, end_date, db)
            
            # STEP 2: Incrementally load historical data from database into Redis cache
            # If Redis key exists, only loads new data after last timestamp
            # If Redis key doesn't exist, loads last 30 days (default)
            self.logger.info("Step 2: Incrementally loading historical data from database into Redis...")
            await self._load_historical_data_from_database_to_redis(df, db, start_date=None)
            
            self.logger.info("Database-First Historical Data Preloading Completed!")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in database-first preloading: {e}")
            return False
        finally:
            # Close database connection
            await db.close_pool()

    async def _fill_database_with_historical_data(self, df, start_date, end_date, db):
        """Fill database with historical data from Zerodha API"""
        db_semaphore = asyncio.Semaphore(2)  # Limit concurrent operations
        
        async def process_single_token(token):
            try:
                # Get symbol for this token
                symbol_row = df[df['instrument_token'] == token]
                if not symbol_row.empty and 'option_name' in symbol_row.columns:
                    opt_name = str(symbol_row['option_name'].iloc[0])
                    if opt_name and opt_name != 'nan':
                        symbol = opt_name
                    else:
                        self.logger.warning(f"Skipping token {token}: missing valid option_name")
                        return
                else:
                    self.logger.warning(f"Skipping token {token}: option_name not available")
                    return
                
                # Fetch historical data from Zerodha API
                self.logger.info(f"Fetching historical data for {symbol} (token {token}) from Zerodha API...")
                candles = self.kite.historical_data(
                    instrument_token=token,
                    from_date=start_date,
                    to_date=end_date,
                    interval="minute",
                    continuous=False
                )
                
                if not candles:
                    self.logger.warning(f"No historical data found for {symbol} (token {token})")
                    return
                
                # Prepare batch data for database insertion
                batch_data = []
                for candle in candles:
                    dt = candle["date"]
                    if isinstance(dt, str):
                        dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
                    else:
                        dt = dt.replace(tzinfo=None)
                    
                    batch_data.append((
                        symbol,
                        dt,
                        float(round(candle["open"], 2)),
                        float(round(candle["high"], 2)),
                        float(round(candle["low"], 2)),
                        float(round(candle["close"], 2))
                    ))

                # Insert to database with duplicate checking
                if batch_data:
                    try:
                        async with db_semaphore:
                            # Check which records already exist in database
                            existing_records = await db.get_existing_ohlc_records(symbol, [row[1] for row in batch_data])
                            existing_timestamps = set(existing_records) if existing_records else set()
                            
                            # Filter out records that already exist
                            new_batch_data = [row for row in batch_data if row[1] not in existing_timestamps]
                            
                            if new_batch_data:
                                # Insert only new records in chunks with retry logic
                                chunk_size = 1000
                                total_inserted = 0
                                max_retries = 3
                                
                                for start in range(0, len(new_batch_data), chunk_size):
                                    end = min(start + chunk_size, len(new_batch_data))
                                    chunk = new_batch_data[start:end]
                                    
                                    retry_count = 0
                                    while retry_count < max_retries:
                                        try:
                                            await db.insert_one_min_batch_ohlc_simple(chunk)
                                            total_inserted += len(chunk)
                                            break
                                        except Exception as e:
                                            if "deadlock" in str(e).lower() or "1213" in str(e):
                                                retry_count += 1
                                                if retry_count < max_retries:
                                                    await asyncio.sleep(0.1 * retry_count)
                                                continue
                                                raise e
                                    
                                    if retry_count >= max_retries:
                                        self.logger.error(f"Failed to insert chunk for {symbol} after {max_retries} retries")
                                        break
                                
                                self.logger.info(f"Inserted {total_inserted} new records for {symbol} (token {token})")
                            else:
                                self.logger.info(f"No new records to insert for {symbol} (token {token})")
                                
                    except Exception as e:
                        self.logger.error(f"Database error for {symbol} (token {token}): {e}")
                        return
                
                self.logger.info(f"Successfully processed {symbol} (token {token})")

            except Exception as e:
                self.logger.error(f"Error processing token {token}: {e}")
                return
        
        # Process all tokens in parallel
        tasks = [process_single_token(token) for token in self.tokens]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _load_historical_data_from_database_to_redis(self, df, db, start_date: Optional[datetime] = None):
        """Load historical data from database into Redis cache (INCREMENTAL).
        
        Strategy:
        1. Check if Redis key exists
        2. If exists, get last timestamp and only load data after that timestamp
        3. If key doesn't exist, load from start_date (or 30 days ago if start_date is None)
        """
        async def load_single_token(token):
            try:
                # Get symbol for this token
                symbol_row = df[df['instrument_token'] == token]
                if not symbol_row.empty and 'option_name' in symbol_row.columns:
                    opt_name = str(symbol_row['option_name'].iloc[0])
                    if opt_name and opt_name != 'nan':
                        symbol = opt_name
                    else:
                        return
                else:
                    return
                
                sorted_set_key = f"{Config.OHLC_SORTED_KEY_PREFIX}{token}"
                
                # Check if Redis key exists and get last timestamp
                last_timestamp = None
                key_exists = self.redis_client.exists(sorted_set_key)
                
                if key_exists:
                    # Get the last (most recent) entry from sorted set
                    try:
                        last_entries = self.redis_client.zrange(sorted_set_key, -1, -1)
                        if last_entries:
                            last_data = json.loads(last_entries[0])
                            last_timestamp_str = last_data.get('timestamp')
                            if last_timestamp_str:
                                last_timestamp = datetime.strptime(last_timestamp_str, "%Y-%m-%d %H:%M:%S")
                                self.logger.info(f"Redis key exists for {symbol} (token {token}). Last timestamp: {last_timestamp}")
                    except Exception as e:
                        self.logger.warning(f"Error reading last timestamp from Redis for {symbol} (token {token}): {e}")
                        # If we can't read, treat as missing and load from start_date
                        last_timestamp = None
                
                # Determine query start date
                query_start_date = None
                if last_timestamp:
                    # Key exists: load only data after last timestamp
                    query_start_date = last_timestamp
                    self.logger.info(f"Incremental load for {symbol} (token {token}): loading data after {query_start_date}")
                elif start_date is not None:
                    # Key doesn't exist but start_date provided: use start_date
                    query_start_date = start_date
                    self.logger.info(f"Initial load for {symbol} (token {token}): loading data from {query_start_date}")
                else:
                    # Key doesn't exist and no start_date: default to 30 days ago
                    query_start_date = datetime.now() - timedelta(days=30)
                    query_start_date = query_start_date.replace(hour=9, minute=15, second=0, microsecond=0)
                    self.logger.info(f"Initial load for {symbol} (token {token}): loading data from {query_start_date} (default 30 days)")
                
                # Query database for historical data
                db_data = []
                async with db.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        try:
                            await cur.execute(
                                """
                                SELECT datetime, open, high, low, close 
                                FROM one_min_ohlc 
                                WHERE symbol = %s AND datetime > %s
                                ORDER BY datetime
                                """,
                                (symbol, query_start_date)
                            )
                            db_data = await cur.fetchall()
                        except Exception as e:
                            self.logger.error(f"Error querying database for {symbol}: {e}")
                
                if not db_data:
                    if last_timestamp:
                        self.logger.info(f"No new data in database for {symbol} (token {token}) since {last_timestamp}")
                    else:
                        self.logger.warning(f"No historical data found in database for {symbol} (token {token}) from {query_start_date}")
                    return
                
                self.logger.info(f"Found {len(db_data)} new records in database for {symbol} (token {token})")
                
                # Load data into Redis cache (filter out pre-market and post-market candles)
                loaded_count = 0
                skipped_count = 0
                duplicate_count = 0
                
                for row in db_data:
                    dt, open_price, high_price, low_price, close_price = row
                    
                    # Filter out pre-market and post-market candles (only store 9:15 to 15:29)
                    hour = dt.hour
                    minute = dt.minute
                    
                    # Skip if before market open (9:15) or after market close (15:29)
                    if hour < 9 or (hour == 9 and minute < 15) or hour > 15 or (hour == 15 and minute > 29):
                        skipped_count += 1
                        continue
                    
                    # Check if entry already exists (avoid duplicates)
                    timestamp_score = int(dt.timestamp())
                    existing = self.redis_client.zrangebyscore(sorted_set_key, timestamp_score, timestamp_score)
                    if existing:
                        duplicate_count += 1
                        continue  # Skip if already exists
                    
                    ohlc = {
                        "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "open": round(open_price, 2),
                        "high": round(high_price, 2),
                        "low": round(low_price, 2),
                        "close": round(close_price, 2),
                        "volume": 0  # Volume not stored in database
                    }
                    
                    # Add to Redis sorted set
                    self.redis_client.zadd(sorted_set_key, {json.dumps(ohlc): timestamp_score})
                    loaded_count += 1
                
                # Set 15-day TTL on ohlc_sorted key (refresh TTL on each update)
                try:
                    self.redis_client.expire(sorted_set_key, 15 * 24 * 60 * 60)  # 15 days in seconds
                except Exception as ttl_err:
                    self.logger.warning(f"Failed to set TTL for {sorted_set_key}: {ttl_err}")
                
                if loaded_count > 0:
                    self.logger.info(f"✅ Loaded {loaded_count} new records (skipped {skipped_count} pre/post-market, {duplicate_count} duplicates) for {symbol} (token {token}) into Redis cache")
                else:
                    self.logger.info(f"ℹ️ No new records to load for {symbol} (token {token}) - Redis already up to date")
                
            except Exception as e:
                self.logger.error(f"Error loading data for token {token}: {e}")
                return
        
        # Process all tokens in parallel
        tasks = [load_single_token(token) for token in self.tokens]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def get_historical_data_from_database(self, symbol, start_date=None, end_date=None):
        """Get historical data from database for a specific symbol"""
        try:
            from background.async_db import dbconnection
            db = dbconnection()
            loop = asyncio.get_event_loop()
            await db.create_pool(loop=loop)
            
            try:
                async with db.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        if start_date and end_date:
                            await cur.execute("""
                                SELECT datetime, open, high, low, close 
                                FROM one_min_ohlc 
                                WHERE symbol = %s AND datetime BETWEEN %s AND %s
                                ORDER BY datetime
                            """, (symbol, start_date, end_date))
                        elif start_date and not end_date:
                            await cur.execute("""
                                SELECT datetime, open, high, low, close 
                                FROM one_min_ohlc 
                                WHERE symbol = %s AND datetime >= %s
                                ORDER BY datetime
                            """, (symbol, start_date))
                        else:
                            await cur.execute("""
                                SELECT datetime, open, high, low, close 
                                FROM one_min_ohlc 
                                WHERE symbol = %s 
                                ORDER BY datetime
                            """, (symbol,))
                        
                        db_data = await cur.fetchall()
                
                return db_data
                
            finally:
                await db.close_pool()
                
        except Exception as e:
            self.logger.error(f"Error getting historical data from database for {symbol}: {e}")
            return []

    async def sync_redis_from_database(self, symbol, token, start_date: Optional[datetime] = None):
        """Sync Redis cache with database for a specific symbol.

        If start_date is provided, only sync rows since that date; otherwise sync all available.
        """
        try:
            # Get data from database
            db_data = await self.get_historical_data_from_database(symbol, start_date=start_date)
            
            if not db_data:
                self.logger.warning(f"No data found in database for {symbol}")
                return False
            
            # Clear existing Redis data for this token
            sorted_set_key = f"{Config.OHLC_SORTED_KEY_PREFIX}{token}"
            self.redis_client.delete(sorted_set_key)
            
            # Load fresh data from database into Redis (filter out pre-market and post-market candles)
            loaded_count = 0
            skipped_count = 0
            
            for row in db_data:
                dt, open_price, high_price, low_price, close_price = row
                
                # Filter out pre-market and post-market candles (only store 9:15 to 15:29)
                hour = dt.hour
                minute = dt.minute
                
                # Skip if before market open (9:15) or after market close (15:29)
                if hour < 9 or (hour == 9 and minute < 15) or hour > 15 or (hour == 15 and minute > 29):
                    skipped_count += 1
                    continue
                
                ohlc = {
                    "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "open": round(open_price, 2),
                    "high": round(high_price, 2),
                    "low": round(low_price, 2),
                    "close": round(close_price, 2),
                    "volume": 0
                }
                
                # Add to Redis sorted set
                self.redis_client.zadd(sorted_set_key, {json.dumps(ohlc): int(dt.timestamp())})
                loaded_count += 1
            
            # Set 15-day TTL on ohlc_sorted key (auto-delete after 15 days)
            try:
                self.redis_client.expire(sorted_set_key, 15 * 24 * 60 * 60)  # 15 days in seconds
            except Exception as ttl_err:
                self.logger.warning(f"Failed to set TTL for {sorted_set_key}: {ttl_err}")
            
            self.logger.info(f"Synced {loaded_count} records (skipped {skipped_count} pre/post-market) for {symbol} (token {token}) from database to Redis")
            return True
            
        except Exception as e:
            self.logger.error(f"Error syncing Redis from database for {symbol}: {e}")
            return False
                
    def finalize_previous_minute(self, instrument_code: int, redis_key: str, stored_data: dict, stored_minute_dt: datetime):
        """Finalize OHLC data for the previous minute"""
        try:
            # Parse stored data safely
            open_price = float(stored_data.get(b"open", b"0").decode())
            high_price = float(stored_data.get(b"high", b"0").decode())
            low_price = float(stored_data.get(b"low", b"0").decode())
            close_price = float(stored_data.get(b"close", b"0").decode())
            total_volume = int(stored_data.get(b"volume", b"0").decode())
            
            # Debug logging for problematic instruments
            if open_price == 0 or high_price == 0 or low_price == 0:
                self.logger.debug(f"Raw data for instrument {instrument_code}: {dict(stored_data)}")
                self.logger.debug(f"Parsed prices: O={open_price}, H={high_price}, L={low_price}, C={close_price}")
            
            # Validate data integrity - more flexible validation
            if (open_price <= 0 or high_price <= 0 or low_price <= 0 or 
                open_price > 1000000 or high_price > 1000000 or low_price > 1000000):
                self.logger.warning(f"Invalid OHLC data for instrument {instrument_code}: O={open_price}, H={high_price}, L={low_price}, skipping finalization")
                return
            
            # Check for reasonable price relationships
            if high_price < low_price or high_price < open_price or low_price > open_price:
                self.logger.warning(f"Invalid price relationships for instrument {instrument_code}: O={open_price}, H={high_price}, L={low_price}, skipping finalization")
                return
            
            # Use the last known price as close if close is still 0 or invalid
            if close_price <= 0:
                close_price = high_price  # Use high as fallback
                self.logger.warning(f"No close price recorded for instrument {instrument_code}, using high price: {close_price}")
            
            # Final validation for close price
            if close_price <= 0 or close_price > 1000000:
                self.logger.warning(f"Invalid close price for instrument {instrument_code}: {close_price}, skipping finalization")
                return
            
            # Filter out pre-market and post-market candles (only store 9:15 to 15:29)
            hour = stored_minute_dt.hour
            minute = stored_minute_dt.minute
            
            # Skip if before market open (9:15) or after market close (15:29)
            if hour < 9 or (hour == 9 and minute < 15) or hour > 15 or (hour == 15 and minute > 29):
                symbol = self.token_to_symbol.get(int(instrument_code), str(instrument_code))
                self.logger.info(f"[SKIP] symbol={symbol} ts={stored_minute_dt.strftime('%Y-%m-%d %H:%M:%S')} - outside market hours (9:15-15:29)")
                return
            
            final_ohlc = {
                "timestamp": stored_minute_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": total_volume
            }
            
            # Store in sorted set with minute timestamp as score
            sorted_set_key = f"{Config.OHLC_SORTED_KEY_PREFIX}{instrument_code}"
            timestamp_score = int(stored_minute_dt.timestamp())
            # Remove any existing entry with this score (unique constraint on score)
            self.redis_client.zremrangebyscore(sorted_set_key, timestamp_score, timestamp_score)
            # Add new entry with unique score
            self.redis_client.zadd(sorted_set_key, {json.dumps(final_ohlc): timestamp_score})
            # Set 15-day TTL on ohlc_sorted key (refresh TTL on each update)
            try:
                self.redis_client.expire(sorted_set_key, 15 * 24 * 60 * 60)  # 15 days in seconds
            except Exception as ttl_err:
                self.logger.warning(f"Failed to set TTL for {sorted_set_key}: {ttl_err}")
            # Verify only one entry exists with this score
            count = self.redis_client.zcount(sorted_set_key, timestamp_score, timestamp_score)
            if count > 1:
                self.logger.warning(f"Duplicate entries detected for score {timestamp_score} in {sorted_set_key}, count: {count}")
            
        
            # Add to processing queues
            self.redis_client.rpush(Config.OHLC_READY_QUEUE, instrument_code)
            self.redis_client.rpush(Config.OHLC_PUSH_QUEUE, instrument_code)

            symbol = self.token_to_symbol.get(int(instrument_code), str(instrument_code))
            self.logger.info(f"[FINALIZE] symbol={symbol} ts={final_ohlc['timestamp']} O={final_ohlc['open']} H={final_ohlc['high']} L={final_ohlc['low']} C={final_ohlc['close']} V={final_ohlc['volume']}")
            self.logger.info(f"[PUSH] symbol={symbol} ts={final_ohlc['timestamp']} queued=ohlc_ready,ohlc_push")
            # Minute separator marker for readability
            self.logger.info(f"----- [MINUTE] {final_ohlc['timestamp'][:16]} -----\n")
            
        except Exception as e:
            self.logger.error(f"Error finalizing previous minute for instrument {instrument_code}: {str(e)}", exc_info=True)
    

    def export_ohlc_to_csv_continuously(self, export_interval=60):
        """
        Continuously export OHLC data from Redis sorted sets to CSV files for each instrument.
        :param export_interval: Time in seconds between exports.
        """
        self.logger.info("Starting continuous OHLC export to CSV...")
        while True:
            try:
                for token in self.tokens:
                    sorted_set_key = f"{Config.OHLC_SORTED_KEY_PREFIX}{token}"
                    # Fetch all OHLC entries from Redis sorted set
                    ohlc_entries = self.redis_client.zrange(sorted_set_key, 0, -1)
                    if not ohlc_entries:
                        continue

                    # Parse JSON entries
                    ohlc_list = [json.loads(entry.decode()) for entry in ohlc_entries]
                    df = pd.DataFrame(ohlc_list)
                    csv_path = f"ohlc_{token}.csv"
                    df.to_csv(csv_path, index=False)
                    self.logger.debug(f"Exported {len(df)} rows to {csv_path}")

                time.sleep(export_interval)
            except Exception as e:
                self.logger.error(f"Error exporting OHLC to CSV: {str(e)}", exc_info=True)
                time.sleep(export_interval)

    def initialize_new_minute(self, redis_key: str, last_price: float, current_minute: str, volume: int):
        """Initialize OHLC data for new minute"""
        try:
            rounded_price = round(last_price, 2)
            
            # Initialize with first tick of the minute
            mapping = {
                "open": rounded_price,
                "low": rounded_price,
                "high": rounded_price,
                "close": rounded_price,  # Always keep close updated
                "volume": volume,
                "current_minute": current_minute,
                "tick_count": 1
            }
            
            self.redis_client.hset(redis_key, mapping=mapping)
            self.logger.debug(f"Initialized new minute at {current_minute} with price {rounded_price}")
            
        except Exception as e:
            self.logger.error(f"Error initializing new minute: {str(e)}", exc_info=True)
    
    def update_ongoing_minute(self, redis_key: str, last_price: float, volume: int, stored_data: dict):
        """Update OHLC data for ongoing minute"""
        try:
            # Get current values
            current_open = float(stored_data.get(b"open", b"0").decode())
            current_low = float(stored_data.get(b"low", b"999999").decode())
            current_high = float(stored_data.get(b"high", b"0").decode())
            current_volume = int(stored_data.get(b"volume", b"0").decode())
            current_tick_count = int(stored_data.get(b"tick_count", b"0").decode())
            
            rounded_price = round(last_price, 2)
            
            # Prepare update mapping
            update_mapping = {
                "close": rounded_price,  # Always update close with latest price
                "volume": current_volume + volume,
                "tick_count": current_tick_count + 1
            }
            
            # Update high if current price is higher
            if rounded_price > current_high:
                update_mapping["high"] = rounded_price
                self.logger.debug(f"New high: {rounded_price} (was {current_high})")
            
            # Update low if current price is lower
            if rounded_price < current_low:
                update_mapping["low"] = rounded_price
                self.logger.debug(f"New low: {rounded_price} (was {current_low})")
            
            # Apply updates atomically
            self.redis_client.hset(redis_key, mapping=update_mapping)
            
            # Log every 100th tick for monitoring
            if current_tick_count % 100 == 0:
                self.logger.debug(f"Processed {current_tick_count} ticks, current price: {rounded_price}")
                
        except Exception as e:
            self.logger.error(f"Error updating ongoing minute: {str(e)}", exc_info=True)
    
    def on_ticks(self, ws, ticks):
        """IMPROVED: Handle incoming tick data with better timing precision"""
        try:
            now = datetime.now()
            
            # Check if market is closed
            if self.is_market_closed():
                self.logger.info("Market closed, initiating shutdown...")
                self.graceful_shutdown(ws)
                return
            
            if not ticks:
                return
            
            # CRITICAL FIX: Use exchange timestamp if available, fallback to local time
            processed_ticks = []
            for tick_data in ticks:
                # Use exchange timestamp if available (more accurate)
                tick_timestamp = tick_data.get('exchange_timestamp', now)
                if isinstance(tick_timestamp, str):
                    tick_timestamp = datetime.strptime(tick_timestamp, "%Y-%m-%d %H:%M:%S")
                
                processed_tick = {
                    'instrument_token': tick_data.get('instrument_token', 0),
                    'price': tick_data.get('last_price', 0.0),
                    'volume': tick_data.get('volume_traded', 0),
                    'timestamp': tick_timestamp
                }
                processed_ticks.append(processed_tick)
            
            # Group by minute boundary for more accurate processing
            ticks_by_minute_and_instrument = {}
            
            for tick in processed_ticks:
                if tick['instrument_token'] == 0 or tick['price'] <= 0:
                    continue
                    
                minute_key = tick['timestamp'].replace(second=0, microsecond=0)
                instrument_key = (tick['instrument_token'], minute_key)
                
                if instrument_key not in ticks_by_minute_and_instrument:
                    ticks_by_minute_and_instrument[instrument_key] = []
                
                ticks_by_minute_and_instrument[instrument_key].append(tick)
            
            # Process each instrument-minute combination
            for (instrument_code, minute_dt), instrument_ticks in ticks_by_minute_and_instrument.items():
                try:
                    # Sort ticks by timestamp within the minute
                    instrument_ticks.sort(key=lambda x: x['timestamp'])
                    
                    minute_changed = self.check_minute_change(instrument_code, minute_dt)
                    self.process_instrument_ticks(instrument_code, instrument_ticks, minute_dt, minute_changed)
                    
                except Exception as e:
                    self.logger.error(f"Error processing ticks for instrument {instrument_code}: {str(e)}")
                    continue
                
        except Exception as e:
            self.logger.error(f"Error in on_ticks: {str(e)}", exc_info=True)
    
    def check_minute_change(self, instrument_code: int, current_minute_dt: datetime) -> bool:
        """Check if minute has changed for a specific instrument (thread-safe)"""
        try:
            redis_key = f"{Config.OHLC_KEY_PREFIX}{instrument_code}"
            stored_minute = self.redis_client.hget(redis_key, "current_minute")
            
            if not stored_minute:
                return True  # First tick for this instrument
            
            stored_minute_str = stored_minute.decode()
            stored_minute_dt = datetime.strptime(stored_minute_str, "%Y-%m-%d %H:%M:%S")
            
            # Note: Race condition is handled in process_ohlc_data where stored_data is checked again
            # before finalize_previous_minute is called, ensuring atomicity
            return stored_minute_dt < current_minute_dt
            
        except Exception as e:
            self.logger.error(f"Error checking minute change for instrument {instrument_code}: {str(e)}")
            return False
    
    def batch_update_ohlc_fixed(self, redis_key: str, ticks: list):
        """FIXED: Batch update OHLC data preserving open price"""
        try:
            if not ticks:
                return
            
            # Get current OHLC data
            current_data = self.redis_client.hgetall(redis_key)
            if not current_data:
                self.logger.warning(f"No existing data for batch update: {redis_key}")
                return
            
            # Parse current values
            current_open = float(current_data.get(b"open", b"0").decode())  # PRESERVE OPEN
            current_low = float(current_data.get(b"low", b"999999").decode())
            current_high = float(current_data.get(b"high", b"0").decode())
            current_volume = int(current_data.get(b"volume", b"0").decode())
            current_tick_count = int(current_data.get(b"tick_count", b"0").decode())
            
            # Process all ticks in batch
            total_volume = 0
            new_low = current_low
            new_high = current_high
            latest_price = current_open  # Default to current open if no ticks
            
            # Sort ticks by timestamp to process in correct order
            sorted_ticks = sorted(ticks, key=lambda x: x['timestamp'])
            
            for tick in sorted_ticks:
                price = round(tick['price'], 2)
                volume = tick['volume']
                
                total_volume += volume
                latest_price = price  # Last tick becomes close
                
                if price < new_low:
                    new_low = price
                if price > new_high:
                    new_high = price
            
            # CRITICAL: Single atomic update - NEVER change the open price
            update_mapping = {
                # "open": current_open,  # DON'T UPDATE - Redis already has correct open
                "low": new_low,
                "high": new_high,
                "close": latest_price,
                "volume": current_volume + total_volume,
                "tick_count": current_tick_count + len(ticks)
            }
            
            self.redis_client.hset(redis_key, mapping=update_mapping)
            
            self.logger.debug(f"Batch updated {len(ticks)} ticks: O={current_open} (preserved), H={new_high}, L={new_low}, C={latest_price}")
                
        except Exception as e:
            self.logger.error(f"Error in batch OHLC update: {str(e)}", exc_info=True)

    def process_instrument_ticks(self, instrument_code: int, ticks: list, current_minute_dt: datetime, minute_changed: bool):
        """Process all ticks for a specific instrument - FIXED VERSION"""
        try:
            redis_key = f"{Config.OHLC_KEY_PREFIX}{instrument_code}"
            current_minute_str = current_minute_dt.strftime("%Y-%m-%d %H:%M:%S")
            
            # Sort ticks by timestamp to ensure proper chronological order
            ticks.sort(key=lambda x: x['timestamp'])
            
            # Handle minute change
            if minute_changed:
                # Get existing data for finalization
                stored_data = self.redis_client.hgetall(redis_key)
                
                if stored_data:
                    stored_minute_str = stored_data.get(b"current_minute", b"").decode()
                    if stored_minute_str:
                        stored_minute_dt = datetime.strptime(stored_minute_str, "%Y-%m-%d %H:%M:%S")
                        self.finalize_previous_minute(instrument_code, redis_key, stored_data, stored_minute_dt)
                
                # CRITICAL FIX: Initialize new minute with the FIRST tick that belongs to new minute
                # Filter ticks that actually belong to the new minute
                new_minute_ticks = [tick for tick in ticks 
                                  if tick['timestamp'].replace(second=0, microsecond=0) == current_minute_dt]
                
                if new_minute_ticks:
                    first_tick = new_minute_ticks[0]  # First tick of the new minute
                    self.initialize_new_minute(redis_key, first_tick['price'], current_minute_str, first_tick['volume'])
                    
                    # Process remaining ticks from the new minute
                    remaining_ticks = new_minute_ticks[1:]
                else:
                    self.logger.warning(f"No ticks found for new minute {current_minute_str} for instrument {instrument_code}")
                    return
            else:
                # Process all ticks for ongoing minute
                remaining_ticks = ticks
            
            # Batch process remaining ticks for this instrument
            if remaining_ticks:
                self.batch_update_ohlc_fixed(redis_key, remaining_ticks)
                
        except Exception as e:
            self.logger.error(f"Error processing instrument ticks for {instrument_code}: {str(e)}")
    
    def batch_update_ohlc(self, redis_key: str, ticks: list):
        """Batch update OHLC data for multiple ticks of the same instrument"""
        try:
            if not ticks:
                return
            
            # Get current OHLC data
            current_data = self.redis_client.hgetall(redis_key)
            if not current_data:
                self.logger.warning(f"No existing data for batch update: {redis_key}")
                return
            
            # Parse current values
            current_low = float(current_data.get(b"low", b"999999").decode())
            current_high = float(current_data.get(b"high", b"0").decode())
            current_volume = int(current_data.get(b"volume", b"0").decode())
            current_tick_count = int(current_data.get(b"tick_count", b"0").decode())
            
            # Process all ticks in batch
            total_volume = 0
            new_low = current_low
            new_high = current_high
            latest_price = 0
            
            for tick in ticks:
                price = round(tick['price'], 2)
                volume = tick['volume']
                
                total_volume += volume
                latest_price = price
                
                if price < new_low:
                    new_low = price
                if price > new_high:
                    new_high = price
            
            # Single atomic update
            update_mapping = {
                "low": new_low,
                "high": new_high,
                "close": latest_price,
                "volume": current_volume + total_volume,
                "tick_count": current_tick_count + len(ticks)
            }
            
            self.redis_client.hset(redis_key, mapping=update_mapping)
            
            # Debug logging for high-frequency instruments
            if len(ticks) > 10:
                self.logger.debug(f"Batch updated {len(ticks)} ticks: L={new_low}, H={new_high}, C={latest_price}, V={total_volume}")
                
        except Exception as e:
            self.logger.error(f"Error in batch OHLC update: {str(e)}", exc_info=True)
    
    def on_connect(self, ws, response):
        """Handle WebSocket connection"""
        try:
            self.logger.info(f"WebSocket connected with response: {response}")
            
            if len(self.tokens) > 0:
                ws.subscribe(self.tokens)
                ws.set_mode(ws.MODE_QUOTE, self.tokens)
                self.logger.info(f"Subscribed to {len(self.tokens)} instruments")
            else:
                self.logger.warning("No tokens to subscribe")
                
        except Exception as e:
            self.logger.error(f"Error in on_connect: {str(e)}", exc_info=True)
    
    def on_close(self, ws, code, reason):
        """Handle WebSocket close"""
        self.logger.info(f"WebSocket connection closed: {code} - {reason}")
        
        now = datetime.now()
        if self.is_market_closed():
            self.logger.info("Market closed, performing cleanup...")
            self.graceful_shutdown(ws)
    
    def on_error(self, ws, code, reason):
        """Handle WebSocket error"""
        self.logger.error(f"WebSocket connection error: {code} - {reason}")
    
    def on_reconnect(self, ws, attempts_count):
        """Handle WebSocket reconnection"""
        self.logger.info(f"WebSocket reconnecting (attempt {attempts_count})")
    
    def on_noreconnect(self, ws):
        """Handle WebSocket reconnection failure"""
        self.logger.error("WebSocket reconnection failed")
    
    async def run(self) -> bool:
        """Main execution method"""
        try:
            self.logger.info("Starting Zerodha tick data handler...")
            
            
            # Initialize all connections
            if not self.initialize_connections():
                return False
            
            # Load instruments first
            if not self.load_instruments():
                return False
            
            # Preload historical data (includes database insertion)
            self.logger.info("Starting historical data preloading...")
            await self.preload_historical_data()
            self.logger.info("Historical data preloading completed.")
            
            # Initialize Redis data
            if not self.initialize_redis_data():
                return False
            
            # Setup WebSocket handlers
            self.setup_websocket_handlers()
            
            # Start CSV export in a background thread
            # export_thread = threading.Thread(target=self.export_ohlc_to_csv_continuously, args=(60,), daemon=True)
            # export_thread.start()
            # Start WebSocket connection
            self.logger.info("Starting WebSocket connection...")
            self.kws.connect()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in main execution: {str(e)}", exc_info=True)
            return False

def signal_handler(signum, frame):
    """Handle system signals"""
    logger = logging.getLogger(__name__)
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)


def main():
    """Main entry point"""
    # Setup logging
    logger = setup_logging()
    logger.info("="*50)
    logger.info("Starting Zerodha Tick Data Handler")
    logger.info("="*50)
    

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 1. Create and run the DatabaseHandler to generate scan_instruments.csv
    db_handler = DatabaseHandler()
    if not db_handler.create_scan_instruments_csv():
        logger.error("Failed to create scan_instruments.csv. Exiting...")
        sys.exit(1)
        
    # Create and run handler
    handler = ZerodhaTickHandler()

    async def run_handler():
        try:
            success = await handler.run()
            if not success:
                logger.error("Failed to start tick data handler")
                sys.exit(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}", exc_info=True)
            sys.exit(1)
        finally:
            logger.info("Tick data handler stopped")

    try:
        asyncio.run(run_handler())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()



# def fetch_and_store_tables(tables):

#     # Establish a database connection
#     db = db_connect()
#     table_data = {}  # Dictionary to store table data

#     try:
#         for table_name in tables:
#             print(f"Fetching data from table: {table_name}")

#             # Read data into a DataFrame
#             query = f"SELECT * FROM {table_name}"
#             df = pd.read_sql(query, db)

#             # Store the DataFrame in a dictionary for later use
#             table_data[table_name] = df
#             print(f"Data from {table_name} stored in variable.")

#     except Exception as e:
#         print(f"An error occurred: {e}")

#     finally:
#         # Close database connection
#         db.close()
#         print("Database connection closed.")

#     return table_data

# # List of tables to fetch
# tables_to_fetch = ['scans', 'filter_options']

# # Call the function and store the tables in a variable
# tables_data = fetch_and_store_tables(tables_to_fetch)

# scans_df = tables_data['scans']
# basket_id_at_scans = scans_df['basket_id'].unique().tolist()

# # Save the filtered DataFrame to a CSV file
# # tables_to_fetch = ['filter_options']
# # tables_data = fetch_and_store_tables(tables_to_fetch)
# filter_options = tables_data['filter_options']   
# filtered_df = filter_options[filter_options['basket_id'].isin(basket_id_at_scans)]    
# filtered_df.to_csv('./scan_instruments.csv', index=False)
# print("Filtered DataFrame has been saved to 'scan_instruments.csv'.")


