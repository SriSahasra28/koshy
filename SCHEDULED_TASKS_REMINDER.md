# Scheduled Tasks Reminder

## Daily Tasks (Run via Windows Task Scheduler)

### 1. **daily_login.py** 
**Schedule:** Daily (before market opens)
**Time:** ~8:45 AM (before 9:15 AM market open)
**Purpose:**
- Zerodha login and get access token
- Download instruments from NFO exchange
- Save access token to database
- Reset database (calls `Resetdb()` stored procedure)
- Save token to `.env` file for server

**What it does:**
- ✅ Login to Zerodha
- ✅ Download NFO instruments
- ✅ Update access token in database
- ✅ Reset database daily
- ✅ Save token to `.env` file

---

### 2. **day_start_async.py**
**Schedule:** Daily (pre-market and post-market)
**Time:** 
- **Pre-market:** ~9:00 AM (before market opens at 9:15 AM)
- **Post-market:** ~3:45 PM (after market closes at 3:30 PM, or after 4 PM)

**Purpose:**
- Download historical OHLC data for all symbols
- Process pre-market steps from database
- Download 1-minute and 1-hour OHLC data
- **Automatically runs `filter_options.py` at the end**

**What it does:**
- ✅ Checks monthly rollover (if day > 20)
- ✅ Downloads OHLC data for all monitor symbols
- ✅ Processes pre-market steps (from `pre_market_steps` table)
- ✅ Downloads 1-minute and 1-hour OHLC data
- ✅ **Runs `filter_options.py` at the end** (line 947)

**Market Hours:**
- Market Open: **9:15 AM**
- Market Close: **3:30 PM** (but candles stored until 15:29)

---

## Continuous Running Tasks (Keep Running All Day)

### 3. **tick_zerodha.py**
**Schedule:** Continuously during market hours
**Start:** Before 9:15 AM
**Stop:** After 3:30 PM (or leave running)

**Purpose:**
- Stream live tick data from Zerodha
- Create 1-minute OHLC candles
- Store candles in Redis
- Push to `ohlc_ready` queue for processing

**What it does:**
- ✅ Connects to Zerodha WebSocket
- ✅ Receives live ticks
- ✅ Aggregates ticks into 1-minute candles
- ✅ Stores in Redis (`ohlc:1minute:<token>`)
- ✅ Pushes to `ohlc_ready` queue when candle completes
- ✅ Only stores candles between **9:15 AM - 3:29 PM**

---

### 4. **main-consumer.py**
**Schedule:** Continuously during market hours
**Start:** Before 9:15 AM (or anytime)
**Stop:** Can run 24/7 (but processes alerts only during market hours)

**Purpose:**
- Processes OHLC data from Redis
- Calculates indicators (PSAR, Stochastic, LRC)
- Checks alert conditions
- Generates alerts and stores in database
- Sends Telegram notifications

**What it does:**
- ✅ Reads from `ohlc_ready` queue
- ✅ Resamples 1-minute data to other timeframes (2min, 3min, 5min, etc.)
- ✅ Calculates PSAR, Stochastic, LRC indicators
- ✅ Checks conditions and generates alerts
- ✅ Stores alerts in MySQL `alerts` table
- ✅ Sends Telegram notifications
- ✅ Publishes alerts to Redis for frontend

---

## Summary Timeline

| Time | Task | Script |
|------|------|--------|
| **8:45 AM** | Daily Login | `daily_login.py` |
| **9:00 AM** | Pre-market Data Download | `day_start_async.py` |
| **9:15 AM** | Market Opens | |
| **9:15 AM - 3:29 PM** | Live Tick Processing | `tick_zerodha.py` (continuous) |
| **9:15 AM - 3:30 PM** | Alert Processing | `main-consumer.py` (continuous) |
| **3:30 PM** | Market Closes | |
| **3:45 PM** | Post-market Data Download | `day_start_async.py` |
| **3:45 PM** | Filter Options | `filter_options.py` (runs automatically after day_start_async.py) |

---

## Key Points to Remember

1. **daily_login.py** must run BEFORE market opens (before 9:15 AM)
2. **tick_zerodha.py** and **main-consumer.py** should be running BEFORE market opens
3. **day_start_async.py** runs twice:
   - Pre-market (before 9:15 AM) - downloads historical data
   - Post-market (after 4 PM) - downloads end-of-day data
4. **filter_options.py** runs automatically after `day_start_async.py` completes
5. All continuous tasks (`tick_zerodha.py`, `main-consumer.py`) should be kept running
6. Database (MySQL) and Redis must be running for all tasks to work

---

## Windows Task Scheduler Setup

To set up scheduled tasks in Windows:

1. Open **Task Scheduler**
2. Create tasks for:
   - `daily_login.py` - Daily at 8:45 AM
   - `day_start_async.py` - Daily at 9:00 AM and 3:45 PM

3. For continuous tasks, you can:
   - Run them manually each morning
   - Set them to start at system boot
   - Use a batch file that keeps them running

---

## Current Status Checklist

Before market opens tomorrow, verify:
- [ ] MySQL is running
- [ ] Redis is running  
- [ ] `daily_login.py` scheduled for 8:45 AM
- [ ] `day_start_async.py` scheduled for 9:00 AM
- [ ] `tick_zerodha.py` is ready to start before 9:15 AM
- [ ] `main-consumer.py` is ready to start before 9:15 AM
- [ ] All scripts have correct paths and environment set up
