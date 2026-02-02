# How to Restart main-consumer.py

## On Windows (PowerShell)

### Method 1: If running in a terminal window
1. **Stop**: Press `Ctrl+C` in the terminal window where it's running
2. **Restart**: Run the command again:
   ```powershell
   cd C:\Users\Administrator\Desktop\koshy-trading-app-client_2025\koshy_python
   python main-consumer.py
   ```

### Method 2: If running in background (task/kill by process)
1. **Find the process**:
   ```powershell
   Get-Process python | Where-Object {$_.Path -like "*koshy*"}
   ```
   
2. **Kill the specific process** (replace PID with actual process ID):
   ```powershell
   Stop-Process -Id <PID> -Force
   ```

3. **Restart**: Run main-consumer.py again

### Method 3: Kill all Python processes (CAREFUL - kills ALL Python scripts)
```powershell
Get-Process python | Stop-Process -Force
```

---

## On Linux/Remote Server (SSH)

### Method 1: Using process name
```bash
# Find process
ps aux | grep main-consumer.py

# Kill it (replace PID)
kill <PID>

# Or kill by name
pkill -f main-consumer.py
```

### Method 2: If running in screen/tmux
```bash
# List sessions
screen -ls
# or
tmux ls

# Attach to session
screen -r <session_id>
# or
tmux attach -t <session_id>

# Press Ctrl+C to stop, then restart
```

---

## Why the Time Delay?

The 2-3 minute delay between candle time and alert storage is **NORMAL** and expected:

1. **Candle Completion Wait** (~1 minute):
   - A 1-minute candle at `11:54:00` represents data from `11:54:00` to `11:54:59`
   - The system waits until `11:55:00` to confirm the candle is complete
   - This ensures no missing ticks

2. **Data Processing** (~30-60 seconds):
   - `tick_zerodha.py` aggregates ticks into the 1-minute candle
   - Pushes to Redis `ohlc_ready` queue

3. **Alert Processing** (~30-90 seconds):
   - `main-consumer.py` picks up from queue
   - Fetches data, calculates indicators (PSAR, Stochastic, LRC)
   - Checks conditions for all symbols/timeframes
   - Processes alerts in batches (30 at a time)
   - Saves to database and sends Telegram

**Total Expected Delay: 2-3 minutes** from candle close to alert delivery

This is acceptable for trading alerts as:
- The candle needs to complete before analysis
- Indicator calculations need historical data
- Batch processing ensures system stability
