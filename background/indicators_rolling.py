from numba import jit
import numpy as np

@jit(nopython=True)
def heikin_ashi_numpy(open_prices, high_prices, low_prices, close_prices):
    open_prices = np.asarray(open_prices)
    high_prices = np.asarray(high_prices)
    low_prices = np.asarray(low_prices)
    close_prices = np.asarray(close_prices)
    ha_open = np.zeros_like(open_prices)
    ha_high = np.zeros_like(high_prices)
    ha_low = np.zeros_like(low_prices)
    ha_close = np.zeros_like(close_prices)
    ha_close[0] = (open_prices[0] + high_prices[0] + low_prices[0] + close_prices[0]) / 4
    ha_open[0] = (open_prices[0] + close_prices[0]) / 2
    ha_high[0] = high_prices[0]
    ha_low[0] = low_prices[0]
    for i in range(1, len(open_prices)):
        ha_close[i] = (open_prices[i] + high_prices[i] + low_prices[i] + close_prices[i]) / 4
        ha_open[i] = (ha_open[i-1] + ha_close[i-1]) / 2
        ha_high[i] = max(high_prices[i], ha_open[i], ha_close[i])
        ha_low[i] = min(low_prices[i], ha_open[i], ha_close[i])
    return ha_open, ha_high, ha_low, ha_close


@jit(nopython=True)
def calc_fastStochastics(low, high, close, lookback_period, d_period, k_smoothing_period=1):
    n = len(close)
    lowest_low = np.full(n, np.nan)
    highest_high = np.full(n, np.nan)
    raw_K = np.full(n, np.nan)
    
    # Calculate lowest low and highest high for the lookback period
    for i in range(lookback_period - 1, n):
        ll = np.min(low[i - lookback_period + 1:i + 1])
        hh = np.max(high[i - lookback_period + 1:i + 1])
        lowest_low[i] = ll
        highest_high[i] = hh
        
        # Check for division by zero
        if hh != ll:
            raw_K[i] = 100 * (close[i] - ll) / (hh - ll)
        else:
            raw_K[i] = 0  # or np.nan
    
    # Smooth the K values
    K = np.full(n, np.nan)
    if k_smoothing_period > 1:
        for i in range(k_smoothing_period - 1, n):
            K[i] = np.mean(raw_K[i - k_smoothing_period + 1:i + 1])
    else:
        K = raw_K
    
    # Calculate the D values
    D = np.full(n, np.nan)
    for i in range(d_period - 1, n):
        D[i] = np.mean(K[i - d_period + 1:i + 1])
    
    return K, D

@jit(nopython=True)
def psar(high, low, close, af0=0.02, af=0.02, max_af=0.2):
    length = len(close)
    psar = np.zeros(length)
    psar[0] = close[0]
    trend = 1  # 1: uptrend, -1: downtrend
    ep = high[0]  # extreme point
    af = af0
    for i in range(1, length):
        psar[i] = psar[i-1] + af * (ep - psar[i-1])
        if trend == 1:
            if high[i] > ep:
                ep = high[i]
                af = min(af + af0, max_af)
            if low[i] < psar[i]:
                trend = -1
                psar[i] = ep
                ep = low[i]
                af = af0
        else:
            if low[i] < ep:
                ep = low[i]
                af = min(af + af0, max_af)
            if high[i] > psar[i]:
                trend = 1
                psar[i] = ep
                ep = high[i]
                af = af0

        if trend == 1:
            psar[i] = min(psar[i], low[i-1], low[i-2])
        else:
            psar[i] = max(psar[i], high[i-1], high[i-2])
    return psar

@jit(nopython=True)
def get_psar_signals(close, psar_values):
    signals = np.zeros(len(close))   
    for i in range(1, len(close)):
        if close[i] > psar_values[i] and close[i-1] <= psar_values[i-1]:
            signals[i] = 1  # Long signal
        elif close[i] < psar_values[i] and close[i-1] >= psar_values[i-1]:
            signals[i] = -1  # Short signal
    
    return signals

@jit(nopython=True)
def linear_regression_channel_numba_sliding(close, period, std_multiplier):
    n = len(close)
    # Initialize arrays to store results
    LRL = np.full(n, np.nan)  # Linear Regression Line
    UCL = np.full(n, np.nan)  # Upper Control Line
    LCL = np.full(n, np.nan)  # Lower Control Line
    angles = np.full(n, np.nan)  # Slope angles in degrees

    for i in range(period - 1, n):
        window_close = close[i - period + 1: i + 1]
        X = np.arange(len(window_close))
        N = len(X)
        sum_X = np.sum(X)
        sum_Y = np.sum(window_close)
        sum_XY = np.sum(X * window_close)
        sum_X2 = np.sum(X * X)

        # Calculate slope and intercept
        slope = 0.0
        intercept = np.mean(window_close)

        denominator_slope = (N * sum_X2 - sum_X * sum_X)
        if denominator_slope != 0:
            slope = (N * sum_XY - sum_X * sum_Y) / denominator_slope

        denominator_intercept = N
        if denominator_intercept != 0:
            intercept = (sum_Y - slope * sum_X) / denominator_intercept

        # Calculate linear regression line for the current window
        lrl = intercept + slope * X
        residuals = window_close - lrl
        std_dev = np.std(residuals)

        # Calculate control lines
        ucl = lrl[-1] + std_multiplier * std_dev
        lcl = lrl[-1] - std_multiplier * std_dev

        # Store the last value of the current window
        LRL[i] = lrl[-1]
        UCL[i] = ucl
        LCL[i] = lcl
        angles[i] = np.degrees(np.arctan(slope))

    return LRL, UCL, LCL, angles




