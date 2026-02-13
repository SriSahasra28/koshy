#!/usr/bin/env python3
"""
Test the exact LRC calculation that's failing in the alert engine
"""

import numpy as np
import sys
from background.indicators import linear_regression_channel_numba_sliding

def test_lrc_with_exact_data():
    """Test LRC with the exact data from the failing alert"""
    
    # The exact 20-candle close data from the diagnostic
    close_data = np.array([
        2261.80, 2262.00, 2262.30, 2262.40, 2262.90,
        2262.20, 2260.60, 2260.40, 2260.80, 2260.60,
        2261.30, 2261.90, 2262.20, 2260.20, 2259.90,
        2261.70, 2259.00, 2259.70, 2261.70, 2258.00
    ], dtype=np.float64)
    
    print(f"Testing LRC with exact failing data:")
    print(f"  Close array shape: {close_data.shape}")
    print(f"  Close array dtype: {close_data.dtype}")
    print(f"  Close data: {close_data}")
    print(f"  Min: {np.min(close_data)}, Max: {np.max(close_data)}")
    print(f"  Any NaN: {np.any(np.isnan(close_data))}")
    print(f"  Any Inf: {np.any(np.isinf(close_data))}")
    
    try:
        # Call the exact same function as the alert engine
        LRL, UCL, LCL, angles = linear_regression_channel_numba_sliding(
            close_data, 
            period=20, 
            std_multiplier=2.0
        )
        
        print(f"\nLRC Results:")
        print(f"  LRL shape: {LRL.shape}, dtype: {LRL.dtype}")
        print(f"  UCL shape: {UCL.shape}, dtype: {UCL.dtype}")
        print(f"  LCL shape: {LCL.shape}, dtype: {LCL.dtype}")
        
        # Check the last value (index 19 for 20-element array)
        last_idx = len(close_data) - 1
        print(f"\nValues at index {last_idx} (target candle):")
        print(f"  LRL[{last_idx}] = {LRL[last_idx]}")
        print(f"  UCL[{last_idx}] = {UCL[last_idx]}")
        print(f"  LCL[{last_idx}] = {LCL[last_idx]}")
        print(f"  Angle[{last_idx}] = {angles[last_idx]}")
        
        # Check for NaN
        if np.isnan(LRL[last_idx]) or np.isnan(UCL[last_idx]) or np.isnan(LCL[last_idx]):
            print(f"\n[ERROR] FOUND THE ISSUE: LRC returned NaN at index {last_idx}")
            
            # Debug the calculation step by step
            print(f"\nDebugging the calculation:")
            
            # Replicate the exact calculation from the function
            window_close = close_data  # For 20-period on 20-element array, this is the full array
            X = np.arange(len(window_close))
            N = len(X)
            sum_X = np.sum(X)
            sum_Y = np.sum(window_close)
            sum_XY = np.sum(X * window_close)
            sum_X2 = np.sum(X * X)
            
            print(f"  Window close: {window_close}")
            print(f"  X: {X}")
            print(f"  N: {N}")
            print(f"  sum_X: {sum_X}")
            print(f"  sum_Y: {sum_Y}")
            print(f"  sum_XY: {sum_XY}")
            print(f"  sum_X2: {sum_X2}")
            
            # Calculate slope and intercept
            slope = 0.0
            intercept = np.mean(window_close)
            
            denominator_slope = (N * sum_X2 - sum_X * sum_X)
            print(f"  denominator_slope: {denominator_slope}")
            
            if denominator_slope != 0:
                slope = (N * sum_XY - sum_X * sum_Y) / denominator_slope
                print(f"  slope: {slope}")
            else:
                print(f"  [ERROR] denominator_slope is zero!")
            
            denominator_intercept = N
            if denominator_intercept != 0:
                intercept = (sum_Y - slope * sum_X) / denominator_intercept
                print(f"  intercept: {intercept}")
            else:
                print(f"  [ERROR] denominator_intercept is zero!")
            
            # Calculate linear regression line
            lrl = intercept + slope * X
            print(f"  lrl: {lrl}")
            
            residuals = window_close - lrl
            print(f"  residuals: {residuals}")
            
            std_dev = np.std(residuals)
            print(f"  std_dev: {std_dev}")
            
            # Calculate control lines
            ucl = lrl[-1] + 2.0 * std_dev
            lcl = lrl[-1] - 2.0 * std_dev
            
            print(f"  Final values:")
            print(f"    lrl[-1]: {lrl[-1]}")
            print(f"    ucl: {ucl}")
            print(f"    lcl: {lcl}")
            
        else:
            print(f"\n[OK] LRC calculation succeeded - no NaN found")
            
    except Exception as e:
        print(f"\n[ERROR] Error during LRC calculation: {e}")
        import traceback
        traceback.print_exc()

def test_lrc_with_500_element_array():
    """Test LRC with a 500-element array like the alert engine uses"""
    
    print(f"\n" + "="*60)
    print(f"Testing LRC with 500-element array (like alert engine)")
    
    # Create a 500-element array with the failing 20 values at the end
    base_data = np.linspace(2250, 2260, 480)  # 480 elements trending upward
    failing_data = np.array([
        2261.80, 2262.00, 2262.30, 2262.40, 2262.90,
        2262.20, 2260.60, 2260.40, 2260.80, 2260.60,
        2261.30, 2261.90, 2262.20, 2260.20, 2259.90,
        2261.70, 2259.00, 2259.70, 2261.70, 2258.00
    ])
    
    full_array = np.concatenate([base_data, failing_data]).astype(np.float64)
    
    print(f"  Full array shape: {full_array.shape}")
    print(f"  Last 20 values: {full_array[-20:]}")
    
    try:
        LRL, UCL, LCL, angles = linear_regression_channel_numba_sliding(
            full_array, 
            period=20, 
            std_multiplier=2.0
        )
        
        # Check the last value (index 499 for 500-element array)
        last_idx = len(full_array) - 1
        print(f"\nValues at index {last_idx} (target candle):")
        print(f"  LRL[{last_idx}] = {LRL[last_idx]}")
        print(f"  UCL[{last_idx}] = {UCL[last_idx]}")
        print(f"  LCL[{last_idx}] = {LCL[last_idx]}")
        
        if np.isnan(LRL[last_idx]) or np.isnan(UCL[last_idx]) or np.isnan(LCL[last_idx]):
            print(f"\n[ERROR] REPRODUCED THE ISSUE: LRC returned NaN at index {last_idx}")
        else:
            print(f"\n[OK] LRC calculation succeeded with 500-element array")
            
    except Exception as e:
        print(f"\n[ERROR] Error during LRC calculation: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_lrc_with_exact_data()
    test_lrc_with_500_element_array()