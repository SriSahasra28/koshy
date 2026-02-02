# Fix Pandas/Numpy Compatibility Issue

## Problem
```
ImportError: numpy.core.multiarray failed to import
```

This happens when pandas was compiled against a different numpy version than what's installed.

## Solution

Run these commands in your conda environment:

```bash
conda activate ThreeNine
pip uninstall pandas numpy -y
pip install numpy==1.24.3
pip install pandas==2.0.3
```

Or if that doesn't work, try:
```bash
conda activate ThreeNine
conda install pandas numpy -y
```

## Alternative: Quick Fix for day_start_async.py

If the above doesn't work, you can add this at the very top of `day_start_async.py` (before any imports):

```python
import numpy
numpy._import_array()  # Force numpy array import
import pandas as pd
```

But the proper fix is reinstalling compatible versions.
