# Installation Guide

## Python Version Requirements

**IMPORTANT**: PyAlgoTrade 0.20 has compatibility issues with Python 3.11+.

### Recommended Setup Options:

#### Option 1: Use Python 3.9 or 3.10 (Recommended)

1. Install Python 3.10:
```bash
# Using pyenv (recommended)
pyenv install 3.10.13
pyenv local 3.10.13

# Or using conda
conda create -n backtesting python=3.10
conda activate backtesting
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

#### Option 2: Use Alternative Backtesting Framework

If you're stuck with Python 3.11+, consider using:

- **Backtrader**: Modern, actively maintained
  ```bash
  pip install backtrader yfinance pandas matplotlib
  ```

- **Zipline**: Professional-grade (requires Python 3.8-3.10)
  ```bash
  pip install zipline-reloaded
  ```

- **VectorBT**: Fast vectorized backtesting
  ```bash
  pip install vectorbt yfinance
  ```

## Installation Steps (Python 3.10)

### 1. Create Virtual Environment

```bash
# Using venv
python3.10 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Or using virtualenv
virtualenv -p python3.10 venv
source venv/bin/activate
```

### 2. Install Dependencies

```bash
cd backtesting
pip install -r requirements.txt
```

### 3. Verify Installation

```bash
python -c "import pyalgotrade; print('Success! PyAlgoTrade version:', pyalgotrade.__version__)"
```

### 4. Run Backtesting System

```bash
python run_backtest.py
```

## Troubleshooting

### Error: "AttributeError: install_layout"

This error occurs with Python 3.11+. Solution:
- Use Python 3.10 or earlier
- Or switch to an alternative framework (see Option 2 above)

### Error: "ModuleNotFoundError: No module named 'pyalgotrade'"

Solution:
```bash
pip install pyalgotrade==0.20
```

### Error: "No data downloaded"

Solution:
- Check internet connection
- Verify ticker symbol is valid
- Try running `python import_data.py` separately

### Error: CSV format issues

PyAlgoTrade expects specific CSV format:
```
Date,Open,High,Low,Close,Volume,Adj Close
2000-01-03,148.25,148.25,143.875,145.438,8164300,115.36
```

## Alternative: Docker Setup

Create a `Dockerfile`:

```dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY backtesting/ /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "run_backtest.py"]
```

Build and run:
```bash
docker build -t backtesting .
docker run backtesting
```

## Alternative: Using Conda

```bash
# Create environment
conda create -n backtesting python=3.10

# Activate environment
conda activate backtesting

# Install dependencies
pip install -r requirements.txt

# Run backtest
python run_backtest.py
```

## Quick Test

Test data download without full backtesting:

```bash
python import_data.py
```

This will download SPY data and validate it without running strategies.
