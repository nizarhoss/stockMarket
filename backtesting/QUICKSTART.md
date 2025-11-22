# Quick Start Guide

Get started with the PyAlgoTrade backtesting system in 5 minutes!

## Option 1: Demo (No PyAlgoTrade Required)

Perfect for testing without installing PyAlgoTrade.

### Step 1: Install Basic Dependencies

```bash
pip install pandas numpy --no-build-isolation
```

### Step 2: Generate Sample Data

```bash
cd backtesting
python create_sample_data.py
```

This creates synthetic SPY data for 2010-2019.

### Step 3: Run Demo

```bash
python demo_basic.py
```

You'll see results like:

```
======================================================================
STRATEGY COMPARISON
======================================================================

Strategy                  Return          Trades
----------------------------------------------------------------------
Buy and Hold                   +449.32%         1
200-Day SMA                    +244.42%        43
50-Day SMA                     +265.08%        91

----------------------------------------------------------------------
Best Strategy: Buy and Hold (+449.32%)
======================================================================
```

## Option 2: Full PyAlgoTrade System (Requires Python 3.10)

For the complete professional-grade backtesting system.

### Step 1: Setup Python 3.10 Environment

```bash
# Using pyenv (recommended)
pyenv install 3.10.13
pyenv local 3.10.13

# Or using conda
conda create -n backtesting python=3.10
conda activate backtesting
```

### Step 2: Install Dependencies

```bash
cd backtesting
pip install -r requirements.txt
```

### Step 3: Run Full Backtest

```bash
python run_backtest.py
```

This will:
- Download real SPY data from Yahoo Finance
- Run 3 strategies (Buy & Hold, 200-Day SMA, 50/200-Day SMA)
- Display comprehensive performance metrics
- Generate comparison reports

## What's Included

### Strategies

1. **Buy and Hold** - Baseline benchmark
2. **200-Day SMA Crossover** - Trend following
3. **50/200-Day Dual SMA** - Golden Cross/Death Cross
4. **Custom strategies** - Easy to add your own!

### Performance Metrics

- Total Return
- Sharpe Ratio
- Maximum Drawdown
- Win Rate
- Trade Statistics
- And more...

## Next Steps

1. **Customize strategies**: Edit files in `strategies/` folder
2. **Test different stocks**: Change `TICKER = 'SPY'` to `'AAPL'`, `'QQQ'`, etc.
3. **Adjust parameters**: Try different SMA periods (50, 100, 200)
4. **Add new indicators**: RSI, MACD, Bollinger Bands, etc.

## File Structure

```
backtesting/
├── demo_basic.py           # ⭐ Start here (no PyAlgoTrade)
├── run_backtest.py         # Full system (requires PyAlgoTrade)
├── create_sample_data.py   # Generate test data
├── import_data.py          # Download real market data
├── strategies/             # Trading strategies
│   ├── sma_strategy.py
│   └── buy_and_hold.py
├── README.md              # Full documentation
├── INSTALLATION.md        # Detailed setup guide
└── requirements.txt       # Python dependencies
```

## Troubleshooting

### Python Version Issues

If you see `AttributeError: install_layout`:
- You're using Python 3.11+
- PyAlgoTrade requires Python 3.10 or earlier
- Use `demo_basic.py` instead, or setup Python 3.10

### Missing Data

If you see "Data file not found":

```bash
python create_sample_data.py
```

Or for real data (requires yfinance):

```bash
python import_data.py
```

## Examples

### Test Different Stock

```python
# In demo_basic.py or run_backtest.py
TICKER = 'AAPL'  # Change from SPY to Apple
```

### Adjust Strategy Parameters

```python
# Test different SMA periods
result = backtester.run_sma_strategy(period=100)  # 100-day instead of 200
```

### Compare Multiple Periods

```python
# Run multiple SMA strategies
for period in [50, 100, 200]:
    result = backtester.run_sma_strategy(period=period)
```

## Support

- Full documentation: See `README.md`
- Installation help: See `INSTALLATION.md`
- Code examples: Check strategy files in `strategies/`

## Performance Note

The demo uses synthetic data and serves as proof of concept. For production use:
- Use real market data (yfinance or other providers)
- Add transaction costs and slippage
- Implement proper position sizing
- Include risk management rules

Happy backtesting! 🚀
