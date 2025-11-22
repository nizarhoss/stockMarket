# PyAlgoTrade Complete Trading Strategy Backtesting System

A production-ready Python backtesting system built with PyAlgoTrade to implement and compare multiple trading strategies for the S&P 500 (SPY).

## Features

- **Automated Data Download**: Fetch historical stock data using yfinance
- **Multiple Trading Strategies**:
  - 200-Day Simple Moving Average (SMA) Crossover
  - 50/200-Day Dual SMA Crossover
  - Buy and Hold (Baseline)
- **Comprehensive Performance Metrics**:
  - Total Return & Cumulative Returns
  - Sharpe Ratio
  - Maximum Drawdown
  - Win Rate & Trade Statistics
- **Strategy Comparison**: Side-by-side performance comparison
- **Production-Ready Code**: Well-structured, documented, and maintainable

## Project Structure

```
backtesting/
├── __init__.py                 # Package initialization
├── import_data.py              # Data download and validation
├── run_backtest.py             # Main backtesting runner
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── strategies/                 # Trading strategies
│   ├── __init__.py
│   ├── sma_strategy.py        # SMA-based strategies
│   └── buy_and_hold.py        # Buy and hold strategy
└── data/                       # Downloaded data (created automatically)
    └── SPY.csv
```

## Installation

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Verify Installation

```bash
python -c "import pyalgotrade; print('PyAlgoTrade version:', pyalgotrade.__version__)"
```

## Usage

### Quick Start

Run the complete backtesting system with default settings:

```bash
cd backtesting
python run_backtest.py
```

This will:
1. Download SPY data from 2000-01-01 to 2019-10-04
2. Run three strategies (Buy & Hold, 200-Day SMA, 50/200-Day SMA)
3. Display detailed performance metrics
4. Compare all strategies side-by-side

### Download Data Only

```bash
python import_data.py
```

This downloads SPY data and validates it.

### Custom Backtesting

```python
from pyalgotrade.barfeed import yahoofeed
from strategies import SMAStrategy
from pyalgotrade.stratanalyzer import returns

# Load data
feed = yahoofeed.Feed()
feed.addBarsFromCSV('SPY', 'data/SPY.csv')

# Create strategy
strategy = SMAStrategy(feed, 'SPY', sma_period=200)

# Add analyzer
returns_analyzer = returns.Returns()
strategy.attachAnalyzer(returns_analyzer)

# Run backtest
strategy.run()

# Get results
print(f"Return: {returns_analyzer.getCumulativeReturns()[-1] * 100:.2f}%")
```

## Strategies

### 1. Buy and Hold (Baseline)

- **Description**: Purchases SPY with all available capital on day 1 and holds until the end
- **Purpose**: Provides a benchmark to compare active strategies against
- **File**: `strategies/buy_and_hold.py`

### 2. 200-Day SMA Crossover

- **Description**: Trend-following strategy using a 200-day simple moving average
- **Rules**:
  - BUY when price crosses above 200-day SMA
  - SELL when price crosses below 200-day SMA
- **File**: `strategies/sma_strategy.py`

### 3. 50/200-Day SMA Crossover

- **Description**: Dual moving average crossover strategy (Golden Cross/Death Cross)
- **Rules**:
  - BUY when 50-day SMA crosses above 200-day SMA (Golden Cross)
  - SELL when 50-day SMA crosses below 200-day SMA (Death Cross)
- **File**: `strategies/sma_strategy.py`

## Performance Metrics

The system calculates and reports:

| Metric | Description |
|--------|-------------|
| **Total Return** | Overall percentage return on investment |
| **Cumulative Return** | Compound return over the period |
| **Sharpe Ratio** | Risk-adjusted return (higher is better) |
| **Maximum Drawdown** | Largest peak-to-trough decline |
| **Win Rate** | Percentage of profitable trades |
| **Total Trades** | Number of completed round-trip trades |
| **Avg Return per Trade** | Mean return across all trades |

## Example Output

```
======================================================================
STRATEGY COMPARISON
======================================================================

Strategy                       Return       Sharpe     Max DD       Trades
----------------------------------------------------------------------
Buy and Hold (Baseline)        +234.56%     0.845      -55.23%      1
200-Day SMA Crossover          +189.12%     0.912      -38.45%      15
50/200-Day SMA Crossover       +156.78%     0.723      -42.11%      8

----------------------------------------------------------------------
Best Performing Strategy: Buy and Hold (Baseline) (+234.56%)
======================================================================
```

## Configuration

Default settings in `run_backtest.py`:

```python
TICKER = 'SPY'                  # Stock ticker
START_DATE = '2000-01-01'       # Backtest start date
END_DATE = '2019-10-04'         # Backtest end date
INITIAL_CASH = 100000.0         # Starting capital ($100K)
DATA_DIR = 'data'               # Data directory
```

Modify these in the `main()` function to test different stocks or time periods.

## Adding New Strategies

1. Create a new file in `strategies/` directory
2. Inherit from `strategy.BacktestingStrategy`
3. Implement `onBars()` method with your trading logic
4. Add the strategy to `strategies/__init__.py`
5. Update `run_backtest.py` to include your strategy

Example:

```python
from pyalgotrade import strategy

class MyStrategy(strategy.BacktestingStrategy):
    def __init__(self, feed, instrument):
        super(MyStrategy, self).__init__(feed)
        self.__instrument = instrument
        self.__position = None

    def onBars(self, bars):
        # Your trading logic here
        pass
```

## Testing Different Instruments

To backtest different stocks:

```python
# In run_backtest.py, modify the main() function:
TICKER = 'AAPL'  # Change from SPY to AAPL
```

Or use the import_data module directly:

```python
from import_data import download_data

# Download Apple stock data
download_data('AAPL', start='2010-01-01', end='2020-12-31')
```

## Requirements

- Python 3.7+
- PyAlgoTrade 0.20
- yfinance 0.2.3+
- pandas 1.3.0+
- numpy 1.21.0+

## Troubleshooting

### "No module named pyalgotrade"

```bash
pip install pyalgotrade==0.20
```

### "No data downloaded"

Check your internet connection and verify the ticker symbol and date range are valid.

### CSV Format Errors

The data must be in Yahoo Finance CSV format with columns:
`Date, Open, High, Low, Close, Volume, Adj Close`

## Further Development

Potential enhancements:

- [ ] Add more technical indicators (RSI, MACD, Bollinger Bands)
- [ ] Implement portfolio optimization
- [ ] Add position sizing strategies
- [ ] Include transaction costs and slippage
- [ ] Generate visual plots of equity curves
- [ ] Add risk management rules (stop-loss, take-profit)
- [ ] Export results to CSV/JSON
- [ ] Add Monte Carlo simulation

## Resources

- [PyAlgoTrade Documentation](https://gbeced.github.io/pyalgotrade/)
- [yfinance Documentation](https://pypi.org/project/yfinance/)
- [Algorithmic Trading Strategies](https://www.investopedia.com/articles/active-trading/101014/basics-algorithmic-trading-concepts-and-examples.asp)

## License

This project is provided as-is for educational and research purposes.

## Disclaimer

This backtesting system is for educational purposes only. Past performance does not guarantee future results. Always do your own research before making investment decisions.
