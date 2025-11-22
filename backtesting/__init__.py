"""
PyAlgoTrade Complete Trading Strategy Backtesting System

A production-ready Python backtesting system using PyAlgoTrade to implement
and compare multiple trading strategies for the S&P 500 (SPY).

Modules:
    - import_data: Data download and validation
    - strategies: Trading strategy implementations
    - run_backtest: Main backtesting orchestration
"""

__version__ = '1.0.0'
__author__ = 'Your Name'

from .import_data import download_data, download_multiple_tickers, validate_data
from .strategies import (
    SMAStrategy,
    SMACrossoverStrategy,
    BuyAndHoldStrategy,
    BuyAndHoldWithDividends,
)

__all__ = [
    'download_data',
    'download_multiple_tickers',
    'validate_data',
    'SMAStrategy',
    'SMACrossoverStrategy',
    'BuyAndHoldStrategy',
    'BuyAndHoldWithDividends',
]
