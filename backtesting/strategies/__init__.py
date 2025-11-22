"""
Trading Strategies Package

This package contains various trading strategies for PyAlgoTrade backtesting:
- SMA Strategy: Simple Moving Average crossover strategy
- Buy and Hold: Baseline benchmark strategy
"""

from .sma_strategy import SMAStrategy, SMACrossoverStrategy
from .buy_and_hold import BuyAndHoldStrategy, BuyAndHoldWithDividends

__all__ = [
    'SMAStrategy',
    'SMACrossoverStrategy',
    'BuyAndHoldStrategy',
    'BuyAndHoldWithDividends',
]
