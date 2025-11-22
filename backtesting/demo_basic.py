"""
Basic Backtesting Demo (No PyAlgoTrade Required)

This script demonstrates basic backtesting concepts without PyAlgoTrade.
Use this to understand the strategies before running the full PyAlgoTrade system.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys


class SimpleBacktest:
    """
    Simple backtesting framework to demonstrate strategy concepts.
    """

    def __init__(self, data, initial_cash=100000.0):
        """
        Initialize the backtester.

        Args:
            data (pd.DataFrame): Price data with Date, Close columns
            initial_cash (float): Starting capital
        """
        self.data = data.copy()
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.shares = 0
        self.trades = []
        self.equity_curve = []

    def buy(self, date, price, shares):
        """Execute a buy order."""
        cost = price * shares
        if cost <= self.cash:
            self.cash -= cost
            self.shares += shares
            self.trades.append({
                'date': date,
                'type': 'BUY',
                'price': price,
                'shares': shares,
                'value': cost
            })
            return True
        return False

    def sell(self, date, price):
        """Execute a sell order (sell all shares)."""
        if self.shares > 0:
            proceeds = price * self.shares
            self.cash += proceeds
            self.trades.append({
                'date': date,
                'type': 'SELL',
                'price': price,
                'shares': self.shares,
                'value': proceeds
            })
            self.shares = 0
            return True
        return False

    def get_equity(self, price):
        """Calculate total equity (cash + stock value)."""
        return self.cash + (self.shares * price)

    def run_buy_and_hold(self):
        """
        Buy and Hold Strategy.

        Buy on first day and hold until the end.
        """
        print("\n" + "=" * 70)
        print("Running Buy and Hold Strategy")
        print("=" * 70)

        # Reset
        self.cash = self.initial_cash
        self.shares = 0
        self.trades = []
        self.equity_curve = []

        # Buy on first day
        first_price = self.data.iloc[0]['Close']
        shares_to_buy = int(self.cash / first_price)
        self.buy(self.data.iloc[0]['Date'], first_price, shares_to_buy)

        # Track equity curve
        for idx, row in self.data.iterrows():
            equity = self.get_equity(row['Close'])
            self.equity_curve.append({
                'date': row['Date'],
                'equity': equity,
                'price': row['Close']
            })

        # Final results
        final_price = self.data.iloc[-1]['Close']
        final_equity = self.get_equity(final_price)
        total_return = ((final_equity / self.initial_cash) - 1) * 100

        print(f"\nBuy and Hold Results:")
        print(f"  Initial Capital: ${self.initial_cash:,.2f}")
        print(f"  Final Value:     ${final_equity:,.2f}")
        print(f"  Total Return:    {total_return:+.2f}%")
        print(f"  Buy Price:       ${first_price:.2f}")
        print(f"  Sell Price:      ${final_price:.2f}")
        print(f"  Shares Held:     {self.shares}")

        return {
            'strategy': 'Buy and Hold',
            'initial_cash': self.initial_cash,
            'final_value': final_equity,
            'total_return': total_return,
            'trades': len(self.trades)
        }

    def run_sma_strategy(self, period=200):
        """
        Simple Moving Average Strategy.

        Buy when price crosses above SMA, sell when crosses below.
        """
        print("\n" + "=" * 70)
        print(f"Running {period}-Day SMA Strategy")
        print("=" * 70)

        # Reset
        self.cash = self.initial_cash
        self.shares = 0
        self.trades = []
        self.equity_curve = []

        # Calculate SMA
        self.data['SMA'] = self.data['Close'].rolling(window=period).mean()

        position = False  # Track if we have a position

        for idx, row in self.data.iterrows():
            if pd.isna(row['SMA']):
                continue

            current_price = row['Close']
            sma_value = row['SMA']

            # Check for signals
            if not position and current_price > sma_value:
                # Buy signal
                shares_to_buy = int(self.cash / current_price)
                if shares_to_buy > 0:
                    self.buy(row['Date'], current_price, shares_to_buy)
                    position = True
                    print(f"BUY  @ ${current_price:.2f} on {row['Date']} (SMA: ${sma_value:.2f})")

            elif position and current_price < sma_value:
                # Sell signal
                self.sell(row['Date'], current_price)
                position = False
                print(f"SELL @ ${current_price:.2f} on {row['Date']} (SMA: ${sma_value:.2f})")

            # Track equity
            equity = self.get_equity(current_price)
            self.equity_curve.append({
                'date': row['Date'],
                'equity': equity,
                'price': current_price
            })

        # Close any open position at the end
        if self.shares > 0:
            final_price = self.data.iloc[-1]['Close']
            self.sell(self.data.iloc[-1]['Date'], final_price)

        # Calculate results
        final_equity = self.cash
        total_return = ((final_equity / self.initial_cash) - 1) * 100

        # Calculate trade statistics
        buy_trades = [t for t in self.trades if t['type'] == 'BUY']
        sell_trades = [t for t in self.trades if t['type'] == 'SELL']

        profitable_trades = 0
        total_profit = 0

        for i in range(min(len(buy_trades), len(sell_trades))):
            buy_price = buy_trades[i]['price']
            sell_price = sell_trades[i]['price']
            profit = (sell_price - buy_price) * buy_trades[i]['shares']
            total_profit += profit
            if profit > 0:
                profitable_trades += 1

        total_trades = min(len(buy_trades), len(sell_trades))
        win_rate = (profitable_trades / total_trades * 100) if total_trades > 0 else 0

        print(f"\n{period}-Day SMA Results:")
        print(f"  Initial Capital:   ${self.initial_cash:,.2f}")
        print(f"  Final Value:       ${final_equity:,.2f}")
        print(f"  Total Return:      {total_return:+.2f}%")
        print(f"  Total Trades:      {total_trades}")
        print(f"  Profitable Trades: {profitable_trades}")
        print(f"  Win Rate:          {win_rate:.2f}%")
        print(f"  Total Profit:      ${total_profit:,.2f}")

        return {
            'strategy': f'{period}-Day SMA',
            'initial_cash': self.initial_cash,
            'final_value': final_equity,
            'total_return': total_return,
            'trades': total_trades,
            'win_rate': win_rate
        }


def main():
    """
    Run basic backtesting demonstration.
    """
    print("=" * 70)
    print("Basic Backtesting System Demo (No PyAlgoTrade Required)")
    print("=" * 70)

    # Configuration
    TICKER = 'SPY'
    START_DATE = '2000-01-01'
    END_DATE = '2019-10-04'
    INITIAL_CASH = 100000.0
    DATA_DIR = 'data'

    # Load data
    print(f"\nLoading {TICKER} data...")
    data_file = os.path.join(DATA_DIR, f'{TICKER}.csv')

    if not os.path.exists(data_file):
        print(f"ERROR: Data file not found: {data_file}")
        print("Please run: python create_sample_data.py")
        return

    # Load data
    data = pd.read_csv(data_file)
    print(f"Loaded {len(data)} bars of data")
    print(f"Date range: {data['Date'].iloc[0]} to {data['Date'].iloc[-1]}")

    # Create backtester
    backtester = SimpleBacktest(data, INITIAL_CASH)

    # Run strategies
    results = []

    # 1. Buy and Hold
    result_bh = backtester.run_buy_and_hold()
    results.append(result_bh)

    # 2. 200-Day SMA
    result_sma200 = backtester.run_sma_strategy(period=200)
    results.append(result_sma200)

    # 3. 50-Day SMA
    result_sma50 = backtester.run_sma_strategy(period=50)
    results.append(result_sma50)

    # Compare results
    print("\n\n" + "=" * 70)
    print("STRATEGY COMPARISON")
    print("=" * 70)
    print(f"\n{'Strategy':<25} {'Return':<15} {'Trades':<10}")
    print("-" * 70)

    for result in results:
        print(f"{result['strategy']:<25} {result['total_return']:>+12.2f}%  {result.get('trades', 1):>8}")

    # Find best strategy
    best = max(results, key=lambda x: x['total_return'])
    print(f"\n{'-' * 70}")
    print(f"Best Strategy: {best['strategy']} ({best['total_return']:+.2f}%)")
    print("=" * 70)


if __name__ == '__main__':
    main()
