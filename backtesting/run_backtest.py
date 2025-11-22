"""
Main Backtesting Runner Script

This script orchestrates the entire backtesting process:
1. Downloads historical data
2. Runs multiple trading strategies
3. Compares performance metrics
4. Generates detailed reports
"""

import os
import sys
from pyalgotrade import plotter
from pyalgotrade.barfeed import yahoofeed
from pyalgotrade.stratanalyzer import returns, sharpe, drawdown, trades
from pyalgotrade.broker import backtesting

# Add the backtesting directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from import_data import download_data, validate_data
from strategies import SMAStrategy, SMACrossoverStrategy, BuyAndHoldStrategy


class BacktestRunner:
    """
    Orchestrates backtesting for multiple strategies and generates performance reports.
    """

    def __init__(self, initial_cash=100000.0):
        """
        Initialize the backtest runner.

        Args:
            initial_cash (float): Starting capital for backtesting (default: $100,000)
        """
        self.initial_cash = initial_cash
        self.results = {}

    def run_strategy(self, strategy_class, feed, instrument, strategy_name, **kwargs):
        """
        Run a single strategy and collect performance metrics.

        Args:
            strategy_class: The strategy class to instantiate
            feed: Data feed
            instrument (str): Ticker symbol
            strategy_name (str): Name for the strategy
            **kwargs: Additional arguments for the strategy

        Returns:
            dict: Performance metrics
        """
        print(f"\n{'=' * 70}")
        print(f"Running {strategy_name}...")
        print(f"{'=' * 70}")

        # Create the strategy
        strat = strategy_class(feed, instrument, **kwargs)

        # Attach analyzers
        returns_analyzer = returns.Returns()
        strat.attachAnalyzer(returns_analyzer)

        sharpe_analyzer = sharpe.SharpeRatio()
        strat.attachAnalyzer(sharpe_analyzer)

        drawdown_analyzer = drawdown.DrawDown()
        strat.attachAnalyzer(drawdown_analyzer)

        trades_analyzer = trades.Trades()
        strat.attachAnalyzer(trades_analyzer)

        # Run the backtest
        strat.run()

        # Collect results
        final_value = strat.getBroker().getEquity()
        total_return = (final_value / self.initial_cash - 1) * 100

        results = {
            'strategy': strategy_name,
            'initial_cash': self.initial_cash,
            'final_value': final_value,
            'total_return': total_return,
            'cumulative_return': returns_analyzer.getCumulativeReturns()[-1] * 100 if returns_analyzer.getCumulativeReturns() else 0,
            'sharpe_ratio': sharpe_analyzer.getSharpeRatio(0.0),
            'max_drawdown': drawdown_analyzer.getMaxDrawDown() * 100,
            'longest_drawdown': drawdown_analyzer.getLongestDrawDownDuration(),
            'total_trades': trades_analyzer.getCount(),
            'profitable_trades': trades_analyzer.getProfitableCount(),
            'unprofitable_trades': trades_analyzer.getUnprofitableCount(),
        }

        # Calculate win rate
        if results['total_trades'] > 0:
            results['win_rate'] = (results['profitable_trades'] / results['total_trades']) * 100
        else:
            results['win_rate'] = 0

        # Calculate average profit per trade
        all_returns = trades_analyzer.getAllReturns()
        if all_returns:
            results['avg_return_per_trade'] = (sum(all_returns) / len(all_returns)) * 100
        else:
            results['avg_return_per_trade'] = 0

        self.results[strategy_name] = results
        return results

    def print_results(self, results):
        """
        Print detailed results for a strategy.

        Args:
            results (dict): Performance metrics
        """
        print(f"\n{results['strategy']} - Performance Summary:")
        print(f"-" * 70)
        print(f"Initial Capital:        ${results['initial_cash']:,.2f}")
        print(f"Final Value:            ${results['final_value']:,.2f}")
        print(f"Total Return:           {results['total_return']:+.2f}%")
        print(f"Cumulative Return:      {results['cumulative_return']:+.2f}%")
        print(f"Sharpe Ratio:           {results['sharpe_ratio']:.3f}" if results['sharpe_ratio'] else "Sharpe Ratio:           N/A")
        print(f"Max Drawdown:           {results['max_drawdown']:.2f}%")
        print(f"Longest DD Duration:    {results['longest_drawdown']} days" if results['longest_drawdown'] else "Longest DD Duration:    N/A")
        print(f"\nTrading Statistics:")
        print(f"Total Trades:           {results['total_trades']}")
        print(f"Profitable Trades:      {results['profitable_trades']}")
        print(f"Unprofitable Trades:    {results['unprofitable_trades']}")
        print(f"Win Rate:               {results['win_rate']:.2f}%")
        print(f"Avg Return per Trade:   {results['avg_return_per_trade']:+.2f}%")

    def compare_strategies(self):
        """
        Print a comparison table of all strategies.
        """
        if not self.results:
            print("No results to compare")
            return

        print(f"\n\n{'=' * 70}")
        print("STRATEGY COMPARISON")
        print(f"{'=' * 70}\n")

        # Print header
        print(f"{'Strategy':<30} {'Return':<12} {'Sharpe':<10} {'Max DD':<12} {'Trades':<8}")
        print(f"{'-' * 70}")

        # Print each strategy
        for strategy_name, results in self.results.items():
            sharpe_str = f"{results['sharpe_ratio']:.3f}" if results['sharpe_ratio'] else "N/A"

            print(
                f"{strategy_name:<30} "
                f"{results['total_return']:>+10.2f}%  "
                f"{sharpe_str:<10} "
                f"{results['max_drawdown']:>10.2f}%  "
                f"{results['total_trades']:<8}"
            )

        # Find best strategy by return
        best_strategy = max(self.results.items(), key=lambda x: x[1]['total_return'])
        print(f"\n{'-' * 70}")
        print(f"Best Performing Strategy: {best_strategy[0]} ({best_strategy[1]['total_return']:+.2f}%)")
        print(f"{'=' * 70}\n")


def main():
    """
    Main function to run the complete backtesting system.
    """
    # Configuration
    TICKER = 'SPY'
    START_DATE = '2000-01-01'
    END_DATE = '2019-10-04'
    INITIAL_CASH = 100000.0
    DATA_DIR = 'data'

    print("=" * 70)
    print("PyAlgoTrade Complete Trading Strategy Backtesting System")
    print("=" * 70)

    # Step 1: Download data
    print(f"\nStep 1: Downloading historical data for {TICKER}...")
    data_file = os.path.join(DATA_DIR, f'{TICKER}.csv')

    if not os.path.exists(data_file):
        data_file = download_data(TICKER, START_DATE, END_DATE, DATA_DIR)
        validate_data(data_file)
    else:
        print(f"Data file already exists: {data_file}")
        validate_data(data_file)

    # Step 2: Initialize backtest runner
    runner = BacktestRunner(initial_cash=INITIAL_CASH)

    # Step 3: Run Buy and Hold strategy (baseline)
    print(f"\nStep 2: Running Buy and Hold strategy (baseline)...")
    feed = yahoofeed.Feed()
    feed.addBarsFromCSV(TICKER, data_file)
    results_bh = runner.run_strategy(
        BuyAndHoldStrategy,
        feed,
        TICKER,
        "Buy and Hold (Baseline)"
    )
    runner.print_results(results_bh)

    # Step 4: Run 200-day SMA strategy
    print(f"\nStep 3: Running 200-Day SMA strategy...")
    feed = yahoofeed.Feed()
    feed.addBarsFromCSV(TICKER, data_file)
    results_sma = runner.run_strategy(
        SMAStrategy,
        feed,
        TICKER,
        "200-Day SMA Crossover",
        sma_period=200
    )
    runner.print_results(results_sma)

    # Step 5: Run 50/200-day SMA crossover strategy
    print(f"\nStep 4: Running 50/200-Day SMA Crossover strategy...")
    feed = yahoofeed.Feed()
    feed.addBarsFromCSV(TICKER, data_file)
    results_dual = runner.run_strategy(
        SMACrossoverStrategy,
        feed,
        TICKER,
        "50/200-Day SMA Crossover",
        short_period=50,
        long_period=200
    )
    runner.print_results(results_dual)

    # Step 6: Compare all strategies
    runner.compare_strategies()

    print("\n" + "=" * 70)
    print("Backtesting Complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
