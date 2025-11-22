"""
Create Sample Data for Testing

This script creates sample SPY data for testing the backtesting system
without requiring yfinance.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os


def generate_sample_spy_data(start_date='2010-01-01', end_date='2019-10-04', output_dir='data'):
    """
    Generate realistic sample SPY data with trend and volatility.

    This creates synthetic price data that mimics real SPY movement for testing.
    """
    # Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate date range (trading days only)
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    dates = pd.bdate_range(start=start, end=end)

    # Starting price for SPY
    initial_price = 110.0

    # Generate realistic price movement
    np.random.seed(42)  # For reproducibility

    # Daily returns with upward drift (matching S&P 500 historical average ~10% annually)
    daily_drift = 0.0004  # ~10% annual return
    daily_volatility = 0.01  # ~16% annual volatility

    returns = np.random.normal(daily_drift, daily_volatility, len(dates))

    # Add some market crashes and rallies for realism
    # Crash in 2011 (around index 250-400)
    returns[250:400] -= 0.002

    # Rally in 2013-2014 (around index 750-1000)
    returns[750:1000] += 0.001

    # Crash in 2015-2016 (around index 1250-1400)
    returns[1250:1400] -= 0.0015

    # Strong rally 2017-2018 (around index 1750-2000)
    returns[1750:2000] += 0.0008

    # Generate close prices
    close_prices = initial_price * (1 + returns).cumprod()

    # Generate OHLC data
    data = []
    for i, (date, close) in enumerate(zip(dates, close_prices)):
        # Add some intraday volatility
        daily_range = close * np.random.uniform(0.005, 0.02)  # 0.5% to 2% daily range

        high = close + np.random.uniform(0, daily_range * 0.7)
        low = close - np.random.uniform(0, daily_range * 0.7)
        open_price = low + np.random.uniform(0, high - low)

        # Ensure OHLC relationships are valid
        high = max(high, open_price, close)
        low = min(low, open_price, close)

        # Generate volume (higher volume on larger price moves)
        base_volume = 100000000  # 100M shares average
        volume_multiplier = 1 + abs(returns[i]) * 50
        volume = int(base_volume * volume_multiplier * np.random.uniform(0.8, 1.2))

        # Adjusted close (same as close for simplicity, in reality would account for dividends/splits)
        adj_close = close

        data.append({
            'Date': date.strftime('%Y-%m-%d'),
            'Open': round(open_price, 2),
            'High': round(high, 2),
            'Low': round(low, 2),
            'Close': round(close, 2),
            'Volume': volume,
            'Adj Close': round(adj_close, 2)
        })

    # Create DataFrame
    df = pd.DataFrame(data)

    # Save to CSV
    output_path = os.path.join(output_dir, 'SPY.csv')
    df.to_csv(output_path, index=False)

    print(f"Generated {len(df)} bars of sample SPY data")
    print(f"Date range: {df['Date'].iloc[0]} to {df['Date'].iloc[-1]}")
    print(f"Price range: ${df['Close'].min():.2f} to ${df['Close'].max():.2f}")
    print(f"Data saved to: {output_path}")

    return output_path


if __name__ == '__main__':
    print("=" * 60)
    print("Creating Sample SPY Data for Testing")
    print("=" * 60)
    print()

    generate_sample_spy_data()

    print("\n" + "=" * 60)
    print("Sample data created successfully!")
    print("You can now run: python demo_basic.py")
    print("=" * 60)
