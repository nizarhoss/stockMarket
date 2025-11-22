#!/usr/bin/env python3
"""
Sample Stock Data Generator
Creates sample CSV files with realistic stock price data for testing P&F charts
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_sample_stock_data(
    symbol='SAMPLE',
    start_date='2024-01-01',
    end_date='2024-12-31',
    initial_price=100.0,
    volatility=0.02,
    trend=0.001,
    output_file=None
):
    """
    Generate realistic sample stock data

    Parameters:
    -----------
    symbol : str
        Stock symbol for reference
    start_date : str
        Start date in 'YYYY-MM-DD' format
    end_date : str
        End date in 'YYYY-MM-DD' format
    initial_price : float
        Starting stock price
    volatility : float
        Daily volatility (0.02 = 2% typical)
    trend : float
        Daily trend (0.001 = slight upward trend)
    output_file : str
        Output CSV filename (default: {symbol}_sample_data.csv)

    Returns:
    --------
    pandas.DataFrame : Generated stock data
    """
    # Generate date range (business days only)
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    dates = pd.bdate_range(start=start, end=end)

    # Initialize price series
    num_days = len(dates)
    prices = [initial_price]

    # Generate realistic price movements
    np.random.seed(42)  # For reproducibility

    for i in range(1, num_days):
        # Random walk with trend
        change = np.random.normal(trend, volatility)
        new_price = prices[-1] * (1 + change)
        prices.append(max(new_price, 0.01))  # Ensure price stays positive

    # Create OHLC data
    data = []
    for i, date in enumerate(dates):
        close = prices[i]

        # Generate realistic high, low, open from close
        daily_range = close * np.random.uniform(0.01, 0.03)
        high = close + np.random.uniform(0, daily_range)
        low = close - np.random.uniform(0, daily_range)
        open_price = np.random.uniform(low, high)

        volume = int(np.random.uniform(1000000, 5000000))

        data.append({
            'Date': date.strftime('%Y-%m-%d'),
            'Open': round(open_price, 2),
            'High': round(high, 2),
            'Low': round(low, 2),
            'Close': round(close, 2),
            'Volume': volume
        })

    df = pd.DataFrame(data)

    # Save to CSV
    if output_file is None:
        output_file = f"{symbol}_sample_data.csv"

    df.to_csv(output_file, index=False)
    print(f"Generated {len(df)} days of sample data")
    print(f"Date range: {df['Date'].iloc[0]} to {df['Date'].iloc[-1]}")
    print(f"Price range: ${df['Low'].min():.2f} - ${df['High'].max():.2f}")
    print(f"Saved to: {output_file}")

    return df


def generate_trending_stock(symbol, direction='up', output_file=None):
    """
    Generate sample data for a trending stock

    Parameters:
    -----------
    symbol : str
        Stock symbol
    direction : str
        'up' for uptrend, 'down' for downtrend, 'sideways' for ranging
    output_file : str
        Output filename

    Returns:
    --------
    pandas.DataFrame : Generated stock data
    """
    trends = {
        'up': 0.002,      # 0.2% daily gain
        'down': -0.002,   # 0.2% daily loss
        'sideways': 0.0   # No trend
    }

    volatilities = {
        'up': 0.015,
        'down': 0.015,
        'sideways': 0.01
    }

    trend = trends.get(direction, 0.0)
    volatility = volatilities.get(direction, 0.02)

    if output_file is None:
        output_file = f"{symbol}_{direction}_sample.csv"

    return generate_sample_stock_data(
        symbol=symbol,
        start_date='2024-01-01',
        end_date='2024-12-31',
        initial_price=100.0,
        volatility=volatility,
        trend=trend,
        output_file=output_file
    )


def main():
    """
    Generate various sample stock datasets
    """
    print("=" * 60)
    print("Sample Stock Data Generator")
    print("=" * 60)
    print()

    # Generate different market scenarios
    print("Generating uptrending stock...")
    generate_trending_stock('BULL', 'up', 'sample_uptrend.csv')
    print()

    print("Generating downtrending stock...")
    generate_trending_stock('BEAR', 'down', 'sample_downtrend.csv')
    print()

    print("Generating sideways stock...")
    generate_trending_stock('FLAT', 'sideways', 'sample_sideways.csv')
    print()

    print("Generating volatile stock...")
    generate_sample_stock_data(
        symbol='VOLATILE',
        volatility=0.04,
        trend=0.0,
        output_file='sample_volatile.csv'
    )
    print()

    print("=" * 60)
    print("Sample files created successfully!")
    print("=" * 60)
    print("\nYou can now use these files with the P&F chart analyzer:")
    print("  python stock_pnf_analyzer.py")
    print("  (Select CSV data source and provide the filename)")
    print()


if __name__ == "__main__":
    main()
