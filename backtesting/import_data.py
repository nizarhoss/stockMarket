"""
Data Download Module for PyAlgoTrade Backtesting System

This module handles downloading historical stock data using yfinance
and preparing it for use with PyAlgoTrade.
"""

import yfinance as yf
import pandas as pd
import os
from datetime import datetime


def download_data(ticker='SPY', start='2000-01-01', end='2019-10-04', output_dir='data'):
    """
    Download historical stock data using yfinance and save to CSV.

    Args:
        ticker (str): Stock ticker symbol (default: 'SPY')
        start (str): Start date in YYYY-MM-DD format (default: '2000-01-01')
        end (str): End date in YYYY-MM-DD format (default: '2019-10-04')
        output_dir (str): Directory to save the CSV file (default: 'data')

    Returns:
        str: Path to the saved CSV file
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Downloading {ticker} data from {start} to {end}...")

    # Download data from yfinance
    data = yf.download(ticker, start=start, end=end, progress=False)

    if data.empty:
        raise ValueError(f"No data downloaded for {ticker}")

    # Prepare the data for PyAlgoTrade
    # PyAlgoTrade expects columns: Date, Open, High, Low, Close, Volume, Adj Close
    data = data.reset_index()

    # Rename columns to match PyAlgoTrade format
    data.columns = ['Date', 'Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']

    # Reorder columns to standard format
    data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']]

    # Format date as YYYY-MM-DD
    data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')

    # Save to CSV
    output_path = os.path.join(output_dir, f'{ticker}.csv')
    data.to_csv(output_path, index=False)

    print(f"Downloaded {len(data)} bars for {ticker}")
    print(f"Data saved to: {output_path}")
    print(f"Date range: {data['Date'].iloc[0]} to {data['Date'].iloc[-1]}")

    return output_path


def download_multiple_tickers(tickers, start='2000-01-01', end='2019-10-04', output_dir='data'):
    """
    Download data for multiple tickers.

    Args:
        tickers (list): List of ticker symbols
        start (str): Start date in YYYY-MM-DD format
        end (str): End date in YYYY-MM-DD format
        output_dir (str): Directory to save the CSV files

    Returns:
        dict: Dictionary mapping ticker symbols to file paths
    """
    results = {}

    for ticker in tickers:
        try:
            file_path = download_data(ticker, start, end, output_dir)
            results[ticker] = file_path
        except Exception as e:
            print(f"Error downloading {ticker}: {e}")
            results[ticker] = None

    return results


def validate_data(file_path):
    """
    Validate the downloaded data file.

    Args:
        file_path (str): Path to the CSV file

    Returns:
        bool: True if data is valid, False otherwise
    """
    try:
        data = pd.read_csv(file_path)

        # Check required columns
        required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
        missing_columns = [col for col in required_columns if col not in data.columns]

        if missing_columns:
            print(f"Missing columns: {missing_columns}")
            return False

        # Check for NaN values
        if data.isnull().any().any():
            print("Warning: Data contains NaN values")
            print(data.isnull().sum())

        # Check data integrity
        print(f"\nData validation for {file_path}:")
        print(f"  Rows: {len(data)}")
        print(f"  Date range: {data['Date'].iloc[0]} to {data['Date'].iloc[-1]}")
        print(f"  Price range: ${data['Close'].min():.2f} - ${data['Close'].max():.2f}")

        return True

    except Exception as e:
        print(f"Error validating data: {e}")
        return False


if __name__ == '__main__':
    # Example usage
    print("=" * 60)
    print("PyAlgoTrade Data Download Module")
    print("=" * 60)

    # Download SPY data
    spy_file = download_data('SPY', start='2000-01-01', end='2019-10-04')

    # Validate the data
    print("\n" + "=" * 60)
    validate_data(spy_file)
    print("=" * 60)
