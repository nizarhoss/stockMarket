"""
Point and Figure Chart Calculator and Plotter
Calculates P&F chart data and visualizes it with user-defined parameters
"""

import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


class PointAndFigureChart:
    """
    Point and Figure chart calculator

    Parameters:
    -----------
    box_size : float
        The size of each box (price movement required to add a new mark)
    reversal_amount : int
        Number of boxes required to reverse direction and start a new column
    """

    def __init__(self, box_size, reversal_amount=3):
        self.box_size = box_size
        self.reversal_amount = reversal_amount
        self.columns = []  # List of columns, each column is a dict with 'type', 'boxes', 'start_price'

    def calculate(self, prices):
        """
        Calculate P&F chart from price data

        Parameters:
        -----------
        prices : pandas.Series or list
            Price data (typically high/low or close prices)

        Returns:
        --------
        list : List of columns for the P&F chart
        """
        if len(prices) == 0:
            return []

        prices = pd.Series(prices).dropna()

        # Initialize first column
        current_price = prices.iloc[0]
        current_box = int(current_price / self.box_size)
        current_direction = None  # 'X' for up, 'O' for down
        current_column_boxes = []

        for price in prices:
            box_number = int(price / self.box_size)

            if current_direction is None:
                # First column - determine direction from first significant move
                if box_number > current_box:
                    current_direction = 'X'
                    current_column_boxes = list(range(current_box, box_number + 1))
                    current_box = box_number
                elif box_number < current_box:
                    current_direction = 'O'
                    current_column_boxes = list(range(box_number, current_box + 1))
                    current_box = box_number

            elif current_direction == 'X':
                # Currently in an up column (X's)
                if box_number > current_box:
                    # Continue up
                    current_column_boxes.extend(range(current_box + 1, box_number + 1))
                    current_box = box_number
                elif box_number <= current_box - self.reversal_amount:
                    # Reversal down
                    if len(current_column_boxes) > 0:
                        self.columns.append({
                            'type': 'X',
                            'boxes': current_column_boxes.copy(),
                            'high': max(current_column_boxes),
                            'low': min(current_column_boxes)
                        })
                    current_direction = 'O'
                    current_box = box_number
                    current_column_boxes = list(range(box_number, current_box + self.reversal_amount + 1))

            elif current_direction == 'O':
                # Currently in a down column (O's)
                if box_number < current_box:
                    # Continue down
                    new_boxes = list(range(box_number, current_box))
                    for box in new_boxes:
                        if box not in current_column_boxes:
                            current_column_boxes.append(box)
                    current_box = box_number
                elif box_number >= current_box + self.reversal_amount:
                    # Reversal up
                    if len(current_column_boxes) > 0:
                        self.columns.append({
                            'type': 'O',
                            'boxes': current_column_boxes.copy(),
                            'high': max(current_column_boxes),
                            'low': min(current_column_boxes)
                        })
                    current_direction = 'X'
                    current_box = box_number
                    current_column_boxes = list(range(current_box - self.reversal_amount, box_number + 1))

        # Add the last column
        if len(current_column_boxes) > 0 and current_direction is not None:
            self.columns.append({
                'type': current_direction,
                'boxes': current_column_boxes,
                'high': max(current_column_boxes),
                'low': min(current_column_boxes)
            })

        return self.columns

    def plot(self, title="Point and Figure Chart", figsize=(14, 10)):
        """
        Plot the P&F chart

        Parameters:
        -----------
        title : str
            Chart title
        figsize : tuple
            Figure size (width, height)
        """
        if len(self.columns) == 0:
            print("No data to plot. Calculate the chart first.")
            return

        fig, ax = plt.subplots(figsize=figsize)

        # Find the overall range
        all_boxes = []
        for col in self.columns:
            all_boxes.extend(col['boxes'])

        min_box = min(all_boxes)
        max_box = max(all_boxes)

        # Plot each column
        for col_idx, column in enumerate(self.columns):
            x_pos = col_idx

            if column['type'] == 'X':
                # Draw X's
                for box in column['boxes']:
                    # Draw an X
                    y_center = box * self.box_size + self.box_size / 2
                    ax.plot([x_pos - 0.3, x_pos + 0.3],
                           [y_center - self.box_size * 0.3, y_center + self.box_size * 0.3],
                           'g-', linewidth=2)
                    ax.plot([x_pos - 0.3, x_pos + 0.3],
                           [y_center + self.box_size * 0.3, y_center - self.box_size * 0.3],
                           'g-', linewidth=2)
            else:
                # Draw O's
                for box in column['boxes']:
                    # Draw a circle
                    y_center = box * self.box_size + self.box_size / 2
                    circle = plt.Circle((x_pos, y_center), self.box_size * 0.3,
                                      color='red', fill=False, linewidth=2)
                    ax.add_patch(circle)

        # Set up the grid and labels
        ax.set_xlim(-1, len(self.columns))
        ax.set_ylim(min_box * self.box_size - self.box_size,
                   max_box * self.box_size + self.box_size)

        ax.set_xlabel('Column', fontsize=12)
        ax.set_ylabel(f'Price (Box Size: ${self.box_size})', fontsize=12)
        ax.set_title(f'{title}\nBox Size: ${self.box_size} | Reversal: {self.reversal_amount} boxes',
                    fontsize=14, fontweight='bold')

        # Add horizontal grid lines at each box level
        y_ticks = np.arange(min_box, max_box + 1) * self.box_size
        ax.set_yticks(y_ticks)
        ax.grid(True, alpha=0.3)

        # Format y-axis to show price values
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'${y:.2f}'))

        plt.tight_layout()
        return fig


def download_stock_data(symbol, period='1y'):
    """
    Download stock data from Yahoo Finance

    Parameters:
    -----------
    symbol : str
        Stock ticker symbol
    period : str
        Time period (e.g., '1mo', '3mo', '6mo', '1y', '2y', '5y', 'max')

    Returns:
    --------
    pandas.DataFrame : Stock data with OHLC prices
    """
    try:
        print(f"Attempting to download data from Yahoo Finance...")
        stock = yf.Ticker(symbol)
        df = stock.history(period=period)

        if df.empty:
            raise ValueError(f"No data found for symbol {symbol}")

        return df
    except Exception as e:
        error_msg = str(e).lower()
        if '403' in error_msg or 'access denied' in error_msg or 'forbidden' in error_msg:
            raise Exception(
                f"Yahoo Finance access denied (403 error).\n"
                f"This may be due to rate limiting or network restrictions.\n"
                f"Suggestions:\n"
                f"  1. Wait a few minutes and try again\n"
                f"  2. Use a VPN or different network connection\n"
                f"  3. Load data from a CSV file instead\n"
                f"Original error: {str(e)}"
            )
        raise Exception(f"Error downloading data for {symbol}: {str(e)}")


def load_data_from_csv(csv_path):
    """
    Load stock data from a CSV file

    Parameters:
    -----------
    csv_path : str
        Path to CSV file with columns: Date, Open, High, Low, Close, Volume

    Returns:
    --------
    pandas.DataFrame : Stock data with OHLC prices
    """
    try:
        df = pd.read_csv(csv_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)

        required_columns = ['Open', 'High', 'Low', 'Close']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"CSV file must contain '{col}' column")

        return df
    except Exception as e:
        raise Exception(f"Error loading CSV file: {str(e)}")


def create_pnf_chart(symbol, box_size, reversal_amount=3, period='1y',
                     price_type='hl', data_source='yfinance', csv_path=None):
    """
    Create and plot a Point and Figure chart for a stock

    Parameters:
    -----------
    symbol : str
        Stock ticker symbol (or name for display if using CSV)
    box_size : float
        Box size for the P&F chart
    reversal_amount : int
        Number of boxes for reversal (default: 3)
    period : str
        Time period for data (default: '1y') - only for yfinance
    price_type : str
        'hl' for high/low, 'close' for closing prices
    data_source : str
        'yfinance' to download from Yahoo Finance, 'csv' to load from file
    csv_path : str
        Path to CSV file (required if data_source='csv')

    Returns:
    --------
    PointAndFigureChart : The calculated P&F chart object
    """
    # Load data
    if data_source == 'csv':
        if not csv_path:
            raise ValueError("csv_path must be provided when data_source='csv'")
        print(f"\nLoading data from {csv_path}...")
        df = load_data_from_csv(csv_path)
        print(f"Loaded {len(df)} days of data from {df.index[0].date()} to {df.index[-1].date()}")
    else:
        print(f"\nDownloading data for {symbol}...")
        df = download_stock_data(symbol, period)
        print(f"Downloaded {len(df)} days of data from {df.index[0].date()} to {df.index[-1].date()}")

    # Prepare price series
    if price_type == 'hl':
        # Use both high and low prices
        prices = []
        for _, row in df.iterrows():
            prices.append(row['High'])
            prices.append(row['Low'])
    else:
        # Use closing prices
        prices = df['Close'].values

    # Calculate P&F chart
    print(f"\nCalculating Point and Figure chart...")
    print(f"Box Size: ${box_size}")
    print(f"Reversal Amount: {reversal_amount} boxes")

    pnf = PointAndFigureChart(box_size, reversal_amount)
    pnf.calculate(prices)

    print(f"Generated {len(pnf.columns)} columns")

    # Plot the chart
    pnf.plot(title=f"{symbol} Point and Figure Chart")

    return pnf
