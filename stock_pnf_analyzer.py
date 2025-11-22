#!/usr/bin/env python3
"""
Stock Point and Figure Chart Analyzer
Interactive tool for creating P&F charts with user-defined parameters
"""

import sys
from pnf_chart import create_pnf_chart, download_stock_data
import matplotlib.pyplot as plt


def get_user_input():
    """
    Get user input for stock analysis parameters

    Returns:
    --------
    dict : Dictionary containing user inputs
    """
    print("=" * 60)
    print("Stock Point and Figure Chart Analyzer")
    print("=" * 60)
    print()

    # Get stock symbol
    while True:
        symbol = input("Enter stock symbol (e.g., AAPL, TSLA, MSFT): ").strip().upper()
        if symbol:
            break
        print("Please enter a valid stock symbol.")

    # Get box size
    while True:
        try:
            box_size = input("\nEnter box size (e.g., 1.0, 2.5, 5.0): ").strip()
            box_size = float(box_size)
            if box_size > 0:
                break
            print("Box size must be greater than 0.")
        except ValueError:
            print("Please enter a valid number.")

    # Get reversal amount
    while True:
        try:
            reversal = input("\nEnter reversal amount (number of boxes, e.g., 3): ").strip()
            reversal = int(reversal)
            if reversal > 0:
                break
            print("Reversal amount must be greater than 0.")
        except ValueError:
            print("Please enter a valid integer.")

    # Get time period
    print("\nSelect time period:")
    print("  1. 1 month")
    print("  2. 3 months")
    print("  3. 6 months")
    print("  4. 1 year (default)")
    print("  5. 2 years")
    print("  6. 5 years")
    print("  7. Max")

    period_map = {
        '1': '1mo',
        '2': '3mo',
        '3': '6mo',
        '4': '1y',
        '5': '2y',
        '6': '5y',
        '7': 'max'
    }

    while True:
        choice = input("\nEnter choice (1-7) [default: 4]: ").strip() or '4'
        if choice in period_map:
            period = period_map[choice]
            break
        print("Please enter a number between 1 and 7.")

    # Get price type
    print("\nSelect price type:")
    print("  1. High/Low (default - more sensitive)")
    print("  2. Close only")

    while True:
        choice = input("\nEnter choice (1-2) [default: 1]: ").strip() or '1'
        if choice in ['1', '2']:
            price_type = 'hl' if choice == '1' else 'close'
            break
        print("Please enter 1 or 2.")

    return {
        'symbol': symbol,
        'box_size': box_size,
        'reversal_amount': reversal,
        'period': period,
        'price_type': price_type
    }


def main():
    """
    Main function to run the interactive P&F chart analyzer
    """
    try:
        # Get user inputs
        params = get_user_input()

        print("\n" + "=" * 60)
        print("Generating Point and Figure Chart...")
        print("=" * 60)

        # Create and display the P&F chart
        pnf_chart = create_pnf_chart(
            symbol=params['symbol'],
            box_size=params['box_size'],
            reversal_amount=params['reversal_amount'],
            period=params['period'],
            price_type=params['price_type']
        )

        print("\nChart generated successfully!")
        print(f"Total columns: {len(pnf_chart.columns)}")

        # Show pattern summary
        x_columns = sum(1 for col in pnf_chart.columns if col['type'] == 'X')
        o_columns = sum(1 for col in pnf_chart.columns if col['type'] == 'O')
        print(f"Up columns (X): {x_columns}")
        print(f"Down columns (O): {o_columns}")

        if len(pnf_chart.columns) > 0:
            last_column = pnf_chart.columns[-1]
            trend = "Bullish (X)" if last_column['type'] == 'X' else "Bearish (O)"
            print(f"Current trend: {trend}")

        print("\nDisplaying chart...")
        plt.show()

        # Ask if user wants to analyze another stock
        print("\n" + "=" * 60)
        another = input("Analyze another stock? (y/n): ").strip().lower()
        if another == 'y':
            print("\n")
            main()  # Recursive call for another analysis

    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
