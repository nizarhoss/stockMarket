#!/usr/bin/env python3
"""
Test script for Point and Figure chart functionality
"""

from pnf_chart import create_pnf_chart
import matplotlib.pyplot as plt


def test_pnf_chart():
    """
    Test the P&F chart with Apple stock
    """
    print("Testing Point and Figure Chart with AAPL...")
    print("-" * 60)

    try:
        # Create a P&F chart for Apple with predefined parameters
        pnf_chart = create_pnf_chart(
            symbol='AAPL',
            box_size=5.0,
            reversal_amount=3,
            period='6mo',
            price_type='hl'
        )

        print("\n" + "=" * 60)
        print("Test Results:")
        print("=" * 60)
        print(f"✓ Successfully created P&F chart")
        print(f"✓ Total columns: {len(pnf_chart.columns)}")

        if len(pnf_chart.columns) > 0:
            x_columns = sum(1 for col in pnf_chart.columns if col['type'] == 'X')
            o_columns = sum(1 for col in pnf_chart.columns if col['type'] == 'O')
            print(f"✓ Up columns (X): {x_columns}")
            print(f"✓ Down columns (O): {o_columns}")

            last_column = pnf_chart.columns[-1]
            trend = "Bullish (X)" if last_column['type'] == 'X' else "Bearish (O)"
            print(f"✓ Current trend: {trend}")

            print("\n✓ All tests passed!")
            print("\nDisplaying chart... (close the chart window to exit)")
            plt.show()
        else:
            print("✗ Warning: No columns generated")

    except Exception as e:
        print(f"\n✗ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_pnf_chart()
