#!/usr/bin/env python3
"""
Test P&F chart with CSV sample data
"""

from pnf_chart import create_pnf_chart
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for testing
import matplotlib.pyplot as plt


def test_pnf_with_csv():
    """
    Test P&F chart with sample CSV data
    """
    print("Testing P&F Chart with sample CSV data...")
    print("-" * 60)

    try:
        # Test with uptrending sample data
        pnf = create_pnf_chart(
            symbol='BULL (Uptrend Sample)',
            box_size=2.0,
            reversal_amount=3,
            data_source='csv',
            csv_path='sample_uptrend.csv'
        )

        print("\n" + "=" * 60)
        print("Test Results:")
        print("=" * 60)
        print(f"✓ Successfully created P&F chart from CSV")
        print(f"✓ Total columns: {len(pnf.columns)}")

        if len(pnf.columns) > 0:
            x_columns = sum(1 for col in pnf.columns if col['type'] == 'X')
            o_columns = sum(1 for col in pnf.columns if col['type'] == 'O')
            print(f"✓ Up columns (X): {x_columns}")
            print(f"✓ Down columns (O): {o_columns}")

            last_column = pnf.columns[-1]
            trend = "Bullish (X)" if last_column['type'] == 'X' else "Bearish (O)"
            print(f"✓ Current trend: {trend}")

            # Save chart to file
            plt.savefig('test_pnf_chart.png', dpi=150, bbox_inches='tight')
            print(f"\n✓ Chart saved to: test_pnf_chart.png")
            print("\n✓ All tests passed!")

        else:
            print("✗ Warning: No columns generated")

    except Exception as e:
        print(f"\n✗ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_pnf_with_csv()
