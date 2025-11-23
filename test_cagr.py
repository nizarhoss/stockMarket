#!/usr/bin/env python3
"""
Test script to diagnose the FCF and CAGR calculation issues
"""

# Simulate the issue
def calculate_cagr_current(values):
    """Current implementation"""
    if len(values) < 2:
        return 0.0

    valid_values = [v for v in values if v is not None and v > 0]

    if len(valid_values) < 2:
        return 0.0

    start_value = valid_values[0]
    end_value = valid_values[-1]
    num_periods = len(valid_values) - 1

    if start_value <= 0:
        return 0.0

    cagr = (end_value / start_value) ** (1 / num_periods) - 1
    return cagr


# Test with sample data
print("Testing CAGR Calculation")
print("=" * 60)

# Test 1: Normal growth - oldest to newest
fcf_growing = [100, 110, 121, 133.1]
cagr1 = calculate_cagr_current(fcf_growing)
print(f"\nTest 1 - Growing FCF (oldest to newest): {fcf_growing}")
print(f"CAGR: {cagr1*100:.2f}% (Expected: ~10%)")

# Test 2: Same data but reversed (newest to oldest)
fcf_reversed = [133.1, 121, 110, 100]
cagr2 = calculate_cagr_current(fcf_reversed)
print(f"\nTest 2 - Same data reversed (newest to oldest): {fcf_reversed}")
print(f"CAGR: {cagr2*100:.2f}% (Expected: ~10%, Got negative if order wrong)")

# Test 3: What yfinance might return
print("\n" + "=" * 60)
print("Simulating yfinance data order:")
print("yfinance.cashflow typically returns columns in reverse chronological order")
print("(newest year first, oldest year last)")
print()

# Simulate yfinance returning: [2023, 2022, 2021, 2020]
yfinance_order = [133.1, 121, 110, 100]
print(f"As received from yfinance: {yfinance_order}")

# Current code does: historic_fcf.reverse()
yfinance_order.reverse()
print(f"After .reverse(): {yfinance_order}")
print(f"CAGR: {calculate_cagr_current(yfinance_order)*100:.2f}%")

print("\n" + "=" * 60)
print("\nDIAGNOSIS:")
print("If yfinance returns data in reverse chronological order (newest first),")
print("and we reverse it to get chronological order (oldest first),")
print("the CAGR should be correct (positive for growth).")
print()
print("If getting negative CAGR, possible issues:")
print("1. Data not in expected order after reverse")
print("2. FCF calculation (OCF - CapEx) has sign issue")
print("3. Data extraction from DataFrame is incorrect")
