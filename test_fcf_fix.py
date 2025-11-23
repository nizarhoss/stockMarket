#!/usr/bin/env python3
"""
Test the fixed FCF data ordering and CAGR calculation
"""

from dcf_valuation import calculate_cagr

print("Testing Fixed CAGR Calculation")
print("=" * 70)

# Test 1: Growing FCF (should give positive CAGR)
print("\nTest 1: Growing FCF")
fcf_growing = [100, 110, 121, 133.1, 146.41]
cagr = calculate_cagr(fcf_growing)
print(f"FCF (oldest to newest): {fcf_growing}")
print(f"CAGR: {cagr*100:.2f}% (Expected: ~10%)")
print(f"✓ PASS" if 9.5 <= cagr*100 <= 10.5 else "✗ FAIL")

# Test 2: Declining FCF (should give negative CAGR)
print("\nTest 2: Declining FCF")
fcf_declining = [150, 135, 121.5, 109.35]
cagr = calculate_cagr(fcf_declining)
print(f"FCF (oldest to newest): {fcf_declining}")
print(f"CAGR: {cagr*100:.2f}% (Expected: ~-10%)")
print(f"✓ PASS" if -10.5 <= cagr*100 <= -9.5 else "✗ FAIL")

# Test 3: Stable FCF (should give ~0% CAGR)
print("\nTest 3: Stable FCF")
fcf_stable = [100, 101, 99, 100]
cagr = calculate_cagr(fcf_stable)
print(f"FCF (oldest to newest): {fcf_stable}")
print(f"CAGR: {cagr*100:.2f}% (Expected: ~0%)")
print(f"✓ PASS" if -1 <= cagr*100 <= 1 else "✗ FAIL")

# Test 4: Edge case - only 2 values
print("\nTest 4: Two values only")
fcf_two = [100, 110]
cagr = calculate_cagr(fcf_two)
print(f"FCF: {fcf_two}")
print(f"CAGR: {cagr*100:.2f}% (Expected: 10%)")
print(f"✓ PASS" if 9.5 <= cagr*100 <= 10.5 else "✗ FAIL")

print("\n" + "=" * 70)
print("All tests completed!")
print("\nKey Points:")
print("- Data should always be in chronological order (oldest to newest)")
print("- Positive CAGR = growing cash flows")
print("- Negative CAGR = declining cash flows")
print("- The fix ensures yfinance data is sorted by date before processing")
