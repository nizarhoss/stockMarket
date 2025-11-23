#!/usr/bin/env python3
"""
DCF (Discounted Cash Flow) Valuation Calculator

This script calculates the intrinsic value of a stock using the DCF method.
It projects future free cash flows, calculates terminal value, and discounts
them back to present value.
"""

def get_float_input(prompt, default=None):
    """Get float input from user with optional default value."""
    while True:
        try:
            if default is not None:
                user_input = input(f"{prompt} (default: {default}): ").strip()
                if not user_input:
                    return default
            else:
                user_input = input(f"{prompt}: ").strip()

            return float(user_input)
        except ValueError:
            print("Invalid input. Please enter a valid number.")


def get_int_input(prompt, default=None):
    """Get integer input from user with optional default value."""
    while True:
        try:
            if default is not None:
                user_input = input(f"{prompt} (default: {default}): ").strip()
                if not user_input:
                    return default
            else:
                user_input = input(f"{prompt}: ").strip()

            return int(user_input)
        except ValueError:
            print("Invalid input. Please enter a valid integer.")


def get_fcf_projections(years):
    """Get FCF projections from user."""
    print(f"\nEnter Free Cash Flow projections for {years} years (in millions):")
    fcf_list = []

    for i in range(years):
        fcf = get_float_input(f"  Year {i+1} FCF")
        fcf_list.append(fcf)

    return fcf_list


def get_fcf_with_growth(initial_fcf, growth_rate, years):
    """Calculate FCF projections based on initial FCF and growth rate."""
    fcf_list = []
    for i in range(years):
        fcf = initial_fcf * ((1 + growth_rate) ** i)
        fcf_list.append(fcf)
    return fcf_list


def dcf_valuation(fcf_list, wacc, terminal_growth, shares):
    """
    Calculate DCF valuation.

    Parameters:
    -----------
    fcf_list : list
        List of projected free cash flows (in millions)
    wacc : float
        Weighted Average Cost of Capital (as decimal, e.g., 0.10 for 10%)
    terminal_growth : float
        Terminal growth rate (as decimal, e.g., 0.025 for 2.5%)
    shares : float
        Shares outstanding (in millions)

    Returns:
    --------
    dict : Dictionary containing:
        - intrinsic_value: Intrinsic value per share
        - pv_fcf: Present value of projected FCFs
        - terminal_value: Terminal value
        - pv_terminal_value: Present value of terminal value
        - enterprise_value: Total enterprise value
    """

    # Validate inputs
    if wacc <= terminal_growth:
        raise ValueError("WACC must be greater than terminal growth rate")

    if wacc <= 0 or terminal_growth < 0:
        raise ValueError("WACC must be positive and terminal growth cannot be negative")

    if shares <= 0:
        raise ValueError("Shares outstanding must be positive")

    # Project and discount FCF for explicit forecast period
    pv_fcf = 0
    fcf_details = []

    for year, fcf in enumerate(fcf_list, start=1):
        discount_factor = (1 + wacc) ** year
        pv = fcf / discount_factor
        pv_fcf += pv
        fcf_details.append({
            'year': year,
            'fcf': fcf,
            'discount_factor': discount_factor,
            'pv': pv
        })

    # Calculate terminal value using Gordon Growth Model
    last_fcf = fcf_list[-1]
    terminal_fcf = last_fcf * (1 + terminal_growth)
    terminal_value = terminal_fcf / (wacc - terminal_growth)

    # Discount terminal value to present
    terminal_year = len(fcf_list)
    terminal_discount_factor = (1 + wacc) ** terminal_year
    pv_terminal_value = terminal_value / terminal_discount_factor

    # Calculate enterprise value
    enterprise_value = pv_fcf + pv_terminal_value

    # Calculate intrinsic value per share
    intrinsic_value = enterprise_value / shares

    return {
        'intrinsic_value': intrinsic_value,
        'pv_fcf': pv_fcf,
        'terminal_value': terminal_value,
        'pv_terminal_value': pv_terminal_value,
        'enterprise_value': enterprise_value,
        'fcf_details': fcf_details,
        'terminal_fcf': terminal_fcf
    }


def display_results(ticker, result, wacc, terminal_growth, shares):
    """Display DCF valuation results in a formatted way."""
    print("\n" + "="*70)
    print(f"DCF VALUATION RESULTS FOR {ticker.upper()}")
    print("="*70)

    print("\nPROJECTED FREE CASH FLOWS (Discounted to Present Value):")
    print("-" * 70)
    print(f"{'Year':<6} {'FCF ($M)':<15} {'Discount Factor':<18} {'PV ($M)':<15}")
    print("-" * 70)

    for detail in result['fcf_details']:
        print(f"{detail['year']:<6} {detail['fcf']:>13,.2f}  "
              f"{detail['discount_factor']:>16,.4f}  {detail['pv']:>13,.2f}")

    print("-" * 70)
    print(f"{'TOTAL':<6} {'':<15} {'':<18} {result['pv_fcf']:>13,.2f}")

    print("\nTERMINAL VALUE CALCULATION:")
    print("-" * 70)
    print(f"Terminal Year FCF (growing at {terminal_growth*100:.2f}%): ${result['terminal_fcf']:,.2f}M")
    print(f"Terminal Value (Gordon Growth Model):      ${result['terminal_value']:,.2f}M")
    print(f"PV of Terminal Value:                       ${result['pv_terminal_value']:,.2f}M")

    print("\nVALUATION SUMMARY:")
    print("="*70)
    print(f"PV of Projected FCFs:                       ${result['pv_fcf']:,.2f}M")
    print(f"PV of Terminal Value:                       ${result['pv_terminal_value']:,.2f}M")
    print("-" * 70)
    print(f"Enterprise Value:                           ${result['enterprise_value']:,.2f}M")
    print(f"Shares Outstanding:                         {shares:,.2f}M")
    print("="*70)
    print(f"INTRINSIC VALUE PER SHARE:                  ${result['intrinsic_value']:.2f}")
    print("="*70)

    print("\nKEY ASSUMPTIONS:")
    print(f"  - WACC (Discount Rate):      {wacc*100:.2f}%")
    print(f"  - Terminal Growth Rate:      {terminal_growth*100:.2f}%")
    print(f"  - Projection Period:         {len(result['fcf_details'])} years")


def main():
    """Main function to run DCF valuation calculator."""
    print("="*70)
    print("DCF (DISCOUNTED CASH FLOW) VALUATION CALCULATOR")
    print("="*70)

    # Get stock ticker
    ticker = input("\nEnter stock ticker symbol: ").strip().upper()

    # Get projection period
    print("\n--- PROJECTION PERIOD ---")
    years = get_int_input("Number of years to project FCF", default=5)

    # Get FCF projections
    print("\n--- FREE CASH FLOW PROJECTIONS ---")
    print("Choose input method:")
    print("  1. Enter FCF for each year manually")
    print("  2. Use growth rate from initial FCF")

    method = input("Enter choice (1 or 2): ").strip()

    if method == "2":
        initial_fcf = get_float_input("\nEnter current/initial FCF (in millions)")
        growth_rate = get_float_input("Enter FCF growth rate (as decimal, e.g., 0.15 for 15%)")
        fcf_list = get_fcf_with_growth(initial_fcf, growth_rate, years)

        print("\nProjected FCF:")
        for i, fcf in enumerate(fcf_list, 1):
            print(f"  Year {i}: ${fcf:,.2f}M")
    else:
        fcf_list = get_fcf_projections(years)

    # Get WACC
    print("\n--- DISCOUNT RATE (WACC) ---")
    wacc = get_float_input("Enter WACC (as decimal, e.g., 0.10 for 10%)", default=0.10)

    # Get terminal growth rate
    print("\n--- TERMINAL GROWTH RATE ---")
    terminal_growth = get_float_input("Enter terminal growth rate (as decimal, e.g., 0.025 for 2.5%)", default=0.025)

    # Get shares outstanding
    print("\n--- SHARES OUTSTANDING ---")
    shares = get_float_input("Enter shares outstanding (in millions)")

    # Calculate DCF valuation
    try:
        result = dcf_valuation(fcf_list, wacc, terminal_growth, shares)
        display_results(ticker, result, wacc, terminal_growth, shares)

        # Optional: show sensitivity analysis prompt
        print("\n" + "="*70)
        sensitivity = input("\nWould you like to run a sensitivity analysis? (y/n): ").strip().lower()

        if sensitivity == 'y':
            print("\n--- SENSITIVITY ANALYSIS ---")
            print("Intrinsic Value at Different WACC and Terminal Growth Rates:")
            print("-" * 70)

            wacc_range = [wacc - 0.02, wacc - 0.01, wacc, wacc + 0.01, wacc + 0.02]
            growth_range = [terminal_growth - 0.01, terminal_growth - 0.005,
                          terminal_growth, terminal_growth + 0.005, terminal_growth + 0.01]

            # Filter out invalid combinations
            wacc_range = [w for w in wacc_range if w > 0]
            growth_range = [g for g in growth_range if g >= 0]

            # Header
            header = "WACC \\ TG"
            print(f"{header:<10}", end="")
            for g in growth_range:
                print(f"{g*100:>10.2f}%", end="")
            print()
            print("-" * 70)

            # Sensitivity table
            for w in wacc_range:
                print(f"{w*100:>6.2f}%   ", end="")
                for g in growth_range:
                    if w > g:
                        try:
                            sens_result = dcf_valuation(fcf_list, w, g, shares)
                            print(f"${sens_result['intrinsic_value']:>9.2f}", end="")
                        except:
                            print(f"{'N/A':>10}", end="")
                    else:
                        print(f"{'N/A':>10}", end="")
                print()

    except ValueError as e:
        print(f"\nError: {e}")
        return
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        return

    print("\nThank you for using the DCF Valuation Calculator!")


if __name__ == "__main__":
    main()
