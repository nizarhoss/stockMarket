#!/usr/bin/env python3
"""
DCF (Discounted Cash Flow) Valuation Calculator with API Integration

This script calculates the intrinsic value of a stock using the DCF method.
It automatically fetches historical financial data from Yahoo Finance API,
calculates growth rates, and projects future free cash flows.
"""

import sys
import json
from statistics import mean

# Try to import yfinance, fall back to manual data entry if not available
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    print("Note: yfinance not installed. Install with: pip install yfinance")
    print("Continuing with manual data entry mode...\n")


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


def calculate_cagr(values):
    """
    Calculate Compound Annual Growth Rate from a list of values.

    Parameters:
    -----------
    values : list
        List of values in chronological order

    Returns:
    --------
    float : CAGR as decimal (e.g., 0.15 for 15%)
    """
    if len(values) < 2:
        return 0.0

    # Remove None values and zeros
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


def fetch_stock_data(ticker):
    """
    Fetch historical financial data for a stock using yfinance.

    Parameters:
    -----------
    ticker : str
        Stock ticker symbol

    Returns:
    --------
    dict : Dictionary containing:
        - shares_outstanding: Number of shares (in millions)
        - historic_fcf: List of historical free cash flows
        - fcf_growth_rate: Calculated CAGR of FCF
        - current_price: Current stock price
        - market_cap: Market capitalization
    """
    if not YFINANCE_AVAILABLE:
        return None

    try:
        stock = yf.Ticker(ticker)

        # Get shares outstanding
        info = stock.info
        shares = info.get('sharesOutstanding', None)
        if shares:
            shares = shares / 1_000_000  # Convert to millions

        # Get historical cash flow data
        cash_flow = stock.cashflow

        if cash_flow.empty:
            print(f"Warning: No cash flow data available for {ticker}")
            return None

        # Get Free Cash Flow (Operating Cash Flow - Capital Expenditures)
        # Note: yfinance returns columns in reverse chronological order (newest first)
        historic_fcf = []

        # Get columns sorted in chronological order (oldest to newest)
        sorted_columns = sorted(cash_flow.columns)

        # Try to get FCF directly
        if 'Free Cash Flow' in cash_flow.index:
            fcf_row = cash_flow.loc['Free Cash Flow']
            # Extract values in chronological order (oldest to newest)
            for col in sorted_columns:
                value = fcf_row[col]
                # Check if value is valid (not NaN)
                if value is not None and not (isinstance(value, float) and value != value):
                    historic_fcf.append(value / 1_000_000)  # Convert to millions
        else:
            # Calculate FCF = Operating Cash Flow - Capital Expenditures
            ocf_row = None
            capex_row = None

            for idx in cash_flow.index:
                if 'Operating Cash Flow' in str(idx) or 'Total Cash From Operating Activities' in str(idx):
                    ocf_row = cash_flow.loc[idx]
                if 'Capital Expenditure' in str(idx) or 'Capital Expenditures' in str(idx):
                    capex_row = cash_flow.loc[idx]

            if ocf_row is not None and capex_row is not None:
                # Extract values in chronological order (oldest to newest)
                for col in sorted_columns:
                    ocf = ocf_row[col]
                    capex = capex_row[col]
                    if ocf is not None and capex is not None:
                        # Check for NaN
                        if not (isinstance(ocf, float) and ocf != ocf) and not (isinstance(capex, float) and capex != capex):
                            # CapEx is usually negative, so we add it (OCF + CapEx)
                            # If CapEx is positive in the data, we need to subtract it
                            if capex > 0:
                                fcf = (ocf - capex) / 1_000_000
                            else:
                                fcf = (ocf + capex) / 1_000_000
                            historic_fcf.append(fcf)

        # Calculate growth rate (CAGR)
        fcf_growth_rate = calculate_cagr(historic_fcf) if len(historic_fcf) >= 2 else 0.0

        # Debug: Show FCF trend to verify ordering is correct
        if len(historic_fcf) >= 2:
            trend = "increasing" if historic_fcf[-1] > historic_fcf[0] else "decreasing"
            # Uncomment for debugging:
            # print(f"Debug: FCF trend is {trend} (oldest: ${historic_fcf[0]:.2f}M, newest: ${historic_fcf[-1]:.2f}M)")

        # Get current price and market cap
        current_price = info.get('currentPrice', info.get('regularMarketPrice', None))
        market_cap = info.get('marketCap', None)
        if market_cap:
            market_cap = market_cap / 1_000_000  # Convert to millions

        return {
            'shares_outstanding': shares,
            'historic_fcf': historic_fcf,
            'fcf_growth_rate': fcf_growth_rate,
            'current_price': current_price,
            'market_cap': market_cap,
            'company_name': info.get('longName', ticker)
        }

    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return None


def display_stock_data(ticker, data):
    """Display fetched stock data."""
    print("\n" + "="*70)
    print(f"FETCHED DATA FOR {ticker.upper()}")
    if 'company_name' in data and data['company_name']:
        print(f"Company: {data['company_name']}")
    print("="*70)

    if data['current_price']:
        print(f"Current Price: ${data['current_price']:.2f}")

    if data['market_cap']:
        print(f"Market Cap: ${data['market_cap']:,.2f}M")

    if data['shares_outstanding']:
        print(f"Shares Outstanding: {data['shares_outstanding']:,.2f}M")

    if data['historic_fcf']:
        print(f"\nHistorical Free Cash Flow ({len(data['historic_fcf'])} years, oldest to newest):")
        for i, fcf in enumerate(data['historic_fcf']):
            # Show as "Year 1" (oldest), "Year 2", ... "Year N" (newest/most recent)
            year_label = f"Year {i+1}" + (" (oldest)" if i == 0 else " (most recent)" if i == len(data['historic_fcf'])-1 else "")
            print(f"  {year_label}: ${fcf:,.2f}M")

        # Show trend indicator
        if len(data['historic_fcf']) >= 2:
            fcf_change = data['historic_fcf'][-1] - data['historic_fcf'][0]
            fcf_change_pct = (fcf_change / abs(data['historic_fcf'][0])) * 100
            trend_symbol = "↗" if fcf_change > 0 else "↘"
            print(f"\n  Trend: {trend_symbol} ${abs(fcf_change):,.2f}M ({fcf_change_pct:+.1f}%) over period")

        print(f"\nCalculated Historical FCF Growth Rate (CAGR): {data['fcf_growth_rate']*100:.2f}%")

    print("="*70)


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
    for i in range(1, years + 1):
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


def display_results(ticker, result, wacc, terminal_growth, shares, current_price=None):
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

    # Show comparison with current market price if available
    if current_price:
        print(f"\nCurrent Market Price:                       ${current_price:.2f}")
        diff = result['intrinsic_value'] - current_price
        diff_pct = (diff / current_price) * 100

        if diff > 0:
            print(f"Potential Upside:                           ${diff:.2f} ({diff_pct:.2f}%)")
            print("=> Stock appears UNDERVALUED")
        else:
            print(f"Potential Downside:                         ${abs(diff):.2f} ({abs(diff_pct):.2f}%)")
            print("=> Stock appears OVERVALUED")

    print("\nKEY ASSUMPTIONS:")
    print(f"  - WACC (Discount Rate):      {wacc*100:.2f}%")
    print(f"  - Terminal Growth Rate:      {terminal_growth*100:.2f}%")
    print(f"  - Projection Period:         {len(result['fcf_details'])} years")


def main():
    """Main function to run DCF valuation calculator."""
    print("="*70)
    print("DCF (DISCOUNTED CASH FLOW) VALUATION CALCULATOR")
    print("With Automatic Financial Data Fetching")
    print("="*70)

    # Get stock ticker
    ticker = input("\nEnter stock ticker symbol: ").strip().upper()

    # Try to fetch stock data automatically
    stock_data = None
    if YFINANCE_AVAILABLE:
        print(f"\nFetching financial data for {ticker}...")
        stock_data = fetch_stock_data(ticker)

        if stock_data:
            display_stock_data(ticker, stock_data)

    # Get projection period
    print("\n--- PROJECTION PERIOD ---")
    years = get_int_input("Number of years to project FCF", default=5)

    # Get FCF projections
    print("\n--- FREE CASH FLOW PROJECTIONS ---")

    # Offer different input methods based on available data
    if stock_data and stock_data.get('historic_fcf') and len(stock_data['historic_fcf']) > 0:
        print("Choose input method:")
        print("  1. Use historical growth rate (AUTO)")
        print("  2. Use custom growth rate")
        print("  3. Enter FCF for each year manually")
        method = input("Enter choice (1, 2, or 3): ").strip()
    else:
        print("Choose input method:")
        print("  1. Enter FCF for each year manually")
        print("  2. Use growth rate from initial FCF")
        method_map = {"1": "3", "2": "2"}
        method = method_map.get(input("Enter choice (1 or 2): ").strip(), "3")

    if method == "1" and stock_data:
        # Use historical data and growth rate
        latest_fcf = stock_data['historic_fcf'][-1]
        growth_rate = stock_data['fcf_growth_rate']

        print(f"\nUsing latest FCF: ${latest_fcf:,.2f}M")
        print(f"Using historical growth rate: {growth_rate*100:.2f}%")

        # Ask if user wants to adjust
        adjust = input("Would you like to adjust the growth rate? (y/n): ").strip().lower()
        if adjust == 'y':
            growth_rate = get_float_input("Enter adjusted FCF growth rate (as decimal)")

        fcf_list = get_fcf_with_growth(latest_fcf, growth_rate, years)

        print("\nProjected FCF:")
        for i, fcf in enumerate(fcf_list, 1):
            print(f"  Year {i}: ${fcf:,.2f}M")

    elif method == "2":
        initial_fcf = get_float_input("\nEnter current/initial FCF (in millions)")

        if stock_data and stock_data.get('fcf_growth_rate'):
            default_growth = stock_data['fcf_growth_rate']
            print(f"\nHistorical growth rate: {default_growth*100:.2f}%")
            growth_rate = get_float_input("Enter FCF growth rate (as decimal, e.g., 0.15 for 15%)",
                                         default=default_growth)
        else:
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
    if stock_data and stock_data.get('shares_outstanding'):
        default_shares = stock_data['shares_outstanding']
        shares = get_float_input("Enter shares outstanding (in millions)", default=default_shares)
    else:
        shares = get_float_input("Enter shares outstanding (in millions)")

    # Calculate DCF valuation
    try:
        result = dcf_valuation(fcf_list, wacc, terminal_growth, shares)

        current_price = stock_data.get('current_price') if stock_data else None
        display_results(ticker, result, wacc, terminal_growth, shares, current_price)

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
