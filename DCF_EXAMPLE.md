# DCF Valuation Calculator - Usage Guide

## Overview
The DCF (Discounted Cash Flow) Valuation Calculator automatically fetches financial data from Yahoo Finance and calculates the intrinsic value of stocks using fundamental analysis.

## Features

✅ **Automatic Data Fetching** - Pulls historical cash flows from Yahoo Finance API
✅ **Growth Rate Calculation** - Computes CAGR from historical FCF data
✅ **Automatic Shares Outstanding** - Fetches current share count
✅ **Market Price Comparison** - Shows if stock is undervalued or overvalued
✅ **Manual Fallback** - Works with or without yfinance library
✅ **Sensitivity Analysis** - Tests different WACC and growth rate scenarios

## Installation

For automatic data fetching, install yfinance:

```bash
pip install yfinance
```

The script will work without yfinance but requires manual data entry.

## Running the Script

```bash
python3 dcf_valuation.py
```

## Example Usage

### Example 1: Automatic Mode (Recommended)

When yfinance is installed, the script automatically fetches financial data:

```
Enter stock ticker symbol: AAPL

Fetching financial data for AAPL...

======================================================================
FETCHED DATA FOR AAPL
Company: Apple Inc.
======================================================================
Current Price: $178.45
Market Cap: $2,750,000.00M
Shares Outstanding: 15,400.00M

Historical Free Cash Flow (4 years):
  Year -3: $92,953.00M
  Year -2: $99,584.00M
  Year -1: $111,443.00M
  Year 0: $99,584.00M

Calculated Historical FCF Growth Rate (CAGR): 2.31%
======================================================================

--- PROJECTION PERIOD ---
Number of years to project FCF (default: 5):

--- FREE CASH FLOW PROJECTIONS ---
Choose input method:
  1. Use historical growth rate (AUTO)
  2. Use custom growth rate
  3. Enter FCF for each year manually
Enter choice (1, 2, or 3): 1

Using latest FCF: $99,584.00M
Using historical growth rate: 2.31%
Would you like to adjust the growth rate? (y/n): n

Projected FCF:
  Year 1: $101,884.27M
  Year 2: $104,237.81M
  Year 3: $106,646.15M
  Year 4: $109,110.87M
  Year 5: $111,633.60M

--- DISCOUNT RATE (WACC) ---
Enter WACC (as decimal, e.g., 0.10 for 10%) (default: 0.10): 0.09

--- TERMINAL GROWTH RATE ---
Enter terminal growth rate (as decimal, e.g., 0.025 for 2.5%) (default: 0.025):

--- SHARES OUTSTANDING ---
Enter shares outstanding (in millions) (default: 15400.0):

[Results displayed with automatic market price comparison]
```

### Example 2: Manual Mode

Without yfinance or when you want full control:

```
Enter stock ticker symbol: XYZ

--- PROJECTION PERIOD ---
Number of years to project FCF (default: 5): 5

--- FREE CASH FLOW PROJECTIONS ---
Choose input method:
  1. Enter FCF for each year manually
  2. Use growth rate from initial FCF
Enter choice (1 or 2): 2

Enter current/initial FCF (in millions): 500
Enter FCF growth rate (as decimal, e.g., 0.15 for 15%): 0.12

Projected FCF:
  Year 1: $560.00M
  Year 2: $627.20M
  Year 3: $702.46M
  Year 4: $786.76M
  Year 5: $881.17M

Enter WACC (as decimal, e.g., 0.10 for 10%) (default: 0.10):
Enter terminal growth rate (as decimal, e.g., 0.025 for 2.5%) (default: 0.025):
Enter shares outstanding (in millions): 1000
```

## What Gets Automatically Fetched

When using yfinance, the script automatically retrieves:

1. **Historical Free Cash Flow** - Last 3-5 years of FCF data
   - Calculated as: Operating Cash Flow - Capital Expenditures
   - Or pulled directly if available

2. **FCF Growth Rate (CAGR)** - Compound Annual Growth Rate calculated from historical data
   - Formula: (End Value / Start Value)^(1/Years) - 1

3. **Shares Outstanding** - Current number of shares (in millions)

4. **Current Market Price** - For comparison with intrinsic value

5. **Market Capitalization** - Current market cap

6. **Company Name** - For display

## Key Parameters Explained

### Free Cash Flow (FCF)
- Cash generated after capital expenditures
- **Automatically fetched** from Yahoo Finance
- Can be manually entered if needed
- Usually expressed in millions

### FCF Growth Rate
- **Automatically calculated** from historical data using CAGR
- You can adjust the calculated rate
- Or enter a custom growth rate for projections
- Typical range: 5-15% for growing companies

### WACC (Weighted Average Cost of Capital)
- The discount rate for future cash flows
- Represents the company's cost of capital
- Default: 10%
- Typical range: 8-12%
- Higher for riskier companies

### Terminal Growth Rate
- Perpetual growth rate after projection period
- Should be conservative (usually 2-3%)
- Default: 2.5%
- Cannot exceed long-term GDP growth rate

### Shares Outstanding
- **Automatically fetched** from Yahoo Finance
- Total number of company shares
- Usually expressed in millions

## Output Explanation

The script provides:

1. **Fetched Data Summary** (when using yfinance)
   - Company information
   - Historical FCF with CAGR
   - Current market metrics

2. **Year-by-year FCF Breakdown**
   - Each projected FCF and its present value
   - Discount factors applied

3. **Terminal Value Calculation**
   - Value of all cash flows beyond projection period
   - Using Gordon Growth Model

4. **Valuation Summary**
   - Enterprise Value (total business value)
   - Intrinsic Value per Share

5. **Market Comparison** (when current price available)
   - Shows if stock is undervalued or overvalued
   - Potential upside/downside percentage

6. **Optional Sensitivity Analysis**
   - Tests different WACC and growth rate combinations
   - Shows how assumptions affect valuation

## Example Output

```
======================================================================
DCF VALUATION RESULTS FOR AAPL
======================================================================

PROJECTED FREE CASH FLOWS (Discounted to Present Value):
----------------------------------------------------------------------
Year   FCF ($M)        Discount Factor    PV ($M)
----------------------------------------------------------------------
1        101,884.27           1.0900        93,470.52
2        104,237.81           1.1881        87,736.99
3        106,646.15           1.2950        82,337.00
4        109,110.87           1.4116        77,294.61
5        111,633.60           1.5386        72,556.37
----------------------------------------------------------------------
TOTAL                                      413,395.49

TERMINAL VALUE CALCULATION:
----------------------------------------------------------------------
Terminal Year FCF (growing at 2.50%): $114,424.44M
Terminal Value (Gordon Growth Model):      $1,760,376.00M
PV of Terminal Value:                       $1,144,145.17M

VALUATION SUMMARY:
======================================================================
PV of Projected FCFs:                       $413,395.49M
PV of Terminal Value:                       $1,144,145.17M
----------------------------------------------------------------------
Enterprise Value:                           $1,557,540.66M
Shares Outstanding:                         15,400.00M
======================================================================
INTRINSIC VALUE PER SHARE:                  $101.14
======================================================================

Current Market Price:                       $178.45
Potential Downside:                         $77.31 (-43.33%)
=> Stock appears OVERVALUED

KEY ASSUMPTIONS:
  - WACC (Discount Rate):      9.00%
  - Terminal Growth Rate:      2.50%
  - Projection Period:         5 years
```

## Sensitivity Analysis Example

```
--- SENSITIVITY ANALYSIS ---
Intrinsic Value at Different WACC and Terminal Growth Rates:
----------------------------------------------------------------------
WACC \ TG       1.50%      2.00%      2.50%      3.00%      3.50%
----------------------------------------------------------------------
 8.00%      $110.23    $114.67    $119.58    $125.04    $131.16
 9.00%       $97.89    $101.14    $104.71    $108.64    $112.99
10.00%       $87.92     $90.45     $93.20     $96.20     $99.47
11.00%       $79.73     $81.78     $83.99     $86.37     $88.95
12.00%       $72.90     $74.58     $76.37     $78.28     $80.33
```

## Tips for Best Results

1. **Use yfinance for accurate data** - Install it for automatic data fetching
   ```bash
   pip install yfinance
   ```

2. **Verify historical data** - Check if fetched FCF data looks reasonable

3. **Adjust growth rates** - Historical growth may not be sustainable

4. **Conservative assumptions** - Better to underestimate than overestimate

5. **Run sensitivity analysis** - See how different assumptions affect valuation

6. **Compare with market** - Use the automatic price comparison feature

7. **Check for consistency** - Ensure WACC > Terminal Growth Rate

## How Historical Growth Rate is Calculated

The script uses CAGR (Compound Annual Growth Rate):

```
CAGR = (End Value / Start Value)^(1 / Number of Years) - 1
```

For example, if FCF grew from $100M to $133.1M over 3 years:
```
CAGR = ($133.1 / $100)^(1/3) - 1 = 0.10 or 10%
```

## Limitations

- **Data accuracy** - Relies on Yahoo Finance data quality
- **Assumption sensitivity** - Results highly dependent on WACC and growth rates
- **Best for mature companies** - Less reliable for startups or cyclical businesses
- **Requires internet** - For automatic data fetching
- **Historical ≠ Future** - Past growth doesn't guarantee future performance

## Troubleshooting

**"No cash flow data available"**
- Company may not have financial data on Yahoo Finance
- Switch to manual entry mode

**"Error fetching data"**
- Check internet connection
- Verify ticker symbol is correct
- Try again or use manual mode

**"WACC must be greater than terminal growth rate"**
- Adjust your WACC or terminal growth assumptions
- This is a mathematical requirement for the model

## Advanced Usage

### Custom Growth Scenarios

You can create custom growth scenarios:

1. Choose option 3 (manual entry)
2. Enter different FCF for each year
3. Model scenarios like:
   - High growth early, then slowing
   - Recovery from temporary setback
   - Impact of specific business initiatives

### Comparing Multiple Stocks

Run the script multiple times to compare intrinsic values of different stocks and identify the best investment opportunities.

## Formula Reference

**Free Cash Flow (FCF)**
```
FCF = Operating Cash Flow - Capital Expenditures
```

**Present Value of FCF**
```
PV = FCF_n / (1 + WACC)^n
```

**Terminal Value (Gordon Growth Model)**
```
Terminal Value = FCF_final × (1 + g) / (WACC - g)
```

**Intrinsic Value per Share**
```
Intrinsic Value = Enterprise Value / Shares Outstanding
Enterprise Value = PV of Projected FCFs + PV of Terminal Value
```
