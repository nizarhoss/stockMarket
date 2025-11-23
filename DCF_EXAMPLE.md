# DCF Valuation Calculator - Usage Guide

## Overview
The DCF (Discounted Cash Flow) Valuation Calculator helps you determine the intrinsic value of a stock using fundamental analysis.

## Running the Script

```bash
python3 dcf_valuation.py
```

## Example Usage

### Example 1: Manual FCF Entry
```
Enter stock ticker symbol: AAPL
Number of years to project FCF (default: 5): 5
Choose input method:
  1. Enter FCF for each year manually
  2. Use growth rate from initial FCF
Enter choice (1 or 2): 1

Enter Free Cash Flow projections for 5 years (in millions):
  Year 1 FCF: 100000
  Year 2 FCF: 110000
  Year 3 FCF: 121000
  Year 4 FCF: 133100
  Year 5 FCF: 146410

Enter WACC (as decimal, e.g., 0.10 for 10%) (default: 0.10): 0.10
Enter terminal growth rate (as decimal, e.g., 0.025 for 2.5%) (default: 0.025): 0.025
Enter shares outstanding (in millions): 15000
```

### Example 2: Using Growth Rate
```
Enter stock ticker symbol: MSFT
Number of years to project FCF (default: 5): 5
Choose input method:
  1. Enter FCF for each year manually
  2. Use growth rate from initial FCF
Enter choice (1 or 2): 2

Enter current/initial FCF (in millions): 75000
Enter FCF growth rate (as decimal, e.g., 0.15 for 15%): 0.12

Enter WACC (as decimal, e.g., 0.10 for 10%) (default: 0.10): 0.09
Enter terminal growth rate (as decimal, e.g., 0.025 for 2.5%) (default: 0.025): 0.03
Enter shares outstanding (in millions): 7400
```

## Key Parameters Explained

### Free Cash Flow (FCF)
- The cash a company generates after accounting for capital expenditures
- Can be found in company financial statements
- Usually expressed in millions

### WACC (Weighted Average Cost of Capital)
- The discount rate used to discount future cash flows
- Represents the company's cost of capital
- Typical range: 8-12%
- Higher for riskier companies

### Terminal Growth Rate
- The rate at which FCF is expected to grow perpetually after the projection period
- Should be conservative (usually 2-3%)
- Cannot exceed the long-term GDP growth rate

### Shares Outstanding
- Total number of company shares
- Can be found in company financial reports
- Usually expressed in millions

## Output Explanation

The script provides:
1. **Year-by-year FCF breakdown** - Shows each projected FCF and its present value
2. **Terminal Value calculation** - The value of all cash flows beyond the projection period
3. **Enterprise Value** - Total value of the business
4. **Intrinsic Value per Share** - The estimated fair value of one share

## Sensitivity Analysis

The script offers an optional sensitivity analysis that shows how the intrinsic value changes with different WACC and terminal growth rate assumptions.

## Example Calculation

For a company with:
- Current FCF: $100M growing at 10% annually for 5 years
- WACC: 10%
- Terminal Growth: 2.5%
- Shares Outstanding: 1,000M

The calculation would be:
1. Project FCF: $100M, $110M, $121M, $133.1M, $146.41M
2. Calculate PV of each year's FCF
3. Calculate Terminal Value = $146.41M × 1.025 / (0.10 - 0.025) = $2,000.94M
4. Discount Terminal Value to present
5. Sum all present values = Enterprise Value
6. Divide by shares outstanding = Intrinsic Value per Share

## Tips for Accurate Valuation

1. **Use realistic growth rates** - Extremely high growth rates are rarely sustainable
2. **Conservative terminal growth** - Long-term growth usually converges to GDP growth (2-3%)
3. **Appropriate WACC** - Research the company's beta and cost of capital
4. **Multiple scenarios** - Run the calculation with different assumptions
5. **Compare to market price** - If intrinsic value > market price, stock may be undervalued

## Limitations

- DCF is highly sensitive to assumptions
- Requires accurate financial projections
- Best for mature, profitable companies with predictable cash flows
- Less reliable for startups or cyclical businesses
