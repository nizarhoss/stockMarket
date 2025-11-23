# DCF Valuation Calculator

A comprehensive Python tool for calculating the intrinsic value of stocks using Discounted Cash Flow (DCF) analysis with automatic financial data fetching.

## Quick Start

```bash
# Optional: Install yfinance for automatic data fetching
pip install yfinance

# Run the calculator
python3 dcf_valuation.py
```

## Features

### 🚀 Automatic Data Fetching
- Pulls historical Free Cash Flow from Yahoo Finance API
- Automatically calculates historical growth rates (CAGR)
- Fetches current shares outstanding
- Gets current market price for comparison

### 📊 DCF Valuation
- Projects 5-10 years of Free Cash Flow
- Calculates terminal value using Gordon Growth Model
- Discounts all cash flows to present value
- Computes intrinsic value per share

### 🎯 Smart Analysis
- Shows if stock is undervalued or overvalued
- Compares intrinsic value with market price
- Optional sensitivity analysis for different assumptions
- Handles both automatic and manual data entry

### 🔄 Flexible Input Methods
1. **Automatic Mode** (with yfinance)
   - Uses historical growth rate
   - Fetches all financial data automatically

2. **Custom Growth Rate**
   - Use initial FCF with custom growth rate
   - Can reference historical growth as default

3. **Manual Entry**
   - Enter FCF for each year individually
   - Full control over projections

## Installation

### Requirements
- Python 3.x
- `yfinance` (optional but recommended)

### Install yfinance

```bash
pip install yfinance
```

The script works without yfinance but requires manual data entry.

## Usage Example

### With Automatic Data Fetching

```bash
$ python3 dcf_valuation.py

Enter stock ticker symbol: MSFT

Fetching financial data for MSFT...

FETCHED DATA FOR MSFT
Company: Microsoft Corporation
Current Price: $374.23
Shares Outstanding: 7,430.00M

Historical Free Cash Flow (4 years):
  Year -3: $45,234.00M
  Year -2: $56,118.00M
  Year -1: $65,149.00M
  Year 0: $72,546.00M

Calculated Historical FCF Growth Rate (CAGR): 17.12%

Choose input method:
  1. Use historical growth rate (AUTO)
  2. Use custom growth rate
  3. Enter FCF for each year manually
```

### Manual Mode

If yfinance is not installed, the script automatically falls back to manual entry mode.

## How It Works

### 1. Data Collection
- **Automatic**: Fetches historical cash flow statements from Yahoo Finance
- **Manual**: User enters FCF projections

### 2. Growth Rate Calculation
Computes CAGR (Compound Annual Growth Rate) from historical FCF:

```
CAGR = (Ending Value / Beginning Value)^(1/Years) - 1
```

### 3. FCF Projection
Projects future FCF using growth rate:

```
FCF_year_n = FCF_current × (1 + growth_rate)^n
```

### 4. Present Value Calculation
Discounts each future FCF to present value:

```
PV = FCF_n / (1 + WACC)^n
```

### 5. Terminal Value
Calculates perpetual value using Gordon Growth Model:

```
Terminal Value = FCF_final × (1 + terminal_growth) / (WACC - terminal_growth)
```

### 6. Intrinsic Value
Sums all present values and divides by shares:

```
Intrinsic Value = (PV of FCFs + PV of Terminal Value) / Shares Outstanding
```

## Parameters

| Parameter | Description | Typical Range | Auto-Fetched |
|-----------|-------------|---------------|--------------|
| **Ticker** | Stock symbol | e.g., AAPL, MSFT | ✅ Validated |
| **FCF** | Free Cash Flow | Company specific | ✅ Yes |
| **Growth Rate** | FCF CAGR | 5-15% | ✅ Calculated |
| **Years** | Projection period | 5-10 years | ❌ User input |
| **WACC** | Discount rate | 8-12% | ❌ User input (default 10%) |
| **Terminal Growth** | Perpetual growth | 2-3% | ❌ User input (default 2.5%) |
| **Shares** | Shares outstanding | Company specific | ✅ Yes |

## Output

### Valuation Results
```
INTRINSIC VALUE PER SHARE: $125.50

Current Market Price: $115.23
Potential Upside: $10.27 (8.91%)
=> Stock appears UNDERVALUED
```

### Detailed Breakdown
- Year-by-year FCF projections
- Discount factors and present values
- Terminal value calculation
- Enterprise value summary

### Optional Sensitivity Analysis
Shows intrinsic value at different WACC and growth rate combinations:

```
WACC \ TG    2.00%     2.50%     3.00%
 9.00%      $130.45   $135.22   $140.58
10.00%      $115.23   $119.34   $123.87
11.00%      $102.56   $105.89   $109.52
```

## Files

- `dcf_valuation.py` - Main script
- `DCF_EXAMPLE.md` - Detailed usage guide
- `README_DCF.md` - This file

## Key Features

### Automatic API Integration
- ✅ Fetches historical Free Cash Flow
- ✅ Calculates CAGR from historical data
- ✅ Gets shares outstanding
- ✅ Retrieves current market price
- ✅ Shows company name and market cap

### Robust Fallback
- ✅ Works with or without yfinance
- ✅ Graceful error handling
- ✅ Manual entry mode always available

### User-Friendly
- ✅ Clear prompts with defaults
- ✅ Input validation
- ✅ Formatted output
- ✅ Market comparison
- ✅ Sensitivity analysis

## Limitations

- Requires historical financial data for automatic mode
- DCF is sensitive to assumptions (WACC, growth rates)
- Best for mature, profitable companies
- Less reliable for startups or highly cyclical businesses
- Past growth doesn't guarantee future performance

## Tips

1. **Verify fetched data** - Always review historical FCF for accuracy
2. **Adjust growth rates** - Historical growth may not be sustainable
3. **Use conservative estimates** - Better to underestimate than overestimate
4. **Run sensitivity analysis** - Test different scenarios
5. **Compare multiple stocks** - Find the best investment opportunities

## Formulas

### Free Cash Flow
```
FCF = Operating Cash Flow - Capital Expenditures
```

### CAGR
```
CAGR = (End Value / Start Value)^(1/n) - 1
```

### Present Value
```
PV = Future Value / (1 + discount_rate)^years
```

### Terminal Value
```
TV = FCF_terminal × (1 + g) / (WACC - g)
```

### Intrinsic Value
```
Intrinsic Value = Enterprise Value / Shares Outstanding
Enterprise Value = Σ(PV of FCFs) + PV(Terminal Value)
```

## Example Calculation

Given:
- Latest FCF: $100M
- Historical Growth Rate: 10%
- Projection: 5 years
- WACC: 10%
- Terminal Growth: 2.5%
- Shares: 1,000M

Projected FCF:
1. Year 1: $110M → PV: $100M
2. Year 2: $121M → PV: $100M
3. Year 3: $133.1M → PV: $100M
4. Year 4: $146.41M → PV: $100M
5. Year 5: $161.05M → PV: $100M

Terminal Value: $2,200M → PV: $1,365.6M

Enterprise Value: $500M + $1,365.6M = $1,865.6M
**Intrinsic Value: $1.87 per share**

## Contributing

Suggestions and improvements welcome!

## License

Open source - feel free to use and modify.

## Author

Created for fundamental stock analysis and investment research.

---

**Disclaimer**: This tool is for educational and research purposes only. Always do your own research and consult with financial professionals before making investment decisions.
