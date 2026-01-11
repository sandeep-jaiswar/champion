# Fundamentals Data Verification Queries

This document provides SQL queries to verify and analyze the quarterly financials and shareholding pattern data.

## ClickHouse Verification Queries

### Quarterly Financials

#### 1. Basic Data Verification

```sql
-- Count total records
SELECT COUNT(*) as total_records
FROM champion_market.quarterly_financials;

-- Count records by year
SELECT 
    year,
    COUNT(*) as records,
    COUNT(DISTINCT symbol) as unique_symbols
FROM champion_market.quarterly_financials
GROUP BY year
ORDER BY year DESC;

-- Count records by quarter
SELECT 
    year,
    quarter,
    COUNT(*) as records,
    COUNT(DISTINCT symbol) as unique_symbols
FROM champion_market.quarterly_financials
GROUP BY year, quarter
ORDER BY year DESC, quarter DESC;
```

#### 2. Financial Metrics Analysis

```sql
-- Average financial metrics by symbol (latest quarter)
SELECT 
    symbol,
    period_end_date,
    revenue,
    net_profit,
    eps,
    roe,
    roa,
    debt_to_equity,
    net_margin
FROM champion_market.quarterly_financials
WHERE period_end_date = (
    SELECT MAX(period_end_date) 
    FROM champion_market.quarterly_financials
)
ORDER BY revenue DESC
LIMIT 20;

-- ROE analysis (Return on Equity)
SELECT 
    symbol,
    period_end_date,
    roe,
    net_profit,
    equity
FROM champion_market.quarterly_financials
WHERE roe IS NOT NULL
ORDER BY roe DESC
LIMIT 20;

-- Debt-to-Equity ratio analysis
SELECT 
    symbol,
    period_end_date,
    debt_to_equity,
    total_debt,
    equity
FROM champion_market.quarterly_financials
WHERE debt_to_equity IS NOT NULL
ORDER BY debt_to_equity ASC
LIMIT 20;

-- Revenue growth analysis (YoY comparison)
SELECT 
    q2.symbol,
    q2.period_end_date as current_period,
    q2.revenue as current_revenue,
    q1.revenue as previous_year_revenue,
    ((q2.revenue - q1.revenue) / q1.revenue * 100) as yoy_growth_percent
FROM champion_market.quarterly_financials q2
JOIN champion_market.quarterly_financials q1 
    ON q2.symbol = q1.symbol 
    AND q2.quarter = q1.quarter
    AND q2.year = q1.year + 1
WHERE q2.revenue IS NOT NULL 
  AND q1.revenue IS NOT NULL
  AND q1.revenue > 0
ORDER BY yoy_growth_percent DESC
LIMIT 20;
```

#### 3. PE Ratio Computation

```sql
-- Join financials with OHLC data to compute PE ratios
SELECT 
    f.symbol,
    f.period_end_date,
    f.eps,
    o.ClsPric as market_price,
    o.TradDt as trade_date,
    (o.ClsPric / f.eps) as pe_ratio
FROM champion_market.quarterly_financials f
ASOF LEFT JOIN champion_market.normalized_equity_ohlc o
    ON f.symbol = o.TckrSymb 
    AND f.period_end_date >= o.TradDt
WHERE f.eps > 0
  AND o.ClsPric IS NOT NULL
ORDER BY f.period_end_date DESC, f.symbol
LIMIT 50;

-- Compare PE ratios across sectors (requires sector enrichment)
SELECT 
    f.symbol,
    f.period_end_date,
    (o.ClsPric / f.eps) as pe_ratio,
    f.roe,
    f.debt_to_equity
FROM champion_market.quarterly_financials f
ASOF LEFT JOIN champion_market.normalized_equity_ohlc o
    ON f.symbol = o.TckrSymb 
    AND f.period_end_date >= o.TradDt
WHERE f.eps > 0
  AND o.ClsPric IS NOT NULL
  AND f.period_end_date = (
      SELECT MAX(period_end_date) 
      FROM champion_market.quarterly_financials
  )
ORDER BY pe_ratio DESC
LIMIT 30;
```

### Shareholding Patterns

#### 1. Basic Data Verification

```sql
-- Count total records
SELECT COUNT(*) as total_records
FROM champion_market.shareholding_pattern;

-- Count records by year and quarter
SELECT 
    year,
    quarter,
    COUNT(*) as records,
    COUNT(DISTINCT symbol) as unique_symbols
FROM champion_market.shareholding_pattern
GROUP BY year, quarter
ORDER BY year DESC, quarter DESC;
```

#### 2. Shareholding Analysis

```sql
-- Latest shareholding pattern by symbol
SELECT 
    symbol,
    quarter_end_date,
    promoter_shareholding_percent,
    public_shareholding_percent,
    fii_shareholding_percent,
    dii_shareholding_percent,
    pledged_promoter_shares_percent
FROM champion_market.shareholding_pattern
WHERE quarter_end_date = (
    SELECT MAX(quarter_end_date) 
    FROM champion_market.shareholding_pattern
)
ORDER BY promoter_shareholding_percent DESC
LIMIT 20;

-- Track promoter shareholding changes over time
SELECT 
    symbol,
    quarter_end_date,
    promoter_shareholding_percent,
    pledged_promoter_shares_percent
FROM champion_market.shareholding_pattern
WHERE symbol = 'RELIANCE'  -- Replace with desired symbol
ORDER BY quarter_end_date DESC;

-- FII shareholding trends (highest FII holding)
SELECT 
    symbol,
    quarter_end_date,
    fii_shareholding_percent,
    fii_shares
FROM champion_market.shareholding_pattern
WHERE quarter_end_date = (
    SELECT MAX(quarter_end_date) 
    FROM champion_market.shareholding_pattern
)
ORDER BY fii_shareholding_percent DESC
LIMIT 20;

-- Companies with high promoter pledge
SELECT 
    symbol,
    quarter_end_date,
    promoter_shareholding_percent,
    pledged_promoter_shares_percent,
    (pledged_promoter_shares_percent / promoter_shareholding_percent * 100) as pledge_ratio
FROM champion_market.shareholding_pattern
WHERE quarter_end_date = (
    SELECT MAX(quarter_end_date) 
    FROM champion_market.shareholding_pattern
)
  AND pledged_promoter_shares_percent > 0
ORDER BY pledged_promoter_shares_percent DESC
LIMIT 20;

-- Institutional ownership changes (QoQ)
SELECT 
    sp2.symbol,
    sp2.quarter_end_date as current_quarter,
    sp2.institutional_shareholding_percent as current_institutional,
    sp1.institutional_shareholding_percent as previous_institutional,
    (sp2.institutional_shareholding_percent - sp1.institutional_shareholding_percent) as change
FROM champion_market.shareholding_pattern sp2
JOIN champion_market.shareholding_pattern sp1
    ON sp2.symbol = sp1.symbol
    AND sp2.year = sp1.year
    AND sp2.quarter = sp1.quarter + 1
WHERE sp2.institutional_shareholding_percent IS NOT NULL
  AND sp1.institutional_shareholding_percent IS NOT NULL
ORDER BY ABS(sp2.institutional_shareholding_percent - sp1.institutional_shareholding_percent) DESC
LIMIT 20;
```

### Multi-modal Analysis (Financials + Shareholding + OHLC)

#### 1. Comprehensive Company Profile

```sql
-- Latest company snapshot (financials + shareholding + market data)
SELECT 
    f.symbol,
    f.period_end_date,
    f.revenue,
    f.net_profit,
    f.eps,
    f.roe,
    f.debt_to_equity,
    sp.promoter_shareholding_percent,
    sp.fii_shareholding_percent,
    sp.pledged_promoter_shares_percent,
    o.ClsPric as latest_price,
    (o.ClsPric / f.eps) as pe_ratio
FROM champion_market.quarterly_financials f
LEFT JOIN champion_market.shareholding_pattern sp
    ON f.symbol = sp.symbol 
    AND f.period_end_date = sp.quarter_end_date
ASOF LEFT JOIN champion_market.normalized_equity_ohlc o
    ON f.symbol = o.TckrSymb 
    AND f.period_end_date >= o.TradDt
WHERE f.period_end_date = (
    SELECT MAX(period_end_date) 
    FROM champion_market.quarterly_financials
)
  AND f.eps > 0
ORDER BY f.revenue DESC
LIMIT 30;
```

#### 2. Screening Queries

```sql
-- Find companies with strong fundamentals
-- High ROE, low debt, increasing institutional holding
WITH latest_financials AS (
    SELECT * 
    FROM champion_market.quarterly_financials
    WHERE period_end_date = (
        SELECT MAX(period_end_date) 
        FROM champion_market.quarterly_financials
    )
),
latest_shareholding AS (
    SELECT *
    FROM champion_market.shareholding_pattern
    WHERE quarter_end_date = (
        SELECT MAX(quarter_end_date)
        FROM champion_market.shareholding_pattern
    )
)
SELECT 
    f.symbol,
    f.roe,
    f.debt_to_equity,
    f.net_margin,
    s.fii_shareholding_percent,
    s.dii_shareholding_percent,
    s.promoter_shareholding_percent
FROM latest_financials f
JOIN latest_shareholding s ON f.symbol = s.symbol
WHERE f.roe > 15
  AND f.debt_to_equity < 1.0
  AND f.net_margin > 10
  AND s.promoter_shareholding_percent > 50
ORDER BY f.roe DESC
LIMIT 20;
```

## Validation Against External Data Sources

### BSE Website Verification

1. Visit BSE website: <https://www.bseindia.com/>
2. Search for a company (e.g., RELIANCE)
3. Navigate to "Financial Results" or "Shareholding Pattern"
4. Compare the data with our database

Example comparison query:

```sql
-- Get RELIANCE data for a specific quarter
SELECT 
    symbol,
    period_end_date,
    revenue,
    net_profit,
    eps,
    roe
FROM champion_market.quarterly_financials
WHERE symbol = 'RELIANCE'
  AND period_end_date = '2024-03-31'
ORDER BY period_end_date DESC;

-- Get RELIANCE shareholding for a specific quarter
SELECT 
    symbol,
    quarter_end_date,
    promoter_shareholding_percent,
    fii_shareholding_percent,
    dii_shareholding_percent
FROM champion_market.shareholding_pattern
WHERE symbol = 'RELIANCE'
  AND quarter_end_date = '2024-03-31';
```

### Market Data Website Comparison

Compare PE ratios with popular financial websites:

- **MoneyControl**: <https://www.moneycontrol.com/>
- **Screener.in**: <https://www.screener.in/>
- **TradingView**: <https://www.tradingview.com/>

Example: Verify TCS PE ratio

```sql
SELECT 
    f.symbol,
    f.period_end_date,
    f.eps,
    o.ClsPric as market_price,
    (o.ClsPric / f.eps) as computed_pe_ratio
FROM champion_market.quarterly_financials f
ASOF LEFT JOIN champion_market.normalized_equity_ohlc o
    ON f.symbol = o.TckrSymb 
    AND f.period_end_date >= o.TradDt
WHERE f.symbol = 'TCS'
  AND f.period_end_date = (
      SELECT MAX(period_end_date) 
      FROM champion_market.quarterly_financials
      WHERE symbol = 'TCS'
  )
  AND f.eps > 0;
```

## Data Quality Checks

```sql
-- Check for missing critical fields in financials
SELECT 
    COUNT(*) as total_records,
    COUNT(revenue) as has_revenue,
    COUNT(net_profit) as has_net_profit,
    COUNT(eps) as has_eps,
    COUNT(roe) as has_roe,
    COUNT(debt_to_equity) as has_debt_equity
FROM champion_market.quarterly_financials;

-- Check for missing critical fields in shareholding
SELECT 
    COUNT(*) as total_records,
    COUNT(promoter_shareholding_percent) as has_promoter,
    COUNT(fii_shareholding_percent) as has_fii,
    COUNT(dii_shareholding_percent) as has_dii
FROM champion_market.shareholding_pattern;

-- Check for data anomalies (negative values, outliers)
SELECT 
    symbol,
    period_end_date,
    revenue,
    net_profit,
    roe,
    debt_to_equity
FROM champion_market.quarterly_financials
WHERE revenue < 0 
   OR net_profit < -10000  -- Large negative profit
   OR roe > 100  -- Unrealistic ROE
   OR debt_to_equity < 0
ORDER BY period_end_date DESC;
```
