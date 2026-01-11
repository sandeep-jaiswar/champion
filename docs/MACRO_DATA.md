# Macro Data Integration

This module provides integration for macroeconomic indicators from RBI (Reserve Bank of India) and MOSPI (Ministry of Statistics and Programme Implementation).

## Overview

The macro data integration ingests key economic indicators including:

**From RBI:**

- Policy rates (Repo Rate, Reverse Repo Rate, CRR, SLR)
- Foreign Exchange (FX) reserves
- Weekly monetary data

**From MOSPI:**

- Consumer Price Index (CPI) - Combined, Rural, Urban, Food
- Wholesale Price Index (WPI) - All Commodities, Food, Fuel, Manufactured
- Index of Industrial Production (IIP)

## Architecture

```text
┌──────────────┐       ┌──────────────┐
│ RBI Scraper  │       │MOSPI Scraper │
└──────┬───────┘       └──────┬───────┘
       │                      │
       │   JSON Files         │
       └──────────┬───────────┘
                  │
          ┌───────▼────────┐
          │ Macro Parser   │
          └───────┬────────┘
                  │
          ┌───────▼────────┐
          │ Parquet Lake   │
          └───────┬────────┘
                  │
          ┌───────▼────────┐
          │  ClickHouse    │
          │ macro_indicators│
          └────────────────┘
```

## Schema

### Parquet Schema

See `schemas/parquet/macro_indicators.json` for the complete Avro schema.

Key fields:

- `indicator_date`: Date of observation
- `indicator_code`: Unique identifier (e.g., REPO_RATE, CPI_COMBINED)
- `indicator_name`: Human-readable name
- `indicator_category`: POLICY_RATE, INFLATION, FX_RESERVE, GDP, EMPLOYMENT
- `value`: Numeric value
- `unit`: Unit of measurement (%, INR Crore, USD Million, etc.)
- `frequency`: DAILY, WEEKLY, MONTHLY, QUARTERLY, ANNUAL

### ClickHouse Table

```sql
CREATE TABLE champion_market.macro_indicators (
    -- Envelope fields
    event_id String,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    source LowCardinality(String),
    schema_version LowCardinality(String),
    entity_id String,
    
    -- Indicator data
    indicator_date Date,
    indicator_code LowCardinality(String),
    indicator_name String,
    indicator_category LowCardinality(String),
    value Float64,
    unit LowCardinality(String),
    frequency LowCardinality(String),
    source_url Nullable(String),
    metadata Map(String, String),
    
    -- Partitioning columns
    year Int64,
    month Int64,
    quarter Int64
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY (indicator_category, toYear(indicator_date))
ORDER BY (indicator_code, indicator_date, event_time)
TTL indicator_date + INTERVAL 20 YEAR;
```

## Usage

### Run ETL Pipeline

```bash
# Default: Last 2 years of data
python run_macro_etl.py

# Custom date range
python run_macro_etl.py --start-date 2023-01-01 --end-date 2025-12-31

# Skip ClickHouse load (Parquet only)
python run_macro_etl.py --no-clickhouse

# Specific indicators
python run_macro_etl.py \
  --rbi-indicators REPO_RATE,FX_RESERVES_TOTAL \
  --mospi-indicators CPI_COMBINED,WPI_ALL
```

### Query Data

**ClickHouse queries:**

```sql
-- View available indicators
SELECT 
    indicator_code,
    indicator_name,
    indicator_category,
    COUNT(*) as observations
FROM champion_market.macro_indicators
GROUP BY indicator_code, indicator_name, indicator_category
ORDER BY indicator_category, indicator_code;

-- Get latest values for all indicators
SELECT 
    indicator_code,
    indicator_name,
    argMax(value, indicator_date) as latest_value,
    argMax(indicator_date, indicator_date) as latest_date,
    unit
FROM champion_market.macro_indicators
GROUP BY indicator_code, indicator_name, unit
ORDER BY indicator_code;

-- Time series for specific indicator
SELECT 
    indicator_date,
    value,
    unit
FROM champion_market.macro_indicators
WHERE indicator_code = 'CPI_COMBINED'
ORDER BY indicator_date DESC
LIMIT 24;

-- Monthly inflation trend
SELECT 
    toStartOfMonth(indicator_date) as month,
    avg(value) as avg_cpi
FROM champion_market.macro_indicators
WHERE indicator_code = 'CPI_COMBINED'
  AND indicator_date >= today() - INTERVAL 2 YEAR
GROUP BY month
ORDER BY month;
```

## Correlation Analysis

### CPI vs Stock Prices

Join macro indicators with equity OHLC to analyze correlations:

```sql
-- CPI vs TCS stock price
WITH cpi_monthly AS (
    SELECT 
        toStartOfMonth(indicator_date) as month,
        avg(value) as cpi_value
    FROM champion_market.macro_indicators
    WHERE indicator_code = 'CPI_COMBINED'
      AND indicator_date >= '2024-01-01'
    GROUP BY month
),
tcs_monthly AS (
    SELECT 
        toStartOfMonth(TradDt) as month,
        avg(ClsPric) as avg_close
    FROM champion_market.normalized_equity_ohlc
    WHERE TckrSymb = 'TCS'
      AND TradDt >= '2024-01-01'
    GROUP BY month
)
SELECT 
    c.month,
    c.cpi_value,
    t.avg_close as tcs_price,
    round((t.avg_close - LAG(t.avg_close) OVER (ORDER BY c.month)) / 
          LAG(t.avg_close) OVER (ORDER BY c.month) * 100, 2) as price_change_pct,
    round(c.cpi_value - LAG(c.cpi_value) OVER (ORDER BY c.month), 2) as cpi_change
FROM cpi_monthly c
INNER JOIN tcs_monthly t ON c.month = t.month
ORDER BY c.month;
```

### FX Reserves vs Market Performance

```sql
-- FX reserves trend with market volume
WITH fx_weekly AS (
    SELECT 
        indicator_date,
        value as fx_reserves
    FROM champion_market.macro_indicators
    WHERE indicator_code = 'FX_RESERVES_TOTAL'
      AND indicator_date >= today() - INTERVAL 6 MONTH
),
market_weekly AS (
    SELECT 
        TradDt as trade_date,
        SUM(TtlTrfVal) as total_turnover
    FROM champion_market.normalized_equity_ohlc
    WHERE TradDt >= today() - INTERVAL 6 MONTH
    GROUP BY TradDt
)
SELECT 
    f.indicator_date,
    f.fx_reserves,
    m.total_turnover,
    round(corr(f.fx_reserves, m.total_turnover) OVER (
        ORDER BY f.indicator_date 
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    ), 3) as rolling_correlation
FROM fx_weekly f
ASOF LEFT JOIN market_weekly m ON f.indicator_date = m.trade_date
ORDER BY f.indicator_date;
```

### Policy Rate Changes vs Market Volatility

```sql
-- Repo rate changes and market volatility
WITH repo_rate AS (
    SELECT 
        indicator_date,
        value as repo_rate,
        value - LAG(value) OVER (ORDER BY indicator_date) as rate_change
    FROM champion_market.macro_indicators
    WHERE indicator_code = 'REPO_RATE'
      AND indicator_date >= '2024-01-01'
),
market_volatility AS (
    SELECT 
        TradDt as trade_date,
        stddevPop(ClsPric) as price_volatility,
        COUNT(DISTINCT TckrSymb) as active_symbols
    FROM champion_market.normalized_equity_ohlc
    WHERE TradDt >= '2024-01-01'
    GROUP BY TradDt
)
SELECT 
    r.indicator_date,
    r.repo_rate,
    r.rate_change,
    m.price_volatility,
    m.active_symbols
FROM repo_rate r
ASOF LEFT JOIN market_volatility m ON r.indicator_date = m.trade_date
WHERE r.rate_change != 0  -- Only show when rate changed
ORDER BY r.indicator_date;
```

## Data Quality

The parser includes built-in data quality checks:

- **Null validation**: Checks required fields for null values
- **Outlier detection**: Statistical analysis of numeric values
- **Gap detection**: Identifies large gaps in time series (> 30 days)
- **Duplicate detection**: Finds duplicate entries by indicator + date

Warnings are logged but don't fail the pipeline to allow recovery from partial data.

## Implementation Notes

### Current Implementation

The current implementation generates **synthetic/sample data** for demonstration purposes:

- RBI data: Weekly policy rates and FX reserves with realistic base values
- MOSPI data: Monthly CPI/WPI indices with trends and seasonality

### Production Considerations

For production deployment, replace the sample data generation with:

1. **RBI DBIE API**: Use RBI's Database on Indian Economy API
2. **MOSPI Portal**: Scrape or use MOSPI's data portal
3. **Data Validation**: Implement stricter validation against known ranges
4. **Error Handling**: Add retry logic for API failures
5. **Rate Limiting**: Respect rate limits of government APIs
6. **Authentication**: Handle any API keys or authentication required

### Example API Integration

```python
# Replace in RBIMacroScraper._generate_sample_data()
def _fetch_real_rbi_data(self, start_date, end_date, indicators):
    """Fetch real data from RBI DBIE API."""
    session = self._establish_session()
    
    # Example: RBI DBIE data fetch
    url = "https://api.rbi.org.in/data/indicator/{indicator_code}"
    params = {
        "from_date": start_date.strftime("%Y-%m-%d"),
        "to_date": end_date.strftime("%Y-%m-%d")
    }
    
    response = session.get(url.format(indicator_code=code), params=params)
    return response.json()
```

## MLflow Tracking

All ETL runs are tracked in MLflow:

- **Experiment**: `macro-indicators-etl`
- **Metrics**: Scrape/parse duration, rows processed, indicators count
- **Parameters**: Date range, indicators selected, ClickHouse flag
- **Artifacts**: Parquet files, sample data

View in MLflow UI: `http://localhost:5000`

## Data Retention

- **Parquet Lake**: Unlimited (configurable via storage policies)
- **ClickHouse**: 20 years TTL (configurable in DDL)

## References

- [RBI Database on Indian Economy](https://dbie.rbi.org.in/)
- [MOSPI Data Portal](https://mospi.gov.in/)
- [CPI/WPI Indices Methodology](https://mospi.gov.in/web/mospi/statistics)
