# Fundamentals Data Ingestion

## Quick Start

### 1. Generate Sample Data and Run Pipeline

```bash
# Generate sample data for NIFTY50 companies (past 2 years)
python run_fundamentals_etl.py

# Generate for specific companies
python run_fundamentals_etl.py --symbols RELIANCE TCS INFY HDFCBANK

# Custom date range
python run_fundamentals_etl.py --start-date 2022-01-01 --end-date 2024-12-31

# Generate Parquet files only (skip ClickHouse)
python run_fundamentals_etl.py --no-clickhouse

# Skip P/E ratio computation
python run_fundamentals_etl.py --no-pe
```

### 2. View Generated Data

The pipeline creates Parquet files in:

```text
data/lake/normalized/
├── quarterly_financials/
│   └── quarterly_financials_sample.parquet
└── shareholding_pattern/
    └── shareholding_pattern_sample.parquet
```

### 3. Query Data in ClickHouse

```bash
clickhouse-client --database champion_market

# View quarterly financials
SELECT 
    symbol,
    period_end_date,
    revenue,
    net_profit,
    eps,
    roe,
    debt_to_equity
FROM quarterly_financials
ORDER BY revenue DESC
LIMIT 10;

# View shareholding patterns
SELECT 
    symbol,
    quarter_end_date,
    promoter_shareholding_percent,
    fii_shareholding_percent,
    dii_shareholding_percent
FROM shareholding_pattern
ORDER BY quarter_end_date DESC
LIMIT 10;

# Compute P/E ratios
SELECT 
    f.symbol,
    f.period_end_date,
    f.eps,
    o.ClsPric as market_price,
    (o.ClsPric / f.eps) as pe_ratio
FROM quarterly_financials f
ASOF LEFT JOIN normalized_equity_ohlc o
    ON f.symbol = o.TckrSymb 
    AND f.period_end_date >= o.TradDt
WHERE f.eps > 0
ORDER BY f.period_end_date DESC
LIMIT 10;
```

## Features

### 1. Quarterly Financials

- **Balance Sheet**: Total assets, equity, debt, current assets/liabilities
- **P&L Statement**: Revenue, operating profit, net profit, expenses
- **Per Share Metrics**: EPS, book value per share
- **Computed Ratios**: ROE, ROA, debt-to-equity, current ratio, margins

### 2. Shareholding Patterns

- **Promoter Holdings**: Shareholding percentage and pledged shares
- **Institutional Holdings**: FII, DII, mutual funds, insurance, banks
- **Public Holdings**: Non-institutional shareholders
- **Employee Holdings**: ESOP and employee shareholding

### 3. KPI Computation

Key financial ratios are automatically computed:

- **ROE (Return on Equity)**: (Net Profit / Equity) × 100
- **ROA (Return on Assets)**: (Net Profit / Total Assets) × 100
- **Debt-to-Equity**: Total Debt / Equity
- **Current Ratio**: Current Assets / Current Liabilities
- **Operating Margin**: (Operating Profit / Revenue) × 100
- **Net Margin**: (Net Profit / Revenue) × 100
- **P/E Ratio**: Market Price / EPS

### 4. Multi-modal Analysis

Join fundamental data with OHLC market data:

```sql
-- Find undervalued stocks (low PE, high ROE, low debt)
WITH latest_financials AS (
    SELECT * 
    FROM quarterly_financials
    WHERE period_end_date = (SELECT MAX(period_end_date) FROM quarterly_financials)
)
SELECT 
    f.symbol,
    f.roe,
    f.debt_to_equity,
    o.ClsPric as price,
    (o.ClsPric / f.eps) as pe_ratio
FROM latest_financials f
ASOF LEFT JOIN normalized_equity_ohlc o
    ON f.symbol = o.TckrSymb 
WHERE f.eps > 0
  AND f.roe > 15
  AND f.debt_to_equity < 1.0
  AND (o.ClsPric / f.eps) < 20
ORDER BY f.roe DESC;
```

## Architecture

### Data Flow

```text
BSE/MCA → Scrapers → Parsers → Parquet (Data Lake) → ClickHouse → Analytics
```

### Components

1. **Scrapers** (`src/scrapers/`)
   - `bse_shareholding.py`: BSE shareholding pattern scraper
   - `mca_financials.py`: MCA/BSE financial statements scraper

2. **Parsers** (`src/parsers/`)
   - `shareholding_parser.py`: Parse shareholding data
   - `quarterly_financials_parser.py`: Parse financials and compute ratios

3. **Sample Data Generator** (`src/utils/`)
   - `generate_fundamentals_sample.py`: Generate realistic sample data

4. **Schemas** (`schemas/parquet/`)
   - `quarterly_financials.json`: Avro schema for financials
   - `shareholding_pattern.json`: Avro schema for shareholding

5. **Database Tables** (`warehouse/clickhouse/init/`)
   - `quarterly_financials`: ClickHouse table for financials
   - `shareholding_pattern`: ClickHouse table for shareholding

## Testing

### Run Unit Tests

```bash
cd ingestion/nse-scraper
python3 tests/unit/test_fundamentals.py -v
```

### Test Results

All 14 tests pass:

- ✅ ROE, ROA, debt ratios computation
- ✅ Current ratio, margins computation
- ✅ P/E ratio computation
- ✅ Shareholding calculations
- ✅ Quarter calculations
- ✅ Schema validation

## Documentation

- **Implementation Guide**: [`docs/implementation/fundamentals-ingestion.md`](../../docs/implementation/fundamentals-ingestion.md)
- **Verification Queries**: [`docs/verification/fundamentals-verification-queries.md`](../../docs/verification/fundamentals-verification-queries.md)

## Data Sources

### BSE (Bombay Stock Exchange)

- **Shareholding Pattern**: <https://www.bseindia.com/corporates/shpPromoterNPublic.aspx>
- **Financial Results**: <https://www.bseindia.com/corporates/Comp_Resultsnew.aspx>

### MCA (Ministry of Corporate Affairs)

- **MCA Portal**: <https://www.mca.gov.in/mcafoportal/>
- Note: Requires authentication for direct access

### Market Data Websites (for validation)

- **MoneyControl**: <https://www.moneycontrol.com/>
- **Screener.in**: <https://www.screener.in/>
- **TradingView**: <https://www.tradingview.com/>

## Sample Data

The pipeline generates sample data for 50+ NIFTY50 companies:

- **Date Range**: Past 2 years (8 quarters)
- **Total Records**: ~400 financial records + ~400 shareholding records
- **Companies**: RELIANCE, TCS, HDFCBANK, INFY, and 46 more

### Sample Output

```text
================================================================================
SAMPLE QUARTERLY FINANCIALS DATA
================================================================================
┌─────────┬────────────────┬─────────┬────────────┬───────┬────────┐
│ symbol  │ period_end_date│ revenue │ net_profit │  eps  │  roe   │
├─────────┼────────────────┼─────────┼────────────┼───────┼────────┤
│ RELIANCE│ 2024-12-31     │ 8234.56 │   1245.89  │ 12.46 │  18.54 │
│ TCS     │ 2024-12-31     │ 6543.21 │   1098.76  │ 10.99 │  22.34 │
│ ...     │ ...            │ ...     │   ...      │  ...  │  ...   │
└─────────┴────────────────┴─────────┴────────────┴───────┴────────┘

Total records: 400
Symbols: 50
Date range: 2023-03-31 to 2025-12-31
```

## Limitations

1. **Data Source**: Currently uses sample data; real scrapers require authentication
2. **Historical Data**: Limited to available data from exchanges
3. **Update Frequency**: Batch-based; not real-time
4. **Data Quality**: Depends on exchange disclosure accuracy

## Future Enhancements

1. **Real Data Scraping**: Implement authenticated scrapers for BSE/MCA
2. **Real-time Updates**: Event-driven ingestion on filing publication
3. **Advanced Analytics**: Sector comparisons, trend analysis
4. **ML Integration**: Earnings prediction, anomaly detection
5. **Corporate Actions**: Link with corporate actions for adjustments

## Troubleshooting

### Issue: ClickHouse connection failed

```bash
# Check ClickHouse is running
docker-compose ps clickhouse

# Restart ClickHouse
docker-compose restart clickhouse

# Run without ClickHouse loading
python run_fundamentals_etl.py --no-clickhouse
```

### Issue: Missing dependencies

```bash
# Install dependencies with Poetry
cd ingestion/nse-scraper
poetry install

# Or use pip
pip install polars beautifulsoup4 httpx
```

### Issue: No OHLC data for P/E computation

```bash
# Run OHLC ingestion first
cd ingestion/nse-scraper
poetry run python run_etl.py

# Or skip P/E computation
python run_fundamentals_etl.py --no-pe
```

## Support

For issues, questions, or contributions:

1. Check documentation in `docs/implementation/`
2. Review verification queries in `docs/verification/`
3. Run unit tests to verify installation
4. Open an issue on GitHub
