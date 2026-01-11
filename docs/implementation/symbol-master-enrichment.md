# Symbol Master Enrichment - NSE Instrument Mapping

## Overview

The Symbol Master enrichment system provides comprehensive instrument mapping for NSE securities, resolving the one-to-many relationship between ticker symbols and actual tradeable instruments.

## Problem Statement

NSE bhavcopy data presents a challenge: multiple distinct securities can share the same ticker symbol. For example:

- **IBULHSGFIN** represents:
  - 1 equity security (series: EQ)
  - 18+ Non-Convertible Debenture (NCD) tranches (series: D1, D2, etc.)

Each of these has:
- Unique `FinInstrmId` (Financial Instrument ID)
- Unique `ISIN` (International Securities Identification Number)
- Different `FinInstrmNm` (Instrument Name)
- Separate OHLC data in bhavcopy files

Without proper mapping, these would be incorrectly aggregated or deduplicated.

## Solution: Canonical Instrument IDs

The enrichment system creates canonical instrument identifiers by combining:

```
instrument_id = symbol:fiid:exchange
Example: IBULHSGFIN:30125:NSE (Equity)
         IBULHSGFIN:14678:NSE (NCD Series I)
```

### Key Components

1. **TckrSymb** (Ticker Symbol) - The trading symbol (e.g., "IBULHSGFIN")
2. **FinInstrmId** - NSE's unique instrument identifier
3. **SctySrs** (Security Series) - EQ (equity), D1/D2 (debt), BE (B series equity), etc.
4. **ISIN** - International standard identifier

## Architecture

### Data Sources

1. **NSE EQUITY_L.csv** - Base symbol master
   - URL: `https://archives.nseindia.com/content/equities/EQUITY_L.csv`
   - Contains: Symbol, Company Name, ISIN, Series, Listing Date, Face Value
   - Updated: Daily by NSE

2. **NSE Bhavcopy Files** - Daily trading data
   - Contains: TckrSymb, FinInstrmId, SctySrs, ISIN, FinInstrmNm, OHLC data
   - Used to extract actual tradeable instruments with FinInstrmId

### Enrichment Process

```text
┌─────────────────┐     ┌──────────────────┐
│  EQUITY_L.csv   │     │  Bhavcopy Data   │
│  (NSE Symbol    │     │  (Normalized     │
│   Master)       │     │   OHLC)          │
└────────┬────────┘     └────────┬─────────┘
         │                       │
         │    1. Parse           │  2. Extract Unique
         │                       │     Instruments
         ↓                       ↓
    ┌────────────────────────────────┐
    │   Symbol Enrichment Module     │
    │                                 │
    │  - Join on (Symbol + ISIN)     │
    │  - Fallback to Symbol only     │
    │  - Create canonical IDs        │
    │  - Verify one-to-many cases    │
    └────────────┬───────────────────┘
                 │
                 ↓
    ┌────────────────────────────┐
    │  Enriched Symbol Master    │
    │  with Canonical IDs        │
    │                            │
    │  IBULHSGFIN:30125:NSE (EQ) │
    │  IBULHSGFIN:14678:NSE (D1) │
    │  IBULHSGFIN:17505:NSE (D2) │
    │  ...                       │
    └────────────────────────────┘
```

## Schema

### ClickHouse Table: `symbol_master`

```sql
CREATE TABLE champion_market.symbol_master (
    -- Envelope
    event_id            String,
    event_time          DateTime64(3, 'UTC'),
    ingest_time         DateTime64(3, 'UTC'),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Canonical identifiers
    instrument_id       String,  -- symbol:fiid:exchange
    symbol              String,
    exchange            LowCardinality(String),
    
    -- Company information
    company_name        Nullable(String),
    isin                Nullable(String),
    series              LowCardinality(Nullable(String)),
    
    -- Listing details
    listing_date        Nullable(Date),
    face_value          Nullable(Float64),
    paid_up_value       Nullable(Float64),
    lot_size            Nullable(Int64),
    
    -- Classification (for future enrichment)
    sector              LowCardinality(Nullable(String)),
    industry            LowCardinality(Nullable(String)),
    market_cap_category LowCardinality(Nullable(String)),
    
    -- Trading details
    tick_size           Nullable(Float64),
    is_index_constituent Nullable(Bool),
    indices             Array(String),
    
    -- Status
    status              LowCardinality(String),
    delisting_date      Nullable(Date),
    
    -- Metadata
    metadata            Map(String, String),
    
    -- Temporal validity
    valid_from          Date,
    valid_to            Nullable(Date)
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYear(valid_from)
ORDER BY (symbol, instrument_id, valid_from, event_time);
```

## Usage

### Running the Enrichment Pipeline

```bash
cd ingestion/nse-scraper

# Run full enrichment pipeline
python run_symbol_enrichment.py

# With ClickHouse loading
python run_symbol_enrichment.py --load-clickhouse
```

### Joining with OHLC Data

```sql
-- Join normalized OHLC with symbol master to get company names
SELECT 
    o.TradDt,
    o.TckrSymb,
    o.FinInstrmId,
    s.instrument_id,
    s.company_name,
    s.series,
    o.ClsPric,
    o.TtlTradgVol
FROM champion_market.normalized_equity_ohlc o
LEFT JOIN champion_market.symbol_master s
    ON o.TckrSymb = s.symbol
    AND o.FinInstrmId = toString(s.instrument_id)  -- Match on canonical ID
WHERE o.TradDt = '2024-01-15'
ORDER BY o.TtlTradgVol DESC
LIMIT 100;
```

### Finding Multi-Instrument Tickers

```sql
-- Find tickers with multiple instruments (like IBULHSGFIN)
SELECT 
    symbol,
    count() as instrument_count,
    groupArray(instrument_id) as instruments,
    groupArray(series) as series_list,
    groupArray(company_name) as names
FROM champion_market.symbol_master
GROUP BY symbol
HAVING count() > 1
ORDER BY instrument_count DESC
LIMIT 20;
```

### Verifying IBULHSGFIN

```sql
-- Check all IBULHSGFIN instruments
SELECT 
    instrument_id,
    symbol,
    series,
    company_name,
    isin,
    listing_date,
    status
FROM champion_market.symbol_master
WHERE symbol = 'IBULHSGFIN'
ORDER BY series;
```

Expected result:
```
instrument_id              | symbol      | series | company_name                           | isin
---------------------------|-------------|--------|---------------------------------------|-------------
IBULHSGFIN:30125:NSE      | IBULHSGFIN  | EQ     | Indiabulls Housing Finance Limited    | INE148I01020
IBULHSGFIN:14678:NSE      | IBULHSGFIN  | D1     | Indiabulls Housing Finance - NCD SR.I | INE148I08023
IBULHSGFIN:17505:NSE      | IBULHSGFIN  | D2     | Indiabulls Housing Finance - NCD SR.II| INE148I08031
... (16 more NCD tranches)
```

## Mapping Rules and Caveats

### Matching Rules

1. **Primary Match**: Join on (Symbol + ISIN)
   - Most reliable as ISIN is unique per security
   - Used when both EQUITY_L and bhavcopy have matching ISIN

2. **Fallback Match**: Join on Symbol only
   - Used when ISIN doesn't match or is missing
   - May create multiple candidate matches
   - Enrichment uses the most recent/appropriate match

### Known Limitations

1. **FinInstrmId not in EQUITY_L**
   - EQUITY_L doesn't provide FinInstrmId
   - Must be extracted from bhavcopy data
   - Historical data required for complete coverage

2. **Series Differences**
   - EQUITY_L may list only primary series (EQ)
   - Bhavcopy reveals all traded series (EQ, BE, D1, D2, etc.)
   - Enriched master includes all series from bhavcopy

3. **Coverage**
   - Enrichment depends on available bhavcopy data
   - Symbols not traded during bhavcopy period won't have FinInstrmId
   - Minimum 30 days of bhavcopy recommended for good coverage

4. **Sector/Industry Data**
   - Not available in NSE EQUITY_L or bhavcopy
   - Requires external data sources (e.g., BSE, company filings)
   - Fields left NULL for future enrichment

### Data Freshness

- **EQUITY_L**: Updated daily by NSE (new listings, delistings)
- **Bhavcopy**: Updated daily with trading activity
- **Enrichment**: Should be run weekly or when new symbols appear
- **ClickHouse**: Use ReplacingMergeTree for automatic deduplication

## Testing

### Unit Tests

```bash
cd ingestion/nse-scraper
poetry run pytest tests/unit/test_symbol_master_parser.py -v
```

### Verification Queries

```sql
-- Count total instruments
SELECT count() as total_instruments
FROM champion_market.symbol_master;

-- Count unique symbols
SELECT count(DISTINCT symbol) as unique_symbols
FROM champion_market.symbol_master;

-- Check for one-to-many cases
SELECT 
    count() FILTER (WHERE cnt > 1) as multi_instrument_symbols,
    max(cnt) as max_instruments_per_symbol
FROM (
    SELECT symbol, count() as cnt
    FROM champion_market.symbol_master
    GROUP BY symbol
);

-- Top 10 symbols by instrument count
SELECT 
    symbol,
    count() as instrument_count,
    groupArray(series) as series_list
FROM champion_market.symbol_master
GROUP BY symbol
ORDER BY instrument_count DESC
LIMIT 10;
```

## Acceptance Criteria Verification

### ✅ Coverage: Top 500+ Equities

```sql
-- Check EQ series coverage
SELECT count(DISTINCT symbol)
FROM champion_market.symbol_master
WHERE series = 'EQ';
-- Expected: 2000+ (NSE lists ~2500 equity symbols)
```

### ✅ Unique Addressability

```sql
-- Verify each instrument_id is unique
SELECT 
    instrument_id,
    count() as occurrences
FROM champion_market.symbol_master
GROUP BY instrument_id
HAVING count() > 1;
-- Expected: 0 rows (all unique)
```

### ✅ Duplicate Resolution

```sql
-- Check FinInstrmId uniqueness per symbol
SELECT 
    symbol,
    count(DISTINCT instrument_id) as distinct_instruments
FROM champion_market.symbol_master
GROUP BY symbol
ORDER BY distinct_instruments DESC;
-- Expected: IBULHSGFIN shows 19+ distinct instruments
```

### ✅ Join with Normalized OHLC

```sql
-- Test join produces canonical IDs
SELECT 
    o.TckrSymb,
    o.FinInstrmId,
    concat(o.TckrSymb, ':', toString(o.FinInstrmId), ':NSE') as computed_id,
    s.instrument_id as master_id,
    s.company_name
FROM champion_market.normalized_equity_ohlc o
LEFT JOIN champion_market.symbol_master s
    ON concat(o.TckrSymb, ':', toString(o.FinInstrmId), ':NSE') = s.instrument_id
WHERE o.TradDt = (SELECT max(TradDt) FROM champion_market.normalized_equity_ohlc)
LIMIT 100;
-- Expected: All rows have matching master_id and company_name
```

## Future Enhancements

1. **Sector/Industry Enrichment**
   - Integrate with external sector classification data
   - Use BSE sector data or company filings
   - Update enrichment pipeline to populate sector/industry fields

2. **Index Constituent Tracking**
   - Scrape NIFTY50, NIFTYBANK, SENSEX constituent lists
   - Maintain historical index membership
   - Enable index-based filtering in queries

3. **Market Cap Classification**
   - Calculate or fetch market cap data
   - Classify as LARGE_CAP, MID_CAP, SMALL_CAP
   - Update periodically (quarterly)

4. **Real-time Updates**
   - Monitor NSE for intraday symbol master changes
   - Stream updates to ClickHouse
   - Maintain version history with valid_from/valid_to

## References

- [NSE Symbol Master Download](https://www.nseindia.com/market-data/securities-available-for-trading)
- [NSE Bhavcopy Documentation](https://www.nseindia.com/products-services/archives)
- ClickHouse Schema: `/warehouse/clickhouse/init/01_create_tables.sql`
- Issue: `docs/issues/P1-symbol-master-enrichment.md`
