# Symbol Master Enrichment - Quick Start Guide

## Overview

The Symbol Master Enrichment system resolves NSE's one-to-many ticker-to-instrument mapping by creating canonical instrument IDs that uniquely identify each tradeable security.

**Problem**: Tickers like `IBULHSGFIN` represent multiple instruments:

- 1 equity (EQ series)
- 18+ NCD debt tranches (D1, D2, D3 series)

**Solution**: Create canonical IDs: `symbol:fiid:exchange`

- `IBULHSGFIN:30125:NSE` (Equity)
- `IBULHSGFIN:14678:NSE` (NCD Series I)
- `IBULHSGFIN:17505:NSE` (NCD Series II)

## Quick Start

### 1. Run Enrichment Pipeline

```bash
cd ingestion/nse-scraper

# Run full enrichment
python run_symbol_enrichment.py

# Output: data/lake/reference/symbol_master/symbol_master_latest.parquet
```

### 2. Load to ClickHouse

```bash
# Option 1: Using run_symbol_enrichment.py
python run_symbol_enrichment.py --load-clickhouse

# Option 2: Using batch loader (not yet implemented)
cd ../../warehouse
python -m loader.batch_loader \
    --table symbol_master \
    --source ../data/lake/reference/symbol_master/
```

### 3. Query and Join

```sql
-- Find all instruments for IBULHSGFIN
SELECT 
    instrument_id,
    symbol,
    series,
    company_name,
    isin
FROM champion_market.symbol_master
WHERE symbol = 'IBULHSGFIN'
ORDER BY series;

-- Join OHLC data with symbol master
SELECT 
    o.TradDt,
    o.TckrSymb,
    o.FinInstrmId,
    s.company_name,
    s.series,
    o.ClsPric,
    o.TtlTradgVol
FROM champion_market.normalized_equity_ohlc o
LEFT JOIN champion_market.symbol_master s
    ON concat(o.TckrSymb, ':', toString(o.FinInstrmId), ':NSE') = s.instrument_id
WHERE o.TradDt = '2024-01-15'
ORDER BY o.TtlTradgVol DESC
LIMIT 100;
```

## Architecture

### Data Flow

```text
EQUITY_L.csv (NSE)        Bhavcopy Data (30+ days)
      ↓                            ↓
Symbol Master Parser      Extract Instruments
      ↓                            ↓
      └──────────┬─────────────────┘
                 ↓
        Symbol Enrichment
        (Join on ISIN + Symbol)
                 ↓
    Enriched Symbol Master
    (with canonical IDs)
                 ↓
         Parquet Output
                 ↓
         ClickHouse Table
```

### Components

1. **SymbolMasterParser** (`src/parsers/symbol_master_parser.py`)
   - Parses NSE EQUITY_L.csv
   - Extracts: symbol, ISIN, series, company name, listing date, face value
   - Creates base instrument records

2. **SymbolEnrichment** (`src/parsers/symbol_enrichment.py`)
   - Reads bhavcopy Parquet files
   - Extracts unique (TckrSymb, FinInstrmId, SctySrs, ISIN) combinations
   - Joins with EQUITY_L data
   - Creates canonical `instrument_id = symbol:fiid:exchange`

3. **ClickHouse Table** (`symbol_master`)
   - Stores enriched instrument master
   - Indexed on symbol, instrument_id, ISIN
   - ReplacingMergeTree for automatic deduplication

## Testing

```bash
cd ingestion/nse-scraper

# Run unit tests
python3 -m pytest tests/unit/test_symbol_master_parser.py -v

# Run integration tests
python3 -m pytest tests/integration/test_symbol_enrichment.py -v

# All tests should pass (17 total)
```

## Key Features

### ✅ Handles One-to-Many Cases

```python
# Example: IBULHSGFIN has 4+ instruments
enricher = SymbolEnrichment()
enriched_df = enricher.enrich_from_bhavcopy(symbol_master_df, bhavcopy_files)

stats = enricher.verify_one_to_many_cases(enriched_df)
# Output: {'multi_instrument_symbols': 50+, 'total_instruments': 2500+}
```

### ✅ Canonical Mapping

```python
# Create mapping table for joins
mapping = enricher.create_canonical_mapping(enriched_df)
# Columns: instrument_id, TckrSymb, FinInstrmId, ISIN, CompanyName
```

### ✅ Join with OHLC

```python
# Join enriched master with OHLC data
df_ohlc = df_ohlc.with_columns([
    (pl.col("TckrSymb") + ":" + pl.col("FinInstrmId").cast(str) + ":NSE")
    .alias("instrument_id")
])

joined = df_ohlc.join(
    enriched_df.select(["instrument_id", "CompanyName", "SctySrs"]),
    on="instrument_id",
    how="left"
)
```

## Verification Checklist

- [x] Unit tests pass (11/11)
- [x] Integration tests pass (6/6)
- [x] IBULHSGFIN shows multiple instruments
- [ ] Top 500+ equities covered (requires real data)
- [ ] Join with normalized OHLC works (requires real data)

## Files Changed

- **New Files**:
  - `src/parsers/symbol_master_parser.py` - Parser for EQUITY_L.csv
  - `src/parsers/symbol_enrichment.py` - Enrichment logic
  - `run_symbol_enrichment.py` - Main enrichment script
  - `tests/unit/test_symbol_master_parser.py` - Unit tests
  - `tests/integration/test_symbol_enrichment.py` - Integration tests
  - `docs/implementation/symbol-master-enrichment.md` - Full documentation

- **Modified Files**:
  - `warehouse/clickhouse/init/01_create_tables.sql` - Added symbol_master table
  - `warehouse/loader/batch_loader.py` - Added symbol_master support

## Next Steps

1. **Run with real data**: Download EQUITY_L.csv and process real bhavcopy data
2. **Load to ClickHouse**: Verify schema and indexes work correctly
3. **Join testing**: Test joins with normalized OHLC in production
4. **Enrichment**: Add sector/industry data from external sources
5. **Automation**: Schedule daily enrichment runs

## Known Limitations

1. **FinInstrmId not in EQUITY_L**: Must extract from bhavcopy (requires historical data)
2. **Sector/Industry missing**: Requires external data sources
3. **Coverage depends on bhavcopy**: Symbols not traded won't have FinInstrmId
4. **ISIN mismatches**: Fallback to symbol-only join (may create ambiguity)

## Support

- Documentation: `docs/implementation/symbol-master-enrichment.md`
- Tests: `tests/unit/test_symbol_master_parser.py`, `tests/integration/test_symbol_enrichment.py`
- Schema: `warehouse/clickhouse/init/01_create_tables.sql` (lines 302-381)
