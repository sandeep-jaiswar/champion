# NSE Option Chain Scraper

## Overview

The NSE Option Chain Scraper is a production-grade tool for capturing option chain snapshots from the National Stock Exchange (NSE) of India. It provides real-time access to bid/ask prices, implied volatility (IV), open interest (OI), and Greek values for options on indices (NIFTY, BANKNIFTY, etc.) and equities.

## Features

- **Real-time Scraping**: Fetch current option chain data from NSE API
- **Multiple Symbols**: Support for indices (NIFTY, BANKNIFTY, FINNIFTY) and equities (RELIANCE, TCS, etc.)
- **Polars-based Parsing**: High-performance data processing using Polars
- **Parquet Storage**: Efficient columnar storage with date/symbol partitioning
- **Schema-first Design**: Avro schema for data validation and evolution
- **Rate Limiting**: Polite scraping with retry logic and exponential backoff
- **Null Handling**: Graceful handling of missing data fields
- **Prefect Integration**: Ready for orchestration in daily pipelines

## Usage

### Command Line Interface

Scrape NIFTY option chain every 5 minutes for 1 hour:
```bash
poetry run python src/scrapers/option_chain.py --symbol NIFTY --interval 5m --duration 1h
```

Scrape BANKNIFTY every 15 minutes for 2 hours:
```bash
poetry run python src/scrapers/option_chain.py \
  --symbol BANKNIFTY \
  --interval 15m \
  --duration 2h \
  --output-dir ./data/option_chain
```

Single snapshot of RELIANCE:
```bash
poetry run python src/scrapers/option_chain.py \
  --symbol RELIANCE \
  --interval 1m \
  --duration 1m
```

## Testing

Run unit tests:
```bash
poetry run pytest tests/unit/test_option_chain.py -v
```

Run manual test:
```bash
poetry run python test_option_chain.py
```

## Output Structure

Data is written to Parquet files with Hive-style partitioning:

```
data/option_chain/
├── date=2024-01-15/
│   ├── symbol=NIFTY/
│   │   ├── option_chain_20240115_093000.parquet
│   │   └── option_chain_20240115_093500.parquet
│   └── symbol=BANKNIFTY/
│       └── option_chain_20240115_093000.parquet
└── date=2024-01-16/
    └── symbol=NIFTY/
        └── option_chain_20240116_093000.parquet
```

## Schema

The option chain data follows the Avro schema defined in `schemas/market-data/option_chain_snapshot.avsc`.

Key fields captured:
- **Market Data**: underlying, strike_price, expiry_date, option_type (CE/PE)
- **Pricing**: bid_price, ask_price, last_price
- **Volume & OI**: volume, open_interest, change_in_oi
- **Greeks**: implied_volatility, delta, theta, gamma, vega
