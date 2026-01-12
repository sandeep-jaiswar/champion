# Implementation Documentation

This section details how each component of the Champion platform is implemented.

## Navigation

| Component | Document | Purpose |
|-----------|----------|---------|
| **Overview** | [overview.md](overview.md) | Overall implementation summary |
| **Ingestion** | [fundamentals_ingestion.md](fundamentals_ingestion.md) | Corporate financials pipeline |
| **Processing** | [polars_parser.md](polars_parser.md) | Data parsing & transformation |
| **Orchestration** | [prefect_orchestration.md](prefect_orchestration.md) | Workflow scheduling & monitoring |
| **Warehouse** | [clickhouse.md](clickhouse.md) | Analytics database setup |
| **Features** | [features.md](features.md) | Feature engineering & indicators |
| **Indices** | [index_constituents.md](index_constituents.md) | Index membership tracking |
| **Symbol Master** | [symbol_master_enrichment.md](symbol_master_enrichment.md) | Security reference data |
| **Bulk Deals** | [bulk_block_deals.md](bulk_block_deals.md) | Large transaction tracking |
| **Monitoring** | [prometheus_metrics.md](prometheus_metrics.md) | System metrics & monitoring |

## Implementation Patterns

### Data Pipeline Pattern

Every ETL pipeline follows:

```
Scrape → Parse → Normalize → Validate → Write → Load
```

### Task Structure (Prefect)

```
@flow
├── scrape_data()         # Download from API
├── parse_data()          # Convert to Polars
├── normalize_data()      # Validate & enrich
├── write_parquet()       # Store to lake
└── load_warehouse()      # Insert to ClickHouse
```

## Component Overview

### Ingestion Layer

- NSE Bhavcopy (3,283 securities daily)
- Symbol Master (2,223 companies)
- Fundamentals (quarterly financials)
- Index Constituents, Bulk/Block Deals, Trading Calendar

### Processing Layer

- Parses CSV/JSON/ZIP formats
- Polars for 50-100x performance
- Robust error handling
- Structured logging

### Storage Layer

- Bronze (raw), Silver (cleaned), Gold (analytics)
- Parquet with partitioning
- Configurable retention

### Warehouse Layer

- ClickHouse for OLAP
- Optimized for aggregations
- Real-time insert capability

### Orchestration Layer

- Prefect flows for each pipeline
- Automatic scheduling
- Retry with backoff
- Comprehensive metrics

## File Organization

```
src/champion/
├── scrapers/           # Data collection (NSE/BSE)
├── parsers/            # Polars-based parsers
├── storage/            # Parquet I/O
├── warehouse/          # ClickHouse client & loaders
├── features/           # Feature engineering
├── corporate_actions/  # Corporate actions processing
├── orchestration/      # Prefect flows and tasks
├── validation/         # Validation utilities & flows
└── utils/              # Shared utilities (logging, retry)
```

## Imports (New Paths)

Use the unified `champion.*` package:

```
from champion.scrapers.nse import BhavcopyScraper
from champion.parsers.index_constituent_parser import IndexConstituentParser
from champion.storage.parquet_io import write_partitioned_parquet
from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader
from champion.features.indicators import compute_features
from champion.corporate_actions.price_adjuster import apply_ca_adjustments
from champion.validation.validator import ParquetValidator
from champion.config import config
```

Legacy imports like `from src.parsers...` have been retired.

---

**Need details?** Pick a component above.
