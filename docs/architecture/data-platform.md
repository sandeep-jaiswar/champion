# Champion Data Platform: Polars + Parquet + ClickHouse + Prefect + MLflow

## Vision

A modular, schema-first market data platform optimized for batch and streaming ingestion, columnar analytics, and reproducible ML workflows. We leverage:

- **Polars**: fast, memory-efficient dataframes for parsing/transformations
- **Parquet**: columnar storage with partitioning for efficient IO
- **ClickHouse**: OLAP warehouse for low-latency analytical queries
- **Prefect**: modern orchestration for reliable, observable pipelines
- **MLflow**: experiment tracking and model registry for ML lifecycle

## High-Level Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         Data Platform Flow                           │
└─────────────────────────────────────────────────────────────────────┘

  NSE/Exchange Sources
         │
         ├─────► Ingestion Layer (NSE Scraper)
         │              │
         │              ▼
         │       Raw CSV Files
         │              │
         │              ▼
         │       Polars Processing ◄──┐
         │              │              │
         │              ▼              │
         │       Parquet Data Lake    │
         │              │              │
         │              ├──► raw/      │
         │              ├──► normalized/│
         │              └──► features/  │
         │                      │       │
         │                      ▼       │
         │              ClickHouse OLAP │
         │                      │       │
         │                      ▼       │
         │              Analytics/ML    │
         │                              │
         └───────────────────────────── │
                                        │
         ┌──────────────────────────────┘
         │
    Prefect Orchestration (schedules, retries, lineage)
         │
         └──────► MLflow Tracking (params, metrics, artifacts)
```

## Data Flow Pipeline

1. **Ingestion (NSE Scraper)** → raw CSV files to staging area
2. **Parsing/Normalization (Polars)** → read CSV, transform, validate schema
3. **Parquet Write** → write to partitioned data lake (raw, normalized, features)
4. **Warehouse Load (ClickHouse)** → ingest from Parquet or direct insert via HTTP/local
5. **Orchestration (Prefect)** → schedules, retries, lineage & logs
6. **Experimentation (MLflow)** → track features, training runs, metrics

## Storage Strategy (Parquet Data Lake)

### Directory Structure

```text
data/lake/
├── raw/
│   ├── equity_ohlc/
│   │   ├── date=2024-01-15/
│   │   │   ├── part-00000.parquet
│   │   │   ├── part-00001.parquet
│   │   │   └── _metadata
│   │   └── date=2024-01-16/
│   ├── equity_trade/
│   └── index_ohlc/
├── normalized/
│   ├── equity_ohlc/
│   │   ├── year=2024/month=01/day=15/
│   │   │   ├── part-00000.parquet
│   │   │   └── _metadata
│   │   └── year=2024/month=01/day=16/
│   └── equity_adjusted/
└── features/
    ├── equity_indicators/
    │   ├── year=2024/month=01/day=15/feature_group=momentum/
    │   │   └── part-00000.parquet
    │   └── year=2024/month=01/day=15/feature_group=volatility/
    └── equity_technical/
```

### Dataset Definitions

#### Raw Layer (`raw/`)

- **Purpose**: Preserve source truth exactly as received from exchanges
- **Schema**: Mirrors NSE/BSE raw format (no transformations)
- **Retention**: Long-term (5+ years), immutable
- **Datasets**:
  - `equity_ohlc/` - Daily equity OHLC from bhavcopy
  - `equity_trade/` - Intraday tick/trade data (future)
  - `index_ohlc/` - Index daily OHLC data
  - `derivatives_ohlc/` - F&O segment data (future)

#### Normalized Layer (`normalized/`)

- **Purpose**: Standardized, typed, CA-adjusted data for analytics
- **Schema**: Canonical platform schema with type enforcement
- **Retention**: Medium-term (3 years), can be rebuilt from raw
- **Datasets**:
  - `equity_ohlc/` - Normalized equity OHLC with adjustments
  - `equity_adjusted/` - Split/bonus/dividend adjusted prices
  - `reference_data/` - Symbol master, corporate actions

#### Features Layer (`features/`)

- **Purpose**: Derived indicators and engineered features for ML
- **Schema**: Feature-specific schemas with metadata
- **Retention**: Short-term (1 year), reproducible from normalized
- **Datasets**:
  - `equity_indicators/` - Technical indicators (SMA, EMA, RSI, MACD, BB)
  - `equity_technical/` - Chart patterns, support/resistance
  - `equity_fundamental/` - Fundamental ratios (future)

### Partitioning Strategy

| Layer      | Dataset              | Partition Scheme                                    | Rationale                                    |
|------------|---------------------|-----------------------------------------------------|----------------------------------------------|
| Raw        | equity_ohlc         | `date=YYYY-MM-DD/`                                  | Daily files, simple time-based pruning       |
| Raw        | equity_trade        | `date=YYYY-MM-DD/hour=HH/` (future)                | Intraday data needs finer granularity        |
| Normalized | equity_ohlc         | `year=YYYY/month=MM/day=DD/`                        | Hive-style for Spark/Hudi compatibility      |
| Normalized | equity_adjusted     | `year=YYYY/month=MM/day=DD/`                        | Same as OHLC for join efficiency             |
| Features   | equity_indicators   | `year=YYYY/month=MM/day=DD/feature_group=<group>/` | Group features by type for selective loading |

**Multi-level Partitioning**: Optional symbol-level partitioning (`symbol=<SYM>/`) can be added if:

- Individual date partitions exceed 500 MB
- Queries frequently filter by specific symbols
- Parallelism benefits outweigh small file overhead

### File Sizing Guidelines

| Scenario                    | Target Size   | Action                                      |
|-----------------------------|---------------|---------------------------------------------|
| Small daily files (< 50MB)  | 128-256 MB    | Coalesce multiple days/symbols per file     |
| Large daily files (> 500MB) | 256-512 MB    | Add symbol partitioning or split by size    |
| Feature files               | 100-200 MB    | Group features by type, avoid single-column |
| Historical backfills        | 256-512 MB    | Process in batches, write to date ranges    |

**File Count Targets**:

- Minimize small files (< 10 MB) - causes metadata overhead
- Target 10-50 files per partition for optimal parallelism
- Use `polars.collect()` with `max_rows_per_file` for control

### Compression & Encoding

```python
# Polars write configuration
df.write_parquet(
    path,
    compression="snappy",        # Fast compression (2-3x), good CPU/IO balance
    compression_level=None,      # Default level
    statistics=True,             # Enable column statistics for pruning
    row_group_size=1024*1024,   # 1M rows per row group (tune based on data)
    data_page_size=1024*1024     # 1MB data pages
)
```

**Compression Options**:

- **Snappy**: Default, fast read/write, 2-3x compression
- **Zstd**: Better compression (3-5x), slightly slower reads, good for cold data
- **Gzip**: Maximum compression (4-6x), slower, for archived data

### Metadata Files

- `_metadata`: Consolidated Parquet metadata for all files in dataset
- `_common_metadata`: Common schema for fast schema discovery
- `_SUCCESS`: Marker file indicating complete partition write (Spark-style)

**Usage**:

```python
# Fast schema inference without reading data
schema = pl.read_parquet_schema("data/lake/raw/equity_ohlc/_metadata")

# Parallel read with metadata-based pruning
df = pl.scan_parquet("data/lake/raw/equity_ohlc/**/*.parquet") \
    .filter(pl.col("date") == "2024-01-15") \
    .collect()
```

## Warehouse Strategy (ClickHouse)

### Architecture Overview

```text
┌─────────────────────────────────────────┐
│         ClickHouse Cluster              │
│  ┌───────────────────────────────────┐  │
│  │  Database: champion_market         │  │
│  │                                    │  │
│  │  Tables:                           │  │
│  │    ├─ raw_equity_ohlc              │  │
│  │    ├─ normalized_equity_ohlc       │  │
│  │    ├─ features_equity_indicators   │  │
│  │    └─ symbol_master                │  │
│  │                                    │  │
│  │  Materialized Views:               │  │
│  │    ├─ equity_ohlc_daily_summary    │  │
│  │    └─ equity_volume_leaders        │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
          ▲                    ▲
          │                    │
    Parquet Load         HTTP Insert
```

### Table Schemas & DDL

#### 1. Raw Equity OHLC Table

**Purpose**: Store raw NSE bhavcopy data for audit trail and replay

```sql
CREATE DATABASE IF NOT EXISTS champion_market;

CREATE TABLE champion_market.raw_equity_ohlc
(
    -- Envelope fields
    event_id            String,
    event_time          DateTime64(3),
    ingest_time         DateTime64(3),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Payload fields (NSE BhavCopy_NSE_CM format)
    TradDt              Date,
    BizDt               Date,
    Sgmt                LowCardinality(String),
    Src                 LowCardinality(String),
    FinInstrmTp         LowCardinality(String),
    FinInstrmId         Int64,
    ISIN                String,
    TckrSymb            String,
    SctySrs             LowCardinality(String),
    XpryDt              Nullable(Date),
    FininstrmActlXpryDt Nullable(Date),
    StrkPric            Nullable(Float64),
    OptnTp              Nullable(LowCardinality(String)),
    FinInstrmNm         String,
    OpnPric             Nullable(Float64),
    HghPric             Nullable(Float64),
    LwPric              Nullable(Float64),
    ClsPric             Nullable(Float64),
    LastPric            Nullable(Float64),
    PrvsClsgPric        Nullable(Float64),
    UndrlygPric         Nullable(Float64),
    SttlmPric           Nullable(Float64),
    OpnIntrst           Nullable(Int64),
    ChngInOpnIntrst     Nullable(Int64),
    TtlTradgVol         Nullable(Int64),
    TtlTrfVal           Nullable(Float64),
    TtlNbOfTxsExctd     Nullable(Int64),
    SsnId               Nullable(String),
    NewBrdLotQty        Nullable(Int64),
    Rmks                Nullable(String),
    Rsvd1               Nullable(String),
    Rsvd2               Nullable(String),
    Rsvd3               Nullable(String),
    Rsvd4               Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(TradDt)
ORDER BY (TckrSymb, TradDt, event_time)
TTL TradDt + INTERVAL 5 YEAR
SETTINGS 
    index_granularity = 8192,
    allow_nullable_key = 0;

-- Index for faster ISIN lookups
CREATE INDEX idx_isin ON champion_market.raw_equity_ohlc (ISIN) TYPE bloom_filter GRANULARITY 4;
```

**Key Decisions**:

- `MergeTree`: Standard table engine for immutable inserts
- `PARTITION BY toYYYYMM(TradDt)`: Monthly partitions for efficient data pruning
- `ORDER BY (TckrSymb, TradDt, event_time)`: Primary sorting for symbol-date queries
- `TTL 5 YEAR`: Automatic data expiration after retention period
- `LowCardinality`: Memory optimization for columns with < 10K distinct values
- Bloom filter index on ISIN for set membership queries

#### 2. Normalized Equity OHLC Table

**Purpose**: Clean, CA-adjusted equity data for analytics

```sql
CREATE TABLE champion_market.normalized_equity_ohlc
(
    -- Envelope fields
    event_id            String,
    event_time          DateTime64(3),
    ingest_time         DateTime64(3),
    source              LowCardinality(String),
    schema_version      LowCardinality(String),
    entity_id           String,
    
    -- Normalized payload
    instrument_id       String,
    symbol              String,
    exchange            LowCardinality(String),
    isin                Nullable(String),
    instrument_type     Nullable(LowCardinality(String)),
    series              Nullable(LowCardinality(String)),
    trade_date          Date,
    prev_close          Nullable(Float64),
    open                Float64,
    high                Float64,
    low                 Float64,
    close               Float64,
    last_price          Nullable(Float64),
    settlement_price    Nullable(Float64),
    volume              Int64,
    turnover            Float64,
    trades              Nullable(Int64),
    adjustment_factor   Float64 DEFAULT 1.0,
    adjustment_date     Nullable(Date),
    is_trading_day      Bool
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date, instrument_id)
TTL trade_date + INTERVAL 3 YEAR
SETTINGS 
    index_granularity = 8192;

-- Indexes for common query patterns
CREATE INDEX idx_isin_norm ON champion_market.normalized_equity_ohlc (isin) TYPE bloom_filter GRANULARITY 4;
CREATE INDEX idx_volume ON champion_market.normalized_equity_ohlc (volume) TYPE minmax GRANULARITY 1;
```

**Key Decisions**:

- `ReplacingMergeTree(ingest_time)`: Allows upserts with deduplication on same ORDER BY key
- Keeps latest version based on `ingest_time` (for late-arriving data or corrections)
- Same partition and TTL strategy as raw for consistency
- Additional indexes for volume filtering (top movers queries)

#### 3. Features Equity Indicators Table

**Purpose**: Store computed technical indicators for ML and analytics

```sql
CREATE TABLE champion_market.features_equity_indicators
(
    -- Metadata
    symbol              String,
    trade_date          Date,
    feature_timestamp   DateTime64(3),
    feature_version     LowCardinality(String),
    
    -- Moving averages
    sma_5               Nullable(Float64),
    sma_10              Nullable(Float64),
    sma_20              Nullable(Float64),
    sma_50              Nullable(Float64),
    sma_100             Nullable(Float64),
    sma_200             Nullable(Float64),
    ema_12              Nullable(Float64),
    ema_26              Nullable(Float64),
    ema_50              Nullable(Float64),
    
    -- Momentum indicators
    rsi_14              Nullable(Float64),
    macd                Nullable(Float64),
    macd_signal         Nullable(Float64),
    macd_histogram      Nullable(Float64),
    stochastic_k        Nullable(Float64),
    stochastic_d        Nullable(Float64),
    
    -- Volatility indicators
    bb_upper            Nullable(Float64),
    bb_middle           Nullable(Float64),
    bb_lower            Nullable(Float64),
    bb_width            Nullable(Float64),
    atr_14              Nullable(Float64),
    
    -- Volume indicators
    vwap                Nullable(Float64),
    obv                 Nullable(Int64),
    
    -- Computed timestamp
    computed_at         DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date, feature_timestamp)
TTL trade_date + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192;
```

**Key Decisions**:

- `MergeTree`: Standard engine, no deduplication needed (idempotent compute)
- Feature versioning for reproducibility and A/B testing
- Shorter TTL (1 year) - features can be recomputed from normalized data
- Wide schema design - all indicators in one table for atomic reads

#### 4. Symbol Master Reference Table

**Purpose**: Canonical symbol registry with metadata

```sql
CREATE TABLE champion_market.symbol_master
(
    symbol              String,
    exchange            LowCardinality(String),
    isin                String,
    company_name        String,
    sector              LowCardinality(String),
    industry            LowCardinality(String),
    instrument_type     LowCardinality(String),
    series              LowCardinality(String),
    face_value          Nullable(Float64),
    listing_date        Nullable(Date),
    delisting_date      Nullable(Date),
    is_active           Bool DEFAULT true,
    last_updated        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (exchange, symbol)
SETTINGS 
    index_granularity = 8192;

-- Unique constraint simulation
CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_exchange 
ON champion_market.symbol_master (exchange, symbol);
```

### Materialized Views for Aggregations

#### Daily OHLC Summary

```sql
CREATE MATERIALIZED VIEW champion_market.equity_ohlc_daily_summary
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, exchange)
AS SELECT
    trade_date,
    exchange,
    count() as total_symbols,
    sum(volume) as total_volume,
    sum(turnover) as total_turnover,
    avg(close) as avg_close_price,
    max(high) as max_high_price,
    min(low) as min_low_price
FROM champion_market.normalized_equity_ohlc
GROUP BY trade_date, exchange;
```

#### Volume Leaders View

```sql
CREATE MATERIALIZED VIEW champion_market.equity_volume_leaders
ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, volume)
AS SELECT
    trade_date,
    symbol,
    exchange,
    volume,
    turnover,
    close,
    (close - prev_close) / prev_close * 100 as pct_change
FROM champion_market.normalized_equity_ohlc
WHERE volume > 0
ORDER BY trade_date DESC, volume DESC
LIMIT 100 BY trade_date;
```

### Ingestion Methods

#### 1. Local File Ingestion (Parquet)

```bash
# Insert from Parquet file
clickhouse-client --query="
  INSERT INTO champion_market.raw_equity_ohlc 
  SELECT * FROM file('data/lake/raw/equity_ohlc/date=2024-01-15/*.parquet', Parquet)
"

# Bulk load with parallel processing
clickhouse-client --query="
  INSERT INTO champion_market.normalized_equity_ohlc
  SELECT * FROM file('data/lake/normalized/equity_ohlc/**/*.parquet', Parquet)
  SETTINGS max_insert_threads=4
"
```

#### 2. HTTP API Insert (JSONEachRow)

```python
import requests
import json

data = [
    {"event_id": "...", "symbol": "RELIANCE", "trade_date": "2024-01-15", ...},
    {"event_id": "...", "symbol": "TCS", "trade_date": "2024-01-15", ...}
]

response = requests.post(
    "http://localhost:8123/",
    params={
        "query": "INSERT INTO champion_market.normalized_equity_ohlc FORMAT JSONEachRow"
    },
    data="\n".join(json.dumps(row) for row in data)
)
```

#### 3. Python Client Insert (Polars → ClickHouse)

```python
import polars as pl
from clickhouse_connect import get_client

# Read from Parquet
df = pl.read_parquet("data/lake/normalized/equity_ohlc/year=2024/month=01/**/*.parquet")

# Convert to ClickHouse
client = get_client(host='localhost', port=8123, database='champion_market')
client.insert_df(
    table='normalized_equity_ohlc',
    df=df.to_pandas(),  # Convert Polars to Pandas for compatibility
    settings={'async_insert': 1, 'wait_for_async_insert': 1}
)
```

### Query Optimization

#### Sort Key Selection Rules

- **Primary KEY**: Most selective columns first (e.g., `symbol` before `date`)
- **Date Columns**: Always include for partition pruning
- **Cardinality**: High cardinality columns first (symbol > date > exchange)

#### Performance Settings

```sql
-- Enable parallel query execution
SET max_threads = 8;
SET max_execution_time = 60;

-- Optimize for analytical queries
SET optimize_read_in_order = 1;
SET max_memory_usage = 10000000000;  -- 10GB limit

-- Enable query result caching
SET use_query_cache = 1;
SET query_cache_ttl = 300;  -- 5 minutes
```

### Backup & Disaster Recovery

```sql
-- Backup single partition
ALTER TABLE champion_market.normalized_equity_ohlc 
FREEZE PARTITION '202401' WITH NAME 'backup_202401';

-- Restore from backup
ALTER TABLE champion_market.normalized_equity_ohlc 
ATTACH PARTITION '202401' FROM '/var/lib/clickhouse/backup/backup_202401';
```

## Orchestration (Prefect)

### Orchestration Architecture

```text
┌────────────────────────────────────────────────────────────────┐
│                    Prefect Orchestration                        │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  Daily Market Data Flow                   │  │
│  │                                                            │  │
│  │  ┌──────────────┐         ┌──────────────┐               │  │
│  │  │  scrape_     │────────▶│  parse_      │               │  │
│  │  │  bhavcopy    │         │  raw_csv     │               │  │
│  │  └──────────────┘         └──────┬───────┘               │  │
│  │                                   │                        │  │
│  │                                   ▼                        │  │
│  │                          ┌──────────────┐                 │  │
│  │                          │  write_      │                 │  │
│  │                          │  parquet_raw │                 │  │
│  │                          └──────┬───────┘                 │  │
│  │                                 │                         │  │
│  │         ┌───────────────────────┼─────────────┐          │  │
│  │         ▼                       ▼             ▼          │  │
│  │  ┌─────────────┐    ┌─────────────┐   ┌─────────────┐  │  │
│  │  │ normalize_  │    │ load_       │   │ quality_    │  │  │
│  │  │ equity_ohlc │    │ clickhouse_ │   │ checks      │  │  │
│  │  │             │    │ raw         │   │             │  │  │
│  │  └──────┬──────┘    └─────────────┘   └─────────────┘  │  │
│  │         │                                                │  │
│  │         ▼                                                │  │
│  │  ┌─────────────┐                                        │  │
│  │  │ write_      │                                        │  │
│  │  │ parquet_    │                                        │  │
│  │  │ normalized  │                                        │  │
│  │  └──────┬──────┘                                        │  │
│  │         │                                                │  │
│  │         ├────────────┬────────────────┐                 │  │
│  │         ▼            ▼                ▼                 │  │
│  │  ┌──────────┐  ┌──────────┐    ┌──────────┐           │  │
│  │  │ compute_ │  │ load_    │    │ mlflow_  │           │  │
│  │  │ features │  │ click-   │    │ log      │           │  │
│  │  │          │  │ house_   │    │          │           │  │
│  │  └──────┬───┘  │ normal   │    └──────────┘           │  │
│  │         │      └──────────┘                            │  │
│  │         ▼                                               │  │
│  │  ┌──────────────┐                                      │  │
│  │  │ write_       │                                      │  │
│  │  │ parquet_     │                                      │  │
│  │  │ features     │                                      │  │
│  │  └──────┬───────┘                                      │  │
│  │         │                                               │  │
│  │         ▼                                               │  │
│  │  ┌──────────────┐                                      │  │
│  │  │ load_        │                                      │  │
│  │  │ clickhouse_  │                                      │  │
│  │  │ features     │                                      │  │
│  │  └──────────────┘                                      │  │
│  │                                                          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Schedules: Daily @ 18:30 IST (market close + 30min)          │
│  Retries: 3 attempts with exponential backoff                  │
│  Timeout: 30 minutes per flow                                  │
└────────────────────────────────────────────────────────────────┘
```

### Flow Definitions

#### 1. Main Daily Pipeline Flow

```python
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import polars as pl

@task(
    name="scrape_bhavcopy",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=24)
)
def scrape_bhavcopy(trade_date: str) -> str:
    """Download NSE bhavcopy CSV for given date"""
    from nse_scraper import download_bhavcopy
    
    csv_path = f"/tmp/bhavcopy_{trade_date}.csv"
    download_bhavcopy(trade_date, output_path=csv_path)
    return csv_path

@task(name="parse_raw_csv", retries=2)
def parse_raw_csv(csv_path: str) -> pl.DataFrame:
    """Parse raw CSV into Polars DataFrame with schema validation"""
    df = pl.read_csv(
        csv_path,
        schema_overrides={
            "TradDt": pl.Utf8,
            "TtlTradgVol": pl.Int64,
            "ClsPric": pl.Float64,
        }
    )
    return df

@task(name="write_parquet_raw")
def write_parquet_raw(df: pl.DataFrame, trade_date: str) -> str:
    """Write raw data to partitioned Parquet"""
    output_path = f"data/lake/raw/equity_ohlc/date={trade_date}/"
    df.write_parquet(
        output_path,
        compression="snappy",
        statistics=True,
        partition_by=None  # Already partitioned by directory
    )
    return output_path

@task(name="normalize_equity_ohlc", retries=2)
def normalize_equity_ohlc(raw_df: pl.DataFrame) -> pl.DataFrame:
    """Transform raw data to normalized schema"""
    normalized = raw_df.select([
        pl.lit(str(uuid.uuid4())).alias("event_id"),
        pl.col("TradDt").str.strptime(pl.Date, "%Y-%m-%d").alias("trade_date"),
        pl.col("TckrSymb").alias("symbol"),
        pl.lit("NSE").alias("exchange"),
        pl.col("ISIN").alias("isin"),
        pl.col("OpnPric").alias("open"),
        pl.col("HghPric").alias("high"),
        pl.col("LwPric").alias("low"),
        pl.col("ClsPric").alias("close"),
        pl.col("TtlTradgVol").alias("volume"),
        pl.col("TtlTrfVal").alias("turnover"),
    ])
    return normalized

@task(name="compute_features")
def compute_features(normalized_df: pl.DataFrame) -> pl.DataFrame:
    """Compute technical indicators"""
    features = normalized_df.sort(["symbol", "trade_date"]).with_columns([
        # Simple Moving Averages
        pl.col("close").rolling_mean(window_size=5).over("symbol").alias("sma_5"),
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
        
        # Exponential Moving Averages
        pl.col("close").ewm_mean(span=12).over("symbol").alias("ema_12"),
        pl.col("close").ewm_mean(span=26).over("symbol").alias("ema_26"),
        
        # Volume Moving Average
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("volume_ma_20"),
    ])
    return features

@task(name="load_clickhouse")
def load_clickhouse(df: pl.DataFrame, table: str):
    """Load data into ClickHouse table"""
    from clickhouse_connect import get_client
    
    client = get_client(host='localhost', database='champion_market')
    client.insert_df(table=table, df=df.to_pandas())
    return f"Loaded {len(df)} rows into {table}"

@task(name="mlflow_log_metrics")
def mlflow_log_metrics(df: pl.DataFrame, stage: str):
    """Log metrics to MLflow"""
    import mlflow
    
    with mlflow.start_run(run_name=f"{stage}_{datetime.now().isoformat()}"):
        mlflow.log_param("stage", stage)
        mlflow.log_param("row_count", len(df))
        mlflow.log_param("date_range", f"{df['trade_date'].min()} to {df['trade_date'].max()}")
        mlflow.log_metric("rows_processed", len(df))
        mlflow.log_metric("null_percentage", df.null_count().sum() / (len(df) * len(df.columns)) * 100)

@flow(
    name="daily_market_data_pipeline",
    description="End-to-end daily market data ingestion and processing",
    retries=1,
    retry_delay_seconds=300,
    timeout_seconds=1800
)
def daily_market_data_pipeline(trade_date: str):
    """Main daily pipeline orchestrating all tasks"""
    
    # Stage 1: Scrape and parse
    csv_path = scrape_bhavcopy(trade_date)
    raw_df = parse_raw_csv(csv_path)
    
    # Stage 2: Write raw Parquet
    raw_path = write_parquet_raw(raw_df, trade_date)
    
    # Stage 3: Normalize
    normalized_df = normalize_equity_ohlc(raw_df)
    normalized_path = write_parquet_raw(normalized_df, trade_date)  # Reuse task
    
    # Stage 4: Parallel loading
    load_raw = load_clickhouse.submit(raw_df, "raw_equity_ohlc")
    load_normalized = load_clickhouse.submit(normalized_df, "normalized_equity_ohlc")
    
    # Stage 5: Compute features (depends on normalized)
    features_df = compute_features(normalized_df)
    features_path = write_parquet_raw(features_df, trade_date)
    
    # Stage 6: Load features
    load_features = load_clickhouse(features_df, "features_equity_indicators")
    
    # Stage 7: Log metrics
    mlflow_log_metrics(raw_df, "raw")
    mlflow_log_metrics(normalized_df, "normalized")
    mlflow_log_metrics(features_df, "features")
    
    return {
        "status": "success",
        "raw_rows": len(raw_df),
        "normalized_rows": len(normalized_df),
        "features_rows": len(features_df)
    }
```

#### 2. Backfill Flow

```python
@flow(name="historical_backfill")
def historical_backfill(start_date: str, end_date: str):
    """Backfill historical data for date range"""
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    current = start
    while current <= end:
        trade_date_str = current.strftime("%Y-%m-%d")
        
        # Skip weekends (NSE closed)
        if current.weekday() < 5:  # Monday=0, Friday=4
            daily_market_data_pipeline(trade_date_str)
        
        current += timedelta(days=1)
```

### Scheduling

#### Cron Schedules

```python
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

# Daily market data pipeline - runs at 6:30 PM IST (after market close)
daily_deployment = Deployment.build_from_flow(
    flow=daily_market_data_pipeline,
    name="daily-market-data",
    schedule=CronSchedule(cron="30 13 * * 1-5", timezone="Asia/Kolkata"),  # 1:30 PM UTC = 6:30 PM IST
    parameters={"trade_date": "{{ today }}"},
    work_queue_name="market-data-queue"
)

# Weekly feature recomputation - runs Saturday midnight
weekly_feature_deployment = Deployment.build_from_flow(
    flow=compute_features,
    name="weekly-feature-recompute",
    schedule=CronSchedule(cron="0 18 * * 6", timezone="Asia/Kolkata"),  # Saturday 6:30 PM UTC
    work_queue_name="feature-queue"
)

# Monthly data quality audit
monthly_audit_deployment = Deployment.build_from_flow(
    flow=data_quality_audit,
    name="monthly-audit",
    schedule=CronSchedule(cron="0 2 1 * *", timezone="UTC"),  # 1st of every month
    work_queue_name="audit-queue"
)
```

#### Interval Schedules

```python
from prefect.server.schemas.schedules import IntervalSchedule

# Real-time monitoring (every 5 minutes during market hours)
monitoring_deployment = Deployment.build_from_flow(
    flow=market_health_check,
    name="market-monitoring",
    schedule=IntervalSchedule(interval=timedelta(minutes=5)),
    work_queue_name="monitoring-queue"
)
```

### Retry Policies

#### Task-Level Retries

```python
@task(
    retries=3,                          # Retry up to 3 times
    retry_delay_seconds=60,             # Wait 60s between retries
    retry_jitter_factor=0.5             # Add ±50% jitter to retry delay
)
def flaky_network_task():
    """Task with network calls that may fail transiently"""
    pass

@task(
    retries=5,
    retry_delay_seconds=[10, 30, 60, 120, 300]  # Exponential backoff
)
def critical_database_task():
    """Critical task with custom exponential backoff"""
    pass
```

#### Flow-Level Retries

```python
@flow(
    retries=1,                    # Retry entire flow once
    retry_delay_seconds=600,      # Wait 10 minutes before retry
    timeout_seconds=3600          # 1 hour timeout
)
def critical_pipeline():
    """Pipeline with flow-level retry on complete failure"""
    pass
```

### Error Handling

```python
from prefect.exceptions import FlowRunFailed
import logging

@task
def data_validation_task(df: pl.DataFrame):
    """Validate data quality with custom error handling"""
    logger = logging.getLogger(__name__)
    
    # Check for null values in critical columns
    critical_cols = ["symbol", "trade_date", "close"]
    null_counts = df[critical_cols].null_count()
    
    for col, count in zip(critical_cols, null_counts):
        if count > 0:
            logger.error(f"Critical column {col} has {count} null values")
            raise ValueError(f"Data quality check failed: {col} has nulls")
    
    # Check for price anomalies
    if (df["close"] <= 0).any():
        logger.error("Found non-positive close prices")
        raise ValueError("Invalid price data detected")
    
    return True
```

### Logging & Observability

```python
from prefect import get_run_logger

@task
def observable_task(df: pl.DataFrame):
    """Task with structured logging"""
    logger = get_run_logger()
    
    logger.info("Starting data processing", extra={
        "row_count": len(df),
        "columns": df.columns,
        "memory_usage": df.estimated_size("mb")
    })
    
    # Processing logic
    result = df.filter(pl.col("volume") > 0)
    
    logger.info("Processing complete", extra={
        "rows_filtered": len(df) - len(result),
        "retention_rate": len(result) / len(df) * 100
    })
    
    return result
```

### Prefect Blocks (Configuration Management)

```python
from prefect.blocks.system import Secret, JSON

# Store secrets
clickhouse_creds = Secret(value="clickhouse_password_here")
clickhouse_creds.save("clickhouse-password")

# Store configuration
data_lake_config = JSON(value={
    "base_path": "/data/lake",
    "compression": "snappy",
    "partition_size_mb": 256
})
data_lake_config.save("data-lake-config")

# Use in tasks
@task
def connect_to_clickhouse():
    password = Secret.load("clickhouse-password").get()
    config = JSON.load("data-lake-config").value
    # Use credentials and config
```

### Work Queues & Agents

```bash
# Start Prefect server
prefect server start

# Create work queues
prefect work-queue create market-data-queue --limit 5
prefect work-queue create feature-queue --limit 3
prefect work-queue create monitoring-queue --limit 10

# Start agents
prefect agent start --work-queue market-data-queue --pool default-pool
prefect agent start --work-queue feature-queue --pool default-pool
```

### Deployment Configuration

```yaml
# prefect.yaml
deployments:
  - name: daily-market-data
    entrypoint: flows/market_data.py:daily_market_data_pipeline
    work_pool:
      name: default-pool
    schedule:
      cron: "30 13 * * 1-5"
      timezone: Asia/Kolkata
    parameters:
      trade_date: "{{ today }}"
    tags:
      - production
      - daily
      - market-data
    
  - name: historical-backfill
    entrypoint: flows/market_data.py:historical_backfill
    work_pool:
      name: default-pool
    tags:
      - adhoc
      - backfill
```

### Monitoring & Alerts

```python
from prefect.blocks.notifications import SlackWebhook

@flow(on_failure=[send_slack_alert])
def monitored_pipeline():
    """Pipeline with failure notifications"""
    pass

def send_slack_alert(flow, flow_run, state):
    """Send Slack notification on failure"""
    slack = SlackWebhook.load("engineering-alerts")
    slack.notify(
        body=f"Flow {flow.name} failed with state {state.type}",
        subject=f"Pipeline Failure: {flow.name}"
    )
```

## Experiment Tracking (MLflow)

### Architecture & Setup

```text
┌─────────────────────────────────────────────────────────────┐
│                    MLflow Tracking Server                    │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │               Tracking Store (PostgreSQL)              │ │
│  │  - Experiments, Runs, Parameters, Metrics, Tags        │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │          Artifact Store (S3/Local FileSystem)          │ │
│  │  - Parquet samples, Models, Plots, Notebooks           │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  UI: http://localhost:5000                                  │
└─────────────────────────────────────────────────────────────┘
```

#### Local Development Setup

```bash
# Start MLflow server with local backend
mlflow server \
  --backend-store-uri sqlite:///mlflow.db \
  --default-artifact-root ./mlruns \
  --host 0.0.0.0 \
  --port 5000

# Production setup with PostgreSQL and S3
mlflow server \
  --backend-store-uri postgresql://user:pass@localhost/mlflow \
  --default-artifact-root s3://champion-mlflow-artifacts/ \
  --host 0.0.0.0 \
  --port 5000
```

#### Docker Compose Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  mlflow:
    image: ghcr.io/mlflow/mlflow:v2.10.0
    ports:
      - "5000:5000"
    environment:
      - MLFLOW_BACKEND_STORE_URI=postgresql://mlflow:mlflow@postgres:5432/mlflow
      - MLFLOW_S3_ENDPOINT_URL=http://minio:9000
      - AWS_ACCESS_KEY_ID=minioadmin
      - AWS_SECRET_ACCESS_KEY=minioadmin
    command: >
      mlflow server
      --backend-store-uri postgresql://mlflow:mlflow@postgres:5432/mlflow
      --default-artifact-root s3://mlflow-artifacts/
      --host 0.0.0.0
    depends_on:
      - postgres
      - minio
  
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: mlflow
      POSTGRES_USER: mlflow
      POSTGRES_PASSWORD: mlflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
  
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data

volumes:
  postgres_data:
  minio_data:
```

### Tracking Patterns

#### 1. Data Pipeline Runs

**Track ingestion and transformation metrics**:

```python
import mlflow
import polars as pl
from datetime import datetime

def track_ingestion_run(df: pl.DataFrame, stage: str, trade_date: str):
    """Track data pipeline execution metrics"""
    
    mlflow.set_experiment("market-data-ingestion")
    
    with mlflow.start_run(run_name=f"{stage}_{trade_date}") as run:
        # Log parameters
        mlflow.log_param("stage", stage)
        mlflow.log_param("trade_date", trade_date)
        mlflow.log_param("data_source", "NSE")
        mlflow.log_param("polars_version", pl.__version__)
        
        # Log metrics
        mlflow.log_metric("row_count", len(df))
        mlflow.log_metric("column_count", len(df.columns))
        mlflow.log_metric("memory_usage_mb", df.estimated_size("mb"))
        mlflow.log_metric("null_percentage", 
                         df.null_count().sum() / (len(df) * len(df.columns)) * 100)
        
        # Data quality metrics
        if "volume" in df.columns:
            mlflow.log_metric("total_volume", df["volume"].sum())
            mlflow.log_metric("avg_volume", df["volume"].mean())
            mlflow.log_metric("zero_volume_count", (df["volume"] == 0).sum())
        
        if "close" in df.columns:
            mlflow.log_metric("avg_close_price", df["close"].mean())
            mlflow.log_metric("max_close_price", df["close"].max())
            mlflow.log_metric("min_close_price", df["close"].min())
        
        # Log execution time
        mlflow.log_metric("execution_time_sec", time.time() - start_time)
        
        # Log sample data as artifact
        sample_path = f"/tmp/sample_{stage}_{trade_date}.parquet"
        df.head(100).write_parquet(sample_path)
        mlflow.log_artifact(sample_path, artifact_path="samples")
        
        # Log data statistics
        stats = df.describe()
        stats_path = f"/tmp/stats_{stage}_{trade_date}.csv"
        stats.write_csv(stats_path)
        mlflow.log_artifact(stats_path, artifact_path="statistics")
        
        # Tag run
        mlflow.set_tags({
            "pipeline": "market-data",
            "environment": "production",
            "data_layer": stage
        })
        
        return run.info.run_id
```

#### 2. Feature Engineering Runs

**Track feature computation and versioning**:

```python
def track_feature_engineering(
    input_df: pl.DataFrame,
    output_df: pl.DataFrame,
    feature_config: dict
):
    """Track feature engineering execution"""
    
    mlflow.set_experiment("feature-engineering")
    
    with mlflow.start_run(run_name=f"features_{datetime.now().isoformat()}") as run:
        # Log feature configuration
        mlflow.log_params(feature_config)
        mlflow.log_param("feature_count", len(output_df.columns) - len(input_df.columns))
        mlflow.log_param("window_sizes", [5, 10, 20, 50])
        
        # Log feature statistics
        new_features = [col for col in output_df.columns if col not in input_df.columns]
        for feature in new_features:
            mlflow.log_metric(f"feature_{feature}_mean", output_df[feature].mean())
            mlflow.log_metric(f"feature_{feature}_std", output_df[feature].std())
            mlflow.log_metric(f"feature_{feature}_null_pct", 
                            output_df[feature].null_count() / len(output_df) * 100)
        
        # Log feature correlation matrix
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        corr_matrix = output_df[new_features].corr()
        plt.figure(figsize=(12, 10))
        sns.heatmap(corr_matrix.to_pandas(), annot=False, cmap='coolwarm')
        plt.title("Feature Correlation Matrix")
        plt.tight_layout()
        corr_plot_path = "/tmp/feature_correlation.png"
        plt.savefig(corr_plot_path)
        mlflow.log_artifact(corr_plot_path, artifact_path="visualizations")
        
        # Log feature definitions as JSON
        feature_manifest = {
            "features": new_features,
            "version": "v1.0",
            "computation_date": datetime.now().isoformat(),
            "dependencies": ["normalized_equity_ohlc"]
        }
        mlflow.log_dict(feature_manifest, "feature_manifest.json")
        
        return run.info.run_id
```

#### 3. Model Training Runs

**Track ML model experiments**:

```python
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score

def track_model_training(
    X_train, y_train, X_test, y_test,
    model_params: dict
):
    """Track model training experiment"""
    
    mlflow.set_experiment("equity-price-prediction")
    
    with mlflow.start_run(run_name=f"rf_model_{datetime.now().isoformat()}") as run:
        # Log model parameters
        mlflow.log_params(model_params)
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("feature_count", X_train.shape[1])
        
        # Train model
        model = RandomForestClassifier(**model_params)
        model.fit(X_train, y_train)
        
        # Predictions
        y_pred = model.predict(X_test)
        
        # Log metrics
        mlflow.log_metric("accuracy", accuracy_score(y_test, y_pred))
        mlflow.log_metric("precision", precision_score(y_test, y_pred, average='weighted'))
        mlflow.log_metric("recall", recall_score(y_test, y_pred, average='weighted'))
        
        # Log feature importance
        feature_importance = dict(zip(X_train.columns, model.feature_importances_))
        mlflow.log_dict(feature_importance, "feature_importance.json")
        
        # Log model
        mlflow.sklearn.log_model(
            model,
            "model",
            registered_model_name="equity_price_predictor"
        )
        
        # Log confusion matrix
        from sklearn.metrics import confusion_matrix
        import matplotlib.pyplot as plt
        
        cm = confusion_matrix(y_test, y_pred)
        plt.figure(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
        plt.title("Confusion Matrix")
        plt.ylabel("True Label")
        plt.xlabel("Predicted Label")
        cm_path = "/tmp/confusion_matrix.png"
        plt.savefig(cm_path)
        mlflow.log_artifact(cm_path, artifact_path="plots")
        
        return run.info.run_id
```

#### 4. Data Quality Monitoring

**Track data quality over time**:

```python
def track_data_quality(df: pl.DataFrame, trade_date: str):
    """Track data quality metrics"""
    
    mlflow.set_experiment("data-quality-monitoring")
    
    with mlflow.start_run(run_name=f"quality_check_{trade_date}") as run:
        mlflow.log_param("trade_date", trade_date)
        
        # Completeness metrics
        total_cells = len(df) * len(df.columns)
        null_cells = df.null_count().sum()
        mlflow.log_metric("completeness_pct", (1 - null_cells / total_cells) * 100)
        
        # Validity metrics (price ranges)
        invalid_prices = ((df["close"] <= 0) | (df["close"] > 1000000)).sum()
        mlflow.log_metric("invalid_price_count", invalid_prices)
        mlflow.log_metric("validity_pct", (1 - invalid_prices / len(df)) * 100)
        
        # Consistency metrics (volume vs turnover)
        if "volume" in df.columns and "turnover" in df.columns:
            inconsistent = (df["turnover"] == 0) & (df["volume"] > 0)
            mlflow.log_metric("inconsistent_records", inconsistent.sum())
        
        # Timeliness metrics
        mlflow.log_metric("data_freshness_hours", 
                         (datetime.now() - datetime.fromisoformat(trade_date)).total_seconds() / 3600)
        
        # Log anomalies
        anomalies = df.filter(
            (pl.col("close") <= 0) | 
            (pl.col("volume") < 0) |
            (pl.col("turnover") < 0)
        )
        if len(anomalies) > 0:
            anomaly_path = f"/tmp/anomalies_{trade_date}.csv"
            anomalies.write_csv(anomaly_path)
            mlflow.log_artifact(anomaly_path, artifact_path="anomalies")
        
        return run.info.run_id
```

### MLflow Integration with Prefect

```python
from prefect import flow, task
import mlflow

@task
def mlflow_tracked_task(df: pl.DataFrame, task_name: str):
    """Prefect task with MLflow tracking"""
    
    # Use Prefect run context as MLflow run name
    from prefect.context import get_run_context
    context = get_run_context()
    
    mlflow.set_experiment("prefect-pipelines")
    
    with mlflow.start_run(run_name=f"{context.flow_run.name}_{task_name}") as run:
        # Log Prefect metadata
        mlflow.set_tags({
            "prefect_flow_run_id": str(context.flow_run.id),
            "prefect_task_name": task_name,
            "prefect_flow_name": context.flow.name
        })
        
        # Task logic
        result = process_data(df)
        
        # Log metrics
        mlflow.log_metric("rows_processed", len(result))
        
        return result

@flow(name="mlflow_integrated_pipeline")
def mlflow_integrated_pipeline(trade_date: str):
    """Pipeline with end-to-end MLflow tracking"""
    
    # Parent MLflow run for entire pipeline
    mlflow.set_experiment("daily-market-pipeline")
    
    with mlflow.start_run(run_name=f"pipeline_{trade_date}") as parent_run:
        mlflow.log_param("trade_date", trade_date)
        
        # Child runs for each task
        raw_df = mlflow_tracked_task(None, "scrape")
        normalized_df = mlflow_tracked_task(raw_df, "normalize")
        features_df = mlflow_tracked_task(normalized_df, "features")
        
        # Log pipeline summary
        mlflow.log_metric("total_pipeline_time", time.time() - start)
        mlflow.set_tag("status", "success")
        
        return parent_run.info.run_id
```

### Artifact Management

#### Artifact Organization

```text
mlruns/
├── 0/                          # Default experiment
├── 1/                          # market-data-ingestion
│   ├── run_id_1/
│   │   ├── artifacts/
│   │   │   ├── samples/
│   │   │   │   └── sample_raw_2024-01-15.parquet
│   │   │   └── statistics/
│   │   │       └── stats_raw_2024-01-15.csv
│   │   ├── metrics/
│   │   ├── params/
│   │   └── tags/
│   └── run_id_2/
├── 2/                          # feature-engineering
│   ├── run_id_3/
│   │   ├── artifacts/
│   │   │   ├── visualizations/
│   │   │   │   └── feature_correlation.png
│   │   │   └── feature_manifest.json
└── 3/                          # equity-price-prediction
    └── run_id_4/
        ├── artifacts/
        │   ├── model/
        │   │   ├── model.pkl
        │   │   └── MLmodel
        │   └── plots/
        │       └── confusion_matrix.png
```

#### Artifact Logging Best Practices

```python
# 1. Log Parquet samples efficiently
df.head(1000).write_parquet(sample_path, compression="zstd")
mlflow.log_artifact(sample_path, artifact_path="samples")

# 2. Log large artifacts to S3 directly
mlflow.log_artifact("s3://bucket/large-model.bin", artifact_path="models")

# 3. Log visualizations programmatically
import matplotlib.pyplot as plt

fig, ax = plt.subplots()
df.plot(x="date", y="close", ax=ax)
mlflow.log_figure(fig, "price_trend.png")

# 4. Log structured data as JSON/YAML
config = {"window_sizes": [5, 10, 20], "indicators": ["SMA", "EMA"]}
mlflow.log_dict(config, "feature_config.json")

# 5. Log entire directories
mlflow.log_artifacts("/path/to/analysis/", artifact_path="analysis_results")
```

### Model Registry

```python
import mlflow

# Register model after training
mlflow.sklearn.log_model(
    model,
    "model",
    registered_model_name="equity_momentum_classifier",
    signature=mlflow.models.infer_signature(X_train, y_train)
)

# Transition model to production
client = mlflow.tracking.MlflowClient()
client.transition_model_version_stage(
    name="equity_momentum_classifier",
    version=3,
    stage="Production"
)

# Load production model
production_model = mlflow.sklearn.load_model(
    "models:/equity_momentum_classifier/Production"
)

# Use in scoring pipeline
predictions = production_model.predict(X_new)
```

### Experiment Comparison

```python
# Query runs programmatically
runs = mlflow.search_runs(
    experiment_names=["feature-engineering"],
    filter_string="metrics.feature_sma_20_mean > 100",
    order_by=["metrics.feature_count DESC"]
)

# Compare runs
best_run = runs.iloc[0]
print(f"Best run: {best_run.run_id}")
print(f"Parameters: {best_run.params}")
print(f"Metrics: {best_run.metrics}")
```

### Access Control & Security

```bash
# Enable authentication
export MLFLOW_TRACKING_USERNAME=admin
export MLFLOW_TRACKING_PASSWORD=secure_password

# Use token-based auth
export MLFLOW_TRACKING_TOKEN=mlflow_token_xyz

# Connect with authentication
mlflow.set_tracking_uri("https://mlflow.champion.internal")
os.environ["MLFLOW_TRACKING_USERNAME"] = "data_engineer"
os.environ["MLFLOW_TRACKING_PASSWORD"] = "password"
```

## Schema Management

### Avro → Parquet Type Mapping

| Avro Type                    | Parquet Physical Type | Parquet Logical Type      | Polars Type      | Notes                                          |
|------------------------------|----------------------|---------------------------|------------------|------------------------------------------------|
| `null`                       | -                    | -                         | `Null`           | No data stored                                 |
| `boolean`                    | `BOOLEAN`            | -                         | `Boolean`        | 1-bit values, bit-packed                       |
| `int`                        | `INT32`              | -                         | `Int32`          | Signed 32-bit integer                          |
| `long`                       | `INT64`              | -                         | `Int64`          | Signed 64-bit integer                          |
| `float`                      | `FLOAT`              | -                         | `Float32`        | Single-precision (32-bit)                      |
| `double`                     | `DOUBLE`             | -                         | `Float64`        | Double-precision (64-bit)                      |
| `string`                     | `BYTE_ARRAY`         | `STRING` (UTF8)           | `Utf8`           | Variable-length UTF-8 strings                  |
| `bytes`                      | `BYTE_ARRAY`         | -                         | `Binary`         | Raw binary data                                |
| `fixed`                      | `FIXED_LEN_BYTE_ARRAY` | -                       | `Binary`         | Fixed-length binary                            |
| `enum`                       | `BYTE_ARRAY`         | `ENUM`                    | `Utf8` / `Enum`  | String representation or categorical           |
| `array<T>`                   | `LIST`               | `LIST`                    | `List<T>`        | Repeated values with nesting                   |
| `map<K,V>`                   | `MAP`                | `MAP`                     | `Struct`         | Key-value pairs (Parquet MAP or repeated group)|
| `record` (struct)            | `GROUP`              | -                         | `Struct`         | Nested structure                               |
| `union` (nullable)           | Depends on type      | -                         | `Option<T>`      | Union with null → nullable column              |
| `union` (non-nullable)       | -                    | -                         | Not supported    | Use separate columns or JSON serialization     |
| `int` (logicalType: date)    | `INT32`              | `DATE`                    | `Date`           | Days since Unix epoch (1970-01-01)             |
| `long` (logicalType: timestamp-millis) | `INT64`    | `TIMESTAMP(millis, true)` | `Datetime`       | Milliseconds since Unix epoch (UTC)            |
| `long` (logicalType: timestamp-micros) | `INT64`    | `TIMESTAMP(micros, true)` | `Datetime`       | Microseconds since Unix epoch (UTC)            |
| `fixed[12]` (logicalType: duration) | `FIXED_LEN_BYTE_ARRAY` | `INTERVAL` | Not supported    | Use separate columns (months, days, millis)    |
| `bytes` (logicalType: decimal) | `BYTE_ARRAY`       | `DECIMAL(precision, scale)` | `Decimal`      | Arbitrary precision decimal                    |
| `fixed` (logicalType: decimal) | `FIXED_LEN_BYTE_ARRAY` | `DECIMAL(precision, scale)` | `Decimal`  | Fixed-length decimal representation            |

### Type Conversion Best Practices

1. **Nullable Handling**:

   ```python
   # Avro union [null, T] → Parquet nullable column
   # Polars automatically handles this
   df = pl.DataFrame({
       "optional_value": [1, None, 3],  # Int64 with nulls
   })
   ```

2. **Timestamp Precision**:

   ```python
   # Avro timestamp-millis (int64) → Polars datetime[ms]
   df = df.with_columns([
       pl.col("event_time").cast(pl.Datetime("ms"))
   ])
   ```

3. **Enums to Categorical**:

   ```python
   # Avro enum → Polars categorical for memory efficiency
   df = df.with_columns([
       pl.col("exchange").cast(pl.Categorical)
   ])
   ```

4. **Decimal Precision**:

   ```python
   # Avro decimal → Polars Decimal with explicit scale
   # Use Float64 if precision not critical
   df = df.with_columns([
       pl.col("price").cast(pl.Float64)  # or pl.Decimal(precision=18, scale=4)
   ])
   ```

### Schema Evolution Rules

#### Allowed Changes (Backward Compatible)

✅ **Add optional field with default**:

```json
// Before
{"name": "volume", "type": "long"}

// After - add new optional field
{"name": "volume_weighted_price", "type": ["null", "double"], "default": null}
```

✅ **Add new schema version**:

```json
// Create v2 schema alongside v1
raw_equity_ohlc_v1.avsc  → continues to exist
raw_equity_ohlc_v2.avsc  → new schema with additions
```

✅ **Widen field type** (with care):

```json
// int → long (safe, no data loss)
{"name": "volume", "type": "int"}   → {"name": "volume", "type": "long"}

// float → double (safe, precision increase)
{"name": "price", "type": "float"}  → {"name": "price", "type": "double"}
```

✅ **Add enum value** (for forward compatibility):

```json
// Before
{"name": "series", "type": {"type": "enum", "symbols": ["EQ", "BE"]}}

// After
{"name": "series", "type": {"type": "enum", "symbols": ["EQ", "BE", "SM", "ST"]}}
```

#### Forbidden Changes (Breaking)

❌ **Remove field** - breaks existing readers:

```json
// NEVER remove fields from published schemas
{"name": "deprecated_field", "type": "string"}  → // DELETION NOT ALLOWED
```

**Workaround**: Deprecate field in documentation, populate with default/null, remove in next major version.

❌ **Change field type** (incompatible):

```json
// string → int (data loss, parse errors)
{"name": "symbol", "type": "string"}  → {"name": "symbol", "type": "int"}  // FORBIDDEN
```

❌ **Change field meaning** - semantic breaking change:

```json
// Redefining "volume" from shares to rupees
{"name": "volume", "type": "long", "doc": "Number of shares"}
→ {"name": "volume", "type": "double", "doc": "Volume in INR"}  // FORBIDDEN
```

❌ **Remove enum value** - breaks existing data:

```json
{"symbols": ["EQ", "BE", "SM"]}  → {"symbols": ["EQ", "BE"]}  // FORBIDDEN - SM data exists
```

#### Schema Versioning Strategy

**Semantic Versioning for Schemas**:

- `v1.0.0` → Initial release
- `v1.1.0` → Backward-compatible additions (new optional fields)
- `v2.0.0` → Breaking changes (new topic, parallel schemas)

**Implementation**:

```python
# Schema version embedded in every event
{
  "event_id": "uuid-...",
  "schema_version": "v1.2.0",  # Explicit version tracking
  "payload": {...}
}
```

**Migration Path**:

```text
1. Publish new schema version (v2) alongside v1
2. Update producers to write v2 to NEW topic (raw.equity.ohlc.v2)
3. Consumers read from BOTH topics during transition (6-12 months)
4. Backfill historical data to v2 if needed
5. Deprecate v1 topic after transition period
6. Archive v1 data to cold storage
```

#### Schema Registry Integration (Future)

```yaml
# Confluent Schema Registry or AWS Glue Schema Registry
schema_registry:
  url: http://localhost:8081
  compatibility: BACKWARD  # Enforce backward compatibility
  auto_register: false     # Manual schema registration only
  validation: true         # Validate on produce/consume
```

### Data Quality Checks

#### Schema Validation on Ingest

```python
import polars as pl
import pyarrow.parquet as pq

def validate_schema(parquet_path: str, expected_schema: dict) -> bool:
    """Validate Parquet file schema matches expected Avro-derived schema"""
    actual_schema = pl.read_parquet_schema(parquet_path)
    
    for field_name, expected_type in expected_schema.items():
        if field_name not in actual_schema:
            raise ValueError(f"Missing field: {field_name}")
        if actual_schema[field_name] != expected_type:
            raise ValueError(f"Type mismatch for {field_name}: "
                           f"expected {expected_type}, got {actual_schema[field_name]}")
    return True
```

#### Null Handling Policy

| Field Type       | Null Policy         | Validation                                    |
|------------------|---------------------|-----------------------------------------------|
| Event ID         | NOT NULL            | Reject record if missing                      |
| Event Time       | NOT NULL            | Reject record if missing                      |
| Primary Key      | NOT NULL            | Reject record if missing (symbol, date)       |
| Price/Volume     | NULLABLE            | Allow nulls for non-trading days              |
| Optional Metrics | NULLABLE            | Default to null, document in schema           |

#### Type Coercion Rules

```python
# Safe coercions during Avro → Parquet conversion
coercion_rules = {
    "string_to_int": lambda x: int(x) if x and x.isdigit() else None,
    "string_to_float": lambda x: float(x) if x else None,
    "string_to_date": lambda x: pl.Datetime.strptime(x, "%Y-%m-%d") if x else None,
    "empty_string_to_null": lambda x: None if x == "" else x,
}

# Apply during transformation
df = df.with_columns([
    pl.col("volume_str").map_elements(coercion_rules["string_to_int"]).alias("volume")
])
```

## Security & Governance

### Access Control

#### ClickHouse User Roles

```sql
-- Create read-only role for analysts
CREATE ROLE analyst;
GRANT SELECT ON champion_market.* TO analyst;

-- Create read-write role for pipelines
CREATE ROLE pipeline_writer;
GRANT SELECT, INSERT ON champion_market.* TO pipeline_writer;

-- Create admin role
CREATE ROLE data_admin;
GRANT ALL ON champion_market.* TO data_admin;

-- Create users with roles
CREATE USER analyst_user IDENTIFIED BY 'secure_password';
GRANT analyst TO analyst_user;

CREATE USER pipeline_service IDENTIFIED BY 'service_token';
GRANT pipeline_writer TO pipeline_service;
```

#### Data Lake Permissions

```bash
# Set up file system permissions for data lake
chmod 750 /data/lake
chown -R pipeline-user:data-team /data/lake

# Raw data - read-only after write
chmod 440 /data/lake/raw/**/*.parquet

# Normalized/Features - writable by pipeline
chmod 660 /data/lake/normalized/**/*.parquet
chmod 660 /data/lake/features/**/*.parquet
```

### Secrets Management

**Prefect Blocks**:

```python
from prefect.blocks.system import Secret

# Store database credentials
Secret(value="clickhouse_password").save("clickhouse-password")
Secret(value="mlflow_tracking_token").save("mlflow-token")

# Use in flows
@task
def connect_database():
    password = Secret.load("clickhouse-password").get()
    client = get_client(host="localhost", password=password)
```

**Environment Variables** (local development):

```bash
export CLICKHOUSE_PASSWORD="secure_password"
export MLFLOW_TRACKING_URI="http://localhost:5000"
export AWS_ACCESS_KEY_ID="minio_key"
export AWS_SECRET_ACCESS_KEY="minio_secret"
```

**Vault Integration** (production, future):

```python
import hvac

# Connect to HashiCorp Vault
client = hvac.Client(url='https://vault.champion.internal')
client.token = os.environ['VAULT_TOKEN']

# Read secrets
db_creds = client.secrets.kv.v2.read_secret_version(path='database/clickhouse')
password = db_creds['data']['data']['password']
```

### Data Lineage

#### Lineage Tracking in MLflow

```python
def log_lineage(
    input_datasets: list[str],
    output_datasets: list[str],
    transformation: str
):
    """Log data lineage metadata"""
    
    with mlflow.start_run():
        # Log input/output datasets
        mlflow.log_param("input_datasets", ",".join(input_datasets))
        mlflow.log_param("output_datasets", ",".join(output_datasets))
        mlflow.log_param("transformation", transformation)
        
        # Log lineage graph as artifact
        lineage = {
            "inputs": input_datasets,
            "outputs": output_datasets,
            "transformation": transformation,
            "timestamp": datetime.now().isoformat()
        }
        mlflow.log_dict(lineage, "lineage.json")
```

#### Prefect Lineage via Task Dependencies

```python
@flow(name="lineage_tracked_pipeline")
def lineage_pipeline():
    """Pipeline with explicit lineage tracking"""
    
    # Lineage: NSE CSV → Raw Parquet
    raw_df = scrape_bhavcopy.submit()
    raw_path = write_parquet_raw.submit(raw_df, wait_for=[raw_df])
    
    # Lineage: Raw Parquet → Normalized Parquet
    normalized_df = normalize_equity_ohlc.submit(raw_df, wait_for=[raw_df])
    normalized_path = write_parquet_normalized.submit(normalized_df, wait_for=[normalized_df])
    
    # Lineage: Normalized Parquet → Features Parquet
    features_df = compute_features.submit(normalized_df, wait_for=[normalized_df])
    features_path = write_parquet_features.submit(features_df, wait_for=[features_df])
```

### Audit Logs

#### ClickHouse Query Logging

```sql
-- Enable query logging
SET log_queries = 1;
SET log_query_threads = 1;

-- Query audit table
SELECT
    event_time,
    user,
    query_id,
    query,
    type,
    databases,
    tables,
    query_duration_ms
FROM system.query_log
WHERE event_date = today()
AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 100;
```

#### Prefect Audit Trail

```python
# Query flow runs with filters
from prefect import get_client

async def audit_pipeline_runs(start_date: str, end_date: str):
    """Audit pipeline executions"""
    async with get_client() as client:
        flow_runs = await client.read_flow_runs(
            flow_filter=FlowFilter(name={"any_": ["daily_market_data_pipeline"]}),
            flow_run_filter=FlowRunFilter(
                start_time={"after_": datetime.fromisoformat(start_date)},
                end_time={"before_": datetime.fromisoformat(end_date)}
            )
        )
        
        for run in flow_runs:
            print(f"Run: {run.name}, State: {run.state}, Start: {run.start_time}")
```

### Data Retention & Archival

| Layer      | Retention Period | Archive Policy                              |
|------------|------------------|---------------------------------------------|
| Raw        | 5 years          | Archive to Glacier after 2 years            |
| Normalized | 3 years          | Archive to Glacier after 1 year             |
| Features   | 1 year           | Delete after 1 year (reproducible)          |
| ClickHouse | Per table TTL    | Raw: 5Y, Normalized: 3Y, Features: 1Y       |
| MLflow     | 2 years          | Archive old experiments, keep model registry|

#### Automated Archival Script

```bash
#!/bin/bash
# archive_old_data.sh

# Archive raw data older than 2 years to S3 Glacier
find /data/lake/raw -type f -mtime +730 -name "*.parquet" | \
  xargs -I {} aws s3 cp {} s3://champion-archive/raw/ --storage-class GLACIER

# Delete archived local files
find /data/lake/raw -type f -mtime +730 -name "*.parquet" -delete

# Archive normalized data older than 1 year
find /data/lake/normalized -type f -mtime +365 -name "*.parquet" | \
  xargs -I {} aws s3 cp {} s3://champion-archive/normalized/ --storage-class GLACIER

find /data/lake/normalized -type f -mtime +365 -name "*.parquet" -delete
```

## Performance Benchmarks & KPIs

### Target SLAs

| Metric                              | Target         | Measurement                           |
|-------------------------------------|----------------|---------------------------------------|
| Daily pipeline completion           | < 15 min       | End-to-end scrape → ClickHouse load   |
| Parquet write throughput            | > 100k rows/s  | Polars write_parquet() performance    |
| ClickHouse query P95 latency        | < 200 ms       | Symbol/date range queries             |
| ClickHouse query P99 latency        | < 500 ms       | Complex aggregations                  |
| MLflow UI response time             | < 2 sec        | Experiment list/run details           |
| Prefect task retry success rate     | > 95%          | Successful completion after retries   |
| Data freshness                      | < 2 hours      | Market close to ClickHouse availability|
| Data completeness                   | > 99.5%        | Non-null values in critical columns   |

### Performance Monitoring

```python
import time
from prefect import task

@task
def benchmark_parquet_write(df: pl.DataFrame, path: str):
    """Benchmark Parquet write performance"""
    start = time.time()
    df.write_parquet(path, compression="snappy")
    duration = time.time() - start
    
    throughput = len(df) / duration
    
    # Log to MLflow
    mlflow.log_metric("write_throughput_rows_per_sec", throughput)
    mlflow.log_metric("write_duration_sec", duration)
    mlflow.log_metric("file_size_mb", os.path.getsize(path) / 1024 / 1024)
    
    return throughput

@task
def benchmark_clickhouse_query(query: str):
    """Benchmark ClickHouse query performance"""
    client = get_client(host='localhost', database='champion_market')
    
    start = time.time()
    result = client.query(query)
    duration = time.time() - start
    
    mlflow.log_metric("query_duration_ms", duration * 1000)
    mlflow.log_metric("rows_returned", len(result.result_rows))
    
    return duration
```

### Optimization Guidelines

#### Polars Performance Tips

```python
# Use lazy evaluation for large datasets
lazy_df = pl.scan_parquet("data/lake/**/*.parquet") \
    .filter(pl.col("symbol") == "RELIANCE") \
    .select(["trade_date", "close", "volume"]) \
    .collect()  # Execute query plan

# Use streaming for massive datasets (> memory)
df = pl.scan_parquet("data/lake/**/*.parquet") \
    .collect(streaming=True)

# Optimize joins with sort
df1.sort("symbol").join(df2.sort("symbol"), on="symbol", how="inner")
```

#### ClickHouse Optimization

```sql
-- Use PREWHERE for early filtering (faster than WHERE)
SELECT * FROM normalized_equity_ohlc
PREWHERE trade_date = '2024-01-15'
WHERE volume > 1000000;

-- Enable parallel query execution
SET max_threads = 8;

-- Use sampling for exploratory queries
SELECT * FROM normalized_equity_ohlc
SAMPLE 0.1  -- 10% sample
WHERE trade_date >= '2024-01-01';
```

## Acceptance Criteria Checklist

### Documentation Completeness

✅ **Parquet Storage Layout**:

- [x] Directory structure documented with all layers (raw, normalized, features)
- [x] Partitioning strategy defined per dataset
- [x] File sizing guidance (128-256 MB target)
- [x] Compression and encoding recommendations
- [x] Metadata files explained (`_metadata`, `_common_metadata`)

✅ **ClickHouse Schema & DDL**:

- [x] Complete DDL provided for all tables (raw_equity_ohlc, normalized_equity_ohlc, features_equity_indicators, symbol_master)
- [x] Table engines explained (MergeTree, ReplacingMergeTree)
- [x] Sort keys and partition keys defined
- [x] Materialized views for aggregations
- [x] Ingestion methods documented (Parquet, HTTP, Python client)

✅ **Avro → Parquet Type Mapping**:

- [x] Comprehensive type mapping table
- [x] Logical type conversions (timestamp, date, decimal)
- [x] Nullable handling explained
- [x] Best practices for type coercion

✅ **Schema Evolution Rules**:

- [x] Allowed changes documented (add optional fields, widen types)
- [x] Forbidden changes documented (remove fields, change types)
- [x] Versioning strategy defined
- [x] Migration path outlined

✅ **Prefect Orchestration**:

- [x] Complete flow definitions with code examples
- [x] Task dependencies and flow graph visualized
- [x] Scheduling patterns (cron, interval)
- [x] Retry policies (task-level and flow-level)
- [x] Error handling and logging
- [x] Work queues and deployment configuration

✅ **MLflow Tracking**:

- [x] Tracking patterns for data pipelines, features, models
- [x] Parameter logging examples
- [x] Metric logging examples (data quality, performance)
- [x] Artifact management (Parquet samples, plots, manifests)
- [x] Model registry usage
- [x] Integration with Prefect

✅ **Architecture Diagrams**:

- [x] High-level architecture diagram
- [x] Data flow pipeline diagram
- [x] Parquet directory structure
- [x] ClickHouse architecture
- [x] Prefect orchestration flow graph

## Future Enhancements

### Short-term (Next 3 Months)

- Real-time ingestion via Kafka streaming
- Incremental Parquet updates with Hudi/Delta Lake
- Advanced ClickHouse materialized views for rollups
- dbt integration for SQL transformations
- Great Expectations for data quality validation

### Medium-term (6 Months)

- Multi-region data replication
- S3-based data lake with versioning
- Airflow migration or parallel orchestration
- Advanced feature store with Feast
- Real-time serving with Pinot

### Long-term (1 Year)

- Flink/Spark streaming for low-latency processing
- Distributed training with Ray/Dask
- Model serving with Seldon/KServe
- Data mesh architecture with domain-specific data products
- Compliance automation (GDPR, data retention policies)

## References & Resources

- [Polars Documentation](https://pola-rs.github.io/polars/)
- [Apache Parquet Format Specification](https://parquet.apache.org/docs/)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [Prefect Documentation](https://docs.prefect.io/)
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [Avro Specification](https://avro.apache.org/docs/current/spec.html)
- Champion schemas: `/schemas/` directory in this repository
