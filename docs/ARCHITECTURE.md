# Champion Platform Architecture

## Overview

Champion is a production-grade data platform for stock market analytics built on **clean architecture principles**. The codebase is organized into loosely-coupled domains that can evolve independently while maintaining consistency through well-defined interfaces.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI Layer                             │
│          (champion.cli - User-facing commands)              │
└─────────────────────────────────────────────────────────────┘
                            ▲
                            │
┌─────────────────────────────────────────────────────────────┐
│                    Orchestration Layer                       │
│     (champion.orchestration - Prefect workflows & flows)    │
└─────────────────────────────────────────────────────────────┘
                            ▲
                ┌───────────┼───────────┐
                ▼           ▼           ▼
┌──────────────────────────────────────────────────────────────┐
│               Domain Layers (Use Interfaces)                 │
├──────────────────────────────────────────────────────────────┤
│ • Scrapers        (External → Internal Data)                │
│ • Storage         (Parquet, CSV, etc.)                      │
│ • Warehouse       (ClickHouse OLAP)                         │
│ • Validation      (Data Quality)                            │
│ • Features        (Analytics & Indicators)                  │
│ • Corporate Actn  (Dividends, Splits, etc.)                │
└──────────────────────────────────────────────────────────────┘
                            ▲
┌─────────────────────────────────────────────────────────────┐
│                    Core Layer (Interfaces)                   │
│  (champion.core - Contracts for all domains)               │
└─────────────────────────────────────────────────────────────┘
```

## Core Module (`champion.core`)

Provides foundational infrastructure for all domains:

### 1. **Configuration** (`core/config.py`)
- Centralized Pydantic-based configuration
- Environment-specific settings (dev/staging/prod)
- Configuration hierarchy: Env vars → .env → defaults
- Sub-configs for each service (NSE, BSE, Kafka, ClickHouse, etc.)

```python
from champion.core import get_config
config = get_config()
print(config.nse.bhavcopy_url)
print(config.clickhouse.host)
```

### 2. **Dependency Injection** (`core/di.py`)
- IoC container for service registration
- Service locator pattern for legacy code
- Lifetime management (transient, singleton)
- Enables loose coupling and testability

```python
from champion.core import get_container
container = get_container()
container.register(DataSource, lambda c: FileDataSource())
scraper = container.resolve(DataSource)
```

### 3. **Interfaces** (`core/interfaces.py`)
Abstract base classes defining contracts for all domains:

| Interface | Purpose | Implementations |
|-----------|---------|-----------------|
| `DataSource` | Read data from any source | CSVDataSource, HTTPDataSource, KafkaDataSource |
| `DataSink` | Write data to any destination | ParquetDataSink, ClickHouseSink, S3DataSink |
| `Transformer` | Transform/process data | IndicatorComputer, Normalizer, Aggregator |
| `Validator` | Validate data quality | JSONSchemaValidator, BusinessLogicValidator |
| `Scraper` | Scrape external data | NSEEquityScraper, OptionChainScraper |
| `Repository` | Data access abstraction | SQLRepository, ElasticsearchRepository |
| `CacheBackend` | Caching layer | RedisCache, MemoryCache, NoCache |

### 4. **Error Handling** (`core/errors.py`)
Domain-specific exception hierarchy with recovery hints:

```python
from champion.core import ValidationError, IntegrationError

# Validation failure (not retryable)
raise ValidationError(
    "Invalid OHLC data",
    recovery_hint="Check source data format"
)

# External service failure (retryable)
raise IntegrationError(
    service="ClickHouse",
    message="Connection timeout",
    retryable=True
)
```

### 5. **Logging** (`core/logging.py`)
Structured logging with request tracing:

```python
from champion.core import get_logger, get_request_id

logger = get_logger(__name__)
logger.info("Processing data", request_id=get_request_id())
```

## Domain Modules

### Data Ingestion (`champion.scrapers`)

**Purpose**: Extract data from NSE, BSE, and other sources.

**Structure**:
```
scrapers/
├── adapters.py           # EquityScraper, ReferenceDataScraper base classes
├── nse/
│   ├── bhavcopy.py       # NSE OHLC scraper
│   ├── equity_list.py    # Symbol master scraper
│   ├── option_chain.py   # Option chain scraper
│   └── ...
└── bse/
    ├── bhavcopy.py       # BSE OHLC scraper
    └── ...
```

**Key Classes**:
- `EquityScraper`: Abstract adapter for equity data
- `ReferenceDataScraper`: Abstract adapter for reference data
- `ScraperWithRetry`: Decorator adding retry logic

**Usage**:
```python
from champion.scrapers.nse import NSEBhavcopyScraper
from champion.core import get_config

scraper = NSEBhavcopyScraper(config=get_config().nse)
df = scraper.scrape_date(date(2024, 1, 15))
```

### Storage (`champion.storage`)

**Purpose**: Manage file-based data lake (Parquet, CSV).

**Adapters**:
- `ParquetDataSource` / `ParquetDataSink`
- `CSVDataSource` / `CSVDataSink`

**Utilities**:
- `write_df()`: High-level write function
- `coalesce_small_files()`: Merge small files
- `cleanup_old_partitions()`: Data retention
- `generate_dataset_metadata()`: Schema tracking

**Usage**:
```python
from champion.storage import ParquetDataSink
from champion.core import get_config

sink = ParquetDataSink(get_config().storage.data_dir)
sink.connect()
sink.write(df, file_path="raw/equity_ohlc.parquet")
```

### Warehouse (`champion.warehouse`)

**Purpose**: Load data into ClickHouse for OLAP analysis.

**Adapters**:
- `WarehouseSink`: Abstract warehouse interface
- `ClickHouseSink`: ClickHouse implementation

**Sub-modules**:
- `clickhouse/`: ClickHouse-specific utilities
- `models/`: DDL and data model definitions

**Usage**:
```python
from champion.warehouse import ClickHouseSink
from champion.core import get_config

sink = ClickHouseSink(**get_config().clickhouse.model_dump())
sink.connect()
sink.write(df, table_name="raw_equity_ohlc")
```

### Validation (`champion.validation`)

**Purpose**: Ensure data quality through comprehensive validation.

**Features**:
- JSON schema validation
- Business logic validation (OHLC consistency)
- Quarantine failed records
- Streaming for memory efficiency

**Usage**:
```python
from champion.validation import ParquetValidator
from pathlib import Path

validator = ParquetValidator(schema_dir=Path("schemas"))
result = validator.validate_file(
    Path("data/equity_ohlc.parquet"),
    schema_name="raw_equity_ohlc"
)
```

### Features (`champion.features`)

**Purpose**: Transform raw data into analytics features.

**Sub-modules**:
- `indicators.py`: SMA, EMA, RSI, MACD, etc.
- `portfolio.py`: Portfolio-level metrics
- `risk.py`: VaR, Sharpe ratio, correlation

**Usage**:
```python
from champion.features import compute_indicators

df_with_features = compute_indicators(
    df,
    indicators=["sma_20", "ema_50", "rsi_14"]
)
```

### Corporate Actions (`champion.corporate_actions`)

**Purpose**: Handle dividends, splits, bonus shares.

**Features**:
- Adjustment factor calculation
- Price adjustment
- Event tracking

### Orchestration (`champion.orchestration`)

**Purpose**: Compose reusable workflows using Prefect.

**Structure**:
```
orchestration/
├── config.py              # Flow-specific configuration
├── flows/                 # Reusable Prefect flows
│   ├── flows.py           # Main ETL flows
│   └── ...
└── tasks/                 # Atomic Prefect tasks
    ├── scrape_nse.py
    ├── validate_data.py
    └── ...
```

**Key Flows**:
- `nse_bhavcopy_etl_flow`: NSE OHLC ingestion
- `macro_indicators_flow`: Macro economic data
- `index_constituent_etl_flow`: Index constituents

**Usage**:
```python
from champion.orchestration.flows import nse_bhavcopy_etl_flow

result = nse_bhavcopy_etl_flow(
    trade_date=date(2024, 1, 15),
    load_to_clickhouse=True
)
```

### CLI (`champion.cli`)

**Purpose**: User-facing command-line interface.

**Command Structure**:
```
champion
├── etl-index           # Index ETL
├── etl-macro           # Macro ETL
├── etl-ohlc            # Equity OHLC ETL
├── etl-option-chain    # Option chain ETL
├── etl-trading-calendar # Calendar ETL
├── warehouse           # Warehouse operations
└── show-config         # Display configuration
```

**Usage**:
```bash
# Run equity ETL for yesterday
poetry run champion etl-ohlc

# Run for specific date range
poetry run champion etl-ohlc --start-date 2024-01-01 --end-date 2024-01-31

# Load to warehouse
poetry run champion etl-ohlc --load-to-clickhouse
```

## Data Flow

```
External Data Sources (NSE, BSE)
        ▼
    Scrapers (Extract)
        ▼
    Validation (Verify)
        ▼
    Storage/Lake (Parquet)
        ▼
    Features (Transform)
        ▼
    Warehouse (ClickHouse)
        ▼
    Analytics & Insights
```

## Design Patterns

### 1. **Adapter Pattern**
Decouple domains from specific implementations:
```python
from champion.core import DataSource
from champion.storage import ParquetDataSource

source = ParquetDataSource("data/lake")  # Can swap with HTTPDataSource, S3DataSource, etc.
df = source.read()
```

### 2. **Dependency Injection**
Make components testable and configurable:
```python
container = get_container()
container.register(DataSink, lambda c: MockDataSink())  # For testing
loader = SomeLoader()  # Uses injected sink
```

### 3. **Strategy Pattern**
Different compression/storage strategies:
```python
sink = ParquetDataSink(
    base_path="data",
    compression="snappy"  # Can be "gzip", "lz4", "zstd"
)
```

### 4. **Observer Pattern**
Track operation progress:
```python
observer = MetricsObserver()
observer.on_start(context)
# ... process ...
observer.on_success(result)
```

### 5. **Repository Pattern**
Abstract data access layer:
```python
from champion.core import Repository

class EquityRepository(Repository[Equity]):
    def find_by_symbol(self, symbol: str) -> Equity:
        pass
```

## Coupling Reduction

### Before (Tightly Coupled)
```python
# scraper/nse.py
from warehouse.loader.batch_loader import ClickHouseLoader

class NSEScraper:
    def __init__(self):
        self.loader = ClickHouseLoader()  # Hard dependency!
    
    def scrape(self):
        data = self.fetch_data()
        self.loader.load(data)  # Tightly coupled to specific loader
```

### After (Loosely Coupled)
```python
# scrapers/nse/bhavcopy.py
from champion.core import DataSink, get_logger

class NSEBhavcopyScraper:
    def __init__(self, sink: DataSink):
        self.sink = sink  # Injected dependency
        self.logger = get_logger(__name__)
    
    def scrape(self):
        data = self.fetch_data()
        self.sink.write(data)  # Works with ANY DataSink implementation
```

## Configuration Management

### Hierarchy
1. **Environment Variables** (highest priority)
2. **.env file** (shared defaults)
3. **.env.{environment}** (environment-specific)
4. **Built-in defaults** (lowest priority)

### Usage
```bash
# Development (local)
ENVIRONMENT=dev \
CLICKHOUSE_HOST=localhost \
poetry run champion show-config

# Production (remote)
ENVIRONMENT=prod \
CLICKHOUSE_HOST=analytics.prod.internal \
CLICKHOUSE_PORT=9000 \
poetry run champion etl-ohlc
```

## Testing Strategy

### Unit Tests
- Mock all external dependencies
- Test interface implementations
- Test business logic in isolation

```python
def test_equity_scraper():
    mock_source = MockDataSource()
    mock_sink = MockDataSink()
    scraper = EquityScraper(mock_source, mock_sink)
    
    result = scraper.scrape()
    assert result.success
```

### Integration Tests
- Test across domain boundaries
- Use test containers (Docker)
- Validate real data flows

```python
def test_end_to_end_etl():
    df = scraper.scrape_date(date(2024, 1, 15))
    assert not df.is_empty()
    
    sink.write(df)
    
    result = warehouse.query(f"SELECT * FROM raw_ohlc")
    assert result.row_count() == df.shape[0]
```

### Fixtures & Factories
- Shared test data and factories
- Located in `tests/conftest.py`

```python
from tests.fixtures import sample_ohlc_data

def test_indicators(sample_ohlc_data):
    result = compute_features(sample_ohlc_data)
    assert "sma_20" in result.columns
```

## Performance Considerations

### Memory Efficiency
- Use Polars for zero-copy operations
- Stream large datasets in batches
- Implement cleanup policies

### Parallelization
- Prefect flows support parallel task execution
- Batch operations leverage vectorization

### Caching
- Pluggable cache backends
- TTL support for reference data

## Migration Path

### For Existing Code
Use `core` module re-exports for backward compatibility:

```python
# Old import (deprecated but still works)
from champion.config import config

# New import (recommended)
from champion.core import get_config
config = get_config()
```

### For New Features
Always use new structured approach:

```python
from champion.core import get_logger, DataSink, get_container

logger = get_logger(__name__)
container = get_container()
```

## Best Practices

1. **Always use interfaces**: Depend on abstractions, not implementations
2. **Inject dependencies**: Pass sinks/sources to constructors
3. **Use structured logging**: Include context in all log messages
4. **Handle errors explicitly**: Catch ChampionError subclasses
5. **Test with mocks**: Use dependency injection for testability
6. **Document contracts**: Use docstrings for interface expectations
7. **Version your data models**: Track schema changes
8. **Monitor observables**: Log metrics, errors, and timings

## Future Extensions

Planned architecture improvements:

- [ ] **Plugin System**: Load features dynamically
- [ ] **Feature Store**: Persistent feature management
- [ ] **ML Pipeline**: Integrated model training/serving
- [ ] **Streaming**: Kafka integration for real-time
- [ ] **Multi-Warehouse**: Support Snowflake, BigQuery
- [ ] **Distributed Tracing**: OpenTelemetry integration
- [ ] **GraphQL API**: Data access via GraphQL
- [ ] **Web UI**: Dashboard for monitoring ETLs

---

**Last Updated**: January 17, 2026
**Maintainer**: Champion Team
