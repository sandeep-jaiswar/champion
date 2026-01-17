# Champion Architecture - Quick Reference

## File Structure

```
src/champion/
├── core/                          # Foundation (new)
│   ├── __init__.py               # Public API exports
│   ├── config.py                 # Unified AppConfig
│   ├── di.py                     # Dependency injection container
│   ├── errors.py                 # Exception hierarchy
│   ├── interfaces.py             # Abstract base classes
│   └── logging.py                # Structured logging
│
├── scrapers/                      # Data ingestion
│   ├── adapters.py               # EquityScraper, ReferenceDataScraper
│   ├── nse/
│   │   ├── bhavcopy.py           # NSE OHLC
│   │   ├── equity_list.py        # Symbol master
│   │   ├── corporate_actions.py
│   │   └── option_chain.py
│   └── bse/                       # BSE scrapers
│
├── storage/                       # File-based lake
│   ├── adapters.py               # ParquetDataSink, CSVDataSource, etc
│   ├── parquet_io.py             # I/O utilities
│   └── retention.py              # Cleanup policies
│
├── warehouse/                     # OLAP warehouse
│   ├── adapters.py               # WarehouseSink, ClickHouseSink
│   ├── clickhouse/
│   │   ├── batch_loader.py       # Merged from /warehouse/loader
│   │   └── models/               # DDL definitions
│   └── models/                    # Data models
│
├── validation/                    # Quality checks
│   ├── validator.py              # Main validator (merged from /validation)
│   └── demo.py
│
├── features/                      # Analytics & indicators
│   ├── indicators.py             # SMA, EMA, RSI, etc
│   ├── portfolio.py              # Portfolio metrics
│   └── risk.py                   # Risk calculations
│
├── corporate_actions/             # Dividend/split handling
│   ├── models.py
│   └── processor.py
│
├── orchestration/                 # Workflows
│   ├── config.py                 # (backward compat, redirects to core)
│   ├── flows/
│   │   ├── flows.py              # Main ETL flows
│   │   └── trading_calendar_flow.py
│   └── tasks/                    # Atomic Prefect tasks
│
├── utils/                         # Shared utilities
│   ├── logger.py                 # (use core.logging instead)
│   └── ...
│
├── __init__.py                    # Public API + docs
├── config.py                      # Backward compat re-exports
└── cli.py                         # Unified CLI commands
```

## Core Module Exports

### Configuration

```python
from champion.core import get_config, AppConfig, Environment

config = get_config()
if config.is_prod():
    ...
```

### Logging

```python
from champion.core import get_logger, configure_logging, get_request_id

logger = get_logger(__name__)
logger.info("Processing", request_id=get_request_id())
```

### Errors

```python
from champion.core import (
    ChampionError,
    ValidationError,
    DataError,
    IntegrationError,
    ConfigError,
)

try:
    data = validate(df)
except ValidationError as e:
    print(e.recovery_hint)
```

### Dependency Injection

```python
from champion.core import get_container, Container, ServiceLocator

container = get_container()
container.register(DataSource, lambda c: ParquetDataSource())
source = container.resolve(DataSource)
```

### Interfaces

```python
from champion.core import (
    DataSource,      # Read data
    DataSink,        # Write data
    Transformer,     # Process data
    Validator,       # Check quality
    Scraper,         # Extract data
    Repository,      # Data access
    CacheBackend,    # Caching
    Observer,        # Events
    DataContext,     # Metadata
)
```

## Domain Module Exports

### Scrapers

```python
from champion.scrapers import (
    EquityScraper,              # Abstract base
    ReferenceDataScraper,       # Abstract base
    ScraperWithRetry,           # Decorator
)
from champion.scrapers.nse import NSEBhavcopyScraper
```

### Storage

```python
from champion.storage import (
    ParquetDataSource,
    ParquetDataSink,
    CSVDataSource,
    CSVDataSink,
)
```

### Warehouse

```python
from champion.warehouse import (
    WarehouseSink,              # Abstract
    ClickHouseSink,             # Concrete
)
```

### Features

```python
from champion.features import (
    compute_sma,
    compute_ema,
    compute_rsi,
    compute_features,
)
```

### Validation

```python
from champion.validation import (
    validate_data,
    quarantine_failed_records,
)
```

## Common Patterns

### Pattern 1: Simple Data Processing

```python
from champion.core import get_logger, get_config
from champion.scrapers import EquityScraper
from champion.storage import ParquetDataSink

logger = get_logger(__name__)
config = get_config()

# Scrape
scraper = YourScraper()
data = scraper.scrape_date(date(2024, 1, 15))

# Store
sink = ParquetDataSink(config.storage.data_dir)
sink.connect()
sink.write(data, file_path="raw/equity.parquet")
sink.disconnect()

logger.info("Complete", rows=len(data))
```

### Pattern 2: Using Dependency Injection

```python
from champion.core import DataSink, get_logger

class DataProcessor:
    def __init__(self, sink: DataSink):
        self.sink = sink
        self.logger = get_logger(__name__)
    
    def process(self, data):
        result = self.transform(data)
        return self.sink.write(result)
    
    def transform(self, data):
        # Your logic
        return data

# Usage
from champion.warehouse import ClickHouseSink
processor = DataProcessor(ClickHouseSink())
processor.process(df)
```

### Pattern 3: Error Handling

```python
from champion.core import (
    get_logger,
    ValidationError,
    IntegrationError,
)

logger = get_logger(__name__)

try:
    data = validator.validate(df)
    if data.has_errors:
        raise ValidationError(
            "Data validation failed",
            recovery_hint="Check input format"
        )
    warehouse.write(data)
except ValidationError as e:
    logger.error("Validation", error=e.code, hint=e.recovery_hint)
except IntegrationError as e:
    if e.retryable:
        logger.warning("Retrying", service=e.service)
        # retry logic
    else:
        logger.error("Fatal", error=e.message)
```

### Pattern 4: Configuration

```python
from champion.core import get_config

config = get_config()

# Access nested configs
clickhouse_host = config.clickhouse.host
nse_url = config.nse.bhavcopy_url
storage_dir = config.storage.data_dir
log_level = config.observability.logging.level

# Environment checks
if config.is_prod():
    batch_size = 1000000
else:
    batch_size = 10000
```

### Pattern 5: Registering Custom Implementations

```python
from champion.core import get_container
from champion.core import DataSink

class CustomSink(DataSink):
    def connect(self): pass
    def write(self, data, **kwargs): pass
    def disconnect(self): pass

container = get_container()
container.register(DataSink, lambda c: CustomSink(), lifetime="singleton")

# Later
sink = container.resolve(DataSink)  # Gets your CustomSink
```

## Command Cheat Sheet

```bash
# Show configuration
poetry run champion show-config

# Run ETL flows
poetry run champion etl-ohlc                                    # Run for yesterday
poetry run champion etl-ohlc --start-date 2024-01-01 --end-date 2024-01-31
poetry run champion etl-macro
poetry run champion etl-index --index NIFTY50
poetry run champion etl-trading-calendar

# Warehouse operations
poetry run champion warehouse load --table raw_ohlc --source data/

# Testing
poetry run pytest tests/                                        # All tests
poetry run pytest tests/unit/                                  # Unit only
poetry run pytest tests/integration/                           # Integration only
poetry run pytest tests/ -v                                    # Verbose
poetry run pytest tests/ --cov=champion                        # With coverage

# Code quality
poetry run black .                                             # Format
poetry run ruff check .                                        # Lint
poetry run mypy src/                                           # Type check
```

## Debugging Tips

### 1. Check Configuration

```bash
poetry run champion show-config | grep -i clickhouse
```

### 2. Enable Debug Logging

```bash
LOG_LEVEL=DEBUG poetry run champion etl-ohlc
```

### 3. List Available Services

```python
from champion.core import get_container
container = get_container()
print(container._services.keys())
```

### 4. Test Data Source

```python
from champion.storage import ParquetDataSource
source = ParquetDataSource("data/raw")
source.connect()
df = source.read("equity_ohlc.parquet")
print(df.shape)
```

### 5. Check Imports

```bash
cd src && python -c "import champion; print(champion.__version__)"
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError` | Check import path, use `from champion.core import ...` |
| `ConfigError` | Check `.env` file, verify env variables |
| `ValidationError` | Check data format against schema |
| `IntegrationError` | Check ClickHouse/Kafka connectivity |
| `Cannot resolve service` | Register in DI container first |
| `Circular import` | Use `from champion.core import Interface` not concrete class |

## Performance Tips

1. **Use Polars** for data processing (vectorized, zero-copy)
2. **Batch operations** when writing to warehouse
3. **Stream large files** using `read_batch()`
4. **Enable compression** in ParquetDataSink
5. **Use connection pooling** for ClickHouse
6. **Cache reference data** with CacheBackend

## File Locations

| What | Where |
|------|-------|
| Documentation | `docs/ARCHITECTURE.md`, `docs/MIGRATION.md` |
| Tests | `tests/unit/`, `tests/integration/` |
| Fixtures | `tests/conftest.py` |
| Configuration template | `.env` (create from scratch) |
| Raw data | `data/raw/` |
| Processed data | `data/lake/` |
| Logs | `logs/` |
| ML tracking | `mlruns/` |
| Schemas | `schemas/parquet/`, `schemas/json/` |

## Key Documents

- 📖 **ARCHITECTURE.md** - Complete architecture guide
- 🔄 **MIGRATION.md** - Step-by-step migration guide
- 📋 **ARCHITECTURE_TRANSFORMATION.md** - What was done
- 🔧 **This file** - Quick reference

---

**Champion: From Fragmented to Unified Architecture** ✨

*Built on clean architecture principles for maintainability, scalability, and developer experience*
