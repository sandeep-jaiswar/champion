# Architecture Migration Guide

## Overview

This guide helps developers understand and migrate to the new unified Champion architecture. The refactoring consolidates scattered modules into a clean, loosely-coupled codebase.

## What Changed

### Before: Fragmented Structure

```
champion/
├── src/champion/           # Main package (83 files)
│   ├── scrapers/           # Scrapers (tightly coupled)
│   ├── storage/            # Storage (standalone)
│   ├── warehouse/          # Warehouse (isolated)
│   ├── features/           # Features (mixed dependencies)
│   ├── config.py           # One config (incomplete)
│   └── cli.py              # CLI (incomplete)
├── warehouse/loader/       # Separate package (5 files)
│   ├── batch_loader.py     # Duplicate logic
│   └── tests/
└── validation/             # Separate package (4 files)
    ├── validator.py        # Isolated validation
    └── tests/
```

**Problems**:

- ❌ No clear contracts between domains
- ❌ Duplicate code (batch loaders in 2 places)
- ❌ Hard to test (no dependency injection)
- ❌ Circular imports
- ❌ No centralized configuration
- ❌ Mixed responsibilities

### After: Clean Architecture

```
champion/
├── src/champion/
│   ├── core/               # NEW: Foundation layer
│   │   ├── config.py       # Unified configuration
│   │   ├── di.py           # Dependency injection
│   │   ├── errors.py       # Error hierarchy
│   │   ├── interfaces.py   # Abstract contracts
│   │   └── logging.py      # Structured logging
│   ├── scrapers/           # Ingestion layer
│   │   ├── adapters.py     # Base classes
│   │   └── nse/, bse/      # Implementations
│   ├── storage/            # Storage adapters
│   │   ├── adapters.py     # Parquet, CSV
│   │   └── utilities/
│   ├── warehouse/          # Warehouse adapters
│   │   ├── adapters.py     # Abstract + ClickHouse
│   │   └── clickhouse/
│   ├── validation/         # Quality checks (merged)
│   ├── features/           # Analytics (updated)
│   ├── orchestration/      # Workflows
│   └── cli.py              # Unified CLI
└── tests/                  # NEW: Centralized tests
    ├── conftest.py         # Shared fixtures
    ├── unit/
    └── integration/
```

**Benefits**:

- ✅ Clear contracts (interfaces)
- ✅ Loose coupling (adapters)
- ✅ Easy testing (dependency injection)
- ✅ Centralized config (single source of truth)
- ✅ Reusable components (core library)
- ✅ Observable (structured logging)

## Migration Checklist

### Phase 1: Understanding the New Structure

- [ ] Read [ARCHITECTURE.md](./ARCHITECTURE.md)
- [ ] Review `champion/core` module documentation
- [ ] Understand domain layers and adapters
- [ ] Run tests to see examples: `poetry run pytest tests/`

### Phase 2: Update Your Imports

Replace old imports with new ones:

#### Configuration

**Before**:

```python
from champion.config import config
from champion.orchestration.config import Config, StorageConfig
```

**After**:

```python
from champion.core import get_config, AppConfig

config = get_config()
# or import specific config classes from champion.core
```

#### Logging

**Before**:

```python
import logging
logger = logging.getLogger(__name__)
logger.info(f"Starting ETL for {date}")  # Plain strings
```

**After**:

```python
from champion.core import get_logger

logger = get_logger(__name__)
logger.info("Starting ETL", date=date)  # Structured fields
```

#### Error Handling

**Before**:

```python
try:
    result = scraper.scrape()
except Exception as e:
    print(f"Error: {e}")
```

**After**:

```python
from champion.core import ValidationError, IntegrationError

try:
    result = scraper.scrape()
except ValidationError as e:
    logger.error("Validation failed", error=e.code, recovery=e.recovery_hint)
except IntegrationError as e:
    if e.retryable:
        # Retry logic
        pass
```

#### Data Sources/Sinks

**Before**:

```python
from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

loader = ClickHouseLoader(host="localhost", port=9000)
loader.load_parquet_files(table="raw_ohlc", source_path="data/")
```

**After**:

```python
from champion.warehouse import ClickHouseSink
from champion.core import get_config

sink = ClickHouseSink(**get_config().clickhouse.model_dump())
sink.connect()
sink.write(df, table_name="raw_ohlc")
```

### Phase 3: Implement Adapters for New Features

When adding new data sources/sinks:

**Pattern**: Extend base adapter class

```python
# NEW: champion/scrapers/adapters.py (already exists)
from champion.core import Scraper, DataContext

class EquityScraper(Scraper):
    """Base class for all equity scrapers"""
    
    def scrape(self, **kwargs) -> pl.DataFrame:
        # Your implementation
        pass
    
    def validate_scrape(self, data: pl.DataFrame) -> bool:
        # Validation logic
        pass

# IMPLEMENT: champion/scrapers/nse/your_scraper.py
class YourScraper(EquityScraper):
    def scrape(self, **kwargs) -> pl.DataFrame:
        # Implementation
        pass
```

### Phase 4: Update Tests

**Before**:

```python
import unittest
from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

class TestLoader(unittest.TestCase):
    def test_load(self):
        loader = ClickHouseLoader()  # No mocking
        result = loader.load_parquet_files(...)
```

**After**:

```python
from champion.warehouse import ClickHouseSink
from champion.core import DataSink
from unittest.mock import Mock

def test_load(mock_clickhouse_sink: DataSink):
    """Using pytest fixture with mock"""
    sink = mock_clickhouse_sink
    
    stats = sink.write(sample_df, table_name="test_table")
    assert stats["rows_written"] > 0
```

### Phase 5: Use Dependency Injection

**Before**:

```python
class NSEBhavcopyScraper:
    def __init__(self):
        self.storage = FileStorage()  # Hard dependency
        self.warehouse = ClickHouseLoader()  # Hard dependency
        self.logger = logging.getLogger(__name__)  # Hard dependency
    
    def scrape(self):
        # Can't swap implementations for testing
        self.storage.write(data)
        self.warehouse.load(data)
```

**After**:

```python
from champion.core import DataSink, get_logger, get_container

class NSEBhavcopyScraper:
    def __init__(self, sink: DataSink):
        self.sink = sink  # Injected
        self.logger = get_logger(__name__)  # Structured logger
    
    def scrape(self):
        # Works with any DataSink implementation
        stats = self.sink.write(data)

# Usage
from champion.warehouse import ClickHouseSink

scraper = NSEBhavcopyScraper(ClickHouseSink())

# Testing
from unittest.mock import Mock
mock_sink = Mock(spec=DataSink)
scraper = NSEBhavcopyScraper(mock_sink)
```

## Common Migration Scenarios

### Scenario 1: Adding a New Data Source

**Task**: Add support for scraping from a new exchange

**Steps**:

1. **Define the interface** (already done in `core/interfaces.py`)
2. **Create adapter** in new domain:

   ```python
   # champion/scrapers/new_exchange/__init__.py
   from champion.scrapers import EquityScraper
   
   class NewExchangeScraper(EquityScraper):
       def scrape_date(self, trade_date: date) -> pl.DataFrame:
           # Your implementation
           pass
   ```

3. **Register in container** (if using DI):

   ```python
   from champion.core import get_container
   
   container = get_container()
   container.register(
       EquityScraper,
       lambda c: NewExchangeScraper()
   )
   ```

4. **Add CLI command**:

   ```python
   # champion/cli.py
   @app.command("etl-new-exchange")
   def etl_new_exchange(...):
       scraper = NewExchangeScraper()
       data = scraper.scrape()
       # ...
   ```

### Scenario 2: Adding a New Validator

**Task**: Add custom data quality checks

**Steps**:

1. **Extend Validator interface**:

   ```python
   # champion/validation/custom_validator.py
   from champion.core import Validator
   
   class CustomValidator(Validator):
       def validate(self, data: pl.DataFrame, **kwargs) -> dict:
           errors = []
           # Your validation logic
           return {
               "valid_rows": len(valid),
               "invalid_rows": len(errors),
               "errors": errors,
           }
   ```

2. **Use in workflows**:

   ```python
   validator = CustomValidator()
   result = validator.validate(df)
   if result["invalid_rows"] > 0:
       logger.warning("Validation issues", errors=result["errors"])
   ```

### Scenario 3: Swapping Storage Backend

**Task**: Use S3 instead of local Parquet

**Steps**:

1. **Create S3 adapter**:

   ```python
   # champion/storage/s3_adapter.py
   from champion.core import DataSink
   
   class S3DataSink(DataSink):
       def __init__(self, bucket: str):
           self.bucket = bucket
       
       def write(self, data: pl.DataFrame, **kwargs):
           # Use boto3 to write to S3
           pass
   ```

2. **Update configuration**:

   ```bash
   STORAGE_BACKEND=s3
   S3_BUCKET=my-bucket
   ```

3. **Inject in code**:

   ```python
   from champion.core import get_container, DataSink
   
   container = get_container()
   sink = container.resolve(DataSink)  # Gets S3DataSink based on config
   ```

### Scenario 4: Environment-Specific Behavior

**Task**: Use different warehouse in dev vs prod

**Steps**:

1. **Check environment in code**:

   ```python
   from champion.core import get_config
   
   config = get_config()
   if config.is_prod():
       # Production settings
       batch_size = 1000000
   else:
       # Development settings
       batch_size = 10000
   ```

2. **Or use configuration**:

   ```python
   # .env.prod
   CLICKHOUSE_HOST=warehouse.prod.internal
   WAREHOUSE_BATCH_SIZE=1000000
   
   # .env.dev
   CLICKHOUSE_HOST=localhost
   WAREHOUSE_BATCH_SIZE=10000
   ```

## Backward Compatibility

The old APIs are still available through re-exports for gradual migration:

```python
# These still work (deprecated but functional)
from champion.config import config
from champion.orchestration.config import Config

# But use these instead (new way)
from champion.core import get_config, AppConfig
```

## Troubleshooting

### Issue: "Module not found"

```
ModuleNotFoundError: No module named 'warehouse.loader'
```

**Solution**: Update import

```python
# Old (broken)
from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

# New (works)
from champion.warehouse import ClickHouseSink
```

### Issue: "Circular import"

```
ImportError: cannot import name 'X' from partially initialized module 'champion.Y'
```

**Solution**: Use interfaces instead of concrete classes

```python
# Bad (creates cycle)
from champion.scrapers.nse.bhavcopy import NSEBhavcopyScraper

# Good (depends on interface)
from champion.core import Scraper  # Abstract type
```

### Issue: "Config not loaded"

```
AttributeError: 'NoneType' object has no attribute 'host'
```

**Solution**: Ensure config is initialized

```python
from champion.core import get_config

config = get_config()  # Call this function, don't import
print(config.clickhouse.host)
```

### Issue: "Dependency not registered"

```
ResolutionError: Service DataSource not registered
```

**Solution**: Register the dependency

```python
from champion.core import get_container
from champion.storage import ParquetDataSource

container = get_container()
container.register(DataSource, lambda c: ParquetDataSource("data/"))

source = container.resolve(DataSource)
```

## Performance Impact

The new architecture has **no performance penalty**:

- ✅ Interfaces are zero-cost abstractions
- ✅ Dependency injection is compile-time or startup-time
- ✅ Structured logging adds minimal overhead
- ✅ Polars operations unchanged

In fact, you'll see **improvements**:

- 📈 Better caching due to unified configuration
- 📈 Parallel execution in Prefect workflows
- 📈 Memory efficiency through adapters

## Questions & Support

For migration help:

1. Check [ARCHITECTURE.md](./ARCHITECTURE.md)
2. Look at test examples in `tests/`
3. Review adapter implementations in each domain
4. Check domain-specific README files

---

**Last Updated**: January 17, 2026
**Version**: 1.0
