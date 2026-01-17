"""Champion - Stock Market Intelligence Platform.

A production-grade data platform for market analytics built on clean architecture principles:

## Architecture Layers

1. **Core** (`champion.core`)
   - Configuration management
   - Dependency injection
   - Logging and observability
   - Error handling
   - Abstract interfaces

2. **Data Ingestion** (`champion.scrapers`)
   - NSE/BSE equity data scrapers
   - Option chain scraping
   - Reference data (symbols, corporate actions)

3. **Storage** (`champion.storage`)
   - File-based storage (Parquet, CSV)
   - Data lake management

4. **Warehouse** (`champion.warehouse`)
   - ClickHouse integration
   - Batch loading
   - Data modeling

5. **Features** (`champion.features`)
   - Technical indicators
   - Portfolio analytics
   - Risk metrics

6. **Validation** (`champion.validation`)
   - Data quality checks
   - Schema validation
   - Anomaly detection

7. **Orchestration** (`champion.orchestration`)
   - Prefect-based workflows
   - Composable data flows

8. **CLI** (`champion.cli`)
   - Unified command-line interface
   - Interactive tools

## Quick Start

```python
from champion.core import get_config, get_logger
from champion.scrapers import NSEEquityScraper
from champion.storage import ParquetDataSink

# Get configuration
config = get_config()
logger = get_logger(__name__)

# Scrape data
scraper = NSEEquityScraper()
data = scraper.scrape_date("2024-01-15")

# Store data
sink = ParquetDataSink(config.storage.data_dir / "raw")
sink.connect()
sink.write(data, file_path="equity_ohlc_20240115.parquet")
sink.disconnect()
```

## Key Principles

- **Loose Coupling**: Interfaces and adapters decouple domains
- **Testability**: Mock implementations for all abstractions
- **Observable**: Structured logging with request tracing
- **Scalable**: Plugin architecture for features
- **Maintainable**: Clear separation of concerns
- **Extensible**: Easy to add new data sources or sinks
"""

__version__ = "1.0.0"

# Core exports
from .core import (
    AppConfig,
    ChampionError,
    DataError,
    Environment,
    IntegrationError,
    ValidationError,
    configure_logging,
    get_config,
    get_container,
    get_logger,
    get_request_id,
    reload_config,
    set_request_id,
)

# Domain exports (wrapped to handle incomplete implementations)
try:
    from .scrapers import *  # noqa
except (ImportError, AttributeError):
    pass

try:
    from .storage import *  # noqa
except (ImportError, AttributeError):
    pass

try:
    from .warehouse import *  # noqa
except (ImportError, AttributeError):
    pass

try:
    from .validation import *  # noqa
except (ImportError, AttributeError):
    pass

try:
    from .features import *  # noqa
except (ImportError, AttributeError):
    pass

try:
    from .orchestration import *  # noqa
except (ImportError, AttributeError):
    pass

__all__ = [
    # Version
    "__version__",
    # Core
    "AppConfig",
    "Environment",
    "get_config",
    "reload_config",
    "get_container",
    "get_logger",
    "configure_logging",
    "get_request_id",
    "set_request_id",
    # Errors
    "ChampionError",
    "ValidationError",
    "DataError",
    "IntegrationError",
]
