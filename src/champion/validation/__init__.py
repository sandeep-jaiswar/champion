"""Data quality validation layer.

Provides comprehensive data validation capabilities:
- JSON schema validation against Parquet files
- Business logic validation (OHLC consistency, etc)
- Quarantine functionality for failed records
- Prefect flow integration with alerts

All validators implement champion.core.Validator interface.

## Key Features

- Memory-efficient streaming validation for large datasets
- Schema versioning support
- Detailed error reporting
- Custom validation rule support
- Integration with orchestration pipelines
"""

from .validator import ParquetValidator

__version__ = "1.0.0"

__all__ = [
    "ParquetValidator",
]
