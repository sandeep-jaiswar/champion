"""Data storage layer for local file system.

Provides abstraction over storage operations (Parquet, CSV, etc).
All storage operations implement champion.core.DataSource/DataSink interfaces.

## Submodules

- `adapters.py`: Storage backend adapters (Parquet, CSV, etc)
- `parquet_io.py`: Parquet I/O utilities
- `retention.py`: Data retention and cleanup policies
"""

from .adapters import ParquetDataSource, ParquetDataSink, CSVDataSource, CSVDataSink
from .parquet_io import coalesce_small_files, generate_dataset_metadata, write_df
from .retention import calculate_partition_age, cleanup_old_partitions, get_dataset_statistics

__all__ = [
    # Adapters
    "ParquetDataSource",
    "ParquetDataSink",
    "CSVDataSource",
    "CSVDataSink",
    # Utilities
    "write_df",
    "coalesce_small_files",
    "generate_dataset_metadata",
    "cleanup_old_partitions",
    "calculate_partition_age",
    "get_dataset_statistics",
]
