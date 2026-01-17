"""Data warehouse layer for OLAP analysis.

Provides abstraction over warehouse backends (ClickHouse, Snowflake, etc).
All warehouse operations implement champion.core.DataSink interface.

## Submodules

- `adapters.py`: Warehouse backend adapters
- `clickhouse/`: ClickHouse-specific utilities and loaders
- `models/`: Data models for warehouse tables
"""

from .adapters import WarehouseSink, ClickHouseSink

__all__ = [
    "WarehouseSink",
    "ClickHouseSink",
]
