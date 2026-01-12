"""
ClickHouse Batch Loader Package

Utilities for loading Parquet data from the data lake into ClickHouse.
"""

from .batch_loader import ClickHouseLoader

__all__ = ['ClickHouseLoader']
