"""Warehouse domain adapters.

Abstracts warehouse operations (ClickHouse, etc.) behind a consistent interface.
Enables easy switching between different warehouse backends.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any

import polars as pl

from champion.core import DataError, DataSink, IntegrationError, get_logger

logger = get_logger(__name__)


class WarehouseSink(DataSink):
    """Base adapter for writing data to warehouse.

    Implementations: ClickHouseSink, SnowflakeSink, BigQuerySink
    """

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in warehouse."""

    @abstractmethod
    def create_table_if_not_exists(self, table_name: str, schema: dict) -> None:
        """Create table with given schema."""

    @abstractmethod
    def drop_partition(self, table_name: str, partition_spec: dict) -> int:
        """Drop partition(s) and return row count deleted."""

    @abstractmethod
    def get_row_count(self, table_name: str) -> int:
        """Get total row count in table."""

    @abstractmethod
    def optimize_table(self, table_name: str) -> None:
        """Optimize/vacuum table (merges, cleanup)."""


class ClickHouseSink(WarehouseSink):
    """ClickHouse warehouse adapter.

    Writes data to ClickHouse with:
    - Batch optimization
    - Partition management
    - Deduplication support
    - Observability
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        **kwargs,
    ):
        """Initialize ClickHouse sink.
        
        Args:
            host: ClickHouse host (defaults to CHAMPION_CLICKHOUSE_HOST env var or 'localhost')
            port: ClickHouse HTTP port (defaults to CHAMPION_CLICKHOUSE_PORT env var or 8123)
            user: ClickHouse user (defaults to CHAMPION_CLICKHOUSE_USER env var or 'default')
            password: ClickHouse password (defaults to CHAMPION_CLICKHOUSE_PASSWORD env var or '')
            database: ClickHouse database (defaults to CHAMPION_CLICKHOUSE_DATABASE env var or 'champion')
        """
        import os
        
        self.host = host or os.getenv("CHAMPION_CLICKHOUSE_HOST", "localhost")
        self.port = port or int(os.getenv("CHAMPION_CLICKHOUSE_PORT", "8123"))
        self.user = user or os.getenv("CHAMPION_CLICKHOUSE_USER", "default")
        self.password = password or os.getenv("CHAMPION_CLICKHOUSE_PASSWORD", "")
        self.database = database or os.getenv("CHAMPION_CLICKHOUSE_DATABASE", "champion")
        self.client: Any = None

    def connect(self) -> None:
        """Establish connection to ClickHouse."""
        try:
            import clickhouse_connect

            logger.info(
                "Connecting to ClickHouse",
                host=self.host,
                port=self.port,
                database=self.database,
            )
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
            )
            # Test connection
            self.client.query("SELECT 1")
            logger.info("ClickHouse connection successful")
        except Exception as e:
            raise IntegrationError(
                service="ClickHouse",
                message=f"Failed to connect: {e}",
                retryable=True,
            ) from e

    def disconnect(self) -> None:
        """Close connection to ClickHouse."""
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed")

    def write(self, data: pl.DataFrame, **kwargs: Any) -> dict[str, Any]:
        """Write DataFrame to ClickHouse table.

        Args:
            data: Polars DataFrame to write
            **kwargs: Additional options:
                - table_name (str): Target table name (required)
                - mode (str): Insert mode

        Returns:
            Statistics: rows_written, bytes_written, duration_ms, etc
        """
        if not self.client:
            raise IntegrationError(
                service="ClickHouse",
                message="Not connected",
                retryable=False,
            )

        try:
            import time

            start = time.time()
            table_name = kwargs.get("table_name")
            if not table_name:
                raise ValueError("table_name is required in kwargs")

            # Convert to list of dicts
            records = data.to_dicts()

            if not records:
                logger.warning(f"No data to write to {table_name}")
                return {
                    "rows_written": 0,
                    "bytes_written": 0,
                    "duration_ms": int((time.time() - start) * 1000),
                }

            # Insert
            self.client.insert(table_name, records)

            duration_ms = int((time.time() - start) * 1000)

            logger.info(
                "Data written to ClickHouse",
                table=table_name,
                rows=len(records),
                duration_ms=duration_ms,
            )

            return {
                "rows_written": len(records),
                "bytes_written": data.estimated_size(),
                "duration_ms": duration_ms,
            }

        except Exception as e:
            raise DataError(
                message=f"Failed to write to {table_name}: {e}",
                retryable=True,
            ) from e

    def write_batch(self, batches: list[pl.DataFrame], **kwargs: Any) -> dict[str, Any]:
        """Write multiple batches to warehouse.

        Args:
            batches: List of DataFrames to write
            **kwargs: Options including table_name (required)
        """
        total_rows = 0
        total_bytes = 0
        total_duration = 0

        for batch in batches:
            stats = self.write(batch, **kwargs)
            total_rows += stats.get("rows_written", 0)
            total_bytes += stats.get("bytes_written", 0)
            total_duration += stats.get("duration_ms", 0)

        return {
            "rows_written": total_rows,
            "bytes_written": total_bytes,
            "duration_ms": total_duration,
            "batches": len(batches),
        }

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        try:
            self.client.query(f"SHOW TABLES FROM {self.database} LIKE '{table_name}'")
            return True
        except Exception:
            return False

    def create_table_if_not_exists(self, table_name: str, schema: dict) -> None:
        """Create table with given schema."""
        if self.table_exists(table_name):
            logger.info(f"Table {table_name} already exists")
            return

        # Schema should be a dict like {"col_name": "ClickHouse_TYPE"}
        cols = ", ".join(f"{name} {col_type}" for name, col_type in schema.items())
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                {cols}
            ) ENGINE = MergeTree()
            ORDER BY (date, symbol)
        """
        self.client.query(create_sql)
        logger.info(f"Created table {table_name}")

    def drop_partition(self, table_name: str, partition_spec: dict) -> int:
        """Drop partition from table."""
        # Implement based on partition structure
        raise NotImplementedError("Implement based on your schema")

    def get_row_count(self, table_name: str) -> int:
        """Get row count from table."""
        try:
            result = self.client.query(f"SELECT count() FROM {self.database}.{table_name}")
            return result.result_rows[0][0]
        except Exception as e:
            logger.error(f"Failed to get row count: {e}")
            return 0

    def optimize_table(self, table_name: str) -> None:
        """Optimize table."""
        try:
            self.client.query(f"OPTIMIZE TABLE {self.database}.{table_name} FINAL")
            logger.info(f"Optimized table {table_name}")
        except Exception as e:
            logger.warning(f"Failed to optimize table: {e}")
