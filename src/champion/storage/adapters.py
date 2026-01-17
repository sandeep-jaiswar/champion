"""Storage domain adapters.

Abstracts file storage operations (Parquet, CSV, etc) behind a consistent interface.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from champion.core import DataError, DataSink, DataSource, get_logger

logger = get_logger(__name__)


class ParquetDataSource(DataSource):
    """Read Parquet files from storage."""

    def __init__(self, base_path: Path | str):
        self.base_path = Path(base_path)
        self.connected = False

    def connect(self) -> None:
        """Verify path exists."""
        if not self.base_path.exists():
            raise DataError(f"Path does not exist: {self.base_path}")
        self.connected = True

    def read(self, file_path: Path | str | None = None, **kwargs) -> pl.DataFrame:
        """Read Parquet file(s)."""
        if not self.connected:
            self.connect()

        target_path = Path(file_path) if file_path else self.base_path

        try:
            logger.info(f"Reading Parquet: {target_path}")
            return pl.read_parquet(target_path, **kwargs)
        except Exception as e:
            raise DataError(f"Failed to read Parquet: {e}") from e

    def read_batch(self, batch_size: int = 10000) -> Any:
        """Read file in batches."""
        if not self.connected:
            self.connect()

        df = self.read()
        for i in range(0, len(df), batch_size):
            yield df[i : i + batch_size]

    def disconnect(self) -> None:
        """No-op for file storage."""
        self.connected = False


class ParquetDataSink(DataSink):
    """Write Parquet files to storage."""

    def __init__(
        self,
        base_path: Path | str,
        compression: str = "snappy",
        **kwargs,
    ):
        self.base_path = Path(base_path)
        self.compression = compression
        self.connected = False

    def connect(self) -> None:
        """Create directory if needed."""
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.connected = True

    def write(
        self, data: pl.DataFrame, file_path: Path | str | None = None, **kwargs
    ) -> dict[str, Any]:
        """Write DataFrame to Parquet file."""
        if not self.connected:
            self.connect()

        if file_path is None:
            raise DataError("file_path must be specified")

        target_path = self.base_path / file_path
        target_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            logger.info(
                f"Writing Parquet: {target_path}",
                rows=len(data),
                compression=self.compression,
            )
            # Cast compression to proper Literal type for mypy
            compression_type: Any = self.compression
            data.write_parquet(
                target_path,
                compression=compression_type,
                **kwargs,
            )

            return {
                "rows_written": len(data),
                "bytes_written": target_path.stat().st_size,
                "file": str(target_path),
            }
        except Exception as e:
            raise DataError(f"Failed to write Parquet: {e}") from e

    def write_batch(
        self, batches: list[pl.DataFrame], file_path: Path | str | None = None, **kwargs
    ) -> dict[str, Any]:
        """Write batches to single or multiple files."""
        total_rows = 0
        total_bytes = 0

        for i, batch in enumerate(batches):
            batch_path_str: str
            if len(batches) > 1:
                # Multiple batches, write to separate files
                batch_path_str = f"{file_path}_batch_{i}.parquet"
            else:
                batch_path_str = str(file_path) if not isinstance(file_path, str) else file_path

            stats = self.write(batch, batch_path_str, **kwargs)
            total_rows += stats.get("rows_written", 0)
            total_bytes += stats.get("bytes_written", 0)

        return {
            "rows_written": total_rows,
            "bytes_written": total_bytes,
            "batches": len(batches),
        }

    def disconnect(self) -> None:
        """No-op for file storage."""
        self.connected = False


class CSVDataSource(DataSource):
    """Read CSV files from storage."""

    def __init__(self, base_path: Path | str):
        self.base_path = Path(base_path)
        self.connected = False

    def connect(self) -> None:
        """Verify path exists."""
        if not self.base_path.exists():
            raise DataError(f"Path does not exist: {self.base_path}")
        self.connected = True

    def read(self, file_path: Path | str | None = None, **kwargs) -> pl.DataFrame:
        """Read CSV file(s)."""
        if not self.connected:
            self.connect()

        target_path = Path(file_path) if file_path else self.base_path

        try:
            logger.info(f"Reading CSV: {target_path}")
            return pl.read_csv(target_path, **kwargs)
        except Exception as e:
            raise DataError(f"Failed to read CSV: {e}") from e

    def read_batch(self, batch_size: int = 10000) -> Any:
        """Read file in batches."""
        if not self.connected:
            self.connect()

        df = self.read()
        for i in range(0, len(df), batch_size):
            yield df[i : i + batch_size]

    def disconnect(self) -> None:
        """No-op for file storage."""
        self.connected = False


class CSVDataSink(DataSink):
    """Write CSV files to storage."""

    def __init__(self, base_path: Path | str):
        self.base_path = Path(base_path)
        self.connected = False

    def connect(self) -> None:
        """Create directory if needed."""
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.connected = True

    def write(
        self, data: pl.DataFrame, file_path: Path | str | None = None, **kwargs
    ) -> dict[str, Any]:
        """Write DataFrame to CSV file."""
        if not self.connected:
            self.connect()

        if file_path is None:
            raise DataError("file_path must be specified")

        target_path = self.base_path / file_path
        target_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            logger.info(f"Writing CSV: {target_path}", rows=len(data))
            data.write_csv(target_path, **kwargs)

            return {
                "rows_written": len(data),
                "bytes_written": target_path.stat().st_size,
                "file": str(target_path),
            }
        except Exception as e:
            raise DataError(f"Failed to write CSV: {e}") from e

    def write_batch(
        self, batches: list[pl.DataFrame], file_path: Path | str | None = None, **kwargs
    ) -> dict[str, Any]:
        """Write batches to files."""
        total_rows = 0
        total_bytes = 0

        for i, batch in enumerate(batches):
            batch_path = f"{file_path}_batch_{i}.csv"
            stats = self.write(batch, batch_path, **kwargs)
            total_rows += stats.get("rows_written", 0)
            total_bytes += stats.get("bytes_written", 0)

        return {
            "rows_written": total_rows,
            "bytes_written": total_bytes,
            "batches": len(batches),
        }

    def disconnect(self) -> None:
        """No-op for file storage."""
        self.connected = False
