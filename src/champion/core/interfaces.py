"""Abstract interfaces defining contracts across all domains.

This module establishes the core abstractions that enable:
- Loose coupling between components
- Easy testing with mock implementations
- Plugin architecture
- Alternative implementations (e.g., different storage backends)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, Protocol, TypeVar, runtime_checkable

import polars as pl

# Type variables for generic interfaces
T = TypeVar("T")
TData = TypeVar("TData")


@dataclass
class DataContext:
    """Metadata about data being processed.

    Attributes:
        source: Where data came from
        timestamp: When data was acquired
        schema_version: Data format version
        metadata: Custom key-value pairs
    """

    source: str
    timestamp: datetime
    schema_version: str = "1.0"
    metadata: dict[str, Any] | None = None


@runtime_checkable
class Observer(Protocol):
    """Observer pattern for event notifications."""

    def on_start(self, context: DataContext) -> None:
        """Called when operation starts."""

    def on_success(self, result: Any, elapsed_ms: float) -> None:
        """Called on successful completion."""

    def on_error(self, error: Exception, elapsed_ms: float) -> None:
        """Called on error."""


class DataSource(ABC):
    """Contract for reading data from any source.

    Implementations:
        - CSVDataSource, ParquetDataSource
        - HTTPDataSource (for web scraping)
        - KafkaDataSource (for streaming)
        - ClickHouseDataSource (for warehoused data)
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to source."""

    @abstractmethod
    def read(self, **kwargs) -> pl.DataFrame:
        """Read data from source.

        Returns:
            Polars DataFrame with data
        """

    @abstractmethod
    def read_batch(self, batch_size: int = 10000) -> Any:
        """Read data in batches for memory-efficient processing.

        Yields:
            Batches of data (type depends on implementation)
        """

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to source."""


class DataSink(ABC):
    """Contract for writing data to any destination.

    Implementations:
        - ParquetDataSink, CSVDataSink
        - ClickHouseDataSink
        - KafkaDataSink
        - S3DataSink
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to sink."""

    @abstractmethod
    def write(self, data: pl.DataFrame, **kwargs) -> dict[str, Any]:
        """Write data to sink.

        Returns:
            Statistics (rows written, size, duration, etc.)
        """

    @abstractmethod
    def write_batch(self, batches: list[pl.DataFrame], **kwargs) -> dict[str, Any]:
        """Write multiple batches efficiently."""

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to sink."""


class Transformer(ABC, Generic[TData]):
    """Contract for transforming data.

    Examples:
        - NormalizeOHLCTransformer
        - ComputeIndicatorsTransformer
        - DeduplicateRecordsTransformer
    """

    @abstractmethod
    def transform(self, data: TData, context: DataContext | None = None) -> TData:
        """Apply transformation to data.

        Args:
            data: Input data
            context: Optional metadata about the data

        Returns:
            Transformed data
        """


class Validator(ABC):
    """Contract for validating data quality.

    Examples:
        - JSONSchemaValidator
        - BusinessLogicValidator
        - DuplicateDetector
    """

    @abstractmethod
    def validate(self, data: pl.DataFrame, **kwargs) -> dict[str, Any]:
        """Validate data and return result.

        Returns:
            Dictionary with:
                - valid_rows: Count of valid records
                - invalid_rows: Count of invalid records
                - errors: List of validation errors
                - warnings: List of warnings
        """

    @abstractmethod
    def should_quarantine(self, error: Exception) -> bool:
        """Determine if failed record should be quarantined."""


class Scraper(ABC):
    """Contract for scraping data from external sources.

    Examples:
        - NSEBhavcopyScraper
        - NSESymbolMasterScraper
        - OptionChainScraper
    """

    @abstractmethod
    def scrape(self, **kwargs) -> pl.DataFrame:
        """Scrape data from source.

        Returns:
            Polars DataFrame with scraped data
        """

    @abstractmethod
    def validate_scrape(self, data: pl.DataFrame) -> bool:
        """Validate scraped data sanity."""


class Repository(ABC, Generic[T]):
    """Contract for repository pattern (data access).

    Provides abstraction over specific databases/storage.
    """

    @abstractmethod
    def find_by_id(self, id_: Any) -> T | None:
        """Find single record by ID."""

    @abstractmethod
    def find_all(self, **filters) -> list[T]:
        """Find multiple records by criteria."""

    @abstractmethod
    def save(self, entity: T) -> T:
        """Save entity (insert or update)."""

    @abstractmethod
    def delete(self, id_: Any) -> bool:
        """Delete entity by ID."""


class CacheBackend(ABC):
    """Contract for caching layer.

    Implementations:
        - RedisCache
        - LocalMemoryCache
        - NoCache (passthrough)
    """

    @abstractmethod
    def get(self, key: str) -> Any | None:
        """Get value from cache."""

    @abstractmethod
    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        """Set value in cache with optional TTL."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete value from cache."""

    @abstractmethod
    def clear(self) -> None:
        """Clear all cache entries."""
