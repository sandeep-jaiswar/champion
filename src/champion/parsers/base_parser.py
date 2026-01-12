"""Base parser class for all data parsers.

This module provides an abstract base class that establishes a common interface
for all parsers in the champion platform. It ensures consistency and provides
shared functionality like metadata handling.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Union

import polars as pl


class Parser(ABC):
    """Base class for all data parsers.

    This abstract base class defines the common interface and shared functionality
    for all parsers in the system. Subclasses must implement the parse() method
    according to their specific needs.

    Attributes:
        SCHEMA_VERSION: Version identifier for the parser's output schema.
                       Used for tracking compatibility and schema evolution.
    """

    SCHEMA_VERSION: str = "v1.0"

    @abstractmethod
    def parse(
        self, file_path: Path, *args: Any, **kwargs: Any
    ) -> Union[pl.DataFrame, list[dict[str, Any]]]:
        """Parse file and return parsed data.

        This method must be implemented by all subclasses. The return type
        can vary based on the specific parser's needs.

        Args:
            file_path: Path to the file to parse
            *args: Additional positional arguments specific to the parser
            **kwargs: Additional keyword arguments specific to the parser

        Returns:
            Parsed data in one of the following formats:
            - pl.DataFrame: Structured tabular data
            - list[dict[str, Any]]: List of event dictionaries

        Raises:
            FileNotFoundError: If the file doesn't exist
            Exception: If parsing fails for any reason
        """
        pass

    def validate_schema(self, df: pl.DataFrame) -> None:
        """Validate DataFrame schema matches expected format.

        This method can be optionally implemented by subclasses to validate
        that the parsed data conforms to the expected schema.

        Args:
            df: DataFrame to validate

        Raises:
            NotImplementedError: If the subclass doesn't implement validation
            ValueError: If validation fails
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not implement schema validation")

    def add_metadata(self, df: pl.DataFrame, parsed_at: datetime | None = None) -> pl.DataFrame:
        """Add standard metadata columns to DataFrame.

        This method adds common metadata columns that can be useful for
        tracking data lineage and versioning. Subclasses can override this
        method to add additional metadata or customize the behavior.

        Args:
            df: DataFrame to add metadata to
            parsed_at: Optional timestamp for when the data was parsed.
                      If None, uses current timestamp. For batch processing,
                      consider passing the same timestamp to multiple calls
                      for consistency.

        Returns:
            DataFrame with added metadata columns:
            - _schema_version: Version of the parser schema
            - _parsed_at: Timestamp when the data was parsed

        Example:
            >>> parser = MyParser()
            >>> timestamp = datetime.now()
            >>> # Use same timestamp for multiple files in a batch
            >>> df1 = parser.add_metadata(df1, parsed_at=timestamp)
            >>> df2 = parser.add_metadata(df2, parsed_at=timestamp)
        """
        if parsed_at is None:
            parsed_at = datetime.now()

        return df.with_columns(
            [
                pl.lit(self.SCHEMA_VERSION).alias("_schema_version"),
                pl.lit(parsed_at).alias("_parsed_at"),
            ]
        )
