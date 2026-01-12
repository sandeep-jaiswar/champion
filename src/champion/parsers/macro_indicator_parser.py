"""Parser for macro indicator JSON files.

Parses JSON files from RBI and MOSPI scrapers and converts to Polars DataFrame.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path

import polars as pl

from champion.utils.logger import get_logger

logger = get_logger(__name__)

# Data quality constants
MAX_DATE_GAP_DAYS = 30  # Maximum acceptable gap between data points (in days)


class MacroIndicatorParser:
    """Parser for macro indicator JSON files."""

    def __init__(self):
        """Initialize parser."""
        pass

    def parse(self, json_file_path: Path) -> pl.DataFrame:
        """Parse macro indicator JSON file.

        Args:
            json_file_path: Path to JSON file from scraper

        Returns:
            Polars DataFrame with macro indicator data
        """
        logger.info("Parsing macro indicators", file=str(json_file_path))

        # Load JSON data
        with open(json_file_path) as f:
            data = json.load(f)

        # Extract indicators array
        indicators = data.get("indicators", [])

        if not indicators:
            logger.warning("No indicators found in file", file=str(json_file_path))
            return self.create_empty_dataframe()

        # Convert to DataFrame
        df = pl.DataFrame(indicators)

        # Add envelope fields for event sourcing
        ingestion_timestamp = datetime.now().isoformat()
        df = df.with_columns(
            [
                pl.lit(str(uuid.uuid4())).alias("event_id"),
                pl.lit(ingestion_timestamp).alias("event_time"),
                pl.lit(ingestion_timestamp).alias("ingest_time"),
                pl.col("source").alias("source"),
                pl.lit("1.0").alias("schema_version"),
                pl.concat_str(
                    [
                        pl.col("indicator_code"),
                        pl.lit(":"),
                        pl.col("indicator_date"),
                    ]
                ).alias("entity_id"),
            ]
        )

        # Ensure correct data types
        df = df.with_columns(
            [
                pl.col("indicator_date").str.strptime(pl.Date, "%Y-%m-%d"),
                pl.col("value").cast(pl.Float64),
            ]
        )

        # Add metadata as JSON string if not present
        if "metadata" not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("metadata"))

        # Reorder columns to match schema
        df = df.select(
            [
                "event_id",
                "event_time",
                "ingest_time",
                "source",
                "schema_version",
                "entity_id",
                "indicator_date",
                "indicator_code",
                "indicator_name",
                "indicator_category",
                "value",
                "unit",
                "frequency",
                "source_url",
                "metadata",
            ]
        )

        # Data quality checks
        self._validate_data(df)

        logger.info(
            "Macro indicators parsed",
            rows=len(df),
            indicators=df.select("indicator_code").unique().height,
            date_range=f"{df['indicator_date'].min()!r} to {df['indicator_date'].max()!r}",
        )

        return df

    def create_empty_dataframe(self) -> pl.DataFrame:
        """Create empty DataFrame with correct schema.

        Returns:
            Empty Polars DataFrame
        """
        return pl.DataFrame(
            schema={
                "event_id": pl.Utf8,
                "event_time": pl.Utf8,
                "ingest_time": pl.Utf8,
                "source": pl.Utf8,
                "schema_version": pl.Utf8,
                "entity_id": pl.Utf8,
                "indicator_date": pl.Date,
                "indicator_code": pl.Utf8,
                "indicator_name": pl.Utf8,
                "indicator_category": pl.Utf8,
                "value": pl.Float64,
                "unit": pl.Utf8,
                "frequency": pl.Utf8,
                "source_url": pl.Utf8,
                "metadata": pl.Utf8,
            }
        )

    def _validate_data(self, df: pl.DataFrame) -> None:
        """Validate parsed data for quality issues.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If critical validation fails
        """
        # Check for nulls in required fields
        required_fields = [
            "indicator_date",
            "indicator_code",
            "indicator_name",
            "value",
        ]
        for field in required_fields:
            null_count = df[field].null_count()
            if null_count > 0:
                logger.warning(
                    "Null values found in required field",
                    field=field,
                    null_count=null_count,
                )

        # Check for outliers in numeric values
        value_stats = df["value"].describe()
        logger.info("Value statistics", stats=value_stats)

        # Check for date gaps (warn only, don't fail)
        dates = df.select("indicator_date").unique().sort("indicator_date")
        if len(dates) > 1:
            date_gaps = dates.with_columns(
                (pl.col("indicator_date").diff().dt.total_days() - 1).alias("gap_days")
            ).filter(pl.col("gap_days") > MAX_DATE_GAP_DAYS)

            if len(date_gaps) > 0:
                logger.warning("Large gaps found in date series", gaps=len(date_gaps))

        # Check for duplicate entries
        duplicates = (
            df.group_by(["indicator_code", "indicator_date"]).count().filter(pl.col("count") > 1)
        )
        if len(duplicates) > 0:
            logger.warning("Duplicate entries found", duplicates=len(duplicates))
