"""Prefect task for Polars bhavcopy parser."""

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import structlog
from prefect import task

from src.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser

logger = structlog.get_logger()


@task(name="parse-bhavcopy-to-parquet", retries=2)
def parse_bhavcopy_to_parquet(
    csv_file_path: str,
    trade_date: str,
    output_base_path: str = "data/lake",
) -> str:
    """Parse NSE bhavcopy CSV file and write to partitioned Parquet.

    This task uses the Polars-based parser for high-performance parsing
    and writes the output to Parquet format with Hive-style partitioning.

    Args:
        csv_file_path: Path to the bhavcopy CSV file
        trade_date: Trading date in YYYY-MM-DD format
        output_base_path: Base path for data lake output

    Returns:
        Path to the written Parquet file

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        Exception: If parsing fails
    """
    logger.info(
        "starting_bhavcopy_parse",
        csv_file_path=csv_file_path,
        trade_date=trade_date,
        output_base_path=output_base_path,
    )

    parser = PolarsBhavcopyParser()

    # Parse date
    parsed_date = date.fromisoformat(trade_date)

    # Parse CSV to DataFrame
    df = parser.parse_to_dataframe(
        file_path=Path(csv_file_path),
        trade_date=parsed_date,
    )

    # Write to Parquet
    output_file = parser.write_parquet(
        df=df,
        trade_date=parsed_date,
        base_path=Path(output_base_path),
    )

    logger.info(
        "bhavcopy_parse_complete",
        csv_file_path=csv_file_path,
        output_file=str(output_file),
        rows=len(df),
    )

    return str(output_file)


@task(name="parse-bhavcopy-to-events", retries=2)
def parse_bhavcopy_to_events(
    csv_file_path: str,
    trade_date: str,
    output_parquet: bool = False,
    output_base_path: str | None = None,
) -> list[dict[str, Any]]:
    """Parse NSE bhavcopy CSV file to event dictionaries.

    This task uses the Polars-based parser to generate event dictionaries
    compatible with the existing Kafka pipeline.

    Args:
        csv_file_path: Path to the bhavcopy CSV file
        trade_date: Trading date in YYYY-MM-DD format
        output_parquet: If True, also write to Parquet
        output_base_path: Base path for Parquet output (required if output_parquet=True)

    Returns:
        List of event dictionaries

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        Exception: If parsing fails
    """
    logger.info(
        "starting_bhavcopy_event_parse",
        csv_file_path=csv_file_path,
        trade_date=trade_date,
        output_parquet=output_parquet,
    )

    parser = PolarsBhavcopyParser()

    # Parse date
    parsed_date = date.fromisoformat(trade_date)

    # Parse to events
    events = parser.parse(
        file_path=Path(csv_file_path),
        trade_date=parsed_date,
        output_parquet=False,  # We'll handle Parquet separately
    )

    # Optionally write to Parquet
    if output_parquet:
        if not output_base_path:
            raise ValueError("output_base_path required when output_parquet=True")

        df = parser.parse_to_dataframe(
            file_path=Path(csv_file_path),
            trade_date=parsed_date,
        )

        output_file = parser.write_parquet(
            df=df,
            trade_date=parsed_date,
            base_path=Path(output_base_path),
        )

        logger.info(
            "bhavcopy_parquet_written",
            output_file=str(output_file),
            rows=len(df),
        )

    logger.info(
        "bhavcopy_event_parse_complete",
        csv_file_path=csv_file_path,
        events=len(events),
    )

    return events


@task(name="read-parquet-partition", retries=2)
def read_parquet_partition(
    base_path: str,
    year: int,
    month: int,
    day: int,
) -> pl.DataFrame:
    """Read a partitioned Parquet dataset for a specific date.

    Args:
        base_path: Base path for data lake
        year: Year of partition
        month: Month of partition (1-12)
        day: Day of partition (1-31)

    Returns:
        Polars DataFrame with data from the partition

    Raises:
        FileNotFoundError: If partition doesn't exist
    """
    partition_path = (
        Path(base_path)
        / "normalized"
        / "ohlc"
        / f"year={year}"
        / f"month={month:02d}"
        / f"day={day:02d}"
    )

    logger.info(
        "reading_parquet_partition",
        partition_path=str(partition_path),
    )

    # Read all parquet files in the partition
    df = pl.read_parquet(partition_path / "*.parquet")

    logger.info(
        "parquet_partition_read_complete",
        partition_path=str(partition_path),
        rows=len(df),
        columns=len(df.columns),
    )

    return df
