"""Polars-based bhavcopy CSV parser with Parquet output.

This parser provides:
- Explicit schema with robust type casting
- Fast parsing using Polars
- Parquet output with partitioned layout
- Type consistency for ClickHouse compatibility
"""

import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from src.utils.logger import get_logger
from src.utils.metrics import rows_parsed

logger = get_logger(__name__)

# Define explicit schema matching NSE CM Bhavcopy format
BHAVCOPY_SCHEMA = {
    "TradDt": pl.Utf8,
    "BizDt": pl.Utf8,
    "Sgmt": pl.Utf8,
    "Src": pl.Utf8,
    "FinInstrmTp": pl.Utf8,
    "FinInstrmId": pl.Int64,
    "ISIN": pl.Utf8,
    "TckrSymb": pl.Utf8,
    "SctySrs": pl.Utf8,
    "XpryDt": pl.Utf8,
    "FininstrmActlXpryDt": pl.Utf8,
    "StrkPric": pl.Float64,
    "OptnTp": pl.Utf8,
    "FinInstrmNm": pl.Utf8,
    "OpnPric": pl.Float64,
    "HghPric": pl.Float64,
    "LwPric": pl.Float64,
    "ClsPric": pl.Float64,
    "LastPric": pl.Float64,
    "PrvsClsgPric": pl.Float64,
    "UndrlygPric": pl.Float64,
    "SttlmPric": pl.Float64,
    "OpnIntrst": pl.Int64,
    "ChngInOpnIntrst": pl.Int64,
    "TtlTradgVol": pl.Int64,
    "TtlTrfVal": pl.Float64,
    "TtlNbOfTxsExctd": pl.Int64,
    "SsnId": pl.Utf8,
    "NewBrdLotQty": pl.Int64,
    "Rmks": pl.Utf8,
    "Rsvd1": pl.Utf8,
    "Rsvd2": pl.Utf8,
    "Rsvd3": pl.Utf8,
    "Rsvd4": pl.Utf8,
}


class PolarsBhavcopyParser:
    """High-performance parser for NSE CM Bhavcopy CSV files using Polars."""

    def __init__(self) -> None:
        """Initialize the parser."""
        return None

    def parse(
        self, file_path: Path, trade_date: date, output_parquet: bool = False
    ) -> list[dict[str, Any]]:
        """Parse bhavcopy CSV file into event structures using Polars.

        Args:
            file_path: Path to CSV file
            trade_date: Trading date
            output_parquet: If True, also write output to Parquet format

        Returns:
            List of event dictionaries ready for Kafka

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            Exception: If parsing fails
        """
        logger.info("Parsing bhavcopy file with Polars", path=str(file_path))

        try:
            # Read CSV with explicit schema and robust null handling
            df = pl.read_csv(
                file_path,
                schema_overrides=BHAVCOPY_SCHEMA,
                null_values=["-", "", "null", "NULL", "N/A"],
                ignore_errors=False,
            )

            # Filter out rows with empty symbols
            df = df.filter(pl.col("TckrSymb").is_not_null() & (pl.col("TckrSymb") != ""))

            # Add event metadata columns
            df = self._add_event_metadata(df, trade_date)

            # Normalize and validate
            df = self._normalize_schema(df)

            logger.info(
                "Parsed bhavcopy file", path=str(file_path), rows=len(df), columns=len(df.columns)
            )

            # Convert to list of event dictionaries
            events = self._dataframe_to_events(df)

            # Update metrics
            rows_parsed.labels(scraper="polars_bhavcopy", status="success").inc(len(events))

            # Write to Parquet if requested
            if output_parquet:
                self.write_parquet(df, trade_date)

            return events

        except Exception as e:
            logger.error("Failed to parse CSV file", path=str(file_path), error=str(e))
            rows_parsed.labels(scraper="polars_bhavcopy", status="failed").inc()
            raise

    def _add_event_metadata(self, df: pl.DataFrame, trade_date: date) -> pl.DataFrame:
        """Add event envelope metadata columns.

        Args:
            df: Input DataFrame
            trade_date: Trading date

        Returns:
            DataFrame with added metadata columns
        """
        # Generate deterministic event_id for each row
        event_ids = [
            str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    f"nse_cm_bhavcopy:{trade_date}:{symbol}",
                )
            )
            for symbol in df["TckrSymb"].to_list()
        ]

        # Calculate timestamps
        event_time = int(datetime.combine(trade_date, datetime.min.time()).timestamp() * 1000)
        ingest_time = int(datetime.now().timestamp() * 1000)

        # Add metadata columns
        df = df.with_columns(
            [
                pl.Series("event_id", event_ids),
                pl.lit(event_time).alias("event_time"),
                pl.lit(ingest_time).alias("ingest_time"),
                pl.lit("nse_cm_bhavcopy").alias("source"),
                pl.lit("v1").alias("schema_version"),
                (pl.col("TckrSymb") + pl.lit(":NSE")).alias("entity_id"),
            ]
        )

        return df

    def _normalize_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize column names and enforce nullability.

        Args:
            df: Input DataFrame

        Returns:
            Normalized DataFrame with consistent schema
        """
        # Ensure all expected columns exist
        # Reorder columns to match canonical schema
        metadata_cols = [
            "event_id",
            "event_time",
            "ingest_time",
            "source",
            "schema_version",
            "entity_id",
        ]

        payload_cols = list(BHAVCOPY_SCHEMA.keys())

        # Select and reorder columns
        df = df.select(metadata_cols + payload_cols)

        return df

    def _dataframe_to_events(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Convert DataFrame to list of event dictionaries.

        Args:
            df: Parsed DataFrame with metadata

        Returns:
            List of event dictionaries
        """
        events = []

        # Extract metadata columns
        metadata_cols = [
            "event_id",
            "event_time",
            "ingest_time",
            "source",
            "schema_version",
            "entity_id",
        ]

        # Convert DataFrame to list of dictionaries
        for row in df.iter_rows(named=True):
            # Build event with envelope and payload
            event = {col: row[col] for col in metadata_cols}

            # Build payload with remaining columns
            payload = {k: v for k, v in row.items() if k not in metadata_cols}

            event["payload"] = payload
            events.append(event)

        return events

    def write_parquet(
        self,
        df: pl.DataFrame,
        trade_date: date,
        base_path: Path = Path("data/lake"),
    ) -> Path:
        """Write DataFrame to Parquet with partitioned layout.

        Args:
            df: DataFrame to write
            trade_date: Trading date for partitioning
            base_path: Base path for data lake

        Returns:
            Path to written Parquet file
        """
        # Add partition columns
        year = trade_date.year
        month = trade_date.month
        day = trade_date.day

        # Create partition path (Hive-style partitioning)
        partition_path = (
            base_path
            / "normalized"
            / "ohlc"
            / f"year={year}"
            / f"month={month:02d}"
            / f"day={day:02d}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        # Write to Parquet with compression
        output_file = partition_path / f"bhavcopy_{trade_date.strftime('%Y%m%d')}.parquet"

        # Convert to PyArrow table for better control
        # Don't include partition columns in the file - they're in the path
        arrow_table = df.to_arrow()

        # Write with Snappy compression for ClickHouse compatibility
        pq.write_table(
            arrow_table,
            output_file,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
        )

        logger.info(
            "Wrote Parquet file",
            path=str(output_file),
            rows=len(df),
            size_mb=output_file.stat().st_size / (1024 * 1024),
        )

        return output_file

    def parse_to_dataframe(self, file_path: Path, trade_date: date) -> pl.DataFrame:
        """Parse bhavcopy CSV file directly to Polars DataFrame.

        This is useful for bulk processing and benchmarking.

        Args:
            file_path: Path to CSV file
            trade_date: Trading date

        Returns:
            Parsed and normalized DataFrame
        """
        logger.info("Parsing bhavcopy to DataFrame", path=str(file_path))

        # Read and parse
        df = pl.read_csv(
            file_path,
            schema_overrides=BHAVCOPY_SCHEMA,
            null_values=["-", "", "null", "NULL", "N/A"],
            ignore_errors=False,
        )

        # Filter out empty symbols
        df = df.filter(pl.col("TckrSymb").is_not_null() & (pl.col("TckrSymb") != ""))

        # Add metadata
        df = self._add_event_metadata(df, trade_date)

        # Normalize
        df = self._normalize_schema(df)

        return df
