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

from champion.utils.logger import get_logger
from champion.utils.metrics import rows_parsed
from champion.validation.validator import ParquetValidator

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
    "Rsvd01": pl.Utf8,
    "Rsvd02": pl.Utf8,
    "Rsvd03": pl.Utf8,
    "Rsvd04": pl.Utf8,
}


class PolarsBhavcopyParser:
    """High-performance parser for NSE CM Bhavcopy CSV files using Polars."""

    def __init__(self) -> None:
        """Initialize the parser."""

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
        # Generate deterministic event_id for each row (including FinInstrmId for uniqueness)
        event_ids = [
            str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    f"nse_cm_bhavcopy:{trade_date}:{symbol}:{fin_instrm_id}",
                )
            )
            for symbol, fin_instrm_id in zip(
                df["TckrSymb"].to_list(), df["FinInstrmId"].to_list(), strict=False
            )
        ]

        # Calculate timestamps
        event_time = int(datetime.combine(trade_date, datetime.min.time()).timestamp() * 1000)
        ingest_time = int(datetime.now().timestamp() * 1000)

        # Add metadata columns (entity_id also includes FinInstrmId)
        df = df.with_columns(
            [
                pl.Series("event_id", event_ids),
                pl.lit(event_time).alias("event_time"),
                pl.lit(ingest_time).alias("ingest_time"),
                pl.lit("nse_cm_bhavcopy").alias("source"),
                pl.lit("v1").alias("schema_version"),
                (
                    pl.col("TckrSymb")
                    + pl.lit(":")
                    + pl.col("FinInstrmId").cast(pl.Utf8)
                    + pl.lit(":NSE")
                ).alias("entity_id"),
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
        validate: bool = True,
    ) -> Path:
        """Write DataFrame to Parquet with validation and partitioned layout.

        Args:
            df: DataFrame to write
            trade_date: Trading date for partitioning
            base_path: Base path for data lake
            validate: Whether to validate data before writing (default: True)

        Returns:
            Path to written Parquet file

        Raises:
            ValueError: If validation fails
        """
        # Validate data before writing if enabled
        if validate:
            logger.info(
                "Validating data before write",
                rows=len(df),
                trade_date=str(trade_date),
            )

            try:
                validator = ParquetValidator(schema_dir=Path("schemas/parquet"))
                result = validator.validate_dataframe(df, schema_name="normalized_equity_ohlc")

                if result.critical_failures > 0:
                    error_msg = (
                        f"Validation failed: {result.critical_failures} critical errors "
                        f"out of {result.total_rows} rows"
                    )
                    logger.error(
                        "Validation failed before write",
                        critical_failures=result.critical_failures,
                        error_details=result.error_details[:5],
                    )
                    raise ValueError(error_msg)

                logger.info(
                    "Validation passed",
                    total_rows=result.total_rows,
                    valid_rows=result.valid_rows,
                )
            except ValueError:
                # Re-raise validation errors
                raise
            except Exception as e:
                # Log but don't fail on validation errors (e.g., schema not found)
                logger.warning(
                    "Validation check failed, continuing with write",
                    error=str(e),
                )

        # Add partition columns
        year = trade_date.year
        month = trade_date.month
        day = trade_date.day

        # Create partition path (Hive-style partitioning)
        partition_path = (
            base_path
            / "normalized"
            / "equity_ohlc"
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

    def parse_raw_csv(self, file_path: str | Path) -> pl.DataFrame:
        """Parse raw CSV file without normalization (for step-by-step ETL).

        Args:
            file_path: Path to CSV file (string or Path)

        Returns:
            Raw DataFrame with NSE column names
        """
        if isinstance(file_path, str):
            file_path = Path(file_path)
        logger.info("Parsing raw CSV", path=str(file_path))

        # Read CSV with explicit schema
        df = pl.read_csv(
            file_path,
            schema_overrides=BHAVCOPY_SCHEMA,
            null_values=["-", "", "null", "NULL", "N/A"],
            ignore_errors=False,
        )

        # Filter out empty symbols
        df = df.filter(pl.col("TckrSymb").is_not_null() & (pl.col("TckrSymb") != ""))

        rows_parsed.labels(scraper="bhavcopy", status="success").inc(len(df))
        logger.info("Parsed raw CSV", rows=len(df))

        return df

    def normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize raw DataFrame to standardized schema.

        Args:
            df: Raw DataFrame with NSE column names

        Returns:
            Normalized DataFrame with standardized column names and types
        """
        logger.info("Normalizing DataFrame", rows=len(df))

        # Rename NSE columns to normalized names
        normalized_df: pl.DataFrame = df.rename(
            {
                "TckrSymb": "symbol",
                "TradDt": "trade_date",
                "OpnPric": "open",
                "HghPric": "high",
                "LwPric": "low",
                "ClsPric": "close",
                "LastPric": "last_price",
                "PrvsClsgPric": "prev_close",
                "SttlmPric": "settlement_price",
                "TtlTradgVol": "total_traded_quantity",
                "TtlTrfVal": "total_traded_value",
                "TtlNbOfTxsExctd": "total_trades",
                "ISIN": "isin",
                "FinInstrmId": "instrument_id",
                "FinInstrmNm": "instrument_name",
                "Sgmt": "segment",
                "SctySrs": "series",
            }
        )

        # Add computed columns
        normalized_df = normalized_df.with_columns(
            [
                pl.lit("NSE").alias("exchange"),
                pl.lit(1.0).alias("adjustment_factor"),
                pl.lit(None).cast(pl.Date).alias("adjustment_date"),
                pl.lit(True).alias("is_trading_day"),
            ]
        )

        logger.info("Normalized DataFrame", rows=len(normalized_df))

        return normalized_df
