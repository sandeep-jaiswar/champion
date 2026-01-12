"""Polars-based BSE bhavcopy CSV parser with Parquet output.

This parser provides:
- Explicit schema for BSE equity bhavcopy format
- Fast parsing using Polars
- Normalization to match NSE normalized schema
- ISIN-based deduplication support
- Parquet output with partitioned layout

BSE Bhavcopy CSV Format:
- Columns: SC_CODE, SC_NAME, SC_GROUP, SC_TYPE, OPEN, HIGH, LOW, CLOSE, LAST,
           PREVCLOSE, NO_TRADES, NO_OF_SHRS, NET_TURNOV, TDCLOINDI, ISIN_CODE
- Example: 500325,RELIANCE,A,R,2750.00,2780.50,2740.00,2765.25,2765.00,2750.00,50000,5000000,13826250000.00,IEP,INE002A01018
"""

import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from champion.parsers.base_parser import Parser
from champion.utils.logger import get_logger
from champion.utils.metrics import rows_parsed

logger = get_logger(__name__)

# Define explicit schema matching BSE Equity Bhavcopy format
BSE_BHAVCOPY_SCHEMA = {
    "SC_CODE": pl.Utf8,  # BSE scrip code
    "SC_NAME": pl.Utf8,
    "SC_GROUP": pl.Utf8,  # Security group (A, B, T, etc.)
    "SC_TYPE": pl.Utf8,  # Security type (R=Regular, N=New)
    "OPEN": pl.Float64,
    "HIGH": pl.Float64,
    "LOW": pl.Float64,
    "CLOSE": pl.Float64,
    "LAST": pl.Float64,
    "PREVCLOSE": pl.Float64,
    "NO_TRADES": pl.Int64,
    "NO_OF_SHRS": pl.Int64,
    "NET_TURNOV": pl.Float64,
    "TDCLOINDI": pl.Utf8,  # Trading closure indicator
    "ISIN_CODE": pl.Utf8,
}


class PolarsBseParser(Parser):
    """High-performance parser for BSE Equity Bhavcopy CSV files using Polars.

    Attributes:
        SCHEMA_VERSION: Parser schema version for tracking compatibility.
    """

    SCHEMA_VERSION = "v1.0"

    def __init__(self) -> None:
        """Initialize the parser."""
        return None

    def parse(
        self, file_path: Path, trade_date: date, output_parquet: bool = False
    ) -> list[dict[str, Any]]:
        """Parse BSE bhavcopy CSV file into event structures using Polars.

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
        logger.info("Parsing BSE bhavcopy file with Polars", path=str(file_path))

        try:
            # Read CSV with explicit schema and robust null handling
            df = pl.read_csv(
                file_path,
                schema_overrides=BSE_BHAVCOPY_SCHEMA,
                null_values=["-", "", "null", "NULL", "N/A"],
                ignore_errors=False,
            )

            # Validate schema version - check for column mismatches
            self._validate_schema(df, BSE_BHAVCOPY_SCHEMA)

            # Filter out rows with empty symbols or scrip codes
            df = df.filter(pl.col("SC_CODE").is_not_null() & (pl.col("SC_CODE") != ""))
            df = df.filter(pl.col("SC_NAME").is_not_null() & (pl.col("SC_NAME") != ""))

            # Add event metadata columns
            df = self._add_event_metadata(df, trade_date)

            # Normalize to match NSE schema
            df = self._normalize_schema(df, trade_date)

            logger.info(
                "Parsed BSE bhavcopy file",
                path=str(file_path),
                rows=len(df),
                columns=len(df.columns),
            )

            # Convert to list of event dictionaries
            events = self._dataframe_to_events(df)

            # Update metrics
            rows_parsed.labels(scraper="polars_bse", status="success").inc(len(events))

            # Write to Parquet if requested
            if output_parquet:
                self.write_parquet(df, trade_date)

            return events

        except Exception as e:
            logger.error("Failed to parse CSV file", path=str(file_path), error=str(e))
            rows_parsed.labels(scraper="polars_bse", status="failed").inc()
            raise

    def _validate_schema(
        self, df: pl.DataFrame, expected_schema: dict[str, Any]
    ) -> None:
        """Validate that DataFrame columns match expected schema.

        Args:
            df: DataFrame to validate
            expected_schema: Expected schema dictionary (column_name -> polars type)

        Raises:
            ValueError: If schema mismatch is detected
        """
        actual_cols = set(df.columns)
        expected_cols = set(expected_schema.keys())

        if actual_cols != expected_cols:
            missing = expected_cols - actual_cols
            extra = actual_cols - expected_cols

            error_msg = f"Schema mismatch (version {self.SCHEMA_VERSION}): "
            if missing:
                error_msg += f"missing columns={sorted(missing)}"
            if extra:
                if missing:
                    error_msg += ", "
                error_msg += f"extra columns={sorted(extra)}"

            logger.error(
                "Schema validation failed",
                schema_version=self.SCHEMA_VERSION,
                missing=sorted(missing) if missing else [],
                extra=sorted(extra) if extra else [],
            )
            raise ValueError(error_msg)

    def _add_event_metadata(self, df: pl.DataFrame, trade_date: date) -> pl.DataFrame:
        """Add event envelope metadata columns.

        Args:
            df: Input DataFrame
            trade_date: Trading date

        Returns:
            DataFrame with added metadata columns
        """
        # Generate deterministic event_id for each row (using SC_CODE for uniqueness)
        event_ids = [
            str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    f"bse_eq_bhavcopy:{trade_date}:{sc_code}",
                )
            )
            for sc_code in df["SC_CODE"].to_list()
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
                pl.lit("bse_eq_bhavcopy").alias("source"),
                pl.lit("v1").alias("schema_version"),
                (pl.col("SC_NAME") + pl.lit(":") + pl.col("SC_CODE") + pl.lit(":BSE")).alias(
                    "entity_id"
                ),
            ]
        )

        return df

    def _normalize_schema(self, df: pl.DataFrame, trade_date: date) -> pl.DataFrame:
        """Normalize BSE columns to match NSE normalized schema.

        Maps BSE columns to NSE-equivalent columns for unified processing.

        Args:
            df: Input DataFrame with BSE columns
            trade_date: Trading date

        Returns:
            Normalized DataFrame matching NSE schema structure
        """
        # Map BSE columns to NSE equivalents
        normalized_df = df.with_columns(
            [
                # Core identifiers
                pl.col("SC_NAME").alias("TckrSymb"),  # Symbol
                pl.col("ISIN_CODE").alias("ISIN"),
                pl.col("SC_CODE").alias("FinInstrmId"),  # Use BSE scrip code as instrument ID
                pl.lit(trade_date.strftime("%Y-%m-%d")).alias("TradDt"),
                pl.lit(trade_date.strftime("%Y-%m-%d")).alias("BizDt"),
                # Price fields
                pl.col("OPEN").alias("OpnPric"),
                pl.col("HIGH").alias("HghPric"),
                pl.col("LOW").alias("LwPric"),
                pl.col("CLOSE").alias("ClsPric"),
                pl.col("LAST").alias("LastPric"),
                pl.col("PREVCLOSE").alias("PrvsClsgPric"),
                # Volume and trades
                pl.col("NO_OF_SHRS").alias("TtlTradgVol"),
                pl.col("NET_TURNOV").alias("TtlTrfVal"),
                pl.col("NO_TRADES").alias("TtlNbOfTxsExctd"),
                # BSE-specific fields
                pl.col("SC_GROUP").alias("Sgmt"),  # Security group
                pl.lit("BSE").alias("Src"),  # Source
                pl.lit("STK").alias("FinInstrmTp"),  # Instrument type
                pl.col("SC_GROUP").alias("SctySrs"),  # Series equivalent
                # Null fields (not available in BSE)
                pl.lit(None).cast(pl.Date).alias("XpryDt"),
                pl.lit(None).cast(pl.Date).alias("FininstrmActlXpryDt"),
                pl.lit(None).cast(pl.Float64).alias("StrkPric"),
                pl.lit(None).cast(pl.Utf8).alias("OptnTp"),
                pl.col("SC_NAME").alias("FinInstrmNm"),
                pl.lit(None).cast(pl.Float64).alias("UndrlygPric"),
                pl.lit(None).cast(pl.Float64).alias("SttlmPric"),
                pl.lit(None).cast(pl.Int64).alias("OpnIntrst"),
                pl.lit(None).cast(pl.Int64).alias("ChngInOpnIntrst"),
                pl.col("TDCLOINDI").alias("SsnId"),  # Trading closure indicator
                pl.lit(None).cast(pl.Int64).alias("NewBrdLotQty"),
                pl.lit(None).cast(pl.Utf8).alias("Rmks"),
                pl.lit(None).cast(pl.Utf8).alias("Rsvd01"),
                pl.lit(None).cast(pl.Utf8).alias("Rsvd02"),
                pl.lit(None).cast(pl.Utf8).alias("Rsvd03"),
                pl.lit(None).cast(pl.Utf8).alias("Rsvd04"),
            ]
        )

        # Select only normalized columns (matching NSE schema)
        metadata_cols = [
            "event_id",
            "event_time",
            "ingest_time",
            "source",
            "schema_version",
            "entity_id",
        ]

        payload_cols = [
            "TradDt",
            "BizDt",
            "Sgmt",
            "Src",
            "FinInstrmTp",
            "FinInstrmId",
            "ISIN",
            "TckrSymb",
            "SctySrs",
            "XpryDt",
            "FininstrmActlXpryDt",
            "StrkPric",
            "OptnTp",
            "FinInstrmNm",
            "OpnPric",
            "HghPric",
            "LwPric",
            "ClsPric",
            "LastPric",
            "PrvsClsgPric",
            "UndrlygPric",
            "SttlmPric",
            "OpnIntrst",
            "ChngInOpnIntrst",
            "TtlTradgVol",
            "TtlTrfVal",
            "TtlNbOfTxsExctd",
            "SsnId",
            "NewBrdLotQty",
            "Rmks",
            "Rsvd01",
            "Rsvd02",
            "Rsvd03",
            "Rsvd04",
        ]

        normalized_df = normalized_df.select(metadata_cols + payload_cols)

        logger.info("Normalized BSE DataFrame to NSE schema", rows=len(normalized_df))

        return normalized_df

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
            / "equity_ohlc"
            / f"year={year}"
            / f"month={month:02d}"
            / f"day={day:02d}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)

        # Write to Parquet with compression
        output_file = partition_path / f"bhavcopy_bse_{trade_date.strftime('%Y%m%d')}.parquet"

        # Convert to PyArrow table
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
        """Parse BSE bhavcopy CSV file directly to Polars DataFrame.

        This is useful for bulk processing and benchmarking.

        Args:
            file_path: Path to CSV file
            trade_date: Trading date

        Returns:
            Parsed and normalized DataFrame
        """
        logger.info("Parsing BSE bhavcopy to DataFrame", path=str(file_path))

        # Read and parse
        df = pl.read_csv(
            file_path,
            schema_overrides=BSE_BHAVCOPY_SCHEMA,
            null_values=["-", "", "null", "NULL", "N/A"],
            ignore_errors=False,
        )

        # Filter out empty symbols
        df = df.filter(pl.col("SC_CODE").is_not_null() & (pl.col("SC_CODE") != ""))
        df = df.filter(pl.col("SC_NAME").is_not_null() & (pl.col("SC_NAME") != ""))

        # Add metadata
        df = self._add_event_metadata(df, trade_date)

        # Normalize
        df = self._normalize_schema(df, trade_date)

        return df

    def parse_raw_csv(self, file_path: str | Path) -> pl.DataFrame:
        """Parse raw CSV file without normalization (for step-by-step ETL).

        Args:
            file_path: Path to CSV file (string or Path)

        Returns:
            Raw DataFrame with BSE column names
        """
        file_path = Path(file_path) if isinstance(file_path, str) else file_path
        logger.info("Parsing raw BSE CSV", path=str(file_path))

        # Read CSV with explicit schema
        df = pl.read_csv(
            file_path,
            schema_overrides=BSE_BHAVCOPY_SCHEMA,
            null_values=["-", "", "null", "NULL", "N/A"],
            ignore_errors=False,
        )

        # Filter out empty symbols
        df = df.filter(pl.col("SC_CODE").is_not_null() & (pl.col("SC_CODE") != ""))
        df = df.filter(pl.col("SC_NAME").is_not_null() & (pl.col("SC_NAME") != ""))

        rows_parsed.labels(scraper="bse_bhavcopy", status="success").inc(len(df))
        logger.info("Parsed raw BSE CSV", rows=len(df))

        return df
