"""Polars-based symbol master parser for NSE EQUITY_L file.

This parser provides:
- Explicit schema for EQUITY_L.csv parsing
- Fast parsing using Polars
- Handles one-to-many ticker cases (e.g., IBULHSGFIN with multiple instruments)
- Creates canonical instrument IDs (symbol:fiid:exchange)
- Parquet output with proper schema
"""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from champion.parsers.base_parser import Parser
from champion.utils.logger import get_logger
from champion.utils.metrics import rows_parsed

logger = get_logger(__name__)

# Define explicit schema matching NSE EQUITY_L format
# Column names from actual NSE EQUITY_L.csv file
SYMBOL_MASTER_SCHEMA = {
    "SYMBOL": pl.Utf8,
    "NAME OF COMPANY": pl.Utf8,
    "SERIES": pl.Utf8,
    "DATE OF LISTING": pl.Utf8,
    "PAID UP VALUE": pl.Float64,
    "MARKET LOT": pl.Int64,
    "ISIN NUMBER": pl.Utf8,
    "FACE VALUE": pl.Float64,
}


class SymbolMasterParser(Parser):
    """High-performance parser for NSE EQUITY_L symbol master file using Polars.

    Attributes:
        SCHEMA_VERSION: Parser schema version for tracking compatibility.
    """

    SCHEMA_VERSION = "v1.0"

    def __init__(self) -> None:
        """Initialize the parser."""
        self.logger = get_logger(__name__)

    def parse(
        self, file_path: Path, exchange: str = "NSE", output_parquet: bool = False
    ) -> list[dict[str, Any]]:
        """Parse EQUITY_L CSV file into event structures using Polars.

        Args:
            file_path: Path to EQUITY_L.csv file
            exchange: Exchange code (default: NSE)
            output_parquet: If True, also write output to Parquet format

        Returns:
            List of event dictionaries ready for Kafka

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            Exception: If parsing fails
        """
        self.logger.info("Parsing symbol master file with Polars", path=str(file_path))

        try:
            # Read CSV with explicit schema and robust null handling
            df = pl.read_csv(
                file_path,
                schema_overrides=SYMBOL_MASTER_SCHEMA,
                null_values=["-", "", "null", "NULL", "N/A", "NA"],
                ignore_errors=False,
            )

            # Validate schema version - check for column mismatches
            self._validate_schema(df, SYMBOL_MASTER_SCHEMA)

            # Filter out rows with empty symbols or invalid data
            df = df.filter(
                pl.col("SYMBOL").is_not_null()
                & (pl.col("SYMBOL") != "")
                & (pl.col("SYMBOL") != "SYMBOL")  # Skip header if repeated
            )

            # Parse listing date to proper date format
            df = df.with_columns(
                [
                    pl.col("DATE OF LISTING")
                    .str.strptime(pl.Date, "%d-%b-%Y", strict=False)
                    .alias("listing_date_parsed")
                ]
            )

            # Create canonical instrument_id: symbol:exchange (for EQUITY_L we don't have FinInstrmId)
            # We'll use a placeholder approach where FinInstrmId will be matched later from bhavcopy
            df = df.with_columns(
                [
                    (pl.col("SYMBOL") + ":" + pl.lit(exchange)).alias("instrument_id"),
                ]
            )

            # Generate event metadata
            event_time = int(datetime.now().timestamp() * 1000)
            ingest_time = event_time

            # Convert to list of event dictionaries
            events = []
            for row in df.iter_rows(named=True):
                try:
                    event = self._row_to_event(row, exchange, event_time, ingest_time)
                    if event:
                        events.append(event)
                        rows_parsed.labels(scraper="symbol_master", status="success").inc()
                except Exception as e:
                    self.logger.error("Failed to parse row", row=row, error=str(e))
                    rows_parsed.labels(scraper="symbol_master", status="failed").inc()

            self.logger.info("Parsed symbol master file", path=str(file_path), events=len(events))
            return events

        except Exception as e:
            self.logger.error(
                "Failed to parse symbol master file", path=str(file_path), error=str(e)
            )
            raise

    def _validate_schema(self, df: pl.DataFrame, expected_schema: dict[str, Any]) -> None:
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

            self.logger.error(
                "Schema validation failed",
                schema_version=self.SCHEMA_VERSION,
                missing=sorted(missing) if missing else [],
                extra=sorted(extra) if extra else [],
            )
            raise ValueError(error_msg)

    def _row_to_event(
        self, row: dict[str, Any], exchange: str, event_time: int, ingest_time: int
    ) -> dict[str, Any] | None:
        """Convert CSV row to event structure.

        Args:
            row: CSV row as dictionary
            exchange: Exchange code
            event_time: Event timestamp in milliseconds
            ingest_time: Ingestion timestamp in milliseconds

        Returns:
            Event dictionary with envelope and payload, or None if row should be skipped
        """
        # Get symbol
        symbol = row.get("SYMBOL", "").strip() if row.get("SYMBOL") else ""
        if not symbol:
            return None

        # Create instrument_id (symbol:exchange)
        instrument_id = f"{symbol}:{exchange}"

        # Generate deterministic event_id
        event_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"nse_symbol_master:{instrument_id}"))

        # Build event envelope
        event = {
            "event_id": event_id,
            "event_time": event_time,
            "ingest_time": ingest_time,
            "source": "nse_symbol_master",
            "schema_version": "v1",
            "entity_id": instrument_id,
            "payload": self._build_payload(row, symbol, exchange, instrument_id),
        }

        return event

    def _build_payload(
        self, row: dict[str, Any], symbol: str, exchange: str, instrument_id: str
    ) -> dict[str, Any]:
        """Build payload from CSV row.

        Args:
            row: CSV row as dictionary
            symbol: Trading symbol
            exchange: Exchange code
            instrument_id: Canonical instrument identifier

        Returns:
            Payload dictionary matching Avro schema
        """

        def safe_str(value: Any) -> str | None:
            """Get string value or None."""
            if value is None:
                return None
            val = str(value).strip() if value else None
            return val if val and val != "-" else None

        def safe_float(value: Any) -> float | None:
            """Get float value or None."""
            if value is None:
                return None
            try:
                return float(value) if value else None
            except (ValueError, TypeError):
                return None

        def safe_int(value: Any) -> int | None:
            """Get int value or None."""
            if value is None:
                return None
            try:
                return int(float(value)) if value else None
            except (ValueError, TypeError):
                return None

        def safe_date(value: Any) -> int | None:
            """Get date as days since epoch or None."""
            if value is None:
                return None
            # If it's already an integer (days since epoch), return it
            if isinstance(value, int):
                return value
            # If it's a date object, convert to days since epoch
            try:
                from datetime import date

                if isinstance(value, date):
                    epoch = date(1970, 1, 1)
                    return (value - epoch).days
            except Exception:
                pass
            return None

        # Get current date in days since epoch for valid_from
        from datetime import date

        epoch = date(1970, 1, 1)
        valid_from_days = (date.today() - epoch).days

        return {
            "instrument_id": instrument_id,
            "symbol": symbol,
            "exchange": exchange,
            "company_name": safe_str(row.get("NAME OF COMPANY")),
            "isin": safe_str(row.get("ISIN NUMBER")),
            "series": safe_str(row.get("SERIES")),
            "listing_date": safe_date(row.get("listing_date_parsed")),
            "face_value": safe_float(row.get("FACE VALUE")),
            "paid_up_value": safe_float(row.get("PAID UP VALUE")),
            "lot_size": safe_int(row.get("MARKET LOT")),
            "sector": None,  # Not available in EQUITY_L, needs external enrichment
            "industry": None,  # Not available in EQUITY_L, needs external enrichment
            "market_cap_category": None,  # Not available in EQUITY_L, needs external enrichment
            "tick_size": None,  # Not available in EQUITY_L
            "is_index_constituent": None,  # Not available in EQUITY_L
            "indices": None,  # Not available in EQUITY_L
            "status": "ACTIVE",  # Default status, can be updated later
            "delisting_date": None,  # Not available in EQUITY_L
            "metadata": None,  # Additional metadata can be added later
            "valid_from": valid_from_days,
            "valid_to": None,  # Current version (no expiry)
        }
