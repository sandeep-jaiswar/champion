"""Polars-based parser for NSE index constituent data.

This parser provides:
- Parsing of NSE index constituent JSON data
- Extraction of symbol, weight, and metadata
- Generation of event structures compatible with Avro schema
- Support for tracking adds, removes, and rebalances
"""

import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from src.utils.logger import get_logger
from src.utils.metrics import rows_parsed

logger = get_logger(__name__)


class IndexConstituentParser:
    """High-performance parser for NSE index constituent data using Polars."""

    def __init__(self) -> None:
        """Initialize the parser."""
        self.logger = get_logger(__name__)

    def parse(
        self,
        file_path: Path,
        index_name: str,
        effective_date: date | None = None,
        action: str = "ADD",
    ) -> list[dict[str, Any]]:
        """Parse index constituent JSON file into event structures.

        Args:
            file_path: Path to JSON file with index constituent data
            index_name: Name of the index (e.g., 'NIFTY50')
            effective_date: Date when constituents are effective (defaults to today)
            action: Default action type ('ADD', 'REMOVE', or 'REBALANCE')

        Returns:
            List of event dictionaries ready for Kafka/Parquet

        Raises:
            FileNotFoundError: If JSON file doesn't exist
            Exception: If parsing fails
        """
        self.logger.info(
            "Parsing index constituent file",
            path=str(file_path),
            index_name=index_name
        )

        if effective_date is None:
            effective_date = date.today()

        try:
            import json
            
            # Read JSON file
            with open(file_path, "r") as f:
                data = json.load(f)

            # Extract constituent data
            constituents = data.get("data", [])
            
            if not constituents:
                self.logger.warning(
                    "No constituent data found in file",
                    path=str(file_path)
                )
                return []

            # Convert to Polars DataFrame for efficient processing
            df = pl.DataFrame(constituents)
            
            # Filter for equity series (exclude indices and other instruments)
            if "series" in df.columns:
                df = df.filter(pl.col("series").is_in(["EQ", "BE"]))

            # Generate event metadata
            event_time = int(datetime.now().timestamp() * 1000)
            ingest_time = event_time

            # Convert to list of event dictionaries
            events = []
            for row in df.iter_rows(named=True):
                try:
                    event = self._row_to_event(
                        row,
                        index_name,
                        effective_date,
                        action,
                        event_time,
                        ingest_time
                    )
                    if event:
                        events.append(event)
                        rows_parsed.labels(
                            scraper="index_constituent",
                            status="success"
                        ).inc()
                except Exception as e:
                    self.logger.error(
                        "Failed to parse row",
                        row=row,
                        error=str(e)
                    )
                    rows_parsed.labels(
                        scraper="index_constituent",
                        status="failed"
                    ).inc()

            self.logger.info(
                "Parsed index constituent file",
                path=str(file_path),
                index_name=index_name,
                events=len(events)
            )
            return events

        except Exception as e:
            self.logger.error(
                "Failed to parse index constituent file",
                path=str(file_path),
                error=str(e)
            )
            raise

    def _row_to_event(
        self,
        row: dict[str, Any],
        index_name: str,
        effective_date: date,
        action: str,
        event_time: int,
        ingest_time: int,
    ) -> dict[str, Any] | None:
        """Convert constituent row to event structure.

        Args:
            row: Constituent row as dictionary
            index_name: Name of the index
            effective_date: Date when constituent is effective
            action: Action type (ADD, REMOVE, REBALANCE)
            event_time: Event timestamp in milliseconds
            ingest_time: Ingestion timestamp in milliseconds

        Returns:
            Event dictionary with envelope and payload, or None if row should be skipped
        """
        # Extract symbol - NSE API uses 'symbol' field
        symbol = row.get("symbol", "").strip()
        if not symbol:
            return None

        # Create entity_id (index_name:symbol)
        entity_id = f"{index_name}:{symbol}"

        # Generate deterministic event_id
        event_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_DNS,
                f"nse_index_constituent:{entity_id}:{effective_date.isoformat()}"
            )
        )

        # Build event envelope
        event = {
            "event_id": event_id,
            "event_time": event_time,
            "ingest_time": ingest_time,
            "source": "nse_index_constituents",
            "schema_version": "v1",
            "entity_id": entity_id,
            "payload": self._build_payload(
                row,
                index_name,
                symbol,
                effective_date,
                action
            ),
        }

        return event

    def _build_payload(
        self,
        row: dict[str, Any],
        index_name: str,
        symbol: str,
        effective_date: date,
        action: str,
    ) -> dict[str, Any]:
        """Build payload from constituent row.

        Args:
            row: Constituent row as dictionary
            index_name: Name of the index
            symbol: Trading symbol
            effective_date: Date when constituent is effective
            action: Action type (ADD, REMOVE, REBALANCE)

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

        # Convert effective_date to days since epoch
        epoch = date(1970, 1, 1)
        effective_date_days = (effective_date - epoch).days

        # Extract index weight (percentage)
        # NSE API may provide this in different fields depending on the index
        weight = None
        if "indexWeight" in row:
            weight = safe_float(row.get("indexWeight"))
        elif "weightage" in row:
            weight = safe_float(row.get("weightage"))

        return {
            "index_name": index_name,
            "symbol": symbol,
            "isin": safe_str(row.get("meta", {}).get("isin")) or safe_str(row.get("isin")),
            "company_name": safe_str(row.get("meta", {}).get("companyName")) or safe_str(row.get("companyName")),
            "effective_date": effective_date_days,
            "action": action,
            "weight": weight,
            "free_float_market_cap": safe_float(row.get("ffmc")),
            "shares_for_index": safe_int(row.get("sharesForIndex")),
            "announcement_date": None,  # Not available in current API
            "index_category": self._get_index_category(index_name),
            "sector": safe_str(row.get("meta", {}).get("sector")) or safe_str(row.get("sector")),
            "industry": safe_str(row.get("meta", {}).get("industry")) or safe_str(row.get("industry")),
            "metadata": None,  # Can be populated with additional data if needed
        }

    def _get_index_category(self, index_name: str) -> str | None:
        """Determine index category based on index name.

        Args:
            index_name: Name of the index

        Returns:
            Index category string or None
        """
        if index_name in ["NIFTY50", "NIFTY100", "NIFTY500"]:
            return "Broad Market"
        elif "BANK" in index_name or "IT" in index_name or "PHARMA" in index_name:
            return "Sectoral"
        elif "MIDCAP" in index_name or "SMALLCAP" in index_name:
            return "Market Cap"
        else:
            return None

    def write_parquet(
        self,
        events: list[dict[str, Any]],
        output_base_path: Path,
        index_name: str,
        effective_date: date,
    ) -> Path:
        """Write parsed events to partitioned Parquet files.

        Args:
            events: List of event dictionaries
            output_base_path: Base path for Parquet output
            index_name: Name of the index (for partitioning)
            effective_date: Effective date (for partitioning)

        Returns:
            Path to written Parquet file

        Raises:
            Exception: If write fails
        """
        if not events:
            raise ValueError("No events to write")

        # Flatten events for Parquet (envelope + payload fields)
        flattened = []
        for event in events:
            flat = {
                "event_id": event["event_id"],
                "event_time": event["event_time"],
                "ingest_time": event["ingest_time"],
                "source": event["source"],
                "schema_version": event["schema_version"],
                "entity_id": event["entity_id"],
            }
            # Add payload fields
            flat.update(event["payload"])
            flattened.append(flat)

        # Create DataFrame
        df = pl.DataFrame(flattened)

        # Create partitioned output path
        year = effective_date.year
        month = effective_date.month
        day = effective_date.day
        
        output_dir = (
            output_base_path
            / "normalized"
            / "index_constituent"
            / f"index_name={index_name}"
            / f"year={year}"
            / f"month={month:02d}"
            / f"day={day:02d}"
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"data_{effective_date.isoformat()}.parquet"

        # Write Parquet with compression
        df.write_parquet(output_file, compression="snappy")

        self.logger.info(
            "Wrote index constituent parquet",
            path=str(output_file),
            rows=len(df)
        )

        return output_file
