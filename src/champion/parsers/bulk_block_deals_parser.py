"""Polars-based bulk and block deals parser.

Parses NSE bulk and block deals data into standardized events for downstream processing.

Schema:
- deal_date: Date of the deal
- symbol: Trading symbol
- client_name: Name of the client/entity
- quantity: Number of shares traded
- avg_price: Average deal price
- deal_type: 'BULK' or 'BLOCK'
- exchange: Exchange name (NSE)
"""

import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from champion.parsers.base_parser import Parser
from champion.utils.logger import get_logger
from champion.utils.metrics import rows_parsed

logger = get_logger(__name__)


class BulkBlockDealsParser(Parser):
    """High-performance parser for NSE bulk and block deals data using Polars.

    Attributes:
        SCHEMA_VERSION: Parser schema version for tracking compatibility.
    """

    SCHEMA_VERSION = "v1.0"

    def __init__(self) -> None:
        """Initialize the parser."""
        self.logger = get_logger(__name__)

    def parse(
        self,
        file_path: Path,
        deal_date: date,
        deal_type: str,
    ) -> list[dict[str, Any]]:
        """Parse bulk or block deals JSON file into event structures.

        Args:
            file_path: Path to JSON file with deals data
            deal_date: Date of the deals
            deal_type: 'BULK' or 'BLOCK'

        Returns:
            List of event dictionaries ready for downstream processing

        Raises:
            FileNotFoundError: If JSON file doesn't exist
            Exception: If parsing fails
        """
        self.logger.info(
            "Parsing bulk/block deals file",
            path=str(file_path),
            deal_date=str(deal_date),
            deal_type=deal_type,
        )

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            import json

            # Read JSON file
            with open(file_path) as f:
                data = json.load(f)

            # Handle empty data
            if not data or (isinstance(data, list) and len(data) == 0):
                self.logger.warning("No deals found in file", path=str(file_path))
                return []

            # Convert to Polars DataFrame based on NSE data structure
            # NSE bulk deals format (typical structure):
            # {
            #   "symbol": "TATASTEEL",
            #   "clientName": "ABC SECURITIES",
            #   "buyQty": 1000000,
            #   "sellQty": 0,
            #   "buyAvgPrice": 120.50,
            #   "sellAvgPrice": 0,
            #   "dealDate": "10-JAN-2026"
            # }

            # Create DataFrame from JSON data
            if isinstance(data, list):
                df = pl.DataFrame(data)
            else:
                # If data is wrapped in another structure, extract it
                self.logger.error("Unexpected data format", data_type=type(data).__name__)
                return []

            # Normalize column names (handle variations in NSE API response)
            column_mapping = {
                "symbol": "symbol",
                "Symbol": "symbol",
                "SYMBOL": "symbol",
                "clientName": "client_name",
                "ClientName": "client_name",
                "CLIENT_NAME": "client_name",
                "buyQty": "buy_quantity",
                "BuyQty": "buy_quantity",
                "sellQty": "sell_quantity",
                "SellQty": "sell_quantity",
                "buyAvgPrice": "buy_avg_price",
                "BuyAvgPrice": "buy_avg_price",
                "sellAvgPrice": "sell_avg_price",
                "SellAvgPrice": "sell_avg_price",
                "dealDate": "deal_date_str",
                "DealDate": "deal_date_str",
            }

            # Rename columns if they exist
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df = df.rename({old_name: new_name})

            # Check for required columns
            if "symbol" not in df.columns:
                self.logger.error("Missing required column: symbol", columns=df.columns)
                return []

            # Process deals - create separate rows for buy and sell
            events = []

            for row in df.iter_rows(named=True):
                symbol = row.get("symbol", "").strip()
                client_name = row.get("client_name", "").strip()

                if not symbol:
                    continue

                # Process buy transaction if quantity > 0
                buy_qty = row.get("buy_quantity", 0) or 0
                buy_price = row.get("buy_avg_price", 0.0) or 0.0

                if buy_qty > 0:
                    events.append(
                        self._create_event(
                            deal_date=deal_date,
                            symbol=symbol,
                            client_name=client_name,
                            quantity=buy_qty,
                            avg_price=buy_price,
                            deal_type=deal_type,
                            transaction_type="BUY",
                        )
                    )

                # Process sell transaction if quantity > 0
                sell_qty = row.get("sell_quantity", 0) or 0
                sell_price = row.get("sell_avg_price", 0.0) or 0.0

                if sell_qty > 0:
                    events.append(
                        self._create_event(
                            deal_date=deal_date,
                            symbol=symbol,
                            client_name=client_name,
                            quantity=sell_qty,
                            avg_price=sell_price,
                            deal_type=deal_type,
                            transaction_type="SELL",
                        )
                    )

            rows_parsed.labels(scraper="bulk_block_deals", status="success").inc(len(events))

            self.logger.info(
                "Bulk/block deals parse complete",
                path=str(file_path),
                deals=len(events),
                deal_type=deal_type,
            )

            return events

        except Exception as e:
            self.logger.error("Failed to parse bulk/block deals", error=str(e), path=str(file_path))
            raise

    def _create_event(
        self,
        deal_date: date,
        symbol: str,
        client_name: str,
        quantity: int | float,
        avg_price: float,
        deal_type: str,
        transaction_type: str,
    ) -> dict[str, Any]:
        """Create a standardized event dictionary for a deal.

        Args:
            deal_date: Date of the deal
            symbol: Trading symbol
            client_name: Client name
            quantity: Quantity traded
            avg_price: Average price
            deal_type: 'BULK' or 'BLOCK'
            transaction_type: 'BUY' or 'SELL'

        Returns:
            Event dictionary
        """
        now = datetime.now(UTC)
        event_id = str(uuid.uuid4())
        entity_id = f"{symbol}:{deal_type}:{transaction_type}:{deal_date.strftime('%Y%m%d')}"

        return {
            # Envelope fields (metadata)
            "event_id": event_id,
            "event_time": now,
            "ingest_time": now,
            "source": "nse.bulk_block_deals",
            "schema_version": "1.0.0",
            "entity_id": entity_id,
            # Deal payload
            "deal_date": deal_date,
            "symbol": symbol.upper(),
            "client_name": client_name,
            "quantity": int(quantity) if isinstance(quantity, int | float) else 0,
            "avg_price": float(avg_price) if avg_price else 0.0,
            "deal_type": deal_type.upper(),
            "transaction_type": transaction_type.upper(),
            "exchange": "NSE",
            # Partition columns
            "year": deal_date.year,
            "month": deal_date.month,
            "day": deal_date.day,
        }

    def write_parquet(
        self,
        events: list[dict[str, Any]],
        output_base_path: Path,
        deal_date: date,
        deal_type: str,
    ) -> Path:
        """Write events to partitioned Parquet file.

        Args:
            events: List of event dictionaries
            output_base_path: Base path for Parquet output (e.g., data/lake)
            deal_date: Date of the deals (for partitioning)
            deal_type: 'BULK' or 'BLOCK' (for partitioning)

        Returns:
            Path to written Parquet file

        Raises:
            ValueError: If no events to write
        """
        if not events:
            raise ValueError("No events to write")

        self.logger.info(
            "Writing bulk/block deals to Parquet",
            events=len(events),
            deal_date=str(deal_date),
            deal_type=deal_type,
        )

        # Create DataFrame
        df = pl.DataFrame(events)

        # Ensure proper types
        df = df.with_columns(
            [
                pl.col("deal_date").cast(pl.Date),
                pl.col("event_time").cast(pl.Datetime),
                pl.col("ingest_time").cast(pl.Datetime),
                pl.col("quantity").cast(pl.Int64),
                pl.col("avg_price").cast(pl.Float64),
                pl.col("year").cast(pl.Int64),
                pl.col("month").cast(pl.Int64),
                pl.col("day").cast(pl.Int64),
            ]
        )

        # Create output directory with partitioning
        # Structure: data/lake/bulk_block_deals/deal_type=BULK/year=2026/month=01/day=10/
        output_dir = (
            output_base_path
            / "bulk_block_deals"
            / f"deal_type={deal_type.upper()}"
            / f"year={deal_date.year}"
            / f"month={deal_date.month:02d}"
            / f"day={deal_date.day:02d}"
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        # Drop partition columns from dataframe to avoid duplication
        df_to_write = df.drop(["year", "month", "day", "deal_type"])

        # Write Parquet file
        output_file = (
            output_dir / f"{deal_type.lower()}_deals_{deal_date.strftime('%Y%m%d')}.parquet"
        )
        df_to_write.write_parquet(output_file, compression="snappy")

        self.logger.info(
            "Bulk/block deals Parquet write complete",
            file=str(output_file),
            rows=len(df_to_write),
        )

        return output_file
