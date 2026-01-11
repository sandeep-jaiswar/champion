"""Prefect tasks for bulk and block deals ingestion pipeline."""

import os
from datetime import date
from pathlib import Path
from typing import Any

import clickhouse_connect
import polars as pl
import structlog
from prefect import task

from src.parsers.bulk_block_deals_parser import BulkBlockDealsParser
from src.scrapers.bulk_block_deals import BulkBlockDealsScraper

logger = structlog.get_logger()


@task(name="scrape-bulk-block-deals", retries=3, retry_delay_seconds=60)
def scrape_bulk_block_deals(
    target_date: str,
    deal_type: str = "both",
    output_dir: str | None = None,
) -> dict[str, str]:
    """Scrape NSE bulk and block deals data.

    Args:
        target_date: Date to scrape in YYYY-MM-DD format
        deal_type: Type of deals - 'bulk', 'block', or 'both' (default)
        output_dir: Directory to save JSON files

    Returns:
        Dictionary mapping deal type to saved file path

    Raises:
        RuntimeError: If scraping fails
    """
    logger.info(
        "starting_bulk_block_deals_scrape",
        target_date=target_date,
        deal_type=deal_type,
        output_dir=output_dir,
    )

    # Parse target date
    parsed_date = date.fromisoformat(target_date)
    output_path = Path(output_dir) if output_dir else None

    with BulkBlockDealsScraper() as scraper:
        results = scraper.scrape(
            target_date=parsed_date,
            deal_type=deal_type,
            output_dir=output_path,
            dry_run=False,
        )

    # Convert Path objects to strings for Prefect serialization
    results_str = {k: str(v) for k, v in results.items()}

    logger.info(
        "bulk_block_deals_scrape_complete",
        deal_types=list(results_str.keys()),
        files=list(results_str.values()),
    )

    return results_str


@task(name="parse-bulk-block-deals", retries=2, retry_delay_seconds=30)
def parse_bulk_block_deals(
    file_path: str,
    deal_date: str,
    deal_type: str,
) -> list[dict[str, Any]]:
    """Parse bulk or block deals JSON file to event dictionaries.

    Args:
        file_path: Path to JSON file with deals data
        deal_date: Date of the deals in YYYY-MM-DD format
        deal_type: 'BULK' or 'BLOCK'

    Returns:
        List of event dictionaries

    Raises:
        FileNotFoundError: If JSON file doesn't exist
        Exception: If parsing fails
    """
    logger.info(
        "starting_bulk_block_deals_parse",
        file_path=file_path,
        deal_date=deal_date,
        deal_type=deal_type,
    )

    # Parse deal date
    parsed_date = date.fromisoformat(deal_date)

    parser = BulkBlockDealsParser()
    events = parser.parse(
        file_path=Path(file_path),
        deal_date=parsed_date,
        deal_type=deal_type,
    )

    logger.info(
        "bulk_block_deals_parse_complete",
        file_path=file_path,
        deal_type=deal_type,
        events=len(events),
    )

    return events


@task(name="write-bulk-block-deals-parquet", retries=2, retry_delay_seconds=30)
def write_bulk_block_deals_parquet(
    events: list[dict[str, Any]],
    deal_date: str,
    deal_type: str,
    output_base_path: str = "data/lake",
) -> str:
    """Write bulk/block deals events to partitioned Parquet files.

    Args:
        events: List of event dictionaries
        deal_date: Date of deals in YYYY-MM-DD format (for partitioning)
        deal_type: 'BULK' or 'BLOCK' (for partitioning)
        output_base_path: Base path for Parquet output

    Returns:
        Path to written Parquet file

    Raises:
        ValueError: If no events to write
        Exception: If write fails
    """
    logger.info(
        "starting_bulk_block_deals_parquet_write",
        deal_date=deal_date,
        deal_type=deal_type,
        events=len(events),
        output_base_path=output_base_path,
    )

    if not events:
        logger.warning("No events to write", deal_type=deal_type)
        return ""

    # Parse deal date
    parsed_date = date.fromisoformat(deal_date)

    parser = BulkBlockDealsParser()
    output_file = parser.write_parquet(
        events=events,
        output_base_path=Path(output_base_path),
        deal_date=parsed_date,
        deal_type=deal_type,
    )

    logger.info(
        "bulk_block_deals_parquet_write_complete",
        output_file=str(output_file),
        rows=len(events),
    )

    return str(output_file)


@task(name="load-bulk-block-deals-clickhouse", retries=2, retry_delay_seconds=30)
def load_bulk_block_deals_clickhouse(
    parquet_file: str,
    deal_type: str,
) -> int:
    """Load bulk/block deals data from Parquet to ClickHouse.

    Args:
        parquet_file: Path to Parquet file
        deal_type: 'BULK' or 'BLOCK'

    Returns:
        Number of rows loaded

    Raises:
        Exception: If load fails
    """
    logger.info(
        "starting_bulk_block_deals_clickhouse_load",
        parquet_file=parquet_file,
        deal_type=deal_type,
    )

    if not parquet_file:
        logger.warning("No parquet file to load", deal_type=deal_type)
        return 0

    try:
        # Read Parquet file
        df = pl.read_parquet(parquet_file)

        if len(df) == 0:
            logger.warning("Empty parquet file", parquet_file=parquet_file)
            return 0

        # Get ClickHouse connection parameters
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = int(os.getenv("CLICKHOUSE_PORT", "9000"))
        user = os.getenv("CLICKHOUSE_USER", "champion_user")
        password = os.getenv("CLICKHOUSE_PASSWORD", "champion_pass")
        database = os.getenv("CLICKHOUSE_DATABASE", "champion_market")

        # Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
        )

        # Convert DataFrame to list of tuples for insertion
        data = []
        for row in df.iter_rows(named=True):
            data.append(
                (
                    row["event_id"],
                    row["event_time"],
                    row["ingest_time"],
                    row["source"],
                    row["schema_version"],
                    row["entity_id"],
                    row["deal_date"],
                    row["symbol"],
                    row["client_name"],
                    row["quantity"],
                    row["avg_price"],
                    row["deal_type"],
                    row["transaction_type"],
                    row["exchange"],
                )
            )

        # Insert data
        client.insert(
            "bulk_block_deals",
            data,
            column_names=[
                "event_id",
                "event_time",
                "ingest_time",
                "source",
                "schema_version",
                "entity_id",
                "deal_date",
                "symbol",
                "client_name",
                "quantity",
                "avg_price",
                "deal_type",
                "transaction_type",
                "exchange",
            ],
        )

        rows_loaded = len(data)

        logger.info(
            "bulk_block_deals_clickhouse_load_complete",
            parquet_file=parquet_file,
            deal_type=deal_type,
            rows_loaded=rows_loaded,
        )

        return rows_loaded

    except Exception as e:
        logger.error(
            "bulk_block_deals_clickhouse_load_failed",
            parquet_file=parquet_file,
            error=str(e),
        )
        raise
