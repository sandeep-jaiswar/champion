"""Prefect tasks for index constituent ingestion pipeline."""

import os
from datetime import date
from pathlib import Path
from typing import Any

import clickhouse_connect
import polars as pl
import structlog
from prefect import task

from src.parsers.index_constituent_parser import IndexConstituentParser
from src.scrapers.index_constituent import IndexConstituentScraper

logger = structlog.get_logger()


@task(name="scrape-index-constituents", retries=3, retry_delay_seconds=60)
def scrape_index_constituents(
    indices: list[str] | None = None,
    output_dir: str | None = None,
) -> dict[str, str]:
    """Scrape NSE index constituent data.

    Args:
        indices: List of index names to scrape (e.g., ['NIFTY50', 'BANKNIFTY'])
                 If None, scrapes default indices
        output_dir: Directory to save JSON files

    Returns:
        Dictionary mapping index name to saved file path

    Raises:
        RuntimeError: If scraping fails
    """
    logger.info(
        "starting_index_constituent_scrape",
        indices=indices,
        output_dir=output_dir,
    )

    # Default to NIFTY50 and BANKNIFTY if not specified
    if indices is None:
        indices = ["NIFTY50", "BANKNIFTY"]

    output_path = Path(output_dir) if output_dir else None

    with IndexConstituentScraper() as scraper:
        results = scraper.scrape(
            indices=indices,
            output_dir=output_path,
            dry_run=False,
        )

    # Convert Path objects to strings for Prefect serialization
    results_str = {k: str(v) for k, v in results.items()}

    logger.info(
        "index_constituent_scrape_complete",
        indices=list(results_str.keys()),
        files=list(results_str.values()),
    )

    return results_str


@task(name="parse-index-constituents", retries=2, retry_delay_seconds=30)
def parse_index_constituents(
    file_path: str,
    index_name: str,
    effective_date: str | None = None,
    action: str = "ADD",
) -> list[dict[str, Any]]:
    """Parse index constituent JSON file to event dictionaries.

    Args:
        file_path: Path to JSON file with index constituent data
        index_name: Name of the index (e.g., 'NIFTY50')
        effective_date: Date when constituents are effective (YYYY-MM-DD), defaults to today
        action: Action type ('ADD', 'REMOVE', or 'REBALANCE'), defaults to 'ADD'

    Returns:
        List of event dictionaries

    Raises:
        FileNotFoundError: If JSON file doesn't exist
        Exception: If parsing fails
    """
    logger.info(
        "starting_index_constituent_parse",
        file_path=file_path,
        index_name=index_name,
        effective_date=effective_date,
        action=action,
    )

    # Parse effective date
    parsed_date = date.fromisoformat(effective_date) if effective_date else date.today()

    parser = IndexConstituentParser()
    events = parser.parse(
        file_path=Path(file_path),
        index_name=index_name,
        effective_date=parsed_date,
        action=action,
    )

    logger.info(
        "index_constituent_parse_complete",
        file_path=file_path,
        index_name=index_name,
        events=len(events),
    )

    return events


@task(name="write-index-constituents-parquet", retries=2, retry_delay_seconds=30)
def write_index_constituents_parquet(
    events: list[dict],
    index_name: str,
    effective_date: str,
    output_base_path: str = "data/lake",
) -> str:
    """Write index constituent events to partitioned Parquet files.

    Args:
        events: List of event dictionaries
        index_name: Name of the index (for partitioning)
        effective_date: Effective date in YYYY-MM-DD format (for partitioning)
        output_base_path: Base path for Parquet output

    Returns:
        Path to written Parquet file

    Raises:
        ValueError: If no events to write
        Exception: If write fails
    """
    logger.info(
        "starting_index_constituent_parquet_write",
        index_name=index_name,
        effective_date=effective_date,
        events=len(events),
        output_base_path=output_base_path,
    )

    if not events:
        logger.warning("No events to write", index_name=index_name)
        return ""

    # Parse effective date
    parsed_date = date.fromisoformat(effective_date)

    parser = IndexConstituentParser()
    output_file = parser.write_parquet(
        events=events,
        output_base_path=Path(output_base_path),
        index_name=index_name,
        effective_date=parsed_date,
    )

    logger.info(
        "index_constituent_parquet_write_complete",
        output_file=str(output_file),
        rows=len(events),
    )

    return str(output_file)


@task(name="load-index-constituents-clickhouse", retries=2, retry_delay_seconds=30)
def load_index_constituents_clickhouse(
    parquet_file: str,
    index_name: str,
) -> int:
    """Load index constituent data from Parquet to ClickHouse.

    Args:
        parquet_file: Path to Parquet file
        index_name: Name of the index

    Returns:
        Number of rows loaded

    Raises:
        Exception: If load fails
    """
    logger.info(
        "starting_index_constituent_clickhouse_load",
        parquet_file=parquet_file,
        index_name=index_name,
    )

    if not parquet_file:
        logger.warning("No parquet file to load", index_name=index_name)
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
        # Match the order of columns in the ClickHouse table
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
                    row["index_name"],
                    row["symbol"],
                    row.get("isin"),
                    row.get("company_name"),
                    row["effective_date"],
                    row["action"],
                    row.get("weight"),
                    row.get("free_float_market_cap"),
                    row.get("shares_for_index"),
                    row.get("announcement_date"),
                    row.get("index_category"),
                    row.get("sector"),
                    row.get("industry"),
                    row.get("metadata") if row.get("metadata") is not None else {},
                )
            )

        # Insert data
        client.insert(
            "index_constituent",
            data,
            column_names=[
                "event_id",
                "event_time",
                "ingest_time",
                "source",
                "schema_version",
                "entity_id",
                "index_name",
                "symbol",
                "isin",
                "company_name",
                "effective_date",
                "action",
                "weight",
                "free_float_market_cap",
                "shares_for_index",
                "announcement_date",
                "index_category",
                "sector",
                "industry",
                "metadata",
            ],
        )

        rows_loaded = len(data)

        logger.info(
            "index_constituent_clickhouse_load_complete",
            parquet_file=parquet_file,
            index_name=index_name,
            rows_loaded=rows_loaded,
        )

        return rows_loaded

    except Exception as e:
        logger.error(
            "index_constituent_clickhouse_load_failed",
            parquet_file=parquet_file,
            error=str(e),
        )
        raise
