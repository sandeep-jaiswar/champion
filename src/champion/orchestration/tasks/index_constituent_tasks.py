from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import structlog

from champion.parsers.index_constituent_parser import IndexConstituentParser
from champion.scrapers.nse.index_constituent import IndexConstituentScraper
from champion.utils.idempotency import (
    check_idempotency_marker,
    create_idempotency_marker,
    is_task_completed,
)

logger = structlog.get_logger()


def scrape_index_constituents(
    indices: list[str] | None = None,
    output_dir: Path | None = None,
) -> dict[str, Path]:
    """Scrape index constituents for given indices.

    Returns a mapping of index name to JSON file path.
    """
    with IndexConstituentScraper() as scraper:
        return scraper.scrape(indices=indices, output_dir=output_dir)


def parse_index_constituents(
    file_path: str | Path,
    index_name: str,
    effective_date: str | date,
    action: str = "ADD",
) -> list[dict[str, Any]]:
    """Parse index constituent JSON into event dictionaries."""
    eff_date = (
        effective_date if isinstance(effective_date, date) else date.fromisoformat(effective_date)
    )
    parser = IndexConstituentParser()
    return parser.parse(
        file_path=Path(file_path), index_name=index_name, effective_date=eff_date, action=action
    )


def write_index_constituents_parquet(
    events: list[dict[str, Any]],
    index_name: str,
    effective_date: str | date,
    output_base_path: str | Path = "data/lake",
) -> str:
    """Write events to a partitioned Parquet path and return file path.

    This function is idempotent: it checks for an existing marker file before writing.
    If the task has already completed successfully for the given index and date,
    it returns the path to the existing file without rewriting.

    Args:
        events: List of event dictionaries to write
        index_name: Name of the index
        effective_date: Effective date of constituents
        output_base_path: Base path for data lake

    Returns:
        Path to written Parquet file
    """
    if not events:
        return ""

    eff_date = (
        effective_date if isinstance(effective_date, date) else date.fromisoformat(effective_date)
    )

    base = Path(output_base_path)
    out_dir = (
        base
        / "reference"
        / "index_constituents"
        / f"index={index_name}"
        / f"year={eff_date.year}"
        / f"month={eff_date.month:02d}"
        / f"day={eff_date.day:02d}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{index_name.lower()}_{eff_date.strftime('%Y%m%d')}.parquet"

    # Check idempotency marker using date and index name as key
    date_key = f"{eff_date.isoformat()}-{index_name}"
    if is_task_completed(out_file, date_key):
        marker_data = check_idempotency_marker(out_file, date_key)
        logger.info(
            "index_constituents_already_written_skipping",
            output_file=str(out_file),
            index_name=index_name,
            effective_date=eff_date.isoformat(),
            rows=marker_data.get("rows", 0) if marker_data else 0,
        )
        return str(out_file)

    df = pl.DataFrame(events)
    # Cast common types
    to_write = df.with_columns(
        [
            pl.col("event_time").cast(pl.Datetime, strict=False),
            pl.col("ingest_time").cast(pl.Datetime, strict=False),
        ]
    )
    to_write = to_write.drop(
        [c for c in ("index", "year", "month", "day") if c in to_write.columns]
    )
    to_write.write_parquet(out_file, compression="snappy")

    # Create idempotency marker
    create_idempotency_marker(
        output_file=out_file,
        trade_date=date_key,
        rows=len(df),
        metadata={
            "index_name": index_name,
            "effective_date": eff_date.isoformat(),
            "table": "index_constituents",
        },
    )

    return str(out_file)


def load_index_constituents_clickhouse(
    parquet_file: str | Path,
    index_name: str,
) -> int:
    """Stub loader: return row count from Parquet file.

    Replace with actual ClickHouse load via ClickHouseLoader when configured.
    """
    try:
        df = pl.read_parquet(str(parquet_file))
        return len(df)
    except (FileNotFoundError, OSError) as e:
        logger.error(
            "parquet_read_failed",
            error=str(e),
            path=str(parquet_file),
            index=index_name,
            retryable=True,
        )
        return 0
    except ValueError as e:
        logger.error(
            "parquet_invalid_format",
            error=str(e),
            path=str(parquet_file),
            index=index_name,
            retryable=False,
        )
        return 0
    except Exception as e:
        logger.critical(
            "fatal_parquet_read_error",
            error=str(e),
            path=str(parquet_file),
            index=index_name,
            retryable=False,
        )
        return 0
