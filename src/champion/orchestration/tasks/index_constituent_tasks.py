from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import polars as pl

from champion.config import config
from champion.parsers.index_constituent_parser import IndexConstituentParser
from champion.scrapers.nse.index_constituent import IndexConstituentScraper


def scrape_index_constituents(
    indices: List[str] | None = None,
    output_dir: Path | None = None,
) -> Dict[str, Path]:
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
) -> List[Dict[str, Any]]:
    """Parse index constituent JSON into event dictionaries."""
    eff_date = (
        effective_date if isinstance(effective_date, date) else date.fromisoformat(effective_date)
    )
    parser = IndexConstituentParser()
    return parser.parse(file_path=Path(file_path), index_name=index_name, effective_date=eff_date, action=action)


def write_index_constituents_parquet(
    events: List[Dict[str, Any]],
    index_name: str,
    effective_date: str | date,
    output_base_path: str | Path = "data/lake",
) -> str:
    """Write events to a partitioned Parquet path and return file path."""
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

    df = pl.DataFrame(events)
    # Cast common types
    to_write = df.with_columns([
        pl.col("event_time").cast(pl.Datetime, strict=False),
        pl.col("ingest_time").cast(pl.Datetime, strict=False),
    ])
    to_write = to_write.drop([c for c in ("index", "year", "month", "day") if c in to_write.columns])
    to_write.write_parquet(out_file, compression="snappy")
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
    except Exception:
        return 0
