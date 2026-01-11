from __future__ import annotations

from pathlib import Path

import polars as pl

from champion.scrapers.nse.trading_calendar import TradingCalendarScraper
from champion.parsers.trading_calendar_parser import TradingCalendarParser


def scrape_trading_calendar(year: int) -> Path:
    """Scrape NSE trading calendar JSON for a year."""
    scraper = TradingCalendarScraper()
    return scraper.scrape(year=year)


def parse_trading_calendar(json_path: str | Path, year: int) -> pl.DataFrame:
    """Parse trading calendar JSON into a DataFrame."""
    parser = TradingCalendarParser()
    return parser.parse(Path(json_path), year)


def write_trading_calendar_parquet(df: pl.DataFrame, year: int) -> str:
    """Write calendar DataFrame to Parquet and return file path."""
    out_dir = Path("data/lake/trading_calendar") / f"year={year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"calendar_{year}.parquet"
    df.write_parquet(out_file, compression="snappy")
    return str(out_file)


def load_trading_calendar_clickhouse(parquet_path: str | Path) -> int:
    """Stub loader: return row count from Parquet file."""
    try:
        df = pl.read_parquet(str(parquet_path))
        return len(df)
    except Exception:
        return 0
