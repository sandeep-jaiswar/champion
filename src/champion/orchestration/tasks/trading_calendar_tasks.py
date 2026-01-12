from __future__ import annotations

from pathlib import Path

import polars as pl
import structlog

from champion.parsers.trading_calendar_parser import TradingCalendarParser
from champion.scrapers.nse.trading_calendar import TradingCalendarScraper

logger = structlog.get_logger()


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
    to_write = df.drop([c for c in ("year",) if c in df.columns])
    to_write.write_parquet(out_file, compression="snappy")
    return str(out_file)


def load_trading_calendar_clickhouse(parquet_path: str | Path) -> int:
    """Stub loader: return row count from Parquet file."""
    try:
        df = pl.read_parquet(str(parquet_path))
        return len(df)
    except (FileNotFoundError, OSError) as e:
        logger.error("parquet_read_failed", error=str(e), path=str(parquet_path), retryable=True)
        return 0
    except ValueError as e:
        logger.error(
            "parquet_invalid_format", error=str(e), path=str(parquet_path), retryable=False
        )
        return 0
    except Exception as e:
        logger.critical(
            "fatal_parquet_read_error", error=str(e), path=str(parquet_path), retryable=False
        )
        return 0
