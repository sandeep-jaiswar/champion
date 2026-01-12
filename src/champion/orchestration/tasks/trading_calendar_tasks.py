from __future__ import annotations

from pathlib import Path

import polars as pl
import structlog

from champion.parsers.trading_calendar_parser import TradingCalendarParser
from champion.scrapers.nse.trading_calendar import TradingCalendarScraper
from champion.storage.parquet_io import write_df_safe

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
    """Write calendar DataFrame to Parquet with validation and return file path."""
    base_path = Path("data/lake")
    dataset = f"trading_calendar/year={year}"

    logger.info(
        "writing_trading_calendar_with_validation",
        rows=len(df),
        year=year,
    )

    try:
        # Use write_df_safe with validation
        output_path = write_df_safe(
            df=df,
            dataset=dataset,
            base_path=base_path,
            schema_name="trading_calendar",
            schema_dir="schemas/parquet",
            compression="snappy",
            fail_on_validation_errors=True,
            quarantine_dir="data/lake/quarantine",
        )

        # Return path to the parquet file
        out_file = output_path / "data.parquet"
        return str(out_file)

    except ValueError as e:
        logger.error(
            "trading_calendar_validation_failed",
            error=str(e),
            year=year,
        )
        raise


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
