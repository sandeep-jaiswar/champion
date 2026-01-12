from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import structlog

from champion.scrapers.nse.mospi_macro import MOSPIMacroScraper
from champion.scrapers.nse.rbi_macro import RBIMacroScraper
from champion.storage.parquet_io import write_df_safe

logger = structlog.get_logger()


def scrape_rbi_macro(
    start_date: datetime, end_date: datetime, indicators: list[str] | None = None
) -> Path:
    scraper = RBIMacroScraper()
    return scraper.scrape(start_date=start_date, end_date=end_date, indicators=indicators)


def scrape_mospi_macro(
    start_date: datetime, end_date: datetime, indicators: list[str] | None = None
) -> Path:
    scraper = MOSPIMacroScraper()
    return scraper.scrape(start_date=start_date, end_date=end_date, indicators=indicators)


def fetch_macro_source(
    source: str,
    start_date: datetime,
    end_date: datetime,
    rbi_indicators: list[str] | None,
    mospi_indicators: list[str] | None,
) -> tuple[pl.DataFrame, str | None]:
    """Fetch and parse macro data for a given source.

    Returns a tuple of (DataFrame, json_path or None).
    Unknown sources return an empty DataFrame and None.
    """
    source_upper = source.strip().upper()

    if source_upper == "MOSPI":
        json_path = scrape_mospi_macro(start_date, end_date, mospi_indicators)
        return parse_macro_indicators(json_path), json_path

    if source_upper == "RBI":
        json_path = scrape_rbi_macro(start_date, end_date, rbi_indicators)
        return parse_macro_indicators(json_path), json_path

    if source_upper == "DEA":
        logger.warning("dea_source_not_implemented", source=source)
        return pl.DataFrame(), None

    if source_upper == "NITI AAYOG" or source_upper == "NITI" or source_upper == "NITI AAYOG":
        logger.warning("niti_source_not_implemented", source=source)
        return pl.DataFrame(), None

    logger.warning("unknown_macro_source", source=source)
    return pl.DataFrame(), None


def try_sources_in_order(
    source_order: list[str],
    start_date: datetime,
    end_date: datetime,
    rbi_indicators: list[str] | None,
    mospi_indicators: list[str] | None,
) -> tuple[pl.DataFrame, str | None]:
    """Try macro sources in order; return first non-empty DataFrame and chosen source."""
    for src in source_order:
        logger.info("macro_source_attempt", source=src)
        df, _ = fetch_macro_source(src, start_date, end_date, rbi_indicators, mospi_indicators)
        if df.height > 0:
            logger.info("macro_source_selected", source=src, rows=df.height)
            return df, src
        logger.warning("macro_source_empty", source=src)
    return pl.DataFrame(), None


def parse_macro_indicators(json_path: str | Path) -> pl.DataFrame:
    """Parse macro JSON produced by scrapers into a uniform DataFrame."""
    import json

    p = Path(json_path)
    with open(p) as f:
        data = json.load(f)

    indicators = data.get("indicators", [])
    if not indicators:
        return pl.DataFrame()

    # Flatten indicator series
    rows = []
    for ind in indicators:
        code = ind.get("code")
        name = ind.get("name")
        category = ind.get("category")
        unit = ind.get("unit")
        series = ind.get("series", [])
        for s in series:
            rows.append(
                {
                    "indicator_code": code,
                    "indicator_name": name,
                    "category": category,
                    "unit": unit,
                    "date": s.get("date"),
                    "value": s.get("value"),
                    "source": data.get("source"),
                }
            )

    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(rows)
    # Cast where possible
    return df.with_columns(
        [
            pl.col("date").str.strptime(pl.Date, strict=False),
            pl.col("value").cast(pl.Float64, strict=False),
        ]
    )


def merge_macro_dataframes(dfs: list[pl.DataFrame]) -> pl.DataFrame:
    """Merge multiple macro DataFrames into one."""
    if not dfs:
        return pl.DataFrame()
    df = pl.concat(dfs, how="vertical", rechunk=True)
    return df


def write_macro_parquet(df: pl.DataFrame, start_date: datetime, end_date: datetime) -> str:
    """Write merged macro DataFrame to Parquet with validation; return path."""
    base_path = Path("data/lake")
    dataset = f"macro/start={start_date.strftime('%Y%m%d')}/end={end_date.strftime('%Y%m%d')}"
    
    logger.info(
        "writing_macro_parquet_with_validation",
        rows=len(df),
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )
    
    try:
        # Use write_df_safe with validation
        output_path = write_df_safe(
            df=df,
            dataset=dataset,
            base_path=base_path,
            schema_name="macro_indicators_jsonschema",
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
            "macro_validation_failed",
            error=str(e),
            rows=len(df),
        )
        raise


def load_macro_clickhouse(parquet_path: str | Path) -> int:
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
