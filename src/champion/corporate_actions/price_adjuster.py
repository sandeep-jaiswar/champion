"""Price adjuster for applying corporate action adjustments to OHLC data.

This module applies computed adjustment factors to historical OHLC prices
to ensure price continuity across corporate action events.
"""

from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl


def apply_ca_adjustments(
    ohlc_df: pl.DataFrame,
    ca_factors: pl.DataFrame,
    price_columns: Optional[list[str]] = None,
) -> pl.DataFrame:
    """Apply corporate action adjustments to OHLC prices.

    This function:
    1. Joins OHLC data with CA adjustment factors by symbol
    2. For each date, applies all CA adjustments from future dates
    3. Multiplies historical prices by cumulative adjustment factor

    Args:
        ohlc_df: DataFrame with OHLC data
            Required columns: symbol, trade_date, open, high, low, close
        ca_factors: DataFrame with adjustment factors
            Required columns: symbol, ex_date, cumulative_factor
        price_columns: List of price columns to adjust
            Defaults to ["open", "high", "low", "close", "prev_close", "settlement_price"]

    Returns:
        DataFrame with adjusted prices and adjustment metadata
            Added columns: adjustment_factor, adjustment_date
    """
    if price_columns is None:
        price_columns = [
            "OpnPric",
            "HghPric",
            "LwPric",
            "ClsPric",
            "PrvsClsgPric",
            "SttlmPric",
        ]

    # Handle empty DataFrames
    if ohlc_df.is_empty():
        return ohlc_df.with_columns(
            [
                pl.lit(1.0).alias("adjustment_factor"),
                pl.lit(None, dtype=pl.Date).alias("adjustment_date"),
            ]
        )

    if ca_factors.is_empty():
        # No adjustments to apply
        return ohlc_df.with_columns(
            [
                pl.lit(1.0).alias("adjustment_factor"),
                pl.lit(None, dtype=pl.Date).alias("adjustment_date"),
            ]
        )

    # Ensure TradDt is the column name (NSE format)
    trade_date_col = "TradDt" if "TradDt" in ohlc_df.columns else "trade_date"
    symbol_col = "TckrSymb" if "TckrSymb" in ohlc_df.columns else "symbol"

    # Join OHLC with CA factors
    # For each OHLC record, find all CA events that happened AFTER that date
    # and apply cumulative adjustment
    result = ohlc_df.join(
        ca_factors.select(
            [
                pl.col("symbol"),
                pl.col("ex_date"),
                pl.col("cumulative_factor"),
            ]
        ),
        left_on=symbol_col,
        right_on="symbol",
        how="left",
    )

    # Filter to only CA events after the trade date and compute adjustment
    # Apply the cumulative factor if ex_date > trade_date
    result = result.with_columns(
        [
            pl.when(pl.col("ex_date") > pl.col(trade_date_col))
            .then(pl.col("cumulative_factor"))
            .otherwise(pl.lit(1.0))
            .alias("ca_adjustment")
        ]
    )

    # Group by symbol and trade_date, take the product of all adjustments
    # (in case there are multiple CA events in the future)
    adjustment_agg = (
        result.group_by([symbol_col, trade_date_col])
        .agg(
            [
                pl.col("ca_adjustment").product().alias("adjustment_factor"),
                pl.col("ex_date").max().alias("adjustment_date"),
            ]
        )
    )

    # Join back to original OHLC
    adjusted = ohlc_df.join(
        adjustment_agg,
        on=[symbol_col, trade_date_col],
        how="left",
    )

    # Fill null adjustments with 1.0 (no adjustment)
    adjusted = adjusted.with_columns(
        [
            pl.col("adjustment_factor").fill_null(1.0),
        ]
    )

    # Apply adjustment to price columns
    # Divide prices by adjustment factor (since factor represents split/bonus)
    for col in price_columns:
        if col in adjusted.columns:
            adjusted = adjusted.with_columns(
                [
                    (pl.col(col) / pl.col("adjustment_factor")).alias(col)
                ]
            )

    return adjusted


def apply_ca_adjustments_simple(
    ohlc_df: pl.DataFrame,
    ca_factors: pl.DataFrame,
) -> pl.DataFrame:
    """Simplified CA adjustment that processes data chronologically.

    This approach:
    1. Sorts data by symbol and date
    2. For each symbol, applies adjustments going backward in time
    3. More efficient for large datasets

    Args:
        ohlc_df: DataFrame with OHLC data
        ca_factors: DataFrame with adjustment factors

    Returns:
        DataFrame with adjusted prices
    """
    if ohlc_df.is_empty() or ca_factors.is_empty():
        return ohlc_df.with_columns(
            [
                pl.lit(1.0).alias("adjustment_factor"),
                pl.lit(None, dtype=pl.Date).alias("adjustment_date"),
            ]
        )

    # Identify column names
    trade_date_col = "TradDt" if "TradDt" in ohlc_df.columns else "trade_date"
    symbol_col = "TckrSymb" if "TckrSymb" in ohlc_df.columns else "symbol"

    # Price columns to adjust
    price_cols = []
    for col in [
        "OpnPric",
        "HghPric",
        "LwPric",
        "ClsPric",
        "PrvsClsgPric",
        "SttlmPric",
    ]:
        if col in ohlc_df.columns:
            price_cols.append(col)

    # Create a mapping of ex_date to adjustment factor per symbol
    # This will be used to look up adjustments
    ca_lookup = ca_factors.select(
        [
            pl.col("symbol"),
            pl.col("ex_date"),
            pl.col("adjustment_factor"),
        ]
    )

    # Join OHLC with CA factors on symbol
    # Use asof join to find the most recent CA event for each date
    result = ohlc_df.sort([symbol_col, trade_date_col]).join_asof(
        ca_lookup.sort(["symbol", "ex_date"]),
        left_on=trade_date_col,
        right_on="ex_date",
        by_left=symbol_col,
        by_right="symbol",
        strategy="forward",  # Find next CA event
    )

    # Compute cumulative adjustment by taking product of all factors
    # from current date forward
    result = result.with_columns(
        [
            pl.col("adjustment_factor").fill_null(1.0),
            pl.col("ex_date").alias("adjustment_date"),
        ]
    )

    # Apply adjustments to price columns
    for col in price_cols:
        result = result.with_columns(
            [(pl.col(col) / pl.col("adjustment_factor")).alias(col)]
        )

    return result


def write_adjusted_ohlc(
    adjusted_df: pl.DataFrame,
    output_path: Path,
    trade_date: date,
) -> Path:
    """Write adjusted OHLC data to partitioned Parquet.

    Args:
        adjusted_df: DataFrame with adjusted OHLC data
        output_path: Base path for output
        trade_date: Trading date for partitioning

    Returns:
        Path to written file
    """
    # Create partition path: year=YYYY/month=MM/day=DD
    partition_path = (
        output_path
        / "normalized"
        / "equity_ohlc"
        / f"year={trade_date.year}"
        / f"month={trade_date.month:02d}"
        / f"day={trade_date.day:02d}"
    )
    partition_path.mkdir(parents=True, exist_ok=True)

    # Write to Parquet
    output_file = partition_path / "data.parquet"
    adjusted_df.write_parquet(
        output_file,
        compression="snappy",
        use_pyarrow=True,
    )

    return output_file
