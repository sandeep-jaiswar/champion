"""
Technical Indicators Implementation using Polars.

This module provides functions to compute technical indicators from
normalized OHLC data. All indicators are computed using efficient
windowed operations in Polars.
"""

from pathlib import Path

import polars as pl
import structlog

logger = structlog.get_logger()


def compute_sma(
    df: pl.DataFrame,
    column: str = "close",
    windows: list[int] = None,
) -> pl.DataFrame:
    """
    Compute Simple Moving Average (SMA) for specified windows.

    Args:
        df: DataFrame with OHLC data (must have 'symbol' and 'trade_date' columns)
        column: Column to compute SMA on (default: 'close')
        windows: List of window sizes (default: [5, 20])

    Returns:
        DataFrame with added SMA columns (e.g., 'sma_5', 'sma_20')

    Example:
        >>> df = pl.DataFrame({
        ...     'symbol': ['AAPL'] * 30,
        ...     'trade_date': pl.date_range(date(2024, 1, 1), date(2024, 1, 30), "1d"),
        ...     'close': [150.0] * 30
        ... })
        >>> df_sma = compute_sma(df, column='close', windows=[5, 20])
    """
    if windows is None:
        windows = [5, 20]
    logger.info("Computing SMA", column=column, windows=windows)

    # Sort by symbol and date to ensure correct window calculations
    df = df.sort(["symbol", "trade_date"])

    # Compute SMA for each window size
    for window in windows:
        col_name = f"sma_{window}"
        df = df.with_columns(
            pl.col(column).rolling_mean(window_size=window).over("symbol").alias(col_name)
        )
        logger.debug(f"Computed {col_name}")

    return df


def compute_ema(
    df: pl.DataFrame,
    column: str = "close",
    windows: list[int] = None,
) -> pl.DataFrame:
    """
    Compute Exponential Moving Average (EMA) for specified windows.

    Args:
        df: DataFrame with OHLC data (must have 'symbol' and 'trade_date' columns)
        column: Column to compute EMA on (default: 'close')
        windows: List of window sizes (default: [12, 26])

    Returns:
        DataFrame with added EMA columns (e.g., 'ema_12', 'ema_26')

    Example:
        >>> df = pl.DataFrame({
        ...     'symbol': ['AAPL'] * 30,
        ...     'trade_date': pl.date_range(date(2024, 1, 1), date(2024, 1, 30), "1d"),
        ...     'close': [150.0] * 30
        ... })
        >>> df_ema = compute_ema(df, column='close', windows=[12, 26])
    """
    if windows is None:
        windows = [12, 26]
    logger.info("Computing EMA", column=column, windows=windows)

    # Sort by symbol and date
    df = df.sort(["symbol", "trade_date"])

    # Compute EMA for each window size
    for window in windows:
        col_name = f"ema_{window}"
        # EMA uses exponential weighting
        # alpha = 2 / (window + 1)
        df = df.with_columns(
            pl.col(column).ewm_mean(span=window, adjust=False).over("symbol").alias(col_name)
        )
        logger.debug(f"Computed {col_name}")

    return df


def compute_rsi(
    df: pl.DataFrame,
    column: str = "close",
    window: int = 14,
) -> pl.DataFrame:
    """
    Compute Relative Strength Index (RSI).

    RSI = 100 - (100 / (1 + RS))
    where RS = Average Gain / Average Loss over the window period

    Args:
        df: DataFrame with OHLC data (must have 'symbol' and 'trade_date' columns)
        column: Column to compute RSI on (default: 'close')
        window: Window size for RSI calculation (default: 14)

    Returns:
        DataFrame with added RSI column (e.g., 'rsi_14')

    Example:
        >>> df = pl.DataFrame({
        ...     'symbol': ['AAPL'] * 30,
        ...     'trade_date': pl.date_range(date(2024, 1, 1), date(2024, 1, 30), "1d"),
        ...     'close': [150.0 + i for i in range(30)]
        ... })
        >>> df_rsi = compute_rsi(df, column='close', window=14)
    """
    logger.info("Computing RSI", column=column, window=window)

    # Sort by symbol and date
    df = df.sort(["symbol", "trade_date"])

    col_name = f"rsi_{window}"

    # Calculate price changes
    df = df.with_columns(pl.col(column).diff().over("symbol").alias("price_change"))

    # Separate gains and losses
    df = df.with_columns(
        [
            pl.when(pl.col("price_change") > 0)
            .then(pl.col("price_change"))
            .otherwise(0.0)
            .alias("gain"),
            pl.when(pl.col("price_change") < 0)
            .then(-pl.col("price_change"))
            .otherwise(0.0)
            .alias("loss"),
        ]
    )

    # Calculate average gain and loss using EMA
    df = df.with_columns(
        [
            pl.col("gain").ewm_mean(span=window, adjust=False).over("symbol").alias("avg_gain"),
            pl.col("loss").ewm_mean(span=window, adjust=False).over("symbol").alias("avg_loss"),
        ]
    )

    # Calculate RS and RSI
    df = df.with_columns(
        pl.when(pl.col("avg_loss") != 0)
        .then(pl.col("avg_gain") / pl.col("avg_loss"))
        .otherwise(None)
        .alias("rs")
    )

    df = df.with_columns(
        pl.when(pl.col("rs").is_not_null())
        .then(100.0 - (100.0 / (1.0 + pl.col("rs"))))
        .otherwise(None)
        .alias(col_name)
    )

    # Drop intermediate columns
    df = df.drop(["price_change", "gain", "loss", "avg_gain", "avg_loss", "rs"])

    logger.debug(f"Computed {col_name}")

    return df


def compute_features(
    df: pl.DataFrame,
    output_path: str | Path | None = None,
    sma_windows: list[int] = None,
    ema_windows: list[int] = None,
    rsi_window: int = 14,
    partition_cols: list[str] | None = None,
) -> pl.DataFrame:
    """
    Compute all technical indicators and optionally write to Parquet.

    This is the main function that computes SMA, EMA, and RSI indicators
    from normalized OHLC data and writes the features dataset.

    Args:
        df: Normalized OHLC DataFrame with columns:
            - symbol: Stock symbol
            - trade_date: Trading date
            - close: Close price
            - (and other OHLC columns)
        output_path: Optional path to write Parquet files
        sma_windows: Windows for SMA calculation
        ema_windows: Windows for EMA calculation
        rsi_window: Window for RSI calculation
        partition_cols: Optional partition columns for Parquet output

    Returns:
        DataFrame with all computed features

    Example:
        >>> normalized_df = pl.read_parquet('data/lake/normalized/equity_ohlc/**/*.parquet')
        >>> features_df = compute_features(
        ...     df=normalized_df,
        ...     output_path='data/lake/features/equity',
        ...     partition_cols=['trade_date']
        ... )
    """
    if ema_windows is None:
        ema_windows = [12, 26]
    if sma_windows is None:
        sma_windows = [5, 20]
    logger.info(
        "Computing features",
        rows=len(df),
        sma_windows=sma_windows,
        ema_windows=ema_windows,
        rsi_window=rsi_window,
    )

    # Ensure required columns exist
    required_cols = ["symbol", "trade_date", "close"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Compute all indicators
    df = compute_sma(df, column="close", windows=sma_windows)
    df = compute_ema(df, column="close", windows=ema_windows)
    df = compute_rsi(df, column="close", window=rsi_window)

    # Add metadata columns
    df = df.with_columns(
        [
            pl.lit("v1").alias("feature_version"),
            pl.col("trade_date").cast(pl.Datetime).dt.timestamp("ms").alias("feature_timestamp"),
        ]
    )

    # Select only relevant columns for features table
    feature_cols = ["symbol", "trade_date", "feature_timestamp", "feature_version"]

    # Add all computed SMA columns
    feature_cols.extend([f"sma_{w}" for w in sma_windows])

    # Add all computed EMA columns
    feature_cols.extend([f"ema_{w}" for w in ema_windows])

    # Add RSI column
    feature_cols.append(f"rsi_{rsi_window}")

    # Keep only feature columns that exist in the dataframe
    available_cols = [col for col in feature_cols if col in df.columns]
    df_features = df.select(available_cols)

    logger.info("Features computed", total_features=len(available_cols) - 4)

    # Write to Parquet if output path is provided
    if output_path:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info("Writing features to Parquet", path=str(output_path))

        # Convert to Arrow table for partitioned writes
        import pyarrow.parquet as pq

        arrow_table = df_features.to_arrow()

        if partition_cols:
            pq.write_to_dataset(
                arrow_table,
                root_path=str(output_path),
                partition_cols=partition_cols,
                compression="snappy",
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            output_file = output_path / "features.parquet"
            pq.write_table(arrow_table, output_file, compression="snappy")

        logger.info("Features written successfully", path=str(output_path))

    return df_features
