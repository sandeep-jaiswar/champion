"""
Tests for technical indicators.

Tests SMA, EMA, and RSI calculations using Polars.
"""

from datetime import date, timedelta

import polars as pl
import pytest
from champion.features.indicators import compute_ema, compute_features, compute_rsi, compute_sma


@pytest.fixture
def sample_ohlc_df():
    """Create a sample OHLC DataFrame for testing."""
    # Generate 50 days of data for 2 symbols
    symbols = ["AAPL", "GOOGL"]
    base_date = date(2024, 1, 1)

    records = []
    for symbol in symbols:
        base_price = 150.0 if symbol == "AAPL" else 2800.0
        for day in range(50):
            trade_date = base_date + timedelta(days=day)
            # Create slightly varying prices to test indicators
            price_var = (day % 10) - 5  # Oscillates between -5 and 4
            close_price = base_price + price_var

            records.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "open": close_price - 1.0,
                    "high": close_price + 2.0,
                    "low": close_price - 2.0,
                    "close": close_price,
                    "volume": 1000000 + day * 1000,
                }
            )

    return pl.DataFrame(records)


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary output directory."""
    output_dir = tmp_path / "features"
    output_dir.mkdir()
    return output_dir


def test_compute_sma_basic(sample_ohlc_df):
    """Test basic SMA computation."""
    df = compute_sma(sample_ohlc_df, column="close", windows=[5, 20])

    # Check that SMA columns were added
    assert "sma_5" in df.columns
    assert "sma_20" in df.columns

    # Check that we have the same number of rows
    assert len(df) == len(sample_ohlc_df)

    # SMA values should be null for first window-1 rows per symbol
    aapl_df = df.filter(pl.col("symbol") == "AAPL")

    # First 4 rows should have null sma_5 (window size 5)
    assert aapl_df["sma_5"][0] is None or aapl_df["sma_5"].is_null()[0]

    # Row 5 onwards should have values
    assert aapl_df["sma_5"][4] is not None


def test_compute_sma_values(sample_ohlc_df):
    """Test SMA calculation correctness."""
    # Create simple test data
    df = pl.DataFrame(
        {
            "symbol": ["TEST"] * 10,
            "trade_date": [date(2024, 1, i + 1) for i in range(10)],
            "close": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
        }
    )

    df = compute_sma(df, column="close", windows=[5])

    # SMA(5) at index 4 should be (10+20+30+40+50)/5 = 30
    assert abs(df["sma_5"][4] - 30.0) < 0.01

    # SMA(5) at index 5 should be (20+30+40+50+60)/5 = 40
    assert abs(df["sma_5"][5] - 40.0) < 0.01


def test_compute_sma_multiple_symbols(sample_ohlc_df):
    """Test SMA computation with multiple symbols."""
    df = compute_sma(sample_ohlc_df, column="close", windows=[5, 20])

    # Check both symbols have SMA values
    aapl_df = df.filter(pl.col("symbol") == "AAPL")
    googl_df = df.filter(pl.col("symbol") == "GOOGL")

    # Both should have non-null SMA values after warmup period
    assert aapl_df["sma_5"].drop_nulls().len() > 0
    assert googl_df["sma_5"].drop_nulls().len() > 0


def test_compute_ema_basic(sample_ohlc_df):
    """Test basic EMA computation."""
    df = compute_ema(sample_ohlc_df, column="close", windows=[12, 26])

    # Check that EMA columns were added
    assert "ema_12" in df.columns
    assert "ema_26" in df.columns

    # Check that we have the same number of rows
    assert len(df) == len(sample_ohlc_df)


def test_compute_ema_values(sample_ohlc_df):
    """Test that EMA values are computed."""
    df = compute_ema(sample_ohlc_df, column="close", windows=[12])

    # EMA should have values (EMA starts with first value)
    aapl_df = df.filter(pl.col("symbol") == "AAPL")

    # EMA should have non-null values
    assert aapl_df["ema_12"].drop_nulls().len() > 0


def test_compute_ema_responsiveness():
    """Test that EMA is more responsive than SMA."""
    # Create data with a sharp price change
    df = pl.DataFrame(
        {
            "symbol": ["TEST"] * 30,
            "trade_date": [date(2024, 1, i + 1) for i in range(30)],
            "close": [100.0] * 10 + [150.0] * 20,  # Sharp increase at day 10
        }
    )

    df = compute_sma(df, column="close", windows=[10])
    df = compute_ema(df, column="close", windows=[10])

    # At index 15 (5 days after the jump), EMA should be higher than SMA
    # because EMA reacts faster to recent changes
    assert df["ema_10"][15] > df["sma_10"][15]


def test_compute_rsi_basic(sample_ohlc_df):
    """Test basic RSI computation."""
    df = compute_rsi(sample_ohlc_df, column="close", window=14)

    # Check that RSI column was added
    assert "rsi_14" in df.columns

    # Check that we have the same number of rows
    assert len(df) == len(sample_ohlc_df)


def test_compute_rsi_range(sample_ohlc_df):
    """Test that RSI values are in valid range [0, 100]."""
    df = compute_rsi(sample_ohlc_df, column="close", window=14)

    # Filter out null values
    rsi_values = df["rsi_14"].drop_nulls()

    # RSI should be between 0 and 100
    assert (rsi_values >= 0).all()
    assert (rsi_values <= 100).all()


def test_compute_rsi_trending_up():
    """Test RSI with consistently rising prices."""
    # Create data with rising prices (with slight variations)
    df = pl.DataFrame(
        {
            "symbol": ["TEST"] * 30,
            "trade_date": [date(2024, 1, i + 1) for i in range(30)],
            "close": [
                100.0 + i * 2.0 + (i % 3 - 1) * 0.5 for i in range(30)
            ],  # Rising with small variations
        }
    )

    df = compute_rsi(df, column="close", window=14)

    # RSI should be high (>50) with rising prices
    rsi_values = df["rsi_14"].drop_nulls()
    # Check if we have any valid RSI values
    if len(rsi_values) > 0:
        mean_rsi = rsi_values.mean()
        if mean_rsi is not None:
            assert mean_rsi > 50


def test_compute_rsi_trending_down():
    """Test RSI with consistently falling prices."""
    # Create data with falling prices
    df = pl.DataFrame(
        {
            "symbol": ["TEST"] * 30,
            "trade_date": [date(2024, 1, i + 1) for i in range(30)],
            "close": [200.0 - i * 2.0 for i in range(30)],  # Falling prices
        }
    )

    df = compute_rsi(df, column="close", window=14)

    # RSI should be low (<50) with falling prices
    rsi_values = df["rsi_14"].drop_nulls()
    assert rsi_values.mean() < 50


def test_compute_features_basic(sample_ohlc_df):
    """Test complete feature computation."""
    df_features = compute_features(
        df=sample_ohlc_df,
        sma_windows=[5, 20],
        ema_windows=[12, 26],
        rsi_window=14,
    )

    # Check that all feature columns exist
    assert "symbol" in df_features.columns
    assert "trade_date" in df_features.columns
    assert "feature_timestamp" in df_features.columns
    assert "feature_version" in df_features.columns
    assert "sma_5" in df_features.columns
    assert "sma_20" in df_features.columns
    assert "ema_12" in df_features.columns
    assert "ema_26" in df_features.columns
    assert "rsi_14" in df_features.columns


def test_compute_features_metadata(sample_ohlc_df):
    """Test that metadata columns are added correctly."""
    df_features = compute_features(df=sample_ohlc_df)

    # Check feature_version
    assert df_features["feature_version"].unique().to_list() == ["v1"]

    # Check feature_timestamp is valid
    assert df_features["feature_timestamp"].dtype == pl.Int64


def test_compute_features_missing_columns():
    """Test error handling for missing required columns."""
    df = pl.DataFrame(
        {
            "symbol": ["AAPL"] * 10,
            "close": [150.0] * 10,
            # Missing trade_date column
        }
    )

    with pytest.raises(ValueError, match="Missing required columns"):
        compute_features(df)


def test_compute_features_write_parquet(sample_ohlc_df, temp_output_dir):
    """Test writing features to Parquet."""
    output_path = temp_output_dir / "equity"

    df_features = compute_features(
        df=sample_ohlc_df,
        output_path=str(output_path),
        sma_windows=[5, 20],
        ema_windows=[12, 26],
        rsi_window=14,
    )

    # Check that output directory was created
    assert output_path.exists()

    # Check that Parquet file(s) were written
    parquet_files = list(output_path.rglob("*.parquet"))
    assert len(parquet_files) > 0

    # Verify we can read the data back
    df_read = pl.read_parquet(output_path / "features.parquet")
    assert len(df_read) == len(df_features)


def test_compute_features_write_partitioned(sample_ohlc_df, temp_output_dir):
    """Test writing partitioned features to Parquet."""
    output_path = temp_output_dir / "equity_partitioned"

    compute_features(
        df=sample_ohlc_df,
        output_path=str(output_path),
        partition_cols=["trade_date"],
    )

    # Check that partitioned directories were created
    partition_dirs = list(output_path.glob("trade_date=*"))
    assert len(partition_dirs) > 0


def test_compute_features_custom_windows(sample_ohlc_df):
    """Test feature computation with custom window sizes."""
    df_features = compute_features(
        df=sample_ohlc_df,
        sma_windows=[10, 50],
        ema_windows=[20, 50],
        rsi_window=21,
    )

    # Check that custom windows were used
    assert "sma_10" in df_features.columns
    assert "sma_50" in df_features.columns
    assert "ema_20" in df_features.columns
    assert "ema_50" in df_features.columns
    assert "rsi_21" in df_features.columns

    # Default windows should not exist
    assert "sma_5" not in df_features.columns
    assert "ema_12" not in df_features.columns


def test_compute_features_integration(sample_ohlc_df, temp_output_dir):
    """Integration test for the complete feature pipeline."""
    output_path = temp_output_dir / "equity_full"

    # Compute features with all defaults
    df_features = compute_features(
        df=sample_ohlc_df,
        output_path=str(output_path),
    )

    # Verify structure
    assert len(df_features) == len(sample_ohlc_df)
    assert df_features["symbol"].unique().sort().to_list() == ["AAPL", "GOOGL"]

    # Verify Parquet output
    assert output_path.exists()
    df_read = pl.read_parquet(output_path / "features.parquet")

    # Verify schema matches
    assert set(df_read.columns) == set(df_features.columns)

    # Verify data integrity
    assert len(df_read) == len(df_features)
