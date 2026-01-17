"""
Integration tests for feature computation pipeline.

Tests the complete flow:
1. Compute technical indicators (SMA, EMA, RSI)
2. Aggregate by timeframes
3. Validate calculations
4. Write to features layer
"""

from datetime import date

import polars as pl
import pytest
from champion.features.indicators import compute_ema, compute_features, compute_rsi, compute_sma

from tests.fixtures.sample_data import create_sample_ohlc_data


@pytest.fixture
def sample_normalized_data():
    """Fixture providing normalized OHLC data for feature computation."""
    return create_sample_ohlc_data(
        symbols=["RELIANCE", "TCS", "INFY"], start_date=date(2024, 1, 1), num_days=60
    )


@pytest.fixture
def features_output_dir(tmp_path):
    """Create a temporary directory for features output."""
    features_dir = tmp_path / "features"
    features_dir.mkdir()
    return features_dir


class TestFeatureComputation:
    """Integration tests for feature computation pipeline."""

    def test_compute_sma_indicators(self, sample_normalized_data):
        """Test computing SMA indicators for multiple windows."""
        df = compute_sma(sample_normalized_data, column="close", windows=[5, 20, 50])

        # Verify SMA columns were added
        assert "sma_5" in df.columns
        assert "sma_20" in df.columns
        assert "sma_50" in df.columns

        # Verify no data loss
        assert len(df) == len(sample_normalized_data)

        # Verify SMA values are reasonable
        for symbol in df["symbol"].unique().to_list():
            symbol_df = df.filter(pl.col("symbol") == symbol)

            # SMA should have null values for first (window-1) rows
            # and non-null values after that
            sma_5_values = symbol_df["sma_5"].drop_nulls()
            assert len(sma_5_values) > 0

    def test_compute_ema_indicators(self, sample_normalized_data):
        """Test computing EMA indicators for multiple windows."""
        df = compute_ema(sample_normalized_data, column="close", windows=[12, 26, 50])

        # Verify EMA columns were added
        assert "ema_12" in df.columns
        assert "ema_26" in df.columns
        assert "ema_50" in df.columns

        # Verify no data loss
        assert len(df) == len(sample_normalized_data)

        # EMA should have values (starts from first value)
        for symbol in df["symbol"].unique().to_list():
            symbol_df = df.filter(pl.col("symbol") == symbol)
            ema_values = symbol_df["ema_12"].drop_nulls()
            assert len(ema_values) > 0

    def test_compute_rsi_indicator(self, sample_normalized_data):
        """Test computing RSI indicator."""
        df = compute_rsi(sample_normalized_data, column="close", window=14)

        # Verify RSI column was added
        assert "rsi_14" in df.columns

        # Verify no data loss
        assert len(df) == len(sample_normalized_data)

        # Verify RSI values are in valid range [0, 100]
        rsi_values = df["rsi_14"].drop_nulls()
        if len(rsi_values) > 0:
            assert (rsi_values >= 0).all()
            assert (rsi_values <= 100).all()

    def test_compute_all_features(self, sample_normalized_data):
        """Test computing all features together."""
        df_features = compute_features(
            df=sample_normalized_data,
            sma_windows=[5, 20],
            ema_windows=[12, 26],
            rsi_window=14,
        )

        # Verify all feature columns exist
        assert "sma_5" in df_features.columns
        assert "sma_20" in df_features.columns
        assert "ema_12" in df_features.columns
        assert "ema_26" in df_features.columns
        assert "rsi_14" in df_features.columns

        # Verify metadata columns
        assert "feature_version" in df_features.columns
        assert "feature_timestamp" in df_features.columns

        # Verify no data loss
        assert len(df_features) == len(sample_normalized_data)

    def test_feature_metadata(self, sample_normalized_data):
        """Test that feature metadata is added correctly."""
        df_features = compute_features(df=sample_normalized_data)

        # Check feature_version
        assert df_features["feature_version"].unique().to_list() == ["v1"]

        # Check feature_timestamp is valid (should be > 0)
        assert (df_features["feature_timestamp"] > 0).all()

    def test_write_features_to_parquet(self, sample_normalized_data, features_output_dir):
        """Test writing computed features to Parquet."""
        output_path = features_output_dir / "equity"

        df_features = compute_features(
            df=sample_normalized_data, output_path=str(output_path), partition_cols=None
        )

        # Verify output directory exists
        assert output_path.exists()

        # Verify Parquet file was created
        parquet_files = list(output_path.glob("*.parquet"))
        assert len(parquet_files) > 0

        # Read back and verify
        df_read = pl.read_parquet(parquet_files[0])
        assert len(df_read) == len(df_features)
        assert set(df_read.columns) == set(df_features.columns)

    def test_write_features_partitioned(self, sample_normalized_data, features_output_dir):
        """Test writing partitioned features to Parquet."""
        output_path = features_output_dir / "equity_partitioned"

        compute_features(
            df=sample_normalized_data, output_path=str(output_path), partition_cols=["trade_date"]
        )

        # Verify partitioned directories were created
        partition_dirs = list(output_path.glob("trade_date=*"))
        assert len(partition_dirs) > 0

        # Read back all partitions
        df_read = pl.read_parquet(output_path / "**/*.parquet")
        assert len(df_read) == len(sample_normalized_data)

    def test_features_per_symbol(self, sample_normalized_data):
        """Test that features are computed separately per symbol."""
        df_features = compute_features(df=sample_normalized_data, sma_windows=[5])

        # Verify each symbol has its own feature calculations
        for symbol in df_features["symbol"].unique().to_list():
            symbol_df = df_features.filter(pl.col("symbol") == symbol)

            # Each symbol should have SMA values
            sma_values = symbol_df["sma_5"].drop_nulls()
            assert len(sma_values) > 0

    def test_sma_calculation_accuracy(self):
        """Test SMA calculation accuracy with known values."""
        # Create simple test data with known SMA
        test_df = pl.DataFrame(
            {
                "symbol": ["TEST"] * 10,
                "trade_date": [date(2024, 1, i + 1) for i in range(10)],
                "close": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
            }
        )

        df_sma = compute_sma(test_df, column="close", windows=[5])

        # SMA(5) at index 4 should be (10+20+30+40+50)/5 = 30
        assert abs(df_sma["sma_5"][4] - 30.0) < 0.01

        # SMA(5) at index 9 should be (60+70+80+90+100)/5 = 80
        assert abs(df_sma["sma_5"][9] - 80.0) < 0.01

    def test_ema_responsiveness(self):
        """Test that EMA responds faster to price changes than SMA."""
        # Create data with price jump
        test_df = pl.DataFrame(
            {
                "symbol": ["TEST"] * 30,
                "trade_date": [date(2024, 1, i + 1) for i in range(30)],
                "close": [100.0] * 15 + [150.0] * 15,  # Sharp jump at day 15
            }
        )

        df = compute_sma(test_df, column="close", windows=[10])
        df = compute_ema(df, column="close", windows=[10])

        # After the jump, EMA should be higher than SMA (more responsive)
        # Check at day 20 (5 days after jump)
        if df["ema_10"][19] is not None and df["sma_10"][19] is not None:
            assert df["ema_10"][19] > df["sma_10"][19]

    def test_rsi_range_validation(self, sample_normalized_data):
        """Test that RSI values stay within valid range."""
        df = compute_rsi(sample_normalized_data, column="close", window=14)

        # Get all non-null RSI values
        rsi_values = df["rsi_14"].drop_nulls()

        # All RSI values should be between 0 and 100
        assert (rsi_values >= 0).all()
        assert (rsi_values <= 100).all()

    def test_multiple_timeframe_aggregation(self, sample_normalized_data):
        """Test aggregating features by multiple timeframes."""
        # Compute features
        df_features = compute_features(df=sample_normalized_data)

        # Add week number for aggregation
        df_features = df_features.with_columns(pl.col("trade_date").dt.week().alias("week_number"))

        # Aggregate by week
        weekly_features = df_features.group_by(["symbol", "week_number"]).agg(
            [
                pl.col("close").mean().alias("avg_close"),
                pl.col("sma_5").mean().alias("avg_sma_5"),
                pl.col("rsi_14").mean().alias("avg_rsi"),
            ]
        )

        # Verify weekly aggregation
        assert not weekly_features.is_empty()
        assert "avg_close" in weekly_features.columns
        assert "avg_sma_5" in weekly_features.columns

    def test_feature_consistency_across_runs(self, sample_normalized_data):
        """Test that feature computation is deterministic."""
        # Compute features twice
        df_features_1 = compute_features(df=sample_normalized_data, sma_windows=[5, 20])
        df_features_2 = compute_features(df=sample_normalized_data, sma_windows=[5, 20])

        # Features should be identical (excluding timestamp)
        cols_to_compare = ["symbol", "trade_date", "sma_5", "sma_20"]

        for col in cols_to_compare:
            if col in df_features_1.columns and col in df_features_2.columns:
                # Sort both dataframes for comparison
                df1_sorted = df_features_1.sort(["symbol", "trade_date"])
                df2_sorted = df_features_2.sort(["symbol", "trade_date"])

                assert df1_sorted[col].to_list() == df2_sorted[col].to_list()

    def test_missing_data_handling(self):
        """Test feature computation with missing data."""
        # Create data with gaps
        dates = [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 5), date(2024, 1, 6)]
        test_df = pl.DataFrame(
            {
                "symbol": ["TEST"] * len(dates),
                "trade_date": dates,
                "close": [100.0, 110.0, 120.0, 130.0],
            }
        )

        # Compute features
        df_features = compute_features(df=test_df, sma_windows=[3])

        # Should handle gaps gracefully
        assert not df_features.is_empty()
        assert len(df_features) == len(test_df)

    def test_large_dataset_performance(self):
        """Test feature computation performance with larger dataset."""
        # Create larger dataset
        large_data = create_sample_ohlc_data(
            symbols=["RELIANCE", "TCS", "INFY", "HDFC", "ICICIBANK"],
            start_date=date(2024, 1, 1),
            num_days=100,
        )

        # Compute features
        df_features = compute_features(
            df=large_data, sma_windows=[5, 20, 50], ema_windows=[12, 26], rsi_window=14
        )

        # Verify all data processed
        assert len(df_features) == len(large_data)
        assert len(df_features) == 500  # 5 symbols * 100 days

    def test_end_to_end_feature_pipeline(self, sample_normalized_data, features_output_dir):
        """Test complete feature computation pipeline end-to-end."""
        from champion.storage.parquet_io import write_df

        # Step 1: Start with normalized OHLC data
        assert not sample_normalized_data.is_empty()

        # Step 2: Compute all features
        df_features = compute_features(
            df=sample_normalized_data,
            sma_windows=[5, 20, 50],
            ema_windows=[12, 26],
            rsi_window=14,
        )

        # Verify features computed
        assert not df_features.is_empty()
        assert "sma_5" in df_features.columns
        assert "ema_12" in df_features.columns
        assert "rsi_14" in df_features.columns

        # Step 3: Write features to Parquet (partitioned)
        output_path = write_df(
            df=df_features,
            dataset="features/equity_indicators",
            base_path=features_output_dir,
            partitions=["trade_date"],
        )

        # Verify output
        assert output_path.exists()
        partition_dirs = list(output_path.glob("trade_date=*"))
        assert len(partition_dirs) > 0

        # Step 4: Read back and verify integrity
        df_read = pl.read_parquet(output_path / "**/*.parquet")
        assert len(df_read) == len(df_features)

        # Step 5: Verify feature quality
        # Check RSI range
        rsi_values = df_read["rsi_14"].drop_nulls()
        if len(rsi_values) > 0:
            assert (rsi_values >= 0).all()
            assert (rsi_values <= 100).all()

        # Check SMA values are reasonable
        sma_values = df_read["sma_5"].drop_nulls()
        close_values = df_read["close"]
        if len(sma_values) > 0:
            # SMA should be within reasonable range of close prices
            assert sma_values.min() > 0
            assert sma_values.max() < close_values.max() * 2

    def test_custom_window_configurations(self, sample_normalized_data):
        """Test feature computation with custom window sizes."""
        # Use non-standard windows
        df_features = compute_features(
            df=sample_normalized_data,
            sma_windows=[7, 14, 30],
            ema_windows=[9, 21],
            rsi_window=21,
        )

        # Verify custom windows were used
        assert "sma_7" in df_features.columns
        assert "sma_14" in df_features.columns
        assert "sma_30" in df_features.columns
        assert "ema_9" in df_features.columns
        assert "ema_21" in df_features.columns
        assert "rsi_21" in df_features.columns

        # Default windows should not exist
        assert "sma_5" not in df_features.columns
        assert "ema_12" not in df_features.columns

    def test_feature_version_tracking(self, sample_normalized_data):
        """Test that feature versions are tracked correctly."""
        df_features = compute_features(df=sample_normalized_data)

        # All features should have same version
        assert df_features["feature_version"].unique().to_list() == ["v1"]

        # All rows should have a version
        assert df_features["feature_version"].is_null().sum() == 0

    def test_incremental_feature_computation(self, tmp_path):
        """Test computing features incrementally for new data."""
        from champion.storage.parquet_io import write_df

        # Compute features for first batch
        batch1 = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 1), num_days=30
        )
        features1 = compute_features(df=batch1)

        # Write batch 1
        path1 = write_df(
            df=features1, dataset="features/batch1", base_path=tmp_path, partitions=None
        )

        # Compute features for second batch
        batch2 = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 2, 1), num_days=30
        )
        features2 = compute_features(df=batch2)

        # Write batch 2
        path2 = write_df(
            df=features2, dataset="features/batch2", base_path=tmp_path, partitions=None
        )

        # Verify both batches exist
        df1 = pl.read_parquet(path1 / "*.parquet")
        df2 = pl.read_parquet(path2 / "*.parquet")

        assert len(df1) == 30
        assert len(df2) == 30

        # Combine and verify
        combined = pl.concat([df1, df2])
        assert len(combined) == 60
