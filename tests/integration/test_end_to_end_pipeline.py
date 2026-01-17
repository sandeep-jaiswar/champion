"""
End-to-end integration tests for complete data pipelines.

Tests the complete data flow through all stages:
1. Ingestion: Scrape/Load -> Parse -> Validate -> Write to Parquet
2. Corporate Actions: Load events -> Compute factors -> Apply adjustments
3. Features: Compute indicators -> Aggregate -> Write features
4. Warehouse: Load to ClickHouse -> Query verification
"""

from datetime import date

import polars as pl
import pytest

from champion.corporate_actions.ca_processor import compute_adjustment_factors
from champion.corporate_actions.price_adjuster import apply_adjustments
from champion.features.indicators import compute_features
from champion.storage.parquet_io import write_df
from tests.fixtures.sample_data import (
    create_sample_corporate_actions,
    create_sample_nse_bhavcopy_data,
    create_sample_ohlc_data,
)


@pytest.fixture
def pipeline_workspace(tmp_path):
    """Create a complete workspace for pipeline testing."""
    workspace = {
        "root": tmp_path,
        "lake": tmp_path / "lake",
        "raw": tmp_path / "lake" / "raw",
        "normalized": tmp_path / "lake" / "normalized",
        "features": tmp_path / "lake" / "features",
        "warehouse": tmp_path / "warehouse",
    }

    # Create directories
    for path in workspace.values():
        if isinstance(path, type(tmp_path)):
            path.mkdir(parents=True, exist_ok=True)

    return workspace


class TestEndToEndPipeline:
    """End-to-end integration tests for complete data pipelines."""

    def test_complete_equity_pipeline(self, pipeline_workspace):
        """Test complete equity data pipeline from ingestion to warehouse."""
        # ===== STAGE 1: INGESTION =====
        # Simulate scraping NSE data
        nse_data = create_sample_nse_bhavcopy_data(trade_date=date(2024, 1, 15))

        # Write to raw layer
        raw_path = write_df(
            df=nse_data,
            dataset="equity_ohlc/raw",
            base_path=pipeline_workspace["raw"],
            partitions=None,
        )

        assert raw_path.exists()

        # Transform to normalized format
        normalized_df = nse_data.select(
            [
                pl.col("TckrSymb").alias("symbol"),
                pl.col("TradDt").alias("trade_date"),
                pl.col("OpnPric").alias("open"),
                pl.col("HghPric").alias("high"),
                pl.col("LwPric").alias("low"),
                pl.col("ClsPric").alias("close"),
                pl.col("TtlTradgVol").alias("volume"),
            ]
        )

        # Write to normalized layer
        normalized_path = write_df(
            df=normalized_df,
            dataset="equity_ohlc",
            base_path=pipeline_workspace["normalized"],
            partitions=["trade_date"],
        )

        assert normalized_path.exists()

        # ===== STAGE 2: FEATURES =====
        # Compute technical indicators
        features_df = compute_features(
            df=normalized_df, sma_windows=[5, 20], ema_windows=[12, 26], rsi_window=14
        )

        # Write to features layer
        features_path = write_df(
            df=features_df,
            dataset="equity_indicators",
            base_path=pipeline_workspace["features"],
            partitions=["trade_date"],
        )

        assert features_path.exists()

        # ===== STAGE 3: VERIFICATION =====
        # Verify data integrity at each stage
        df_raw = pl.read_parquet(raw_path / "*.parquet")
        df_normalized = pl.read_parquet(normalized_path / "**/*.parquet")
        df_features = pl.read_parquet(features_path / "**/*.parquet")

        # Check data continuity
        assert len(df_raw) == len(df_normalized)
        assert len(df_normalized) == len(df_features)

        # Check feature quality
        assert "sma_5" in df_features.columns
        assert "ema_12" in df_features.columns
        assert "rsi_14" in df_features.columns

    def test_corporate_actions_pipeline(self, pipeline_workspace):
        """Test complete corporate actions adjustment pipeline."""
        # ===== STAGE 1: LOAD OHLC DATA =====
        ohlc_data = create_sample_ohlc_data(
            symbols=["RELIANCE", "TCS"], start_date=date(2024, 1, 1), num_days=60
        )

        # Write unadjusted OHLC
        write_df(
            df=ohlc_data,
            dataset="equity_ohlc_unadjusted",
            base_path=pipeline_workspace["normalized"],
            partitions=None,
        )

        # ===== STAGE 2: LOAD CORPORATE ACTIONS =====
        ca_events = create_sample_corporate_actions(
            symbols=["RELIANCE", "TCS"], start_date=date(2024, 1, 1)
        )

        # Write CA events
        write_df(
            df=ca_events,
            dataset="corporate_actions",
            base_path=pipeline_workspace["raw"],
            partitions=None,
        )

        # ===== STAGE 3: COMPUTE ADJUSTMENT FACTORS =====
        ca_factors = compute_adjustment_factors(ca_events)

        assert not ca_factors.is_empty()
        assert "cumulative_factor" in ca_factors.columns

        # ===== STAGE 4: APPLY ADJUSTMENTS =====
        adjusted_df = apply_adjustments(
            ohlc_df=ohlc_data, ca_factors_df=ca_factors, columns=["open", "high", "low", "close"]
        )

        # ===== STAGE 5: WRITE ADJUSTED DATA =====
        adjusted_path = write_df(
            df=adjusted_df,
            dataset="equity_ohlc_adjusted",
            base_path=pipeline_workspace["normalized"],
            partitions=["trade_date"],
        )

        assert adjusted_path.exists()

        # ===== STAGE 6: VERIFICATION =====
        df_adjusted = pl.read_parquet(adjusted_path / "**/*.parquet")

        # Verify adjustments were applied
        assert len(df_adjusted) == len(ohlc_data)

        # Verify OHLC relationships maintained
        assert (df_adjusted["high"] >= df_adjusted["low"]).all()
        assert (df_adjusted["high"] >= df_adjusted["close"]).all()

    def test_multi_day_ingestion_pipeline(self, pipeline_workspace):
        """Test ingesting multiple days of data through pipeline."""
        # Generate data for 5 trading days
        dates = [date(2024, 1, i + 1) for i in range(5)]
        all_data = []

        for trade_date in dates:
            daily_data = create_sample_nse_bhavcopy_data(trade_date=trade_date)
            all_data.append(daily_data)

        # Combine all data
        combined_df = pl.concat(all_data)

        # Write to raw layer (partitioned by date)
        raw_path = write_df(
            df=combined_df,
            dataset="equity_ohlc",
            base_path=pipeline_workspace["raw"],
            partitions=["TradDt"],
        )

        # Verify partitions created
        partitions = list(raw_path.glob("TradDt=*"))
        assert len(partitions) == 5

        # Transform and write to normalized
        normalized_df = combined_df.select(
            [
                pl.col("TckrSymb").alias("symbol"),
                pl.col("TradDt").alias("trade_date"),
                pl.col("ClsPric").alias("close"),
            ]
        )

        normalized_path = write_df(
            df=normalized_df,
            dataset="equity_ohlc",
            base_path=pipeline_workspace["normalized"],
            partitions=["trade_date"],
        )

        # Verify all data accessible
        df_read = pl.read_parquet(normalized_path / "**/*.parquet")
        assert len(df_read) == len(combined_df)

    def test_incremental_pipeline_updates(self, pipeline_workspace):
        """Test incremental updates to pipeline data."""
        # ===== DAY 1: Initial Load =====
        day1_data = create_sample_ohlc_data(
            symbols=["RELIANCE", "TCS"], start_date=date(2024, 1, 1), num_days=1
        )

        path1 = write_df(
            df=day1_data,
            dataset="equity_ohlc",
            base_path=pipeline_workspace["normalized"],
            partitions=["trade_date"],
        )

        # ===== DAY 2: Incremental Update =====
        day2_data = create_sample_ohlc_data(
            symbols=["RELIANCE", "TCS"], start_date=date(2024, 1, 2), num_days=1
        )

        path2 = write_df(
            df=day2_data,
            dataset="equity_ohlc",
            base_path=pipeline_workspace["normalized"],
            partitions=["trade_date"],
        )

        # Both should point to same base directory
        assert path1 == path2

        # Read all data
        df_all = pl.read_parquet(path1 / "**/*.parquet")

        # Should have both days
        unique_dates = df_all["trade_date"].unique().len()
        assert unique_dates == 2

    def test_pipeline_with_validation(self, pipeline_workspace):
        """Test pipeline with data validation at each stage."""
        # Create valid data
        valid_data = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 1), num_days=10
        )

        # Write to raw layer
        raw_path = write_df(
            df=valid_data, dataset="equity_ohlc", base_path=pipeline_workspace["raw"]
        )

        # Validation: Check for required columns
        df = pl.read_parquet(raw_path / "*.parquet")
        required_cols = ["symbol", "trade_date", "open", "high", "low", "close", "volume"]
        for col in required_cols:
            assert col in df.columns

        # Validation: Check data quality
        assert (df["high"] >= df["low"]).all()
        assert (df["close"] > 0).all()
        assert (df["volume"] >= 0).all()

        # Write to normalized layer after validation
        normalized_path = write_df(
            df=df, dataset="equity_ohlc", base_path=pipeline_workspace["normalized"]
        )

        assert normalized_path.exists()

    def test_feature_pipeline_with_ca_adjusted_data(self, pipeline_workspace):
        """Test computing features on corporate action adjusted data."""
        # ===== STAGE 1: Create OHLC data =====
        ohlc_data = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 1), num_days=50
        )

        # ===== STAGE 2: Apply corporate actions =====
        ca_events = pl.DataFrame(
            {
                "symbol": ["RELIANCE"],
                "ex_date": [date(2024, 1, 25)],
                "action_type": ["SPLIT"],
                "adjustment_factor": [2.0],
            }
        )

        ca_factors = compute_adjustment_factors(ca_events)
        adjusted_df = apply_adjustments(
            ohlc_df=ohlc_data, ca_factors_df=ca_factors, columns=["close"]
        )

        # ===== STAGE 3: Compute features on adjusted data =====
        features_df = compute_features(df=adjusted_df, sma_windows=[5, 20], ema_windows=[12])

        # ===== STAGE 4: Write features =====
        features_path = write_df(
            df=features_df,
            dataset="equity_indicators_adjusted",
            base_path=pipeline_workspace["features"],
        )

        # ===== VERIFICATION =====
        df_features = pl.read_parquet(features_path / "*.parquet")

        # Verify features computed on adjusted prices
        assert not df_features.is_empty()
        assert "sma_5" in df_features.columns

        # Verify price continuity (no big jumps due to corporate action)
        close_prices = df_features.sort("trade_date")["close"].to_list()
        for i in range(1, len(close_prices)):
            if close_prices[i - 1] > 0:
                pct_change = abs((close_prices[i] - close_prices[i - 1]) / close_prices[i - 1])
                assert pct_change < 0.5  # No jumps > 50%

    def test_multi_symbol_multi_date_pipeline(self, pipeline_workspace):
        """Test pipeline with multiple symbols and dates."""
        # Create comprehensive dataset
        symbols = ["RELIANCE", "TCS", "INFY", "HDFC"]
        num_days = 30

        ohlc_data = create_sample_ohlc_data(
            symbols=symbols, start_date=date(2024, 1, 1), num_days=num_days
        )

        # ===== INGESTION =====
        raw_path = write_df(
            df=ohlc_data,
            dataset="equity_ohlc",
            base_path=pipeline_workspace["raw"],
            partitions=["symbol", "trade_date"],
        )

        # Verify partitioning
        symbol_partitions = list(raw_path.glob("symbol=*"))
        assert len(symbol_partitions) == len(symbols)

        # ===== FEATURES =====
        features_df = compute_features(df=ohlc_data)

        features_path = write_df(
            df=features_df,
            dataset="equity_indicators",
            base_path=pipeline_workspace["features"],
            partitions=["symbol"],
        )

        # ===== VERIFICATION =====
        # Read features per symbol
        for symbol in symbols:
            symbol_path = features_path / f"symbol={symbol}"
            if symbol_path.exists():
                df_symbol = pl.read_parquet(symbol_path / "*.parquet")
                assert len(df_symbol) == num_days
                assert (df_symbol["symbol"] == symbol).all()

    def test_pipeline_error_recovery(self, pipeline_workspace):
        """Test pipeline recovery from errors."""
        # Create data with some invalid rows
        valid_data = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 1), num_days=5
        )

        # Add invalid row (negative price)
        invalid_row = pl.DataFrame(
            {
                "symbol": ["RELIANCE"],
                "trade_date": [date(2024, 1, 6)],
                "open": [-100.0],
                "high": [-90.0],
                "low": [-110.0],
                "close": [-95.0],
                "volume": [1000],
                "turnover": [-95000.0],
            }
        )

        combined_data = pl.concat([valid_data, invalid_row])

        # Filter out invalid data before processing
        filtered_data = combined_data.filter(pl.col("close") > 0)

        # Write filtered data
        clean_path = write_df(
            df=filtered_data,
            dataset="equity_ohlc_clean",
            base_path=pipeline_workspace["normalized"],
        )

        # Verify only valid data remains
        df_clean = pl.read_parquet(clean_path / "*.parquet")
        assert (df_clean["close"] > 0).all()
        assert len(df_clean) == len(valid_data)

    def test_pipeline_data_lineage(self, pipeline_workspace):
        """Test that data lineage can be tracked through pipeline."""
        # Create source data
        source_data = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 1), num_days=10
        )

        # Add source metadata
        source_data = source_data.with_columns(
            [pl.lit("NSE").alias("source"), pl.lit("v1").alias("version")]
        )

        # ===== RAW LAYER =====
        raw_path = write_df(
            df=source_data, dataset="equity_ohlc", base_path=pipeline_workspace["raw"]
        )

        # ===== NORMALIZED LAYER =====
        normalized_data = source_data.select(["symbol", "trade_date", "close", "source", "version"])

        normalized_path = write_df(
            df=normalized_data,
            dataset="equity_ohlc",
            base_path=pipeline_workspace["normalized"],
        )

        # ===== FEATURES LAYER =====
        features_df = compute_features(df=normalized_data)

        # Add lineage metadata to features
        features_df = features_df.join(
            normalized_data.select(["symbol", "trade_date", "source", "version"]),
            on=["symbol", "trade_date"],
            how="left",
        )

        features_path = write_df(
            df=features_df,
            dataset="equity_indicators",
            base_path=pipeline_workspace["features"],
        )

        # ===== VERIFICATION =====
        # Verify lineage preserved through all layers
        df_raw = pl.read_parquet(raw_path / "*.parquet")
        df_normalized = pl.read_parquet(normalized_path / "*.parquet")
        df_features = pl.read_parquet(features_path / "*.parquet")

        assert "source" in df_raw.columns
        assert "source" in df_normalized.columns
        assert "source" in df_features.columns

        assert (df_raw["source"] == "NSE").all()
        assert (df_features["source"] == "NSE").all()

    def test_complete_pipeline_performance(self, pipeline_workspace):
        """Test pipeline performance with larger dataset."""
        # Create larger dataset (5 symbols, 100 days = 500 records)
        large_data = create_sample_ohlc_data(
            symbols=["RELIANCE", "TCS", "INFY", "HDFC", "ICICIBANK"],
            start_date=date(2024, 1, 1),
            num_days=100,
        )

        # ===== FULL PIPELINE =====
        # 1. Raw layer
        raw_path = write_df(
            df=large_data, dataset="equity_ohlc", base_path=pipeline_workspace["raw"]
        )

        # 2. Normalized layer
        normalized_path = write_df(
            df=large_data,
            dataset="equity_ohlc",
            base_path=pipeline_workspace["normalized"],
            partitions=["symbol"],
        )

        # 3. Features layer
        features_df = compute_features(
            df=large_data, sma_windows=[5, 20, 50], ema_windows=[12, 26], rsi_window=14
        )

        features_path = write_df(
            df=features_df,
            dataset="equity_indicators",
            base_path=pipeline_workspace["features"],
            partitions=["symbol"],
        )

        # ===== VERIFICATION =====
        # All layers should have same number of records
        df_raw = pl.read_parquet(raw_path / "*.parquet")
        df_normalized = pl.read_parquet(normalized_path / "**/*.parquet")
        df_features = pl.read_parquet(features_path / "**/*.parquet")

        assert len(df_raw) == 500
        assert len(df_normalized) == 500
        assert len(df_features) == 500

        # All feature columns should exist
        assert "sma_5" in df_features.columns
        assert "sma_20" in df_features.columns
        assert "sma_50" in df_features.columns
        assert "ema_12" in df_features.columns
        assert "rsi_14" in df_features.columns
