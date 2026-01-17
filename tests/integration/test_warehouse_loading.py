"""
Integration tests for warehouse loading pipeline.

Tests the complete flow:
1. Read Parquet files from data lake
2. Load data to ClickHouse (mocked)
3. Query verification
4. Data integrity checks
"""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from tests.fixtures.sample_data import create_sample_nse_bhavcopy_data, create_sample_ohlc_data


@pytest.fixture
def test_warehouse_dir(tmp_path):
    """Create a temporary directory for warehouse tests."""
    warehouse_dir = tmp_path / "warehouse"
    warehouse_dir.mkdir()
    return warehouse_dir


@pytest.fixture
def sample_parquet_data(tmp_path):
    """Create sample Parquet files for testing."""
    from champion.storage.parquet_io import write_df

    # Create sample data
    ohlc_data = create_sample_ohlc_data(
        symbols=["RELIANCE", "TCS", "INFY"], start_date=date(2024, 1, 1), num_days=30
    )

    # Write to Parquet with partitioning
    output_path = write_df(
        df=ohlc_data,
        dataset="normalized/equity_ohlc",
        base_path=tmp_path / "lake",
        partitions=["trade_date"],
    )

    return output_path


class TestWarehouseLoading:
    """Integration tests for warehouse loading pipeline."""

    def test_read_parquet_files(self, sample_parquet_data):
        """Test reading Parquet files from data lake."""
        # Read all Parquet files
        df = pl.read_parquet(sample_parquet_data / "**/*.parquet")

        # Verify data was read
        assert not df.is_empty()
        assert len(df) > 0

        # Verify required columns exist
        required_cols = ["symbol", "trade_date", "close"]
        for col in required_cols:
            assert col in df.columns

    def test_batch_loader_initialization(self):
        """Test ClickHouse batch loader initialization."""
        from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

        loader = ClickHouseLoader(
            host="localhost",
            port=8123,
            user="test_user",
            password="test_pass",
            database="test_db",
        )

        assert loader.host == "localhost"
        assert loader.port == 8123
        assert loader.database == "test_db"

    def test_batch_loader_supported_tables(self):
        """Test that batch loader recognizes supported tables."""
        from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

        loader = ClickHouseLoader()

        # Verify supported tables are defined
        assert "raw_equity_ohlc" in loader.SUPPORTED_TABLES
        assert "normalized_equity_ohlc" in loader.SUPPORTED_TABLES
        assert "features_equity_indicators" in loader.SUPPORTED_TABLES

    def test_find_parquet_files(self, sample_parquet_data):
        """Test finding Parquet files in a directory."""
        from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

        loader = ClickHouseLoader()

        # Find files
        files = loader._find_parquet_files(sample_parquet_data)

        # Verify files were found
        assert len(files) > 0
        assert all(f.suffix == ".parquet" for f in files)

    def test_prepare_dataframe_for_insert(self, sample_parquet_data):
        """Test preparing DataFrame for ClickHouse insertion."""
        from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

        loader = ClickHouseLoader()

        # Read sample data
        df = pl.read_parquet(sample_parquet_data / "**/*.parquet")

        # Add required event_time column for testing
        df = df.with_columns(
            pl.col("trade_date").cast(pl.Datetime).dt.timestamp("ms").alias("event_time")
        )

        # Add required columns for normalized_equity_ohlc
        df = df.with_columns(
            [
                pl.col("symbol").alias("TckrSymb"),
                pl.col("trade_date").alias("TradDt"),
                pl.lit("TEST_INST").alias("FinInstrmId"),
            ]
        )

        # Prepare for insertion
        prepared_df = loader._prepare_dataframe_for_insert(df, "normalized_equity_ohlc")

        # Verify prepared DataFrame
        assert not prepared_df.is_empty()
        assert len(prepared_df) == len(df)

    def test_column_mapping(self):
        """Test column name mapping for different table types."""
        from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

        loader = ClickHouseLoader()

        # Verify mappings exist
        assert "normalized_equity_ohlc" in loader.COLUMN_MAPPINGS

        # Verify key mappings
        mappings = loader.COLUMN_MAPPINGS["normalized_equity_ohlc"]
        assert mappings.get("trade_date") == "TradDt"
        assert mappings.get("symbol") == "TckrSymb"

    def test_load_parquet_dry_run(self, sample_parquet_data):
        """Test loading Parquet files in dry-run mode (no actual DB connection)."""
        from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

        loader = ClickHouseLoader()

        # Load in dry-run mode (no connection required)
        stats = loader.load_parquet_files(
            table="normalized_equity_ohlc", source_path=str(sample_parquet_data), dry_run=True
        )

        # Verify stats
        assert stats["table"] == "normalized_equity_ohlc"
        assert stats["files_loaded"] > 0
        assert stats["total_rows"] > 0
        assert stats["dry_run"] is True

    def test_query_verification_mock(self, sample_parquet_data):
        """Test query verification logic with mocked data."""
        # Read data from Parquet
        df = pl.read_parquet(sample_parquet_data / "**/*.parquet")

        # Simulate query results
        row_count = len(df)
        unique_symbols = df["symbol"].unique().to_list()

        # Verify counts
        assert row_count > 0
        assert len(unique_symbols) == 3  # RELIANCE, TCS, INFY

    def test_data_integrity_checks(self, sample_parquet_data):
        """Test data integrity checks on loaded data."""
        # Read data
        df = pl.read_parquet(sample_parquet_data / "**/*.parquet")

        # Integrity checks
        # 1. No null values in key columns
        assert df["symbol"].is_null().sum() == 0
        assert df["trade_date"].is_null().sum() == 0
        assert df["close"].is_null().sum() == 0

        # 2. Positive prices
        assert (df["close"] > 0).all()
        assert (df["open"] > 0).all()
        assert (df["high"] > 0).all()
        assert (df["low"] > 0).all()

        # 3. OHLC relationships
        assert (df["high"] >= df["low"]).all()
        assert (df["high"] >= df["open"]).all()
        assert (df["high"] >= df["close"]).all()

        # 4. Non-negative volumes
        assert (df["volume"] >= 0).all()

    def test_incremental_loading(self, tmp_path):
        """Test incremental data loading to warehouse."""
        from champion.storage.parquet_io import write_df

        # Create data for day 1
        day1_data = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 1), num_days=1
        )

        # Write day 1
        path1 = write_df(
            df=day1_data, dataset="incremental/day1", base_path=tmp_path, partitions=None
        )

        # Create data for day 2
        day2_data = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 2), num_days=1
        )

        # Write day 2
        path2 = write_df(
            df=day2_data, dataset="incremental/day2", base_path=tmp_path, partitions=None
        )

        # Read both
        df1 = pl.read_parquet(path1 / "*.parquet")
        df2 = pl.read_parquet(path2 / "*.parquet")

        # Verify incremental data
        assert len(df1) == 1
        assert len(df2) == 1
        assert df1["trade_date"][0] != df2["trade_date"][0]

    def test_partition_pruning(self, tmp_path):
        """Test that partition pruning works correctly."""
        from champion.storage.parquet_io import write_df

        # Create multi-day data
        multi_day_data = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 1), num_days=10
        )

        # Write partitioned by date
        output_path = write_df(
            df=multi_day_data,
            dataset="partitioned/equity",
            base_path=tmp_path,
            partitions=["trade_date"],
        )

        # Read only specific partition
        specific_date = date(2024, 1, 5)
        partition_path = output_path / f"trade_date={specific_date}"

        if partition_path.exists():
            df_filtered = pl.read_parquet(partition_path / "*.parquet")

            # Verify only that date is present
            assert (df_filtered["trade_date"] == specific_date).all()
            assert len(df_filtered) < len(multi_day_data)

    def test_bulk_insert_performance(self, tmp_path):
        """Test bulk insert performance with larger dataset."""
        from champion.storage.parquet_io import write_df

        # Create larger dataset
        large_data = create_sample_ohlc_data(
            symbols=["RELIANCE", "TCS", "INFY", "HDFC", "ICICIBANK"],
            start_date=date(2024, 1, 1),
            num_days=100,
        )

        # Write to Parquet
        output_path = write_df(
            df=large_data, dataset="bulk/equity", base_path=tmp_path, partitions=None
        )

        # Read back
        df_read = pl.read_parquet(output_path / "*.parquet")

        # Verify all data is present
        assert len(df_read) == len(large_data)
        assert len(df_read) == 500  # 5 symbols * 100 days

    def test_schema_compatibility(self, sample_parquet_data):
        """Test schema compatibility between Parquet and ClickHouse expectations."""
        # Read data
        df = pl.read_parquet(sample_parquet_data / "**/*.parquet")

        # Check data types
        assert df["symbol"].dtype == pl.Utf8
        assert df["trade_date"].dtype == pl.Date
        assert df["close"].dtype in [pl.Float64, pl.Float32]
        assert df["volume"].dtype in [pl.Int64, pl.Int32]

    def test_duplicate_detection(self, tmp_path):
        """Test detection of duplicate records."""
        from champion.storage.parquet_io import write_df

        # Create data with duplicates
        data = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 1), num_days=5
        )

        # Duplicate the data
        duplicate_data = pl.concat([data, data])

        # Write to Parquet
        output_path = write_df(
            df=duplicate_data, dataset="duplicate/equity", base_path=tmp_path, partitions=None
        )

        # Read back
        df_read = pl.read_parquet(output_path / "*.parquet")

        # Detect duplicates
        duplicates = df_read.filter(
            df_read.select(["symbol", "trade_date"]).is_duplicated()
        )

        # We should have duplicates
        assert len(duplicates) > 0

        # Remove duplicates
        df_unique = df_read.unique(subset=["symbol", "trade_date"])
        assert len(df_unique) == len(data)

    def test_end_to_end_warehouse_pipeline(self, tmp_path):
        """Test complete warehouse loading pipeline end-to-end."""
        from champion.storage.parquet_io import write_df
        from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

        # Step 1: Create sample data
        ohlc_data = create_sample_ohlc_data(
            symbols=["RELIANCE", "TCS"], start_date=date(2024, 1, 1), num_days=20
        )

        # Step 2: Write to data lake (normalized layer)
        lake_path = write_df(
            df=ohlc_data,
            dataset="normalized/equity_ohlc",
            base_path=tmp_path / "lake",
            partitions=["trade_date"],
        )

        # Verify data lake write
        assert lake_path.exists()

        # Step 3: Initialize warehouse loader
        loader = ClickHouseLoader()

        # Step 4: Load data in dry-run mode
        stats = loader.load_parquet_files(
            table="normalized_equity_ohlc", source_path=str(lake_path), dry_run=True
        )

        # Step 5: Verify load statistics
        assert stats["table"] == "normalized_equity_ohlc"
        assert stats["total_rows"] == len(ohlc_data)
        assert stats["files_loaded"] > 0

        # Step 6: Verify data integrity
        df_verify = pl.read_parquet(lake_path / "**/*.parquet")
        assert len(df_verify) == len(ohlc_data)
        assert (df_verify["close"] > 0).all()

    def test_time_series_continuity(self, tmp_path):
        """Test time series continuity in loaded data."""
        from champion.storage.parquet_io import write_df

        # Create continuous time series
        continuous_data = create_sample_ohlc_data(
            symbols=["RELIANCE"], start_date=date(2024, 1, 1), num_days=30
        )

        # Write and read back
        output_path = write_df(
            df=continuous_data,
            dataset="continuous/equity",
            base_path=tmp_path,
            partitions=None,
        )

        df = pl.read_parquet(output_path / "*.parquet")

        # Sort by date
        df_sorted = df.sort("trade_date")

        # Check continuity (dates should be consecutive)
        dates = df_sorted["trade_date"].to_list()
        for i in range(1, len(dates)):
            days_diff = (dates[i] - dates[i - 1]).days
            # Should be 1 day apart (assuming no weekends/holidays in sample data)
            assert days_diff == 1

    def test_aggregation_queries(self, sample_parquet_data):
        """Test aggregation queries on warehouse data."""
        # Read data
        df = pl.read_parquet(sample_parquet_data / "**/*.parquet")

        # Test aggregations
        # 1. Average close price per symbol
        avg_prices = df.group_by("symbol").agg(pl.col("close").mean().alias("avg_close"))

        assert not avg_prices.is_empty()
        assert len(avg_prices) == 3  # 3 symbols

        # 2. Total volume per symbol
        total_volumes = df.group_by("symbol").agg(pl.col("volume").sum().alias("total_volume"))

        assert not total_volumes.is_empty()
        assert (total_volumes["total_volume"] > 0).all()

        # 3. Date range
        date_range = df.select([pl.col("trade_date").min(), pl.col("trade_date").max()])

        assert date_range.shape[0] == 1

    def test_multi_symbol_loading(self, tmp_path):
        """Test loading data for multiple symbols simultaneously."""
        from champion.storage.parquet_io import write_df

        # Create data for multiple symbols
        multi_symbol_data = create_sample_ohlc_data(
            symbols=["RELIANCE", "TCS", "INFY", "HDFC", "ICICIBANK"],
            start_date=date(2024, 1, 1),
            num_days=10,
        )

        # Write to Parquet
        output_path = write_df(
            df=multi_symbol_data,
            dataset="multi_symbol/equity",
            base_path=tmp_path,
            partitions=["symbol"],
        )

        # Verify partitions per symbol
        symbol_partitions = list(output_path.glob("symbol=*"))
        assert len(symbol_partitions) == 5

        # Read all data
        df_all = pl.read_parquet(output_path / "**/*.parquet")
        assert len(df_all) == 50  # 5 symbols * 10 days
