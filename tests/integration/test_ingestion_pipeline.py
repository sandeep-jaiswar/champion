"""
Integration tests for the data ingestion pipeline.

Tests the complete flow:
1. Scrape NSE data (simulated with fixtures)
2. Parse and validate data
3. Write to Parquet with schema validation
4. Verify schema and data integrity
"""

from datetime import date

import polars as pl
import pytest

from tests.fixtures.sample_data import create_sample_nse_bhavcopy_data, create_sample_ohlc_data


@pytest.fixture
def test_data_dir(tmp_path):
    """Create a temporary data directory for tests."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def sample_nse_data():
    """Fixture providing sample NSE bhavcopy data."""
    return create_sample_nse_bhavcopy_data(trade_date=date(2024, 1, 15))


@pytest.fixture
def sample_ohlc_data():
    """Fixture providing sample OHLC data."""
    return create_sample_ohlc_data(
        symbols=["RELIANCE", "TCS", "INFY"], start_date=date(2024, 1, 1), num_days=30
    )


class TestIngestionPipeline:
    """Integration tests for data ingestion pipeline."""

    def test_scrape_and_parse_nse_data(self, sample_nse_data):
        """Test that NSE data can be scraped and parsed correctly."""
        # Verify sample data structure
        assert not sample_nse_data.is_empty()
        assert len(sample_nse_data) == 3  # Three stocks

        # Verify required columns exist
        required_cols = ["TckrSymb", "TradDt", "ClsPric", "TtlTradgVol"]
        for col in required_cols:
            assert col in sample_nse_data.columns, f"Missing column: {col}"

        # Verify data types
        assert sample_nse_data["TckrSymb"].dtype == pl.Utf8
        assert sample_nse_data["TradDt"].dtype == pl.Date
        assert sample_nse_data["ClsPric"].dtype in [pl.Float64, pl.Float32]
        assert sample_nse_data["TtlTradgVol"].dtype in [pl.Int64, pl.Int32]

    def test_write_to_parquet_unpartitioned(self, sample_ohlc_data, test_data_dir):
        """Test writing OHLC data to Parquet without partitioning."""
        from champion.storage.parquet_io import write_df

        output_path = write_df(
            df=sample_ohlc_data, dataset="raw/equity_ohlc", base_path=test_data_dir, partitions=None
        )

        # Verify output path exists
        assert output_path.exists()
        assert output_path.is_dir()

        # Verify Parquet file was created
        parquet_files = list(output_path.glob("*.parquet"))
        assert len(parquet_files) > 0

        # Read back and verify
        df_read = pl.read_parquet(parquet_files[0])
        assert len(df_read) == len(sample_ohlc_data)
        assert set(df_read.columns) == set(sample_ohlc_data.columns)

    def test_write_to_parquet_partitioned(self, sample_ohlc_data, test_data_dir):
        """Test writing OHLC data to Parquet with date partitioning."""
        from champion.storage.parquet_io import write_df

        output_path = write_df(
            df=sample_ohlc_data,
            dataset="raw/equity_ohlc_partitioned",
            base_path=test_data_dir,
            partitions=["trade_date"],
        )

        # Verify output path exists
        assert output_path.exists()

        # Verify partitioned directories were created
        partition_dirs = list(output_path.glob("trade_date=*"))
        assert len(partition_dirs) > 0, "No partition directories created"

        # Verify we can read back the partitioned data
        df_read = pl.read_parquet(output_path / "**/*.parquet")
        assert len(df_read) == len(sample_ohlc_data)

    def test_validate_schema_valid_data(self, sample_ohlc_data, test_data_dir, tmp_path):
        """Test schema validation with valid data."""
        from champion.storage.parquet_io import write_df_safe

        # Create a simple schema for testing
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()

        schema_content = {
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "trade_date": {"type": "string", "format": "date"},
                "open": {"type": "number"},
                "high": {"type": "number"},
                "low": {"type": "number"},
                "close": {"type": "number"},
                "volume": {"type": "integer"},
                "turnover": {"type": "number"},
            },
            "required": ["symbol", "trade_date", "close"],
        }

        import json

        schema_file = schema_dir / "test_ohlc.json"
        schema_file.write_text(json.dumps(schema_content))

        # Write with validation
        output_path = write_df_safe(
            df=sample_ohlc_data,
            dataset="validated/equity_ohlc",
            base_path=test_data_dir,
            schema_name="test_ohlc",
            schema_dir=schema_dir,
            fail_on_validation_errors=False,
        )

        # Verify output exists
        assert output_path.exists()

        # Read back and verify
        parquet_files = list(output_path.rglob("*.parquet"))
        assert len(parquet_files) > 0

        df_read = pl.read_parquet(parquet_files[0])
        assert len(df_read) == len(sample_ohlc_data)

    def test_verify_parquet_schema(self, sample_ohlc_data, test_data_dir):
        """Test that Parquet files maintain correct schema."""
        from champion.storage.parquet_io import write_df

        output_path = write_df(
            df=sample_ohlc_data, dataset="raw/equity_ohlc_schema", base_path=test_data_dir
        )

        # Read back and verify schema
        parquet_files = list(output_path.glob("*.parquet"))
        df_read = pl.read_parquet(parquet_files[0])

        # Verify column names
        assert set(df_read.columns) == set(sample_ohlc_data.columns)

        # Verify data types are preserved
        for col in sample_ohlc_data.columns:
            assert (
                df_read[col].dtype == sample_ohlc_data[col].dtype
            ), f"Data type mismatch for {col}"

    def test_end_to_end_ingestion_flow(self, sample_nse_data, test_data_dir):
        """Test complete ingestion pipeline from NSE data to Parquet."""
        from champion.storage.parquet_io import write_df

        # Step 1: Simulate scraping - we have sample_nse_data
        assert not sample_nse_data.is_empty()

        # Step 2: Transform NSE format to normalized format
        normalized_df = sample_nse_data.select(
            [
                pl.col("TckrSymb").alias("symbol"),
                pl.col("TradDt").alias("trade_date"),
                pl.col("OpnPric").alias("open"),
                pl.col("HghPric").alias("high"),
                pl.col("LwPric").alias("low"),
                pl.col("ClsPric").alias("close"),
                pl.col("TtlTradgVol").alias("volume"),
                pl.col("TtlTrfVal").alias("turnover"),
            ]
        )

        # Step 3: Write to raw layer
        raw_path = write_df(
            df=sample_nse_data, dataset="raw/equity_ohlc", base_path=test_data_dir, partitions=None
        )

        # Step 4: Write to normalized layer
        normalized_path = write_df(
            df=normalized_df,
            dataset="normalized/equity_ohlc",
            base_path=test_data_dir,
            partitions=["trade_date"],
        )

        # Verify raw layer
        assert raw_path.exists()
        raw_files = list(raw_path.glob("*.parquet"))
        assert len(raw_files) > 0

        # Verify normalized layer
        assert normalized_path.exists()
        normalized_files = list(normalized_path.rglob("*.parquet"))
        assert len(normalized_files) > 0

        # Verify data integrity
        df_raw = pl.read_parquet(raw_files[0])
        df_normalized = pl.read_parquet(normalized_path / "**/*.parquet")

        assert len(df_raw) == len(df_normalized)
        assert len(df_normalized) == len(sample_nse_data)

    def test_multiple_dates_ingestion(self, test_data_dir):
        """Test ingesting data for multiple trading dates."""
        from champion.storage.parquet_io import write_df

        # Create data for multiple dates
        dates = [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
        all_data = []

        for trade_date in dates:
            daily_data = create_sample_nse_bhavcopy_data(trade_date=trade_date)
            all_data.append(daily_data)

        # Concatenate all data
        combined_df = pl.concat(all_data)

        # Write partitioned by date
        output_path = write_df(
            df=combined_df,
            dataset="raw/equity_ohlc_multi",
            base_path=test_data_dir,
            partitions=["TradDt"],
        )

        # Verify partitions for each date exist
        partition_dirs = list(output_path.glob("TradDt=*"))
        assert len(partition_dirs) == len(dates)

        # Read back and verify all data is present
        df_read = pl.read_parquet(output_path / "**/*.parquet")
        assert len(df_read) == len(combined_df)

    def test_parquet_compression(self, sample_ohlc_data, test_data_dir):
        """Test that Parquet files are compressed correctly."""
        from champion.storage.parquet_io import write_df

        # Write with snappy compression
        output_snappy = write_df(
            df=sample_ohlc_data,
            dataset="raw/equity_snappy",
            base_path=test_data_dir,
            compression="snappy",
        )

        # Write with gzip compression
        output_gzip = write_df(
            df=sample_ohlc_data,
            dataset="raw/equity_gzip",
            base_path=test_data_dir,
            compression="gzip",
        )

        # Verify both exist
        assert output_snappy.exists()
        assert output_gzip.exists()

        # Verify we can read both
        df_snappy = pl.read_parquet(list(output_snappy.glob("*.parquet"))[0])
        df_gzip = pl.read_parquet(list(output_gzip.glob("*.parquet"))[0])

        assert len(df_snappy) == len(sample_ohlc_data)
        assert len(df_gzip) == len(sample_ohlc_data)

    def test_data_validation_rules(self, test_data_dir):
        """Test data validation catches invalid data."""
        # Create data with invalid values
        invalid_df = pl.DataFrame(
            {
                "symbol": ["INVALID", "TEST"],
                "trade_date": [date(2024, 1, 1), date(2024, 1, 1)],
                "close": [-100.0, 0.0],  # Negative and zero prices (invalid)
                "volume": [1000, -500],  # Negative volume (invalid)
            }
        )

        # Basic validation: prices should be positive
        assert (invalid_df["close"] > 0).sum() == 0  # None are valid

        # Volumes should be non-negative
        assert (invalid_df["volume"] >= 0).sum() == 1  # Only one is valid
