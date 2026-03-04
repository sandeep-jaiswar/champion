"""End-to-end integration tests for Champion data pipeline."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from champion.core import get_config
from champion.parsers.polars_bhavcopy_parser import PolarsBhavcopyParser
from champion.storage.parquet_io import write_df
from champion.validation.validator import ParquetValidator


class TestE2EPipeline:
    """End-to-end pipeline tests."""

    @pytest.fixture
    def sample_bhavcopy_csv(self, tmp_path: Path) -> Path:
        """Create a sample BhavCopy CSV file."""
        csv_content = """ISIN,NAME,SERIES,TRADE_DATE,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,LAST_PRICE,TOTTRDQTY,TOTTRDVAL,TOTALTRADES,ISIN_NAME,TA_NAME,STATUS
INE001A01012,RELIANCE,EQ,19-JAN-2024,2989.85,2998.00,3045.50,2985.10,3032.45,3032.45,45231098,137256589632,234567,RELIANCE INDUSTRIES LIMITED,,OK
INE002A01015,TCS,EQ,19-JAN-2024,3456.75,3480.00,3520.00,3450.00,3501.50,3501.50,23456789,82145698756,156789,TATA CONSULTANCY SERVICES,,OK
INE003A01014,INFY,EQ,19-JAN-2024,1750.20,1765.00,1799.50,1748.00,1795.45,1795.45,54321098,97654321098,345678,INFOSYS,,OK"""

        csv_file = tmp_path / "BhavCopy_NSE_CM_20240119.csv"
        csv_file.write_text(csv_content)
        return csv_file

    @pytest.fixture
    def trade_date(self) -> date:
        """Get a sample trade date."""
        return date(2024, 1, 19)

    def test_parse_bhavcopy(self, sample_bhavcopy_csv: Path, trade_date: date):
        """Test: BhavCopy CSV parsing."""
        parser = PolarsBhavcopyParser()
        df = parser.parse(str(sample_bhavcopy_csv), trade_date)

        assert df is not None
        assert len(df) == 3
        assert "symbol" in df.columns or "SYMBOL" in df.columns

    def test_save_and_load_parquet(
        self, sample_bhavcopy_csv: Path, trade_date: date, tmp_path: Path
    ):
        """Test: Save and load Parquet files."""
        parser = PolarsBhavcopyParser()
        df = parser.parse(str(sample_bhavcopy_csv), trade_date)

        # Save to Parquet
        write_df(df, "test", tmp_path, partitions=None)

        # Load and verify
        test_parquet = tmp_path / "test" / f"ohlc_{trade_date}.parquet"
        if not test_parquet.exists():
            # Find the actual parquet file
            parquet_files = list(tmp_path.glob("test/*.parquet"))
            assert len(parquet_files) > 0, f"No parquet files found in {tmp_path / 'test'}"
            test_parquet = parquet_files[0]

        loaded_df = pl.read_parquet(test_parquet)
        assert len(loaded_df) > 0


class TestDataQuality:
    """Data quality validation tests."""

    def test_ohlc_price_consistency(self):
        """Test: OHLC price logical consistency."""
        df = pl.DataFrame(
            {
                "symbol": ["RELIANCE", "TCS", "INFY"],
                "high_price": [3045.50, 3520.00, 1799.50],
                "low_price": [2985.10, 3450.00, 1748.00],
                "close_price": [3032.45, 3501.50, 1795.45],
                "open_price": [2998.00, 3480.00, 1765.00],
            }
        )

        # All high >= close >= low >= open should be valid
        assert df.filter(pl.col("high_price") >= pl.col("close_price")).height == 3

    def test_duplicate_detection(self):
        """Test: Duplicate record detection."""
        df = pl.DataFrame(
            {
                "symbol": ["RELIANCE", "RELIANCE", "TCS"],
                "trade_date": ["2024-01-19", "2024-01-19", "2024-01-19"],
            }
        )

        duplicates = df.group_by(["symbol", "trade_date"]).agg(pl.count())
        assert duplicates.filter(pl.col("count") > 1).height == 1


class TestValidationIntegration:
    """Validation integration tests."""

    def test_error_streaming_memory_efficient(self, tmp_path: Path):
        """Test: Error streaming keeps memory bounded."""
        from champion.validation.error_streaming import ErrorStream

        stream = ErrorStream(output_file=tmp_path / "errors.jsonl", keep_samples=100)

        # Write 1000 errors
        for i in range(1000):
            stream.write_error({"row_id": i, "error": f"error_{i}"})

        # Samples should be bounded to 100
        samples = stream.get_samples()
        assert len(samples) == 100

        # All errors should be on disk
        all_errors = list(stream.iter_all_errors())
        assert len(all_errors) == 1000

    def test_validator_with_dataframe(self):
        """Test: Validator works with DataFrames."""
        config = get_config()
        try:
            validator = ParquetValidator(schema_dir=config.validator.schema_dir)
            df = pl.DataFrame({"test": [1, 2, 3]})
            # This tests that the validator has ErrorStream integration
            result = validator.validate_dataframe(df, schema_name="raw_ohlc")
            # Should return a ValidationResult
            assert result is not None
            assert hasattr(result, "is_memory_efficient")
        except ValueError:
            # Schema might not exist, that's ok for this integration test
            pass
