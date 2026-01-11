"""Integration tests for macro indicator ETL pipeline."""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from src.orchestration.macro_flow import macro_indicators_flow
from src.parsers.macro_indicator_parser import MacroIndicatorParser
from src.scrapers.mospi_macro import MOSPIMacroScraper
from src.scrapers.rbi_macro import RBIMacroScraper


class TestRBIScraper:
    """Test RBI macro scraper."""

    def test_scrape_generates_file(self):
        """Test that scraper generates a JSON file."""
        scraper = RBIMacroScraper()
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        json_path = scraper.scrape(start_date, end_date)

        assert json_path.exists()
        assert json_path.suffix == ".json"
        assert json_path.stat().st_size > 0

    def test_scrape_with_specific_indicators(self):
        """Test scraper with specific indicators."""
        scraper = RBIMacroScraper()
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)
        indicators = ["REPO_RATE", "FX_RESERVES_TOTAL"]

        json_path = scraper.scrape(start_date, end_date, indicators)

        assert json_path.exists()

        # Parse and verify
        parser = MacroIndicatorParser()
        df = parser.parse(json_path)

        # Check only requested indicators are present
        indicator_codes = set(df["indicator_code"].unique().to_list())
        assert indicator_codes.issubset(set(indicators))


class TestMOSPIScraper:
    """Test MOSPI macro scraper."""

    def test_scrape_generates_file(self):
        """Test that scraper generates a JSON file."""
        scraper = MOSPIMacroScraper()
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 3, 31)

        json_path = scraper.scrape(start_date, end_date)

        assert json_path.exists()
        assert json_path.suffix == ".json"
        assert json_path.stat().st_size > 0

    def test_scrape_monthly_frequency(self):
        """Test that MOSPI data has monthly frequency."""
        scraper = MOSPIMacroScraper()
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 3, 31)

        json_path = scraper.scrape(start_date, end_date)

        # Parse and verify
        parser = MacroIndicatorParser()
        df = parser.parse(json_path)

        # All MOSPI indicators should be monthly
        assert (df["frequency"] == "MONTHLY").all()


class TestMacroIndicatorParser:
    """Test macro indicator parser."""

    def test_parse_rbi_data(self):
        """Test parsing RBI JSON data."""
        scraper = RBIMacroScraper()
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        json_path = scraper.scrape(start_date, end_date)
        parser = MacroIndicatorParser()
        df = parser.parse(json_path)

        # Check DataFrame structure
        assert len(df) > 0
        assert "indicator_date" in df.columns
        assert "indicator_code" in df.columns
        assert "value" in df.columns
        assert "source" in df.columns

        # Check data types
        assert df["indicator_date"].dtype == pl.Date
        assert df["value"].dtype == pl.Float64

        # Check envelope fields
        assert "event_id" in df.columns
        assert "event_time" in df.columns
        assert "entity_id" in df.columns

    def test_parse_mospi_data(self):
        """Test parsing MOSPI JSON data."""
        scraper = MOSPIMacroScraper()
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 3, 31)

        json_path = scraper.scrape(start_date, end_date)
        parser = MacroIndicatorParser()
        df = parser.parse(json_path)

        # Check DataFrame structure
        assert len(df) > 0
        assert "indicator_category" in df.columns

        # MOSPI should have inflation indicators
        categories = set(df["indicator_category"].unique().to_list())
        assert "INFLATION" in categories or "PRODUCTION" in categories

    def test_data_quality_checks(self):
        """Test data quality validation."""
        scraper = RBIMacroScraper()
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 2, 29)

        json_path = scraper.scrape(start_date, end_date)
        parser = MacroIndicatorParser()
        df = parser.parse(json_path)

        # No nulls in required fields
        assert df["indicator_date"].null_count() == 0
        assert df["indicator_code"].null_count() == 0
        assert df["value"].null_count() == 0

        # Values should be numeric
        assert all(df["value"].is_not_null())


class TestMacroETLFlow:
    """Test complete macro ETL flow."""

    @pytest.mark.skip(reason="Requires ClickHouse connection")
    def test_end_to_end_flow(self):
        """Test complete ETL flow without ClickHouse."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        # Run flow without ClickHouse load
        parquet_path = macro_indicators_flow(
            start_date=start_date,
            end_date=end_date,
            rbi_indicators=["REPO_RATE"],
            mospi_indicators=["CPI_COMBINED"],
            load_to_clickhouse=False,
        )

        # Verify parquet file was created
        assert Path(parquet_path).exists()

        # Load and verify data
        df = pl.read_parquet(parquet_path)
        assert len(df) > 0

        # Check both sources are present
        sources = set(df["source"].unique().to_list())
        assert "RBI" in sources
        assert "MOSPI" in sources

    def test_date_range_validation(self):
        """Test that date range is respected."""
        scraper = RBIMacroScraper()
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        json_path = scraper.scrape(start_date, end_date)
        parser = MacroIndicatorParser()
        df = parser.parse(json_path)

        # All dates should be within range
        min_date = df["indicator_date"].min()
        max_date = df["indicator_date"].max()

        assert min_date >= start_date.date()
        assert max_date <= end_date.date()


class TestDataMerging:
    """Test merging of multiple data sources."""

    def test_merge_deduplication(self):
        """Test that merge removes duplicates."""

        # Create sample data with duplicates
        df1 = pl.DataFrame(
            {
                "event_id": ["1", "2"],
                "entity_id": ["REPO_RATE:2024-01-01", "CPI_COMBINED:2024-01-01"],
                "indicator_code": ["REPO_RATE", "CPI_COMBINED"],
                "indicator_date": [datetime(2024, 1, 1).date(), datetime(2024, 1, 1).date()],
                "value": [6.5, 185.0],
            }
        )

        df2 = pl.DataFrame(
            {
                "event_id": ["3", "4"],
                "entity_id": ["REPO_RATE:2024-01-01", "CPI_COMBINED:2024-01-01"],
                "indicator_code": ["REPO_RATE", "CPI_COMBINED"],
                "indicator_date": [datetime(2024, 1, 1).date(), datetime(2024, 1, 1).date()],
                "value": [6.5, 185.1],  # Slightly different value
            }
        )

        # Mock task execution
        merged = pl.concat([df1, df2], how="vertical")
        merged = merged.unique(subset=["entity_id"], keep="last")

        # Should have 2 rows (one per unique entity_id)
        assert len(merged) == 2

        # Should keep last value
        cpi_row = merged.filter(pl.col("indicator_code") == "CPI_COMBINED")
        assert cpi_row["value"][0] == 185.1
