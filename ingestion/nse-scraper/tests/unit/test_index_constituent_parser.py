"""Unit tests for IndexConstituentParser."""

import json
from datetime import date

import polars as pl
import pytest

from src.parsers.index_constituent_parser import IndexConstituentParser


@pytest.fixture
def sample_nifty50_data():
    """Sample NIFTY50 constituent data."""
    return {
        "name": "NIFTY 50",
        "index_name": "NIFTY50",
        "scraped_at": "2026-01-11T10:00:00+05:30",
        "data": [
            {
                "symbol": "RELIANCE",
                "series": "EQ",
                "open": 2850.0,
                "high": 2875.0,
                "low": 2840.0,
                "close": 2860.0,
                "last": 2860.0,
                "previousClose": 2855.0,
                "change": 5.0,
                "pChange": 0.18,
                "totalTradedVolume": 5000000,
                "totalTradedValue": 14300000000,
                "indexWeight": 10.5,
                "ffmc": 1500000.0,
                "sharesForIndex": 500000000,
                "meta": {
                    "isin": "INE002A01018",
                    "companyName": "Reliance Industries Ltd.",
                    "sector": "Energy",
                    "industry": "Refineries",
                },
            },
            {
                "symbol": "HDFCBANK",
                "series": "EQ",
                "open": 1650.0,
                "high": 1670.0,
                "low": 1645.0,
                "close": 1660.0,
                "last": 1660.0,
                "previousClose": 1655.0,
                "change": 5.0,
                "pChange": 0.30,
                "totalTradedVolume": 3000000,
                "totalTradedValue": 4980000000,
                "indexWeight": 8.5,
                "ffmc": 850000.0,
                "sharesForIndex": 400000000,
                "meta": {
                    "isin": "INE040A01034",
                    "companyName": "HDFC Bank Limited",
                    "sector": "Financial Services",
                    "industry": "Banks",
                },
            },
            {
                "symbol": "TCS",
                "series": "EQ",
                "open": 3900.0,
                "high": 3950.0,
                "low": 3890.0,
                "close": 3920.0,
                "last": 3920.0,
                "previousClose": 3910.0,
                "change": 10.0,
                "pChange": 0.26,
                "totalTradedVolume": 2000000,
                "totalTradedValue": 7840000000,
                "indexWeight": 7.2,
                "ffmc": 720000.0,
                "sharesForIndex": 300000000,
                "meta": {
                    "isin": "INE467B01029",
                    "companyName": "Tata Consultancy Services Ltd.",
                    "sector": "Information Technology",
                    "industry": "IT Services & Consulting",
                },
            },
        ],
    }


@pytest.fixture
def sample_banknifty_data():
    """Sample BANKNIFTY constituent data."""
    return {
        "name": "NIFTY BANK",
        "index_name": "BANKNIFTY",
        "scraped_at": "2026-01-11T10:00:00+05:30",
        "data": [
            {
                "symbol": "HDFCBANK",
                "series": "EQ",
                "open": 1650.0,
                "high": 1670.0,
                "low": 1645.0,
                "close": 1660.0,
                "last": 1660.0,
                "previousClose": 1655.0,
                "change": 5.0,
                "pChange": 0.30,
                "totalTradedVolume": 3000000,
                "totalTradedValue": 4980000000,
                "indexWeight": 25.5,
                "ffmc": 850000.0,
                "meta": {
                    "isin": "INE040A01034",
                    "companyName": "HDFC Bank Limited",
                    "sector": "Financial Services",
                    "industry": "Banks",
                },
            },
            {
                "symbol": "ICICIBANK",
                "series": "EQ",
                "open": 1100.0,
                "high": 1115.0,
                "low": 1095.0,
                "close": 1105.0,
                "last": 1105.0,
                "previousClose": 1100.0,
                "change": 5.0,
                "pChange": 0.45,
                "totalTradedVolume": 4000000,
                "totalTradedValue": 4420000000,
                "indexWeight": 22.3,
                "ffmc": 780000.0,
                "meta": {
                    "isin": "INE090A01021",
                    "companyName": "ICICI Bank Limited",
                    "sector": "Financial Services",
                    "industry": "Banks",
                },
            },
        ],
    }


@pytest.fixture
def parser():
    """Create parser instance."""
    return IndexConstituentParser()


class TestIndexConstituentParser:
    """Tests for IndexConstituentParser."""

    def test_parse_nifty50(self, parser, sample_nifty50_data, tmp_path):
        """Test parsing NIFTY50 constituent data."""
        # Write sample data to temporary file
        json_file = tmp_path / "NIFTY50_constituents.json"
        with open(json_file, "w") as f:
            json.dump(sample_nifty50_data, f)

        # Parse
        events = parser.parse(
            file_path=json_file,
            index_name="NIFTY50",
            effective_date=date(2026, 1, 11),
            action="ADD",
        )

        # Verify
        assert len(events) == 3
        assert all(isinstance(event, dict) for event in events)

        # Check first event (RELIANCE)
        event = events[0]
        assert event["event_id"]
        assert event["source"] == "nse_index_constituents"
        assert event["schema_version"] == "v1"
        assert event["entity_id"] == "NIFTY50:RELIANCE"

        payload = event["payload"]
        assert payload["index_name"] == "NIFTY50"
        assert payload["symbol"] == "RELIANCE"
        assert payload["isin"] == "INE002A01018"
        assert payload["company_name"] == "Reliance Industries Ltd."
        assert payload["action"] == "ADD"
        assert payload["weight"] == 10.5
        assert payload["free_float_market_cap"] == 1500000.0
        assert payload["shares_for_index"] == 500000000
        assert payload["sector"] == "Energy"
        assert payload["industry"] == "Refineries"
        assert payload["index_category"] == "Broad Market"

    def test_parse_banknifty(self, parser, sample_banknifty_data, tmp_path):
        """Test parsing BANKNIFTY constituent data."""
        # Write sample data to temporary file
        json_file = tmp_path / "BANKNIFTY_constituents.json"
        with open(json_file, "w") as f:
            json.dump(sample_banknifty_data, f)

        # Parse
        events = parser.parse(
            file_path=json_file,
            index_name="BANKNIFTY",
            effective_date=date(2026, 1, 11),
            action="ADD",
        )

        # Verify
        assert len(events) == 2

        # Check first event (HDFCBANK)
        event = events[0]
        payload = event["payload"]
        assert payload["index_name"] == "BANKNIFTY"
        assert payload["symbol"] == "HDFCBANK"
        assert payload["weight"] == 25.5
        assert payload["index_category"] == "Sectoral"

    def test_parse_empty_data(self, parser, tmp_path):
        """Test parsing file with no constituent data."""
        # Write empty data
        json_file = tmp_path / "EMPTY_constituents.json"
        with open(json_file, "w") as f:
            json.dump({"name": "EMPTY", "data": []}, f)

        # Parse
        events = parser.parse(
            file_path=json_file,
            index_name="EMPTY",
            effective_date=date(2026, 1, 11),
            action="ADD",
        )

        # Verify
        assert len(events) == 0

    def test_parse_with_remove_action(self, parser, sample_nifty50_data, tmp_path):
        """Test parsing with REMOVE action."""
        json_file = tmp_path / "NIFTY50_constituents.json"
        with open(json_file, "w") as f:
            json.dump(sample_nifty50_data, f)

        # Parse with REMOVE action
        events = parser.parse(
            file_path=json_file,
            index_name="NIFTY50",
            effective_date=date(2026, 1, 11),
            action="REMOVE",
        )

        # Verify all events have REMOVE action
        assert len(events) == 3
        for event in events:
            assert event["payload"]["action"] == "REMOVE"

    def test_parse_default_effective_date(self, parser, sample_nifty50_data, tmp_path):
        """Test parsing with default (today's) effective date."""
        json_file = tmp_path / "NIFTY50_constituents.json"
        with open(json_file, "w") as f:
            json.dump(sample_nifty50_data, f)

        # Parse without specifying effective_date
        events = parser.parse(
            file_path=json_file,
            index_name="NIFTY50",
            effective_date=None,  # Should default to today
            action="ADD",
        )

        # Verify effective_date is today
        assert len(events) > 0
        today = date.today()
        epoch = date(1970, 1, 1)
        expected_days = (today - epoch).days

        for event in events:
            assert event["payload"]["effective_date"] == expected_days

    def test_write_parquet(self, parser, sample_nifty50_data, tmp_path):
        """Test writing events to Parquet file."""
        # Parse events
        json_file = tmp_path / "NIFTY50_constituents.json"
        with open(json_file, "w") as f:
            json.dump(sample_nifty50_data, f)

        events = parser.parse(
            file_path=json_file,
            index_name="NIFTY50",
            effective_date=date(2026, 1, 11),
            action="ADD",
        )

        # Write to Parquet
        output_base = tmp_path / "lake"
        output_file = parser.write_parquet(
            events=events,
            output_base_path=output_base,
            index_name="NIFTY50",
            effective_date=date(2026, 1, 11),
        )

        # Verify file exists
        assert output_file.exists()
        assert output_file.suffix == ".parquet"

        # Verify path structure
        assert "index_name=NIFTY50" in str(output_file)
        assert "year=2026" in str(output_file)
        assert "month=01" in str(output_file)
        assert "day=11" in str(output_file)

        # Read and verify content
        df = pl.read_parquet(output_file)
        assert len(df) == 3
        assert "symbol" in df.columns
        assert "index_name" in df.columns
        assert "weight" in df.columns
        assert "action" in df.columns

        # Verify data
        symbols = df["symbol"].to_list()
        assert "RELIANCE" in symbols
        assert "HDFCBANK" in symbols
        assert "TCS" in symbols

    def test_write_parquet_empty_events(self, parser, tmp_path):
        """Test that writing empty events raises ValueError."""
        output_base = tmp_path / "lake"

        with pytest.raises(ValueError, match="No events to write"):
            parser.write_parquet(
                events=[],
                output_base_path=output_base,
                index_name="NIFTY50",
                effective_date=date(2026, 1, 11),
            )

    def test_get_index_category(self, parser):
        """Test index category determination."""
        assert parser._get_index_category("NIFTY50") == "Broad Market"
        assert parser._get_index_category("NIFTY100") == "Broad Market"
        assert parser._get_index_category("NIFTY500") == "Broad Market"
        assert parser._get_index_category("BANKNIFTY") == "Sectoral"
        assert parser._get_index_category("NIFTYIT") == "Sectoral"
        assert parser._get_index_category("NIFTYPHARMA") == "Sectoral"
        assert parser._get_index_category("NIFTYMIDCAP50") == "Market Cap"
        assert parser._get_index_category("NIFTYSMALLCAP100") == "Market Cap"
        assert parser._get_index_category("NIFTYCUSTOM") is None

    def test_parse_missing_weight(self, parser, tmp_path):
        """Test parsing data without weight information."""
        data = {
            "name": "NIFTY IT",
            "index_name": "NIFTYIT",
            "data": [
                {
                    "symbol": "TCS",
                    "series": "EQ",
                    "close": 3920.0,
                    # No indexWeight or weightage field
                    "meta": {
                        "isin": "INE467B01029",
                        "companyName": "Tata Consultancy Services Ltd.",
                    },
                }
            ],
        }

        json_file = tmp_path / "NIFTYIT_constituents.json"
        with open(json_file, "w") as f:
            json.dump(data, f)

        events = parser.parse(
            file_path=json_file,
            index_name="NIFTYIT",
            effective_date=date(2026, 1, 11),
            action="ADD",
        )

        assert len(events) == 1
        assert events[0]["payload"]["weight"] is None

    def test_parse_non_eq_series_filtered(self, parser, tmp_path):
        """Test that non-EQ/BE series are filtered out."""
        data = {
            "name": "NIFTY 50",
            "index_name": "NIFTY50",
            "data": [
                {
                    "symbol": "RELIANCE",
                    "series": "EQ",
                    "close": 2860.0,
                    "meta": {"isin": "INE002A01018"},
                },
                {
                    "symbol": "NIFTY50",
                    "series": "INDEX",  # Should be filtered
                    "close": 18000.0,
                    "meta": {"isin": ""},
                },
                {
                    "symbol": "HDFCBANK",
                    "series": "BE",
                    "close": 1660.0,
                    "meta": {"isin": "INE040A01034"},
                },
            ],
        }

        json_file = tmp_path / "NIFTY50_constituents.json"
        with open(json_file, "w") as f:
            json.dump(data, f)

        events = parser.parse(
            file_path=json_file,
            index_name="NIFTY50",
            effective_date=date(2026, 1, 11),
            action="ADD",
        )

        # Should only include EQ and BE series (2 events, not 3)
        assert len(events) == 2
        symbols = [e["payload"]["symbol"] for e in events]
        assert "RELIANCE" in symbols
        assert "HDFCBANK" in symbols
        assert "NIFTY50" not in symbols
