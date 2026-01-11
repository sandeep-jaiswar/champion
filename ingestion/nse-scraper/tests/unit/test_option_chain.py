"""Unit tests for option chain scraper and parser."""

import json
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from src.scrapers.option_chain import OptionChainScraper, parse_interval


class TestOptionChainParser:
    """Test option chain parsing logic."""

    @pytest.fixture
    def sample_option_chain_data(self):
        """Sample NSE option chain response."""
        return {
            "records": {
                "underlyingValue": 18500.50,
                "data": [
                    {
                        "strikePrice": 18400.0,
                        "expiryDate": "28-Dec-2023",
                        "CE": {
                            "strikePrice": 18400.0,
                            "expiryDate": "28-Dec-2023",
                            "underlying": "NIFTY",
                            "identifier": "OPTIDXNIFTY28-12-2023CE18400.00",
                            "openInterest": 1500000,
                            "changeinOpenInterest": 50000,
                            "totalTradedVolume": 250000,
                            "impliedVolatility": 15.25,
                            "lastPrice": 150.75,
                            "bidprice": 150.50,
                            "askPrice": 151.00,
                            "delta": 0.65,
                            "theta": -5.2,
                            "gamma": 0.002,
                            "vega": 12.5,
                        },
                        "PE": {
                            "strikePrice": 18400.0,
                            "expiryDate": "28-Dec-2023",
                            "underlying": "NIFTY",
                            "identifier": "OPTIDXNIFTY28-12-2023PE18400.00",
                            "openInterest": 1200000,
                            "changeinOpenInterest": -30000,
                            "totalTradedVolume": 180000,
                            "impliedVolatility": 16.50,
                            "lastPrice": 50.25,
                            "bidprice": 50.00,
                            "askPrice": 50.50,
                            "delta": -0.35,
                            "theta": -4.8,
                            "gamma": 0.002,
                            "vega": 12.3,
                        },
                    },
                    {
                        "strikePrice": 18500.0,
                        "expiryDate": "28-Dec-2023",
                        "CE": {
                            "strikePrice": 18500.0,
                            "expiryDate": "28-Dec-2023",
                            "underlying": "NIFTY",
                            "identifier": "OPTIDXNIFTY28-12-2023CE18500.00",
                            "openInterest": 2000000,
                            "changeinOpenInterest": 100000,
                            "totalTradedVolume": 350000,
                            "impliedVolatility": 14.75,
                            "lastPrice": 100.50,
                            "bidprice": 100.25,
                            "askPrice": 100.75,
                            "delta": 0.50,
                            "theta": -6.5,
                            "gamma": 0.003,
                            "vega": 15.0,
                        },
                    },
                ],
            }
        }

    def test_parse_to_dataframe(self, sample_option_chain_data):
        """Test parsing option chain data to DataFrame."""
        scraper = OptionChainScraper()
        df = scraper._parse_to_dataframe(sample_option_chain_data, "NIFTY")

        # Should have 3 rows (2 CE + 1 PE)
        assert len(df) == 3

        # Check columns exist
        required_columns = [
            "event_id",
            "underlying",
            "underlying_value",
            "strike_price",
            "option_type",
            "expiry_date",
            "open_interest",
            "implied_volatility",
            "last_price",
        ]
        for col in required_columns:
            assert col in df.columns

        # Check data types and values
        assert df["underlying"][0] == "NIFTY"
        assert df["underlying_value"][0] == 18500.50
        assert df["strike_price"].to_list() == [18400.0, 18400.0, 18500.0]

        # Check option types
        option_types = df["option_type"].to_list()
        assert "CE" in option_types
        assert "PE" in option_types

        # Verify CE data
        ce_18400 = df.filter((pl.col("strike_price") == 18400.0) & (pl.col("option_type") == "CE"))
        assert len(ce_18400) == 1
        assert ce_18400["open_interest"][0] == 1500000
        assert ce_18400["implied_volatility"][0] == 15.25
        assert ce_18400["last_price"][0] == 150.75

        # Verify PE data
        pe_18400 = df.filter((pl.col("strike_price") == 18400.0) & (pl.col("option_type") == "PE"))
        assert len(pe_18400) == 1
        assert pe_18400["open_interest"][0] == 1200000
        assert pe_18400["implied_volatility"][0] == 16.50

    def test_parse_empty_data(self):
        """Test parsing empty option chain data."""
        scraper = OptionChainScraper()
        empty_data = {"records": {"data": []}}

        df = scraper._parse_to_dataframe(empty_data, "NIFTY")
        assert len(df) == 0

    def test_create_option_record(self):
        """Test creating a single option record."""
        scraper = OptionChainScraper()
        timestamp = datetime(2023, 12, 28, 15, 30, 0)

        option_data = {
            "bidprice": 100.50,
            "askPrice": 101.00,
            "lastPrice": 100.75,
            "totalTradedVolume": 50000,
            "openInterest": 1000000,
            "changeinOpenInterest": 25000,
            "impliedVolatility": 15.5,
            "delta": 0.60,
            "theta": -5.0,
            "gamma": 0.002,
            "vega": 12.0,
        }

        record = scraper._create_option_record(
            symbol="NIFTY",
            underlying_value=18500.0,
            timestamp=timestamp,
            expiry_date="28-Dec-2023",
            strike_price=18500.0,
            option_type="CE",
            option_data=option_data,
        )

        # Check required fields
        assert record["underlying"] == "NIFTY"
        assert record["underlying_value"] == 18500.0
        assert record["strike_price"] == 18500.0
        assert record["option_type"] == "CE"
        assert record["expiry_date"] == "2023-12-28"  # Converted format
        assert record["open_interest"] == 1000000
        assert record["implied_volatility"] == 15.5
        assert record["bid_price"] == 100.50
        assert record["ask_price"] == 101.00
        assert record["last_price"] == 100.75

        # Check Greeks
        assert record["delta"] == 0.60
        assert record["theta"] == -5.0
        assert record["gamma"] == 0.002
        assert record["vega"] == 12.0

    def test_handle_null_values(self):
        """Test handling of null/missing values."""
        scraper = OptionChainScraper()
        timestamp = datetime.now()

        # Option data with missing fields
        option_data = {
            "lastPrice": 100.0,
            # Missing bid/ask, IV, Greeks, etc.
        }

        record = scraper._create_option_record(
            symbol="NIFTY",
            underlying_value=18500.0,
            timestamp=timestamp,
            expiry_date="28-Dec-2023",
            strike_price=18500.0,
            option_type="CE",
            option_data=option_data,
        )

        # Null fields should be None
        assert record["bid_price"] is None
        assert record["ask_price"] is None
        assert record["implied_volatility"] is None
        assert record["open_interest"] is None
        assert record["delta"] is None

        # Non-null fields should have values
        assert record["last_price"] == 100.0
        assert record["strike_price"] == 18500.0

    def test_expiry_date_conversion(self):
        """Test expiry date format conversion."""
        scraper = OptionChainScraper()

        # Test DD-MMM-YYYY format
        record = scraper._create_option_record(
            symbol="NIFTY",
            underlying_value=18500.0,
            timestamp=datetime.now(),
            expiry_date="28-Dec-2023",
            strike_price=18500.0,
            option_type="CE",
            option_data={},
        )
        assert record["expiry_date"] == "2023-12-28"

        # Test different month
        record = scraper._create_option_record(
            symbol="NIFTY",
            underlying_value=18500.0,
            timestamp=datetime.now(),
            expiry_date="30-Jan-2024",
            strike_price=18500.0,
            option_type="PE",
            option_data={},
        )
        assert record["expiry_date"] == "2024-01-30"


class TestIntervalParsing:
    """Test interval string parsing."""

    def test_parse_minutes(self):
        """Test parsing minute intervals."""
        assert parse_interval("5m") == 5
        assert parse_interval("15m") == 15
        assert parse_interval("30m") == 30
        assert parse_interval("1m") == 1

    def test_parse_hours(self):
        """Test parsing hour intervals."""
        assert parse_interval("1h") == 60
        assert parse_interval("2h") == 120
        assert parse_interval("24h") == 1440

    def test_parse_seconds(self):
        """Test parsing second intervals."""
        assert parse_interval("60s") == 1
        assert parse_interval("120s") == 2
        assert parse_interval("30s") == 1  # Rounds to 1 minute minimum

    def test_parse_with_whitespace(self):
        """Test parsing with whitespace."""
        assert parse_interval(" 5m ") == 5
        assert parse_interval("  1h  ") == 60

    def test_parse_case_insensitive(self):
        """Test case-insensitive parsing."""
        assert parse_interval("5M") == 5
        assert parse_interval("1H") == 60
        assert parse_interval("30S") == 1

    def test_parse_invalid_format(self):
        """Test invalid interval format."""
        with pytest.raises(ValueError, match="Invalid interval format"):
            parse_interval("5")

        with pytest.raises(ValueError, match="Invalid interval format"):
            parse_interval("abc")

        with pytest.raises(ValueError, match="Invalid interval format"):
            parse_interval("5x")

    def test_parse_invalid_number(self):
        """Test invalid number in interval."""
        with pytest.raises(ValueError, match="Invalid interval value"):
            parse_interval("abc5m")

        with pytest.raises(ValueError, match="Invalid interval value"):
            parse_interval("5.5m")


class TestSymbolMapping:
    """Test symbol to instrument type mapping."""

    def test_index_symbols(self):
        """Test mapping for index symbols."""
        scraper = OptionChainScraper()

        assert scraper._determine_instrument_type("NIFTY") == "indices"
        assert scraper._determine_instrument_type("BANKNIFTY") == "indices"
        assert scraper._determine_instrument_type("FINNIFTY") == "indices"
        assert scraper._determine_instrument_type("nifty") == "indices"  # lowercase

    def test_equity_symbols(self):
        """Test mapping for equity symbols."""
        scraper = OptionChainScraper()

        assert scraper._determine_instrument_type("RELIANCE") == "equities"
        assert scraper._determine_instrument_type("TCS") == "equities"
        assert scraper._determine_instrument_type("INFY") == "equities"
