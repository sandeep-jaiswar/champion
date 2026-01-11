"""Unit tests for Symbol Master Parser."""

import tempfile
from datetime import date
from pathlib import Path

import pytest

from src.parsers.symbol_master_parser import SymbolMasterParser


class TestSymbolMasterParser:
    """Test cases for SymbolMasterParser."""

    @pytest.fixture
    def sample_equity_l_csv(self) -> Path:
        """Create a sample EQUITY_L.csv file for testing."""
        csv_content = """SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
RELIANCE,Reliance Industries Limited,EQ,29-Nov-1977,10,1,INE002A01018,10
TCS,Tata Consultancy Services Limited,EQ,25-Aug-2004,1,1,INE467B01029,1
INFY,Infosys Limited,EQ,08-Feb-1995,5,1,INE009A01021,5
IBULHSGFIN,Indiabulls Housing Finance Limited,EQ,10-Jul-2013,2,1,INE148I01020,2
IBULHSGFIN,Indiabulls Housing Finance - NCD Series I,D1,15-Jan-2019,1000,1,INE148I08023,1000
HDFCBANK,HDFC Bank Limited,EQ,08-Nov-1995,1,1,INE040A01034,1
20MICRONS,20 Microns Limited,EQ,07-Oct-2008,10,1,INE144J01027,10
"""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        temp_file.write(csv_content)
        temp_file.flush()
        return Path(temp_file.name)

    def test_parser_initialization(self):
        """Test parser can be initialized."""
        parser = SymbolMasterParser()
        assert parser is not None

    def test_parse_basic_equity_l(self, sample_equity_l_csv):
        """Test parsing a basic EQUITY_L.csv file."""
        parser = SymbolMasterParser()
        events = parser.parse(sample_equity_l_csv, exchange="NSE")

        # Should have 7 events (excluding header)
        assert len(events) == 7

        # Check first event structure
        first_event = events[0]
        assert "event_id" in first_event
        assert "event_time" in first_event
        assert "ingest_time" in first_event
        assert "source" in first_event
        assert first_event["source"] == "nse_symbol_master"
        assert "schema_version" in first_event
        assert first_event["schema_version"] == "v1"
        assert "entity_id" in first_event
        assert "payload" in first_event

    def test_parse_payload_structure(self, sample_equity_l_csv):
        """Test payload structure of parsed events."""
        parser = SymbolMasterParser()
        events = parser.parse(sample_equity_l_csv, exchange="NSE")

        # Check RELIANCE payload
        reliance_event = [e for e in events if e["payload"]["symbol"] == "RELIANCE"][0]
        payload = reliance_event["payload"]

        assert payload["symbol"] == "RELIANCE"
        assert payload["exchange"] == "NSE"
        assert payload["company_name"] == "Reliance Industries Limited"
        assert payload["isin"] == "INE002A01018"
        assert payload["series"] == "EQ"
        assert payload["face_value"] == 10.0
        assert payload["paid_up_value"] == 10.0
        assert payload["lot_size"] == 1
        assert payload["status"] == "ACTIVE"
        assert "instrument_id" in payload

    def test_parse_instrument_id_format(self, sample_equity_l_csv):
        """Test instrument ID format is symbol:exchange."""
        parser = SymbolMasterParser()
        events = parser.parse(sample_equity_l_csv, exchange="NSE")

        for event in events:
            payload = event["payload"]
            expected_id = f"{payload['symbol']}:NSE"
            assert payload["instrument_id"] == expected_id
            assert event["entity_id"] == expected_id

    def test_parse_multiple_instruments_same_ticker(self, sample_equity_l_csv):
        """Test parsing handles multiple instruments with same ticker (IBULHSGFIN)."""
        parser = SymbolMasterParser()
        events = parser.parse(sample_equity_l_csv, exchange="NSE")

        # Find all IBULHSGFIN events
        ibulhsgfin_events = [e for e in events if e["payload"]["symbol"] == "IBULHSGFIN"]

        # Should have 2 IBULHSGFIN entries (EQ and D1 series)
        assert len(ibulhsgfin_events) == 2

        # Check series are different
        series_list = [e["payload"]["series"] for e in ibulhsgfin_events]
        assert "EQ" in series_list
        assert "D1" in series_list

        # Check ISINs are different
        isin_list = [e["payload"]["isin"] for e in ibulhsgfin_events]
        assert len(set(isin_list)) == 2

    def test_parse_null_handling(self):
        """Test parser handles null values correctly."""
        csv_content = """SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
TESTCO,Test Company,-,NA,10,1,INE123456789,10
"""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        temp_file.write(csv_content)
        temp_file.flush()

        parser = SymbolMasterParser()
        events = parser.parse(Path(temp_file.name), exchange="NSE")

        assert len(events) == 1
        payload = events[0]["payload"]

        # Series should be None because of "-"
        assert payload["series"] is None
        # Listing date should be None because of "NA"
        assert payload["listing_date"] is None

    def test_parse_listing_date_conversion(self, sample_equity_l_csv):
        """Test listing date is correctly parsed and converted."""
        parser = SymbolMasterParser()
        events = parser.parse(sample_equity_l_csv, exchange="NSE")

        # Find HDFCBANK which has listing date 08-Nov-1995
        hdfcbank_event = [e for e in events if e["payload"]["symbol"] == "HDFCBANK"][0]
        payload = hdfcbank_event["payload"]

        # Listing date should be converted to days since epoch
        assert payload["listing_date"] is not None
        assert isinstance(payload["listing_date"], int)

        # Verify it's a reasonable date (should be in the past)
        # 1995-11-08 is approximately 9443 days since epoch (1970-01-01)
        assert payload["listing_date"] > 9000
        assert payload["listing_date"] < 30000  # Not too far in the future

    def test_parse_empty_symbol_filtered(self):
        """Test that rows with empty symbols are filtered out."""
        csv_content = """SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
,Empty Symbol Company,EQ,01-Jan-2000,10,1,INE000000001,10
VALIDCO,Valid Company,EQ,01-Jan-2000,10,1,INE000000002,10
"""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        temp_file.write(csv_content)
        temp_file.flush()

        parser = SymbolMasterParser()
        events = parser.parse(Path(temp_file.name), exchange="NSE")

        # Should only have 1 event (empty symbol filtered out)
        assert len(events) == 1
        assert events[0]["payload"]["symbol"] == "VALIDCO"

    def test_parse_special_characters_in_company_name(self):
        """Test parser handles special characters in company names."""
        csv_content = """SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
TEST1,Test & Co. Limited,EQ,01-Jan-2000,10,1,INE000000001,10
TEST2,Test (India) Pvt. Ltd.,EQ,01-Jan-2000,10,1,INE000000002,10
"""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        temp_file.write(csv_content)
        temp_file.flush()

        parser = SymbolMasterParser()
        events = parser.parse(Path(temp_file.name), exchange="NSE")

        assert len(events) == 2
        assert "Test & Co. Limited" in [e["payload"]["company_name"] for e in events]
        assert "Test (India) Pvt. Ltd." in [e["payload"]["company_name"] for e in events]

    def test_parse_deterministic_event_ids(self, sample_equity_l_csv):
        """Test that event IDs are deterministic based on instrument_id."""
        parser = SymbolMasterParser()
        events1 = parser.parse(sample_equity_l_csv, exchange="NSE")
        events2 = parser.parse(sample_equity_l_csv, exchange="NSE")

        # Sort both lists by symbol for comparison
        events1_sorted = sorted(events1, key=lambda e: e["payload"]["symbol"])
        events2_sorted = sorted(events2, key=lambda e: e["payload"]["symbol"])

        # Event IDs should be the same across runs
        for e1, e2 in zip(events1_sorted, events2_sorted, strict=True):
            assert e1["event_id"] == e2["event_id"]
            assert e1["payload"]["instrument_id"] == e2["payload"]["instrument_id"]

    def test_parse_valid_from_date(self, sample_equity_l_csv):
        """Test that valid_from date is set to current date."""
        parser = SymbolMasterParser()
        events = parser.parse(sample_equity_l_csv, exchange="NSE")

        # Get today's date in days since epoch
        today = date.today()
        epoch = date(1970, 1, 1)
        expected_valid_from = (today - epoch).days

        for event in events:
            payload = event["payload"]
            assert payload["valid_from"] == expected_valid_from
            assert payload["valid_to"] is None  # Current version should have no expiry
