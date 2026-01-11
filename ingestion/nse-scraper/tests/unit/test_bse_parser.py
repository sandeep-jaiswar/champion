"""Unit tests for BSE bhavcopy parser."""

from datetime import date

import polars as pl
import pytest

from src.parsers.polars_bse_parser import PolarsBseParser


@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample BSE CSV file for testing."""
    csv_content = """SC_CODE,SC_NAME,SC_GROUP,SC_TYPE,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,NO_TRADES,NO_OF_SHRS,NET_TURNOV,TDCLOINDI,ISIN_CODE
500325,RELIANCE,A,R,2750.00,2780.50,2740.00,2765.25,2765.00,2750.00,50000,5000000,13826250000.00,IEP,INE002A01018
532540,TCS,A,R,3550.00,3575.00,3545.00,3560.75,3560.50,3550.00,45000,3500000,12462625000.00,IEP,INE467B01029
500209,INFY,A,R,1450.00,1465.00,1445.00,1458.25,1458.00,1450.00,48000,4200000,6124650000.00,IEP,INE009A01021"""

    csv_file = tmp_path / "test_bse_bhavcopy.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def parser():
    """Create a parser instance."""
    return PolarsBseParser()


@pytest.fixture
def trade_date():
    """Sample trade date."""
    return date(2026, 1, 9)


def test_parse_returns_list_of_events(parser, sample_csv_file, trade_date):
    """Test that parse returns a list of event dictionaries."""
    events = parser.parse(sample_csv_file, trade_date, output_parquet=False)

    assert isinstance(events, list)
    assert len(events) == 3  # 3 rows in sample CSV
    assert all(isinstance(event, dict) for event in events)


def test_parse_event_structure(parser, sample_csv_file, trade_date):
    """Test that each event has the correct structure."""
    events = parser.parse(sample_csv_file, trade_date, output_parquet=False)

    for event in events:
        # Check envelope fields
        assert "event_id" in event
        assert "event_time" in event
        assert "ingest_time" in event
        assert "source" in event
        assert "schema_version" in event
        assert "entity_id" in event
        assert "payload" in event

        # Check event metadata
        assert event["source"] == "bse_eq_bhavcopy"
        assert event["schema_version"] == "v1"
        assert ":BSE" in event["entity_id"]

        # Check payload structure
        payload = event["payload"]
        assert isinstance(payload, dict)
        assert "TckrSymb" in payload  # Normalized symbol name
        assert "ClsPric" in payload  # Normalized close price


def test_parse_data_types(parser, sample_csv_file, trade_date):
    """Test that data types are correctly parsed."""
    events = parser.parse(sample_csv_file, trade_date, output_parquet=False)

    event = events[0]  # RELIANCE
    payload = event["payload"]

    # Check string types
    assert isinstance(payload["TckrSymb"], str)
    assert payload["TckrSymb"] == "RELIANCE"

    # Check ISIN
    assert isinstance(payload["ISIN"], str)
    assert payload["ISIN"] == "INE002A01018"

    # Check numeric types
    assert isinstance(payload["ClsPric"], float)
    assert payload["ClsPric"] == 2765.25


def test_parse_normalizes_to_nse_schema(parser, sample_csv_file, trade_date):
    """Test that BSE data is normalized to NSE schema structure."""
    events = parser.parse(sample_csv_file, trade_date, output_parquet=False)

    event = events[0]
    payload = event["payload"]

    # Check normalized column names (NSE-style)
    assert "TckrSymb" in payload  # Symbol
    assert "OpnPric" in payload  # Open
    assert "HghPric" in payload  # High
    assert "LwPric" in payload  # Low
    assert "ClsPric" in payload  # Close
    assert "TtlTradgVol" in payload  # Volume
    assert "TtlTrfVal" in payload  # Turnover

    # Check source identifier
    assert payload["Src"] == "BSE"


def test_parse_to_dataframe(parser, sample_csv_file, trade_date):
    """Test parsing directly to DataFrame."""
    df = parser.parse_to_dataframe(sample_csv_file, trade_date)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 3

    # Check that normalized columns exist
    assert "TckrSymb" in df.columns
    assert "ClsPric" in df.columns
    assert "ISIN" in df.columns
    assert "event_id" in df.columns
    assert "source" in df.columns

    # Check source is BSE
    sources = df["source"].unique().to_list()
    assert len(sources) == 1
    assert sources[0] == "bse_eq_bhavcopy"


def test_parse_raw_csv(parser, sample_csv_file):
    """Test parsing raw CSV without normalization."""
    df = parser.parse_raw_csv(sample_csv_file)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 3

    # Check raw BSE column names exist
    assert "SC_CODE" in df.columns
    assert "SC_NAME" in df.columns
    assert "CLOSE" in df.columns
    assert "ISIN_CODE" in df.columns


def test_parse_filters_empty_symbols(parser, tmp_path, trade_date):
    """Test that parser filters out rows with empty symbols."""
    csv_content = """SC_CODE,SC_NAME,SC_GROUP,SC_TYPE,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,NO_TRADES,NO_OF_SHRS,NET_TURNOV,TDCLOINDI,ISIN_CODE
500325,RELIANCE,A,R,2750.00,2780.50,2740.00,2765.25,2765.00,2750.00,50000,5000000,13826250000.00,IEP,INE002A01018
,,A,R,3550.00,3575.00,3545.00,3560.75,3560.50,3550.00,45000,3500000,12462625000.00,IEP,INE467B01029
500209,,A,R,1450.00,1465.00,1445.00,1458.25,1458.00,1450.00,48000,4200000,6124650000.00,IEP,INE009A01021"""

    csv_file = tmp_path / "test_empty.csv"
    csv_file.write_text(csv_content)

    events = parser.parse(csv_file, trade_date, output_parquet=False)

    # Should only have 1 valid row (RELIANCE)
    assert len(events) == 1
    assert events[0]["payload"]["TckrSymb"] == "RELIANCE"


def test_parse_handles_nulls(parser, tmp_path, trade_date):
    """Test that parser handles null values correctly."""
    csv_content = """SC_CODE,SC_NAME,SC_GROUP,SC_TYPE,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,NO_TRADES,NO_OF_SHRS,NET_TURNOV,TDCLOINDI,ISIN_CODE
500325,RELIANCE,A,R,2750.00,2780.50,2740.00,2765.25,2765.00,2750.00,50000,5000000,13826250000.00,IEP,INE002A01018
532540,TCS,A,R,-,-,-,3560.75,3560.50,3550.00,45000,3500000,12462625000.00,IEP,INE467B01029"""

    csv_file = tmp_path / "test_nulls.csv"
    csv_file.write_text(csv_content)

    events = parser.parse(csv_file, trade_date, output_parquet=False)

    assert len(events) == 2

    # Check that nulls are handled
    tcs_event = events[1]
    payload = tcs_event["payload"]
    assert payload["OpnPric"] is None  # Open price was '-'


def test_event_id_deterministic(parser, sample_csv_file, trade_date):
    """Test that event IDs are deterministic and unique."""
    events1 = parser.parse(sample_csv_file, trade_date, output_parquet=False)
    events2 = parser.parse(sample_csv_file, trade_date, output_parquet=False)

    # Same input should generate same event IDs
    assert events1[0]["event_id"] == events2[0]["event_id"]

    # Different rows should have different event IDs
    event_ids = [e["event_id"] for e in events1]
    assert len(event_ids) == len(set(event_ids))  # All unique
