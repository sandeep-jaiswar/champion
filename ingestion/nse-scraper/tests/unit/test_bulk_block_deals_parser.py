"""Unit tests for bulk and block deals parser."""

import json
from datetime import date
from pathlib import Path

import pytest

from src.parsers.bulk_block_deals_parser import BulkBlockDealsParser


@pytest.fixture
def sample_bulk_deals_data():
    """Sample bulk deals data in NSE API format."""
    return [
        {
            "symbol": "RELIANCE",
            "clientName": "ABC SECURITIES LTD",
            "buyQty": 1000000,
            "sellQty": 0,
            "buyAvgPrice": 2850.50,
            "sellAvgPrice": 0,
            "dealDate": "10-JAN-2026",
        },
        {
            "symbol": "TCS",
            "clientName": "XYZ INVESTMENTS",
            "buyQty": 0,
            "sellQty": 500000,
            "buyAvgPrice": 0,
            "sellAvgPrice": 3920.75,
            "dealDate": "10-JAN-2026",
        },
        {
            "symbol": "INFY",
            "clientName": "DEF CAPITAL",
            "buyQty": 750000,
            "sellQty": 750000,
            "buyAvgPrice": 1625.25,
            "sellAvgPrice": 1625.50,
            "dealDate": "10-JAN-2026",
        },
    ]


@pytest.fixture
def sample_json_file(tmp_path, sample_bulk_deals_data):
    """Create a temporary JSON file with sample data."""
    file_path = tmp_path / "bulk_deals_20260110.json"
    with open(file_path, "w") as f:
        json.dump(sample_bulk_deals_data, f)
    return file_path


def test_parser_initialization():
    """Test parser can be initialized."""
    parser = BulkBlockDealsParser()
    assert parser is not None


def test_parse_bulk_deals(sample_json_file):
    """Test parsing bulk deals JSON file."""
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    events = parser.parse(
        file_path=sample_json_file,
        deal_date=deal_date,
        deal_type="BULK",
    )

    # Should create 4 events (1 buy + 1 sell + 2 from both)
    assert len(events) == 4

    # Check first event (RELIANCE BUY)
    reliance_buy = next(
        e for e in events if e["symbol"] == "RELIANCE" and e["transaction_type"] == "BUY"
    )
    assert reliance_buy["symbol"] == "RELIANCE"
    assert reliance_buy["client_name"] == "ABC SECURITIES LTD"
    assert reliance_buy["quantity"] == 1000000
    assert reliance_buy["avg_price"] == 2850.50
    assert reliance_buy["deal_type"] == "BULK"
    assert reliance_buy["transaction_type"] == "BUY"
    assert reliance_buy["exchange"] == "NSE"
    assert reliance_buy["deal_date"] == deal_date


def test_parse_separates_buy_and_sell(sample_json_file):
    """Test that BUY and SELL transactions are separated."""
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    events = parser.parse(
        file_path=sample_json_file,
        deal_date=deal_date,
        deal_type="BULK",
    )

    # INFY has both buy and sell
    infy_events = [e for e in events if e["symbol"] == "INFY"]
    assert len(infy_events) == 2

    buy_event = next(e for e in infy_events if e["transaction_type"] == "BUY")
    sell_event = next(e for e in infy_events if e["transaction_type"] == "SELL")

    assert buy_event["quantity"] == 750000
    assert buy_event["avg_price"] == 1625.25

    assert sell_event["quantity"] == 750000
    assert sell_event["avg_price"] == 1625.50


def test_parse_skips_zero_quantities(sample_json_file):
    """Test that zero quantities are skipped."""
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    events = parser.parse(
        file_path=sample_json_file,
        deal_date=deal_date,
        deal_type="BULK",
    )

    # All events should have non-zero quantities
    for event in events:
        assert event["quantity"] > 0


def test_parse_empty_file(tmp_path):
    """Test parsing empty JSON file."""
    empty_file = tmp_path / "empty.json"
    with open(empty_file, "w") as f:
        json.dump([], f)

    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    events = parser.parse(
        file_path=empty_file,
        deal_date=deal_date,
        deal_type="BULK",
    )

    assert len(events) == 0


def test_parse_missing_file():
    """Test parsing non-existent file raises error."""
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    with pytest.raises(FileNotFoundError):
        parser.parse(
            file_path=Path("/nonexistent/file.json"),
            deal_date=deal_date,
            deal_type="BULK",
        )


def test_event_structure(sample_json_file):
    """Test that events have required fields."""
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    events = parser.parse(
        file_path=sample_json_file,
        deal_date=deal_date,
        deal_type="BULK",
    )

    required_fields = [
        "event_id",
        "event_time",
        "ingest_time",
        "source",
        "schema_version",
        "entity_id",
        "deal_date",
        "symbol",
        "client_name",
        "quantity",
        "avg_price",
        "deal_type",
        "transaction_type",
        "exchange",
        "year",
        "month",
        "day",
    ]

    for event in events:
        for field in required_fields:
            assert field in event, f"Missing field: {field}"


def test_event_metadata(sample_json_file):
    """Test event metadata is properly set."""
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    events = parser.parse(
        file_path=sample_json_file,
        deal_date=deal_date,
        deal_type="BLOCK",  # Test with BLOCK type
    )

    for event in events:
        assert event["source"] == "nse.bulk_block_deals"
        assert event["schema_version"] == "1.0.0"
        assert event["deal_type"] == "BLOCK"
        assert event["exchange"] == "NSE"
        assert event["year"] == 2026
        assert event["month"] == 1
        assert event["day"] == 10


def test_write_parquet(sample_json_file, tmp_path):
    """Test writing events to Parquet file."""
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    events = parser.parse(
        file_path=sample_json_file,
        deal_date=deal_date,
        deal_type="BULK",
    )

    output_file = parser.write_parquet(
        events=events,
        output_base_path=tmp_path,
        deal_date=deal_date,
        deal_type="BULK",
    )

    # Check file exists
    assert output_file.exists()

    # Check partitioned structure
    assert "deal_type=BULK" in str(output_file)
    assert "year=2026" in str(output_file)
    assert "month=01" in str(output_file)
    assert "day=10" in str(output_file)

    # Verify Parquet can be read
    import polars as pl

    df = pl.read_parquet(output_file)
    assert len(df) == len(events)


def test_write_parquet_empty_events(tmp_path):
    """Test writing empty events raises error."""
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    with pytest.raises(ValueError, match="No events to write"):
        parser.write_parquet(
            events=[],
            output_base_path=tmp_path,
            deal_date=deal_date,
            deal_type="BULK",
        )


def test_symbol_uppercase(sample_json_file):
    """Test that symbols are converted to uppercase."""
    parser = BulkBlockDealsParser()
    deal_date = date(2026, 1, 10)

    events = parser.parse(
        file_path=sample_json_file,
        deal_date=deal_date,
        deal_type="BULK",
    )

    for event in events:
        assert event["symbol"] == event["symbol"].upper()
