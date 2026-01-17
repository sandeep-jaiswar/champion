"""Tests for comprehensive validation rules."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from champion.validation.validator import ParquetValidator


@pytest.fixture
def schema_dir(tmp_path):
    """Create test schemas."""
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()

    # OHLC schema
    ohlc_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["event_id", "symbol", "open", "high", "low", "close", "volume"],
        "properties": {
            "event_id": {"type": "string"},
            "symbol": {"type": "string"},
            "open": {"type": "number", "minimum": 0},
            "high": {"type": "number", "minimum": 0},
            "low": {"type": "number", "minimum": 0},
            "close": {"type": "number", "minimum": 0},
            "volume": {"type": "integer", "minimum": 0},
            "turnover": {"type": "number", "minimum": 0},
            "trades": {"type": "integer", "minimum": 0},
            "prev_close": {"type": ["number", "null"], "minimum": 0},
            "event_time": {"type": "integer"},
            "ingest_time": {"type": "integer"},
        },
    }

    with open(schema_dir / "test_ohlc.json", "w") as f:
        json.dump(ohlc_schema, f)

    # Normalized schema
    normalized_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["instrument_id", "trade_date", "open", "high", "low", "close", "volume"],
        "properties": {
            "instrument_id": {"type": "string"},
            "trade_date": {"type": "integer"},
            "open": {"type": "number", "minimum": 0},
            "high": {"type": "number", "minimum": 0},
            "low": {"type": "number", "minimum": 0},
            "close": {"type": "number", "minimum": 0},
            "volume": {"type": "integer", "minimum": 0},
            "turnover": {"type": "number", "minimum": 0},
            "adjustment_factor": {"type": "number"},
            "is_trading_day": {"type": "boolean"},
        },
    }

    with open(schema_dir / "normalized_test_ohlc.json", "w") as f:
        json.dump(normalized_schema, f)

    return schema_dir


@pytest.fixture
def validator(schema_dir):
    """Create validator instance."""
    return ParquetValidator(schema_dir=schema_dir, max_price_change_pct=20.0)


def test_ohlc_close_in_range_violation(validator):
    """Test that close outside [low, high] is caught."""
    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["A", "B"],
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [115.0, 210.0],  # First close > high
            "volume": [1000, 2000],
            "turnover": [105000.0, 420000.0],
            "trades": [10, 20],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    assert result.critical_failures > 0
    assert any("close" in e["field"].lower() for e in result.error_details)
    assert any("outside range" in e["message"].lower() for e in result.error_details)


def test_ohlc_open_in_range_violation(validator):
    """Test that open outside [low, high] is caught."""
    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["A", "B"],
            "open": [90.0, 200.0],  # First open < low
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [1000, 2000],
            "turnover": [105000.0, 420000.0],
            "trades": [10, 20],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    assert result.critical_failures > 0
    assert any("open" in e["field"].lower() for e in result.error_details)


def test_volume_consistency_violation(validator):
    """Test volume > 0 when trades > 0."""
    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["A", "B"],
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [0, 2000],  # First row: volume=0 but trades>0
            "turnover": [0.0, 420000.0],
            "trades": [10, 20],  # trades > 0
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    assert result.critical_failures > 0
    assert any("volume" in e["field"].lower() and "trades" in e["field"].lower() for e in result.error_details)


def test_turnover_consistency_warning(validator):
    """Test turnover consistency warning."""
    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["A", "B"],
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [1000, 2000],
            "turnover": [50000.0, 420000.0],  # First row: large deviation
            "trades": [10, 20],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    # Turnover warnings are included in error_details
    assert any("turnover" in e["field"].lower() for e in result.error_details)


def test_price_reasonableness_violation(validator):
    """Test price change exceeds threshold."""
    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["A", "B"],
            "open": [100.0, 200.0],
            "high": [150.0, 220.0],
            "low": [95.0, 195.0],
            "close": [145.0, 210.0],  # First row: 45% change from prev_close
            "volume": [1000, 2000],
            "turnover": [145000.0, 420000.0],
            "trades": [10, 20],
            "prev_close": [100.0, 200.0],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    # Should have warning for excessive price change
    assert any("price change" in e["message"].lower() for e in result.error_details)


def test_duplicate_detection(validator):
    """Test duplicate record detection."""
    df = pl.DataFrame(
        {
            "instrument_id": ["A:NSE", "A:NSE", "B:NSE"],  # Duplicate
            "trade_date": [19000, 19000, 19000],  # Same date
            "open": [100.0, 100.0, 200.0],
            "high": [110.0, 110.0, 220.0],
            "low": [95.0, 95.0, 195.0],
            "close": [105.0, 105.0, 210.0],
            "volume": [1000, 1000, 2000],
            "turnover": [105000.0, 105000.0, 420000.0],
            "adjustment_factor": [1.0, 1.0, 1.0],
            "is_trading_day": [True, True, True],
        }
    )

    result = validator.validate_dataframe(df, "normalized_test_ohlc")
    assert result.critical_failures > 0
    assert any("duplicate" in e["message"].lower() for e in result.error_details)


def test_freshness_check(validator):
    """Test data freshness validation."""
    now_ms = int(datetime.now().timestamp() * 1000)
    old_time = int((datetime.now() - timedelta(days=3)).timestamp() * 1000)

    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["A", "B"],
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [1000, 2000],
            "turnover": [105000.0, 420000.0],
            "trades": [10, 20],
            "event_time": [old_time, now_ms - 1000],  # First is stale
            "ingest_time": [now_ms, now_ms],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    # Should have warning for stale data
    assert any("stale" in e["message"].lower() or "delay" in e["message"].lower() for e in result.error_details)


def test_timestamp_validation(validator):
    """Test timestamp validation."""
    future_time = int((datetime.now() + timedelta(days=2)).timestamp() * 1000)

    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["A", "B"],
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [1000, 2000],
            "turnover": [105000.0, 420000.0],
            "trades": [10, 20],
            "event_time": [future_time, 1000000],  # First is in future
            "ingest_time": [future_time, 2000000],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    assert result.critical_failures > 0
    assert any("timestamp" in e["message"].lower() or "future" in e["message"].lower() for e in result.error_details)


def test_negative_price_validation(validator):
    """Test negative price detection."""
    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["A", "B"],
            "open": [-100.0, 200.0],  # Negative price
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [1000, 2000],
            "turnover": [105000.0, 420000.0],
            "trades": [10, 20],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    assert result.critical_failures > 0
    assert any("negative" in e["message"].lower() and "price" in e["message"].lower() for e in result.error_details)


def test_negative_volume_validation(validator):
    """Test negative volume detection."""
    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["A", "B"],
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [-1000, 2000],  # Negative volume
            "turnover": [105000.0, 420000.0],
            "trades": [10, 20],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    assert result.critical_failures > 0
    assert any("negative" in e["message"].lower() and "volume" in e["message"].lower() for e in result.error_details)


def test_date_range_validation(validator):
    """Test date range validation."""
    df = pl.DataFrame(
        {
            "instrument_id": ["A:NSE", "B:NSE"],
            "trade_date": [1000, 99999],  # First is too old (before 1990)
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [1000, 2000],
            "turnover": [105000.0, 420000.0],
            "adjustment_factor": [1.0, 1.0],
            "is_trading_day": [True, True],
        }
    )

    result = validator.validate_dataframe(df, "normalized_test_ohlc")
    assert result.critical_failures > 0
    assert any("date" in e["message"].lower() and "range" in e["message"].lower() for e in result.error_details)


def test_trading_day_completeness(validator):
    """Test trading day completeness validation."""
    df = pl.DataFrame(
        {
            "instrument_id": ["A:NSE", "B:NSE"],
            "trade_date": [19000, 19001],
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [0, 2000],  # First has volume=0 but is_trading_day=True
            "turnover": [0.0, 420000.0],
            "adjustment_factor": [1.0, 1.0],
            "is_trading_day": [True, True],
        }
    )

    result = validator.validate_dataframe(df, "normalized_test_ohlc")
    # Should have warning for trading day with zero volume
    assert any("trading day" in e["message"].lower() and "volume" in e["message"].lower() for e in result.error_details)


def test_price_continuity_after_ca(validator):
    """Test price continuity after corporate actions."""
    df = pl.DataFrame(
        {
            "instrument_id": ["A:NSE", "B:NSE"],
            "trade_date": [19000, 19001],
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [1000, 2000],
            "turnover": [105000.0, 420000.0],
            "adjustment_factor": [-1.0, 1.0],  # Invalid adjustment factor
            "is_trading_day": [True, True],
        }
    )

    result = validator.validate_dataframe(df, "normalized_test_ohlc")
    assert result.critical_failures > 0
    assert any("adjustment" in e["message"].lower() for e in result.error_details)


def test_custom_validator_registration(validator):
    """Test custom validator registration and execution."""

    def custom_rule(df: pl.DataFrame) -> list[dict]:
        """Custom validation: symbol must be uppercase."""
        errors = []
        if "symbol" in df.columns:
            violations = df.with_row_index("__idx__").filter(
                pl.col("symbol") != pl.col("symbol").str.to_uppercase()
            )
            for row in violations.iter_rows(named=True):
                errors.append(
                    {
                        "row_index": row["__idx__"],
                        "error_type": "critical",
                        "field": "symbol",
                        "message": f"Symbol must be uppercase: {row['symbol']}",
                        "validator": "custom",
                        "record": dict(row),
                    }
                )
        return errors

    validator.register_custom_validator("uppercase_symbol", custom_rule)

    df = pl.DataFrame(
        {
            "event_id": ["1", "2"],
            "symbol": ["aapl", "GOOGL"],  # First is lowercase
            "open": [100.0, 200.0],
            "high": [110.0, 220.0],
            "low": [95.0, 195.0],
            "close": [105.0, 210.0],
            "volume": [1000, 2000],
            "turnover": [105000.0, 420000.0],
            "trades": [10, 20],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    assert result.critical_failures > 0
    assert any("uppercase" in e["message"].lower() for e in result.error_details)
    assert "custom_uppercase_symbol" in result.validation_rules_applied


def test_validation_result_tracking(validator):
    """Test that validation result tracks rules applied."""
    df = pl.DataFrame(
        {
            "event_id": ["1"],
            "symbol": ["A"],
            "open": [100.0],
            "high": [110.0],
            "low": [95.0],
            "close": [105.0],
            "volume": [1000],
            "turnover": [105000.0],
            "trades": [10],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    
    # Should have tracked rules
    assert len(result.validation_rules_applied) > 0
    assert "schema_validation" in result.validation_rules_applied
    assert "ohlc_high_low_consistency" in result.validation_rules_applied
    assert result.validation_timestamp is not None


def test_all_rules_pass(validator):
    """Test data that passes all validation rules."""
    now_ms = int(datetime.now().timestamp() * 1000)

    df = pl.DataFrame(
        {
            "event_id": ["1"],
            "symbol": ["AAPL"],
            "open": [100.0],
            "high": [110.0],
            "low": [95.0],
            "close": [105.0],
            "volume": [1000],
            "turnover": [105000.0],
            "trades": [10],
            "prev_close": [100.0],
            "event_time": [now_ms - 1000],
            "ingest_time": [now_ms],
        }
    )

    result = validator.validate_dataframe(df, "test_ohlc")
    assert result.critical_failures == 0
    assert result.valid_rows == 1
    assert len(result.validation_rules_applied) >= 15  # At least 15 rules applied
