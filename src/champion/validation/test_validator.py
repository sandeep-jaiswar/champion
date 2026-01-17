"""Tests for ParquetValidator."""

import json
from pathlib import Path

import polars as pl
import pytest

from validation.validator import ParquetValidator, ValidationResult


@pytest.fixture
def schema_dir(tmp_path):
    """Create a temporary schema directory with test schemas."""
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()

    # Create a simple test schema
    test_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["event_id", "price", "volume"],
        "properties": {
            "event_id": {"type": "string"},
            "price": {"type": "number", "minimum": 0},
            "volume": {"type": "integer", "minimum": 0},
            "optional_field": {"type": ["string", "null"]},
        },
        "additionalProperties": False,
    }

    with open(schema_dir / "test_schema.json", "w") as f:
        json.dump(test_schema, f)

    # Create OHLC test schema
    ohlc_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["event_id", "open", "high", "low", "close"],
        "properties": {
            "event_id": {"type": "string"},
            "open": {"type": "number", "minimum": 0},
            "high": {"type": "number", "minimum": 0},
            "low": {"type": "number", "minimum": 0},
            "close": {"type": "number", "minimum": 0},
        },
        "additionalProperties": False,
    }

    with open(schema_dir / "test_ohlc.json", "w") as f:
        json.dump(ohlc_schema, f)

    return schema_dir


@pytest.fixture
def validator(schema_dir):
    """Create a ParquetValidator instance."""
    return ParquetValidator(schema_dir=schema_dir)


def test_validator_initialization(schema_dir):
    """Test validator initialization loads schemas."""
    validator = ParquetValidator(schema_dir=schema_dir)
    assert "test_schema" in validator.schemas
    assert "test_ohlc" in validator.schemas


def test_validator_missing_schema_dir():
    """Test validator raises error for missing schema directory."""
    with pytest.raises(ValueError, match="Schema directory does not exist"):
        ParquetValidator(schema_dir=Path("/nonexistent/path"))


def test_validate_dataframe_valid_data(validator):
    """Test validation passes for valid data."""
    df = pl.DataFrame({
        "event_id": ["uuid-1", "uuid-2"],
        "price": [100.0, 200.0],
        "volume": [1000, 2000],
        "optional_field": ["value", None],
    })

    result = validator.validate_dataframe(df, schema_name="test_schema")

    assert result.total_rows == 2
    assert result.valid_rows == 2
    assert result.critical_failures == 0
    assert len(result.error_details) == 0


def test_validate_dataframe_missing_required_field(validator):
    """Test validation fails for missing required field."""
    df = pl.DataFrame({
        "event_id": ["uuid-1", "uuid-2"],
        "price": [100.0, 200.0],
        # Missing required 'volume' field
    })

    result = validator.validate_dataframe(df, schema_name="test_schema")

    assert result.total_rows == 2
    assert result.critical_failures > 0
    assert any("volume" in e["message"] for e in result.error_details)


def test_validate_dataframe_invalid_type(validator):
    """Test validation fails for invalid type."""
    df = pl.DataFrame({
        "event_id": ["uuid-1", "uuid-2"],
        "price": ["invalid", "also-invalid"],  # Should be number
        "volume": [1000, 2000],
    })

    result = validator.validate_dataframe(df, schema_name="test_schema")

    assert result.total_rows == 2
    assert result.critical_failures > 0
    assert any("price" in e["field"] for e in result.error_details)


def test_validate_dataframe_negative_price(validator):
    """Test validation fails for negative price."""
    df = pl.DataFrame({
        "event_id": ["uuid-1", "uuid-2"],
        "price": [100.0, -50.0],  # Negative price
        "volume": [1000, 2000],
    })

    result = validator.validate_dataframe(df, schema_name="test_schema")

    assert result.total_rows == 2
    assert result.critical_failures > 0
    assert any("price" in e["field"] and "-50" in e["message"] for e in result.error_details)


def test_validate_dataframe_negative_volume(validator):
    """Test validation fails for negative volume."""
    df = pl.DataFrame({
        "event_id": ["uuid-1", "uuid-2"],
        "price": [100.0, 200.0],
        "volume": [1000, -500],  # Negative volume
    })

    result = validator.validate_dataframe(df, schema_name="test_schema")

    assert result.total_rows == 2
    assert result.critical_failures > 0
    assert any("volume" in e["field"] for e in result.error_details)


def test_validate_ohlc_consistency_valid(validator):
    """Test OHLC consistency validation passes for valid data."""
    df = pl.DataFrame({
        "event_id": ["uuid-1", "uuid-2"],
        "open": [100.0, 200.0],
        "high": [110.0, 220.0],
        "low": [95.0, 195.0],
        "close": [105.0, 210.0],
    })

    result = validator.validate_dataframe(df, schema_name="test_ohlc")

    assert result.total_rows == 2
    assert result.valid_rows == 2
    assert result.critical_failures == 0


def test_validate_ohlc_consistency_violation(validator):
    """Test OHLC consistency validation fails when high < low."""
    df = pl.DataFrame({
        "event_id": ["uuid-1", "uuid-2"],
        "open": [100.0, 200.0],
        "high": [110.0, 190.0],  # Second row: high < low
        "low": [95.0, 195.0],
        "close": [105.0, 210.0],
    })

    result = validator.validate_dataframe(df, schema_name="test_ohlc")

    assert result.total_rows == 2
    assert result.critical_failures > 0
    assert any("high" in e["field"] and "low" in e["field"] for e in result.error_details)
    assert any("OHLC violation" in e["message"] for e in result.error_details)


def test_validate_file(validator, tmp_path):
    """Test validation of Parquet file."""
    # Create test Parquet file
    df = pl.DataFrame({
        "event_id": ["uuid-1", "uuid-2"],
        "price": [100.0, 200.0],
        "volume": [1000, 2000],
    })
    
    parquet_file = tmp_path / "test_data.parquet"
    df.write_parquet(parquet_file)

    result = validator.validate_file(
        file_path=parquet_file,
        schema_name="test_schema"
    )

    assert result.total_rows == 2
    assert result.valid_rows == 2
    assert result.critical_failures == 0


def test_validate_file_with_quarantine(validator, tmp_path):
    """Test validation with quarantine for failed records."""
    # Create test data with invalid records
    df = pl.DataFrame({
        "event_id": ["uuid-1", "uuid-2", "uuid-3"],
        "price": [100.0, -50.0, 300.0],  # Second row has negative price
        "volume": [1000, 2000, 3000],
    })
    
    parquet_file = tmp_path / "test_data.parquet"
    df.write_parquet(parquet_file)

    quarantine_dir = tmp_path / "quarantine"

    result = validator.validate_file(
        file_path=parquet_file,
        schema_name="test_schema",
        quarantine_dir=quarantine_dir
    )

    assert result.critical_failures > 0
    
    # Check quarantine file was created
    quarantine_file = quarantine_dir / "test_schema_failures.parquet"
    assert quarantine_file.exists()
    
    # Read quarantine file
    quarantined_df = pl.read_parquet(quarantine_file)
    assert len(quarantined_df) > 0
    assert "validation_errors" in quarantined_df.columns
    assert "schema_name" in quarantined_df.columns


def test_validate_unknown_schema(validator):
    """Test validation fails for unknown schema."""
    df = pl.DataFrame({
        "event_id": ["uuid-1"],
        "price": [100.0],
        "volume": [1000],
    })

    with pytest.raises(ValueError, match="Schema 'nonexistent' not found"):
        validator.validate_dataframe(df, schema_name="nonexistent")


def test_validate_additional_properties(validator):
    """Test validation fails for additional properties when not allowed."""
    df = pl.DataFrame({
        "event_id": ["uuid-1"],
        "price": [100.0],
        "volume": [1000],
        "unexpected_field": ["value"],  # Not in schema
    })

    result = validator.validate_dataframe(df, schema_name="test_schema")

    assert result.critical_failures > 0
    assert any("unexpected_field" in e["message"] for e in result.error_details)
