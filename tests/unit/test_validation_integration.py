"""Tests for validation integration in ETL pipeline."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from champion.storage.parquet_io import write_df_safe
from champion.validation.validator import ParquetValidator, ValidationResult


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "indicator_date": ["2024-01-01", "2024-01-02"],
            "indicator_code": ["REPO_RATE", "CPI_COMBINED"],
            "indicator_name": ["Repo Rate", "CPI Combined"],
            "indicator_category": ["POLICY_RATE", "INFLATION"],
            "value": [6.5, 5.2],
            "unit": ["%", "%"],
            "frequency": ["DAILY", "MONTHLY"],
            "source": ["RBI", "MOSPI"],
            "source_url": [None, None],
            "metadata": [None, None],
            "ingestion_timestamp": ["2024-01-01T10:00:00", "2024-01-02T10:00:00"],
        }
    )


@pytest.fixture
def schema_dir(tmp_path):
    """Create a temporary schema directory with test schema."""
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()

    # Create a simple test schema
    test_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": [
            "indicator_date",
            "indicator_code",
            "indicator_name",
            "indicator_category",
            "value",
            "unit",
            "frequency",
            "source",
            "ingestion_timestamp",
        ],
        "properties": {
            "indicator_date": {"type": "string"},
            "indicator_code": {"type": "string"},
            "indicator_name": {"type": "string"},
            "indicator_category": {"type": "string"},
            "value": {"type": "number"},
            "unit": {"type": "string"},
            "frequency": {"type": "string"},
            "source": {"type": "string"},
            "source_url": {"type": ["string", "null"]},
            "metadata": {"type": ["string", "null"]},
            "ingestion_timestamp": {"type": "string"},
        },
    }

    with open(schema_dir / "test_schema.json", "w") as f:
        json.dump(test_schema, f)

    return schema_dir


def test_write_df_safe_success(sample_df, schema_dir, tmp_path):
    """Test write_df_safe successfully writes valid data."""
    base_path = tmp_path / "lake"
    dataset = "test_data"

    output_path = write_df_safe(
        df=sample_df,
        dataset=dataset,
        base_path=base_path,
        schema_name="test_schema",
        schema_dir=schema_dir,
        fail_on_validation_errors=True,
    )

    assert output_path.exists()
    assert (output_path / "data.parquet").exists()

    # Verify data was written correctly
    written_df = pl.read_parquet(output_path / "data.parquet")
    assert len(written_df) == len(sample_df)


def test_write_df_safe_validation_failure(schema_dir, tmp_path):
    """Test write_df_safe raises error on validation failure."""
    # Create invalid data (missing required field)
    invalid_df = pl.DataFrame(
        {
            "indicator_date": ["2024-01-01"],
            "indicator_code": ["REPO_RATE"],
            # Missing required fields
        }
    )

    base_path = tmp_path / "lake"
    dataset = "test_data"

    with pytest.raises(ValueError, match="Validation failed"):
        write_df_safe(
            df=invalid_df,
            dataset=dataset,
            base_path=base_path,
            schema_name="test_schema",
            schema_dir=schema_dir,
            fail_on_validation_errors=True,
        )


def test_write_df_safe_with_quarantine(schema_dir, tmp_path):
    """Test write_df_safe quarantines failed records."""
    # Create data with a row missing a required field
    mixed_df = pl.DataFrame(
        {
            "indicator_date": ["2024-01-01"],
            "indicator_code": ["REPO_RATE"],
            # Missing required fields: indicator_name, indicator_category, value, unit, frequency, source, ingestion_timestamp
        }
    )

    base_path = tmp_path / "lake"
    dataset = "test_data"
    quarantine_dir = tmp_path / "quarantine"

    with pytest.raises(ValueError, match="Validation failed"):
        write_df_safe(
            df=mixed_df,
            dataset=dataset,
            base_path=base_path,
            schema_name="test_schema",
            schema_dir=schema_dir,
            fail_on_validation_errors=True,
            quarantine_dir=quarantine_dir,
        )

    # Verify quarantine file was created
    quarantine_file = quarantine_dir / "test_schema_failures.parquet"
    assert quarantine_file.exists()


def test_write_df_safe_continue_on_errors(schema_dir, tmp_path):
    """Test write_df_safe continues writing when fail_on_validation_errors is False."""
    # Create data with validation errors
    invalid_df = pl.DataFrame(
        {
            "indicator_date": ["2024-01-01"],
            "indicator_code": ["REPO_RATE"],
            # Missing required fields, but we'll continue anyway
            "value": [6.5],
        }
    )

    base_path = tmp_path / "lake"
    dataset = "test_data"

    # This should not raise an error but should log warnings
    output_path = write_df_safe(
        df=invalid_df,
        dataset=dataset,
        base_path=base_path,
        schema_name="test_schema",
        schema_dir=schema_dir,
        fail_on_validation_errors=False,  # Don't fail
    )

    # Data should still be written
    assert output_path.exists()


@patch("champion.storage.parquet_io.ParquetValidator")
def test_write_df_safe_logs_validation_metrics(mock_validator_class, sample_df, tmp_path):
    """Test that write_df_safe logs validation metrics."""
    # Setup mock
    mock_validator = MagicMock()
    mock_validator_class.return_value = mock_validator
    mock_result = ValidationResult(
        total_rows=2, valid_rows=2, critical_failures=0, warnings=0, error_details=[]
    )
    mock_validator.validate_dataframe.return_value = mock_result

    base_path = tmp_path / "lake"
    dataset = "test_data"

    output_path = write_df_safe(
        df=sample_df,
        dataset=dataset,
        base_path=base_path,
        schema_name="test_schema",
        schema_dir=tmp_path / "schemas",
        fail_on_validation_errors=True,
    )

    # Verify validator was called
    mock_validator.validate_dataframe.assert_called_once()
    assert output_path.exists()
