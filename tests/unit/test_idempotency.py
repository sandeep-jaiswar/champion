"""Tests for idempotency utilities."""

import json
import shutil

import pytest
from champion.utils.idempotency import (
    check_idempotency_marker,
    create_idempotency_marker,
    get_completed_result,
    is_task_completed,
)


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    yield output_dir
    # Cleanup
    if output_dir.exists():
        shutil.rmtree(output_dir)


def test_create_idempotency_marker(temp_output_dir):
    """Test creating an idempotency marker file."""
    output_file = temp_output_dir / "data.parquet"
    output_file.write_text("test data")

    trade_date = "2024-01-15"
    rows = 1000

    marker_path = create_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        rows=rows,
        metadata={"source": "test"},
    )

    assert marker_path.exists()
    assert marker_path.name == f".idempotent.{trade_date}.json"

    # Verify marker content
    with open(marker_path) as f:
        marker_data = json.load(f)

    assert marker_data["trade_date"] == trade_date
    assert marker_data["rows"] == rows
    assert marker_data["file_hash"] is not None
    assert marker_data["metadata"]["source"] == "test"
    assert "timestamp" in marker_data


def test_check_idempotency_marker_valid(temp_output_dir):
    """Test checking a valid idempotency marker."""
    output_file = temp_output_dir / "data.parquet"
    output_file.write_text("test data")

    trade_date = "2024-01-15"
    rows = 1000

    # Create marker
    create_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        rows=rows,
    )

    # Check marker
    marker_data = check_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
    )

    assert marker_data is not None
    assert marker_data["trade_date"] == trade_date
    assert marker_data["rows"] == rows


def test_check_idempotency_marker_missing(temp_output_dir):
    """Test checking for a non-existent marker."""
    output_file = temp_output_dir / "data.parquet"
    output_file.write_text("test data")

    marker_data = check_idempotency_marker(
        output_file=output_file,
        trade_date="2024-01-15",
    )

    assert marker_data is None


def test_check_idempotency_marker_file_missing(temp_output_dir):
    """Test checking marker when output file is missing."""
    output_file = temp_output_dir / "data.parquet"
    output_file.write_text("test data")

    trade_date = "2024-01-15"

    # Create marker
    create_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        rows=1000,
    )

    # Delete output file
    output_file.unlink()

    # Check marker - should return None
    marker_data = check_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
    )

    assert marker_data is None


def test_check_idempotency_marker_hash_mismatch(temp_output_dir):
    """Test checking marker when file hash doesn't match."""
    output_file = temp_output_dir / "data.parquet"
    output_file.write_text("original data")

    trade_date = "2024-01-15"

    # Create marker
    create_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        rows=1000,
    )

    # Modify output file
    output_file.write_text("modified data")

    # Check marker - should return None due to hash mismatch
    marker_data = check_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        validate_hash=True,
    )

    assert marker_data is None


def test_check_idempotency_marker_no_hash_validation(temp_output_dir):
    """Test checking marker without hash validation."""
    output_file = temp_output_dir / "data.parquet"
    output_file.write_text("original data")

    trade_date = "2024-01-15"

    # Create marker
    create_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        rows=1000,
    )

    # Modify output file
    output_file.write_text("modified data")

    # Check marker without hash validation - should succeed
    marker_data = check_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        validate_hash=False,
    )

    assert marker_data is not None


def test_is_task_completed(temp_output_dir):
    """Test is_task_completed convenience function."""
    output_file = temp_output_dir / "data.parquet"
    output_file.write_text("test data")

    trade_date = "2024-01-15"

    # Before creating marker
    assert not is_task_completed(output_file, trade_date)

    # After creating marker
    create_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        rows=1000,
    )

    assert is_task_completed(output_file, trade_date)


def test_get_completed_result(temp_output_dir):
    """Test get_completed_result function."""
    output_file = temp_output_dir / "data.parquet"
    output_file.write_text("test data")

    trade_date = "2024-01-15"

    # Create marker
    create_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        rows=1000,
    )

    result = get_completed_result(output_file, trade_date)
    assert result == str(output_file)


def test_get_completed_result_not_completed(temp_output_dir):
    """Test get_completed_result when task is not completed."""
    output_file = temp_output_dir / "data.parquet"

    trade_date = "2024-01-15"

    with pytest.raises(ValueError, match="Task not completed"):
        get_completed_result(output_file, trade_date)


def test_idempotency_with_metadata(temp_output_dir):
    """Test idempotency marker with custom metadata."""
    output_file = temp_output_dir / "data.parquet"
    output_file.write_text("test data")

    trade_date = "2024-01-15"
    metadata = {
        "source": "nse_bhavcopy",
        "table": "normalized_equity_ohlc",
        "validation_passed": True,
    }

    create_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        rows=1000,
        metadata=metadata,
    )

    marker_data = check_idempotency_marker(output_file, trade_date)

    assert marker_data is not None
    assert marker_data["metadata"]["source"] == "nse_bhavcopy"
    assert marker_data["metadata"]["table"] == "normalized_equity_ohlc"
    assert marker_data["metadata"]["validation_passed"] is True


def test_multiple_markers_different_dates(temp_output_dir):
    """Test creating markers for different dates in the same directory."""
    output_dir = temp_output_dir / "partition"
    output_dir.mkdir()

    # Create markers for different dates
    for day in range(1, 4):
        output_file = output_dir / f"data_{day:02d}.parquet"
        output_file.write_text(f"data for day {day}")

        trade_date = f"2024-01-{day:02d}"
        create_idempotency_marker(
            output_file=output_file,
            trade_date=trade_date,
            rows=1000 + day,
        )

    # Verify all markers exist and are independent
    for day in range(1, 4):
        output_file = output_dir / f"data_{day:02d}.parquet"
        trade_date = f"2024-01-{day:02d}"

        marker_data = check_idempotency_marker(output_file, trade_date)
        assert marker_data is not None
        assert marker_data["rows"] == 1000 + day
