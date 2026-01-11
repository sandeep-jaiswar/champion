"""Tests for retention policy utilities."""

import shutil
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from champion.storage.retention import (
    calculate_partition_age,
    cleanup_old_partitions,
    find_old_partitions,
    get_dataset_statistics,
)


@pytest.fixture
def temp_lake_dir(tmp_path):
    """Create a temporary data lake directory."""
    lake_dir = tmp_path / "lake"
    lake_dir.mkdir()
    yield lake_dir
    if lake_dir.exists():
        shutil.rmtree(lake_dir)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame."""
    return pl.DataFrame(
        {
            "symbol": ["AAPL", "GOOGL"],
            "price": [150.0, 2800.0],
            "volume": [1000000, 500000],
        }
    )


def create_dated_partition(base_path: Path, date: datetime, df: pl.DataFrame):
    """Helper to create a partition with a specific date."""
    date_str = date.strftime("%Y-%m-%d")
    partition_dir = base_path / f"date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    file_path = partition_dir / "data.parquet"
    df.write_parquet(file_path)

    return partition_dir


def test_calculate_partition_age():
    """Test calculating partition age."""
    # Test with recent date
    recent_date = datetime.now() - timedelta(days=5)
    date_str = recent_date.strftime("%Y-%m-%d")
    partition_path = Path(f"date={date_str}")

    age = calculate_partition_age(partition_path)
    assert age == 5

    # Test with older date
    old_date = datetime.now() - timedelta(days=100)
    date_str = old_date.strftime("%Y-%m-%d")
    partition_path = Path(f"date={date_str}")

    age = calculate_partition_age(partition_path)
    assert age == 100


def test_calculate_partition_age_without_key():
    """Test calculating age when partition doesn't have key=value format."""
    date = datetime.now() - timedelta(days=10)
    date_str = date.strftime("%Y-%m-%d")
    partition_path = Path(date_str)

    age = calculate_partition_age(partition_path)
    assert age == 10


def test_calculate_partition_age_invalid_format():
    """Test calculating age with invalid date format."""
    partition_path = Path("date=invalid-date")

    age = calculate_partition_age(partition_path)
    assert age == -1  # Should return -1 for invalid dates


def test_find_old_partitions(temp_lake_dir, sample_df):
    """Test finding old partitions."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Create partitions with different ages
    create_dated_partition(dataset_path, datetime.now() - timedelta(days=5), sample_df)
    create_dated_partition(dataset_path, datetime.now() - timedelta(days=35), sample_df)
    create_dated_partition(dataset_path, datetime.now() - timedelta(days=95), sample_df)

    # Find partitions older than 30 days
    old_partitions = find_old_partitions(dataset_path, retention_days=30)

    assert len(old_partitions) == 2  # 35 and 95 day old partitions


def test_find_old_partitions_none_found(temp_lake_dir, sample_df):
    """Test finding old partitions when none exist."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Create only recent partitions
    create_dated_partition(dataset_path, datetime.now() - timedelta(days=1), sample_df)
    create_dated_partition(dataset_path, datetime.now() - timedelta(days=2), sample_df)

    # Find partitions older than 30 days
    old_partitions = find_old_partitions(dataset_path, retention_days=30)

    assert len(old_partitions) == 0


def test_cleanup_old_partitions_dry_run(temp_lake_dir, sample_df):
    """Test cleanup in dry-run mode."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Create partitions
    old_partition = create_dated_partition(
        dataset_path, datetime.now() - timedelta(days=95), sample_df
    )
    recent_partition = create_dated_partition(
        dataset_path, datetime.now() - timedelta(days=5), sample_df
    )

    # Dry run
    deleted = cleanup_old_partitions(dataset_path, retention_days=30, dry_run=True)

    assert deleted == 1  # Reports 1 partition would be deleted
    assert old_partition.exists()  # But partition still exists
    assert recent_partition.exists()


def test_cleanup_old_partitions(temp_lake_dir, sample_df):
    """Test actual cleanup of old partitions."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Create partitions
    old_partition = create_dated_partition(
        dataset_path, datetime.now() - timedelta(days=95), sample_df
    )
    recent_partition = create_dated_partition(
        dataset_path, datetime.now() - timedelta(days=5), sample_df
    )

    # Cleanup
    deleted = cleanup_old_partitions(dataset_path, retention_days=30, dry_run=False)

    assert deleted == 1
    assert not old_partition.exists()  # Old partition deleted
    assert recent_partition.exists()  # Recent partition kept


def test_cleanup_multiple_old_partitions(temp_lake_dir, sample_df):
    """Test cleaning up multiple old partitions."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Create multiple old partitions
    for days_ago in [40, 50, 60, 70]:
        create_dated_partition(dataset_path, datetime.now() - timedelta(days=days_ago), sample_df)

    # Create one recent partition
    create_dated_partition(dataset_path, datetime.now() - timedelta(days=5), sample_df)

    # Cleanup
    deleted = cleanup_old_partitions(dataset_path, retention_days=30, dry_run=False)

    assert deleted == 4  # All 4 old partitions deleted

    # Verify only recent partition remains
    remaining = list(dataset_path.glob("date=*"))
    assert len(remaining) == 1


def test_cleanup_nonexistent_dataset(temp_lake_dir):
    """Test cleanup with non-existent dataset."""
    nonexistent = temp_lake_dir / "nonexistent"

    deleted = cleanup_old_partitions(nonexistent, retention_days=30)
    assert deleted == 0


def test_get_dataset_statistics(temp_lake_dir, sample_df):
    """Test getting dataset statistics."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Create some partitions
    for days_ago in [1, 2, 3]:
        create_dated_partition(dataset_path, datetime.now() - timedelta(days=days_ago), sample_df)

    stats = get_dataset_statistics(dataset_path)

    assert stats["file_count"] == 3
    assert stats["total_size_bytes"] > 0
    assert stats["avg_file_size_mb"] >= 0
    assert stats["dataset_path"] == str(dataset_path)


def test_get_statistics_empty_dataset(temp_lake_dir):
    """Test statistics for empty dataset."""
    dataset_path = temp_lake_dir / "empty"
    dataset_path.mkdir()

    stats = get_dataset_statistics(dataset_path)

    assert stats["file_count"] == 0
    assert stats["total_size_mb"] == 0
    assert stats["avg_file_size_mb"] == 0


def test_get_statistics_nonexistent_dataset(temp_lake_dir):
    """Test statistics for non-existent dataset."""
    nonexistent = temp_lake_dir / "nonexistent"

    stats = get_dataset_statistics(nonexistent)
    assert stats == {}


def test_custom_partition_pattern(temp_lake_dir, sample_df):
    """Test with custom partition pattern."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Create partition with custom pattern (YYYYMMDD)
    date = datetime.now() - timedelta(days=40)
    date_str = date.strftime("%Y%m%d")
    partition_dir = dataset_path / f"date={date_str}"
    partition_dir.mkdir()
    (partition_dir / "data.parquet").write_text("dummy")

    # Find with custom pattern
    old_partitions = find_old_partitions(
        dataset_path, retention_days=30, partition_pattern="%Y%m%d"
    )

    assert len(old_partitions) == 1


def test_custom_partition_key(temp_lake_dir, sample_df):
    """Test with custom partition key."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Create partition with custom key
    date = datetime.now() - timedelta(days=40)
    date_str = date.strftime("%Y-%m-%d")
    partition_dir = dataset_path / f"year={date_str}"
    partition_dir.mkdir()
    sample_df.write_parquet(partition_dir / "data.parquet")

    # Find with custom key
    old_partitions = find_old_partitions(
        dataset_path, retention_days=30, partition_key="year"
    )

    assert len(old_partitions) == 1
