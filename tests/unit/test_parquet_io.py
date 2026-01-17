"""Tests for Parquet I/O utilities."""

import shutil

import polars as pl
import pyarrow.parquet as pq
import pytest

from champion.storage.parquet_io import (
    coalesce_small_files,
    generate_dataset_metadata,
    write_df,
)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "symbol": ["AAPL", "GOOGL", "MSFT", "AMZN"] * 25,
            "price": [150.0, 2800.0, 350.0, 3200.0] * 25,
            "volume": [1000000, 500000, 750000, 600000] * 25,
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"] * 25,
        }
    )


@pytest.fixture
def temp_lake_dir(tmp_path):
    """Create a temporary data lake directory."""
    lake_dir = tmp_path / "lake"
    lake_dir.mkdir()
    yield lake_dir
    # Cleanup
    if lake_dir.exists():
        shutil.rmtree(lake_dir)


def test_write_df_no_partitions(sample_df, temp_lake_dir):
    """Test writing DataFrame without partitions."""
    dataset_path = write_df(
        df=sample_df,
        dataset="raw",
        base_path=temp_lake_dir,
        partitions=None,
    )

    assert dataset_path.exists()
    assert (dataset_path / "data.parquet").exists()

    # Verify data can be read back
    df_read = pl.read_parquet(dataset_path / "data.parquet")
    assert len(df_read) == len(sample_df)


def test_write_df_with_partitions(sample_df, temp_lake_dir):
    """Test writing DataFrame with date partitioning."""
    dataset_path = write_df(
        df=sample_df,
        dataset="raw",
        base_path=temp_lake_dir,
        partitions=["date"],
    )

    assert dataset_path.exists()

    # Check partition directories exist
    partitions = list(dataset_path.glob("date=*"))
    assert len(partitions) > 0

    # Verify data can be read back (use glob pattern for partitioned data)
    df_read = pl.read_parquet(dataset_path / "**/*.parquet")
    assert len(df_read) == len(sample_df)


def test_write_df_with_multiple_partitions(sample_df, temp_lake_dir):
    """Test writing DataFrame with multiple partition columns."""
    # Add year and month columns
    df = sample_df.with_columns(
        [
            pl.col("date").str.slice(0, 4).alias("year"),
            pl.col("date").str.slice(5, 2).alias("month"),
        ]
    )

    dataset_path = write_df(
        df=df,
        dataset="raw",
        base_path=temp_lake_dir,
        partitions=["year", "month"],
    )

    assert dataset_path.exists()

    # Check nested partition structure
    year_partitions = list(dataset_path.glob("year=*"))
    assert len(year_partitions) > 0

    # Verify data (use glob pattern for partitioned data)
    df_read = pl.read_parquet(dataset_path / "**/*.parquet")
    assert len(df_read) == len(df)


def test_write_df_compression_options(sample_df, temp_lake_dir):
    """Test different compression options."""
    for compression in ["snappy", "gzip", "zstd"]:
        dataset_path = write_df(
            df=sample_df,
            dataset=f"raw_{compression}",
            base_path=temp_lake_dir,
            partitions=None,
            compression=compression,
        )

        assert dataset_path.exists()
        assert (dataset_path / "data.parquet").exists()


def test_coalesce_small_files(sample_df, temp_lake_dir):
    """Test coalescing small Parquet files."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Write multiple small files
    for i in range(5):
        small_df = sample_df.head(10)
        file_path = dataset_path / f"file_{i}.parquet"
        small_df.write_parquet(file_path)

    # Count files before coalescing
    files_before = list(dataset_path.glob("*.parquet"))
    assert len(files_before) == 5

    # Coalesce files (use 0.01 MB threshold to ensure our test files are small enough)
    coalesced = coalesce_small_files(
        dataset_path=dataset_path, target_file_size_mb=1, min_file_size_mb=0.01, dry_run=False
    )

    # Should have coalesced some files
    assert coalesced > 0

    # Verify data is still accessible (use glob pattern to read all files)
    df_read = pl.read_parquet(dataset_path / "*.parquet")
    assert len(df_read) == 50  # 5 files * 10 rows


def test_coalesce_dry_run(sample_df, temp_lake_dir):
    """Test coalesce in dry-run mode."""
    dataset_path = temp_lake_dir / "raw"
    dataset_path.mkdir()

    # Write small files
    for i in range(3):
        small_df = sample_df.head(5)
        file_path = dataset_path / f"file_{i}.parquet"
        small_df.write_parquet(file_path)

    files_before = list(dataset_path.glob("*.parquet"))

    # Dry run should not modify files (use 0.01 MB threshold)
    coalesced = coalesce_small_files(
        dataset_path=dataset_path, target_file_size_mb=1, min_file_size_mb=0.01, dry_run=True
    )

    files_after = list(dataset_path.glob("*.parquet"))

    assert coalesced == 3  # Reports 3 files would be coalesced
    assert len(files_before) == len(files_after)  # No files actually changed


def test_generate_dataset_metadata(sample_df, temp_lake_dir):
    """Test generating _metadata and _common_metadata files."""
    # Write a partitioned dataset
    dataset_path = write_df(
        df=sample_df,
        dataset="raw",
        base_path=temp_lake_dir,
        partitions=["date"],
    )

    # Generate metadata
    metadata_file, common_metadata_file = generate_dataset_metadata(dataset_path)

    assert metadata_file.exists()
    assert common_metadata_file.exists()
    assert metadata_file.name == "_metadata"
    assert common_metadata_file.name == "_common_metadata"

    # Verify metadata files are valid Parquet metadata
    schema = pq.read_schema(common_metadata_file)
    assert schema is not None


def test_generate_metadata_force_regenerate(sample_df, temp_lake_dir):
    """Test force regenerating metadata."""
    dataset_path = write_df(
        df=sample_df,
        dataset="raw",
        base_path=temp_lake_dir,
        partitions=None,
    )

    # Generate metadata first time
    metadata_file, _ = generate_dataset_metadata(dataset_path)
    first_mtime = metadata_file.stat().st_mtime

    import time

    time.sleep(0.1)

    # Regenerate with force
    metadata_file, _ = generate_dataset_metadata(dataset_path, force_regenerate=True)
    second_mtime = metadata_file.stat().st_mtime

    assert second_mtime >= first_mtime


def test_generate_metadata_no_files(temp_lake_dir):
    """Test generating metadata for empty dataset."""
    dataset_path = temp_lake_dir / "empty"
    dataset_path.mkdir()

    with pytest.raises(ValueError, match="No Parquet files found"):
        generate_dataset_metadata(dataset_path)


def test_coalesce_no_files(temp_lake_dir):
    """Test coalescing when no files exist."""
    dataset_path = temp_lake_dir / "empty"
    dataset_path.mkdir()

    result = coalesce_small_files(dataset_path)
    assert result == 0


def test_coalesce_nonexistent_path(temp_lake_dir):
    """Test coalescing with non-existent path."""
    nonexistent = temp_lake_dir / "nonexistent"

    result = coalesce_small_files(nonexistent)
    assert result == 0
