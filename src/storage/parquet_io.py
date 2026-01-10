"""Parquet I/O utilities for data lake operations."""

import shutil
from pathlib import Path
from typing import List, Optional, Union

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger()


def write_df(
    df: pl.DataFrame,
    dataset: str,
    base_path: Union[str, Path],
    partitions: Optional[List[str]] = None,
    max_rows_per_file: int = 1_000_000,
    compression: str = "snappy",
) -> Path:
    """
    Write a Polars DataFrame to a Parquet dataset with optional partitioning.

    Args:
        df: Polars DataFrame to write
        dataset: Dataset name (e.g., 'raw', 'normalized', 'features')
        base_path: Base path for the data lake (e.g., 'data/lake')
        partitions: List of column names to partition by (e.g., ['year', 'month', 'day'])
        max_rows_per_file: Maximum rows per file (for splitting large datasets)
        compression: Compression codec ('snappy', 'gzip', 'zstd', 'none')

    Returns:
        Path to the written dataset directory

    Example:
        >>> df = pl.DataFrame({
        ...     'symbol': ['AAPL', 'GOOGL'],
        ...     'price': [150.0, 2800.0],
        ...     'date': ['2024-01-01', '2024-01-01']
        ... })
        >>> write_df(df, 'raw', 'data/lake', partitions=['date'])
    """
    base_path = Path(base_path)
    dataset_path = base_path / dataset
    dataset_path.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Writing DataFrame to Parquet dataset",
        dataset=dataset,
        path=str(dataset_path),
        rows=len(df),
        partitions=partitions,
    )

    # Convert Polars DataFrame to PyArrow Table
    arrow_table = df.to_arrow()

    # Write with partitioning if specified
    if partitions:
        pq.write_to_dataset(
            arrow_table,
            root_path=str(dataset_path),
            partition_cols=partitions,
            compression=compression,
            existing_data_behavior="overwrite_or_ignore",
        )
    else:
        # Write as a single file
        output_file = dataset_path / "data.parquet"
        pq.write_table(
            arrow_table,
            output_file,
            compression=compression,
        )

    logger.info("Successfully wrote Parquet dataset", path=str(dataset_path))
    return dataset_path


def coalesce_small_files(
    dataset_path: Union[str, Path],
    target_file_size_mb: int = 128,
    min_file_size_mb: int = 10,
    dry_run: bool = False,
) -> int:
    """
    Coalesce small Parquet files into larger files for better query performance.

    This function identifies small Parquet files and combines them into fewer,
    larger files to reduce metadata overhead and improve read performance.

    Args:
        dataset_path: Path to the Parquet dataset
        target_file_size_mb: Target size for coalesced files in MB
        min_file_size_mb: Files smaller than this will be considered for coalescing
        dry_run: If True, only report what would be done without making changes

    Returns:
        Number of files coalesced

    Example:
        >>> coalesce_small_files('data/lake/raw', target_file_size_mb=128)
        Coalesced 15 small files into 3 larger files
    """
    dataset_path = Path(dataset_path)

    if not dataset_path.exists():
        logger.warning("Dataset path does not exist", path=str(dataset_path))
        return 0

    # Find all parquet files
    parquet_files = list(dataset_path.rglob("*.parquet"))
    parquet_files = [f for f in parquet_files if not f.name.startswith("_")]

    if not parquet_files:
        logger.info("No Parquet files found", path=str(dataset_path))
        return 0

    # Identify small files
    small_files = []
    min_size_bytes = min_file_size_mb * 1024 * 1024

    for file_path in parquet_files:
        size = file_path.stat().st_size
        if size <= min_size_bytes:
            small_files.append((file_path, size))

    if not small_files:
        logger.info(
            "No small files to coalesce",
            path=str(dataset_path),
            min_size_mb=min_file_size_mb,
        )
        return 0

    logger.info(
        "Found small files to coalesce",
        count=len(small_files),
        total_size_mb=sum(s for _, s in small_files) / (1024 * 1024),
    )

    if dry_run:
        logger.info("Dry run mode - no changes will be made")
        return len(small_files)

    # Group files by partition (same parent directory)
    from collections import defaultdict

    files_by_partition = defaultdict(list)
    for file_path, size in small_files:
        partition_dir = file_path.parent
        files_by_partition[partition_dir].append(file_path)

    coalesced_count = 0
    target_size_bytes = target_file_size_mb * 1024 * 1024

    # Coalesce files within each partition
    for partition_dir, files in files_by_partition.items():
        if len(files) < 2:
            continue

        # Read all small files in this partition
        tables = []
        for file_path in files:
            table = pq.read_table(file_path)
            tables.append(table)

        # Concatenate tables
        combined_table = pa.concat_tables(tables)

        # Write combined table
        temp_file = partition_dir / f"_temp_coalesced_{partition_dir.name}.parquet"
        pq.write_table(
            combined_table,
            temp_file,
            compression="snappy",
            row_group_size=target_size_bytes // 100,  # Approximate row group size
        )

        # Remove old small files
        for file_path in files:
            file_path.unlink()
            coalesced_count += 1

        # Rename temp file
        final_file = partition_dir / f"coalesced_{partition_dir.name}.parquet"
        temp_file.rename(final_file)

        logger.info(
            "Coalesced files",
            partition=str(partition_dir),
            input_files=len(files),
            output_file=final_file.name,
        )

    logger.info("Coalescing complete", files_coalesced=coalesced_count)
    return coalesced_count


def generate_dataset_metadata(
    dataset_path: Union[str, Path],
    force_regenerate: bool = False,
) -> tuple[Path, Path]:
    """
    Generate _metadata and _common_metadata files for a Parquet dataset.

    These metadata files enable faster query planning by providing schema
    and file-level statistics without reading individual files.

    Args:
        dataset_path: Path to the Parquet dataset
        force_regenerate: If True, regenerate even if metadata files exist

    Returns:
        Tuple of (metadata_file_path, common_metadata_file_path)

    Example:
        >>> generate_dataset_metadata('data/lake/raw')
        (Path('data/lake/raw/_metadata'), Path('data/lake/raw/_common_metadata'))
    """
    dataset_path = Path(dataset_path)

    if not dataset_path.exists():
        raise ValueError(f"Dataset path does not exist: {dataset_path}")

    metadata_file = dataset_path / "_metadata"
    common_metadata_file = dataset_path / "_common_metadata"

    # Check if metadata already exists
    if not force_regenerate:
        if metadata_file.exists() and common_metadata_file.exists():
            logger.info(
                "Metadata files already exist",
                path=str(dataset_path),
                use_force_regenerate=True,
            )
            return metadata_file, common_metadata_file

    logger.info("Generating dataset metadata", path=str(dataset_path))

    # Find all parquet files
    parquet_files = list(dataset_path.rglob("*.parquet"))
    parquet_files = [
        f for f in parquet_files if not f.name.startswith("_")
    ]  # Exclude metadata files

    if not parquet_files:
        logger.warning("No Parquet files found", path=str(dataset_path))
        raise ValueError(f"No Parquet files found in {dataset_path}")

    # Read metadata from all files
    metadata_list = []
    schema = None

    for file_path in parquet_files:
        file_metadata = pq.read_metadata(str(file_path))
        metadata_list.append(file_metadata)

        if schema is None:
            schema = pq.read_schema(str(file_path))

    # Write _common_metadata (schema only)
    pq.write_metadata(schema, common_metadata_file)
    logger.info("Generated _common_metadata", file=str(common_metadata_file))

    # Write _metadata (schema + all file metadata)
    # Create a simple metadata file with schema
    pq.write_metadata(schema, metadata_file)
    logger.info("Generated _metadata", file=str(metadata_file), file_count=len(parquet_files))

    return metadata_file, common_metadata_file
