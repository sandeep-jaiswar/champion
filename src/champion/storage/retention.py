"""Retention policy utilities for managing data lake partitions."""

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Union

import structlog

logger = structlog.get_logger()


def calculate_partition_age(partition_path: Path, partition_pattern: str = "%Y-%m-%d") -> int:
    """
    Calculate the age of a partition in days based on its path.

    Assumes partition directories follow a date-based naming convention.

    Args:
        partition_path: Path to the partition directory
        partition_pattern: Date pattern used in partition names (default: %Y-%m-%d)

    Returns:
        Age of partition in days

    Example:
        >>> calculate_partition_age(Path('data/lake/raw/date=2024-01-01'))
        15  # If current date is 2024-01-16
    """
    # Extract date from partition path
    # Handle various partition naming conventions
    partition_name = partition_path.name

    # Try to extract date value from key=value format
    if "=" in partition_name:
        date_str = partition_name.split("=")[-1]
    else:
        date_str = partition_name

    try:
        partition_date = datetime.strptime(date_str, partition_pattern)
        age_days = (datetime.now() - partition_date).days
        return age_days
    except ValueError as e:
        logger.warning(
            "Failed to parse partition date",
            partition=str(partition_path),
            pattern=partition_pattern,
            error=str(e),
        )
        return -1


def find_old_partitions(
    dataset_path: Union[str, Path],
    retention_days: int,
    partition_pattern: str = "%Y-%m-%d",
    partition_key: str = "date",
) -> List[Path]:
    """
    Find partitions older than the specified retention period.

    Args:
        dataset_path: Path to the dataset
        retention_days: Number of days to retain data
        partition_pattern: Date pattern in partition names
        partition_key: Partition key name (e.g., 'date', 'year')

    Returns:
        List of partition paths that are older than retention period

    Example:
        >>> old_partitions = find_old_partitions('data/lake/raw', retention_days=30)
        >>> print(f"Found {len(old_partitions)} partitions to delete")
    """
    dataset_path = Path(dataset_path)

    if not dataset_path.exists():
        logger.warning("Dataset path does not exist", path=str(dataset_path))
        return []

    old_partitions = []
    cutoff_date = datetime.now() - timedelta(days=retention_days)

    # Find all partition directories
    # Look for directories matching partition_key pattern
    partition_dirs = []

    for item in dataset_path.rglob(f"{partition_key}=*"):
        if item.is_dir():
            partition_dirs.append(item)

    # Also check for direct date-based directories
    if not partition_dirs:
        for item in dataset_path.iterdir():
            if item.is_dir() and not item.name.startswith("_"):
                partition_dirs.append(item)

    logger.info(
        "Scanning for old partitions",
        dataset=str(dataset_path),
        partition_count=len(partition_dirs),
        retention_days=retention_days,
    )

    for partition_dir in partition_dirs:
        age_days = calculate_partition_age(partition_dir, partition_pattern)

        if age_days > retention_days:
            old_partitions.append(partition_dir)
            logger.debug(
                "Found old partition",
                partition=str(partition_dir),
                age_days=age_days,
                retention_days=retention_days,
            )

    logger.info(
        "Found old partitions",
        count=len(old_partitions),
        retention_days=retention_days,
    )

    return old_partitions


def cleanup_old_partitions(
    dataset_path: Union[str, Path],
    retention_days: int,
    partition_pattern: str = "%Y-%m-%d",
    partition_key: str = "date",
    dry_run: bool = False,
) -> int:
    """
    Remove partitions older than the specified retention period.

    Args:
        dataset_path: Path to the dataset
        retention_days: Number of days to retain data
        partition_pattern: Date pattern in partition names
        partition_key: Partition key name (e.g., 'date', 'year')
        dry_run: If True, only report what would be deleted without removing

    Returns:
        Number of partitions deleted

    Example:
        >>> # Remove partitions older than 90 days
        >>> deleted = cleanup_old_partitions('data/lake/raw', retention_days=90)
        >>> print(f"Deleted {deleted} old partitions")
        >>>
        >>> # Dry run to see what would be deleted
        >>> cleanup_old_partitions('data/lake/raw', retention_days=90, dry_run=True)
    """
    dataset_path = Path(dataset_path)

    old_partitions = find_old_partitions(
        dataset_path=dataset_path,
        retention_days=retention_days,
        partition_pattern=partition_pattern,
        partition_key=partition_key,
    )

    if not old_partitions:
        logger.info("No old partitions to delete", dataset=str(dataset_path))
        return 0

    if dry_run:
        logger.info(
            "DRY RUN: Would delete partitions",
            count=len(old_partitions),
            partitions=[str(p) for p in old_partitions],
        )
        return len(old_partitions)

    # Delete partitions
    deleted_count = 0
    for partition_dir in old_partitions:
        try:
            import shutil

            shutil.rmtree(partition_dir)
            deleted_count += 1
            logger.info("Deleted partition", partition=str(partition_dir))
        except Exception as e:
            logger.error(
                "Failed to delete partition",
                partition=str(partition_dir),
                error=str(e),
            )

    logger.info(
        "Cleanup complete",
        deleted=deleted_count,
        failed=len(old_partitions) - deleted_count,
    )

    return deleted_count


def get_dataset_statistics(dataset_path: Union[str, Path]) -> dict:
    """
    Get statistics about a Parquet dataset.

    Args:
        dataset_path: Path to the dataset

    Returns:
        Dictionary containing dataset statistics

    Example:
        >>> stats = get_dataset_statistics('data/lake/raw')
        >>> print(f"Total size: {stats['total_size_mb']} MB")
        >>> print(f"File count: {stats['file_count']}")
    """
    dataset_path = Path(dataset_path)

    if not dataset_path.exists():
        logger.warning("Dataset path does not exist", path=str(dataset_path))
        return {}

    parquet_files = list(dataset_path.rglob("*.parquet"))
    parquet_files = [f for f in parquet_files if not f.name.startswith("_")]

    total_size = sum(f.stat().st_size for f in parquet_files)
    total_size_mb = total_size / (1024 * 1024)

    stats = {
        "file_count": len(parquet_files),
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size_mb, 2),
        "avg_file_size_mb": round(total_size_mb / len(parquet_files), 2)
        if parquet_files
        else 0,
        "dataset_path": str(dataset_path),
    }

    logger.info("Dataset statistics", **stats)

    return stats
