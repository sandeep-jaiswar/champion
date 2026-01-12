#!/usr/bin/env python3
"""CLI script for cleaning up old partitions in the data lake.

This script removes partitions older than a specified retention period
to manage storage costs and comply with data retention policies.

Example:
    # Dry run (preview what would be deleted)
    python cleanup_partitions.py --dataset data/lake/raw --retention-days 90 --dry-run

    # Delete partitions older than 90 days
    python cleanup_partitions.py --dataset data/lake/raw --retention-days 90

    # Clean multiple datasets
    python cleanup_partitions.py --dataset data/lake/raw data/lake/normalized --retention-days 90

    # Use custom partition pattern
    python cleanup_partitions.py --dataset data/lake/raw --retention-days 90 --pattern "%Y%m%d"
"""

import argparse
import sys
from pathlib import Path

import structlog

# Add parent directory to path to import storage module
sys.path.insert(0, str(Path(__file__).parent.parent))

from champion.storage.retention import cleanup_old_partitions, get_dataset_statistics

logger = structlog.get_logger()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Clean up old partitions from data lake datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--dataset",
        "-d",
        type=str,
        nargs="+",
        required=True,
        help="Path(s) to dataset(s) to clean up (e.g., data/lake/raw)",
    )

    parser.add_argument(
        "--retention-days",
        "-r",
        type=int,
        required=True,
        help="Number of days to retain data (partitions older than this will be deleted)",
    )

    parser.add_argument(
        "--partition-key",
        "-k",
        type=str,
        default="date",
        help="Partition key name (default: date)",
    )

    parser.add_argument(
        "--pattern",
        "-p",
        type=str,
        default="%Y-%m-%d",
        help="Date pattern in partition names (default: %%Y-%%m-%%d)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )

    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show dataset statistics before cleanup",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Configure logging
    if args.verbose:
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(logging_level=10)
        )

    logger.info(
        "Starting partition cleanup",
        datasets=args.dataset,
        retention_days=args.retention_days,
        dry_run=args.dry_run,
    )

    total_deleted = 0

    for dataset_path in args.dataset:
        dataset_path = Path(dataset_path)

        if not dataset_path.exists():
            logger.error("Dataset path does not exist", path=str(dataset_path))
            continue

        logger.info("Processing dataset", dataset=str(dataset_path))

        # Show statistics if requested
        if args.stats:
            stats = get_dataset_statistics(dataset_path)
            logger.info("Dataset statistics (before cleanup)", **stats)

        # Cleanup old partitions
        deleted = cleanup_old_partitions(
            dataset_path=dataset_path,
            retention_days=args.retention_days,
            partition_pattern=args.pattern,
            partition_key=args.partition_key,
            dry_run=args.dry_run,
        )

        total_deleted += deleted

        logger.info(
            "Completed processing dataset",
            dataset=str(dataset_path),
            deleted=deleted,
        )

        # Show statistics after cleanup if requested
        if args.stats and not args.dry_run:
            stats = get_dataset_statistics(dataset_path)
            logger.info("Dataset statistics (after cleanup)", **stats)

    # Final summary
    logger.info(
        "Partition cleanup completed",
        total_deleted=total_deleted,
        datasets_processed=len(args.dataset),
        dry_run=args.dry_run,
    )

    if args.dry_run:
        logger.info("DRY RUN: No partitions were actually deleted")

    return 0


if __name__ == "__main__":
    sys.exit(main())
