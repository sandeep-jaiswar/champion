"""Idempotency utilities for Prefect tasks.

This module provides utilities to ensure tasks are idempotent by:
- Creating marker files when tasks complete successfully
- Checking marker files before executing tasks
- Validating data integrity using hashes

Usage:
    from champion.utils.idempotency import (
        create_idempotency_marker,
        check_idempotency_marker,
        is_task_completed,
    )

    # Before task execution
    if is_task_completed(output_file, trade_date):
        logger.info("Task already completed, skipping")
        return cached_result

    # After task execution
    create_idempotency_marker(
        output_file=output_file,
        trade_date=trade_date,
        rows=len(df),
        metadata={"source": "nse_bhavcopy"}
    )
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def _get_marker_path(output_file: Path, trade_date: str) -> Path:
    """Get the path for the idempotency marker file.

    Args:
        output_file: Path to the output data file
        trade_date: Trading date in ISO format (YYYY-MM-DD)

    Returns:
        Path to the marker file
    """
    # Store marker in same directory as output file
    marker_name = f".idempotent.{trade_date}.json"
    return output_file.parent / marker_name


def create_idempotency_marker(
    output_file: Path,
    trade_date: str,
    rows: int,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Create an idempotency marker file.

    The marker file contains:
    - timestamp: When the task completed
    - rows: Number of rows written
    - file_hash: SHA256 hash of the output file
    - metadata: Additional task-specific metadata

    Args:
        output_file: Path to the output data file
        trade_date: Trading date in ISO format (YYYY-MM-DD)
        rows: Number of rows written
        metadata: Optional additional metadata

    Returns:
        Path to the created marker file

    Raises:
        OSError: If marker file cannot be written
    """
    try:
        # Calculate file hash for integrity validation
        file_hash = None
        if output_file.exists():
            with open(output_file, "rb") as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()

        # Create marker data
        marker_data = {
            "timestamp": datetime.now().isoformat(),
            "trade_date": trade_date,
            "rows": rows,
            "file_hash": file_hash,
            "output_file": str(output_file),
            "metadata": metadata or {},
        }

        # Write marker file
        marker_path = _get_marker_path(output_file, trade_date)
        marker_path.parent.mkdir(parents=True, exist_ok=True)

        with open(marker_path, "w") as f:
            json.dump(marker_data, f, indent=2)

        logger.info(
            "idempotency_marker_created",
            marker_path=str(marker_path),
            trade_date=trade_date,
            rows=rows,
        )

        return marker_path

    except OSError as e:
        logger.error(
            "failed_to_create_idempotency_marker",
            output_file=str(output_file),
            error=str(e),
        )
        raise


def check_idempotency_marker(
    output_file: Path,
    trade_date: str,
    validate_hash: bool = True,
) -> dict[str, Any] | None:
    """Check if an idempotency marker exists and is valid.

    Args:
        output_file: Path to the output data file
        trade_date: Trading date in ISO format (YYYY-MM-DD)
        validate_hash: Whether to validate file hash (default: True)

    Returns:
        Marker data dictionary if valid, None otherwise
    """
    try:
        marker_path = _get_marker_path(output_file, trade_date)

        if not marker_path.exists():
            return None

        # Read marker data
        with open(marker_path) as f:
            marker_data = json.load(f)

        # Validate that output file still exists
        if not output_file.exists():
            logger.warning(
                "idempotency_marker_exists_but_output_file_missing",
                marker_path=str(marker_path),
                output_file=str(output_file),
            )
            return None

        # Validate file hash if requested
        if validate_hash and marker_data.get("file_hash"):
            with open(output_file, "rb") as f:
                current_hash = hashlib.sha256(f.read()).hexdigest()

            if current_hash != marker_data["file_hash"]:
                logger.warning(
                    "idempotency_marker_hash_mismatch",
                    marker_path=str(marker_path),
                    expected_hash=marker_data["file_hash"][:16],
                    current_hash=current_hash[:16],
                )
                return None

        logger.info(
            "idempotency_marker_valid",
            marker_path=str(marker_path),
            trade_date=trade_date,
            rows=marker_data.get("rows", 0),
        )

        return marker_data

    except (OSError, json.JSONDecodeError) as e:
        logger.warning(
            "failed_to_read_idempotency_marker",
            output_file=str(output_file),
            error=str(e),
        )
        return None


def is_task_completed(output_file: Path, trade_date: str) -> bool:
    """Check if a task has already been completed for the given date.

    This is a convenience function that checks both the marker file
    and the output file existence.

    Args:
        output_file: Path to the output data file
        trade_date: Trading date in ISO format (YYYY-MM-DD)

    Returns:
        True if task is already completed, False otherwise
    """
    marker_data = check_idempotency_marker(output_file, trade_date)
    return marker_data is not None


def get_completed_result(
    output_file: Path,
    trade_date: str,
) -> str:
    """Get the path to a previously completed task's output.

    Args:
        output_file: Path to the output data file
        trade_date: Trading date in ISO format (YYYY-MM-DD)

    Returns:
        String path to the output file

    Raises:
        ValueError: If task is not completed
    """
    marker_data = check_idempotency_marker(output_file, trade_date)

    if marker_data is None:
        raise ValueError(f"Task not completed for {trade_date}")

    return str(output_file)
