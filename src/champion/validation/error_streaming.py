"""
Validator error streaming utilities for memory-efficient large file validation.

Instead of accumulating all errors in memory, stream them to a JSONL file
and keep only a sample in memory for quick feedback.
"""

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any


class ErrorStream:
    """Stream errors to disk to avoid memory issues with large files."""

    def __init__(self, output_file: Path, keep_samples: int = 100):
        """Initialize error stream.

        Args:
            output_file: Path to write errors as JSONL
            keep_samples: Number of sample errors to keep in memory
        """
        self.output_file = output_file
        self.keep_samples = keep_samples
        self.error_samples: list[dict[str, Any]] = []
        self.total_errors = 0

        # Create parent directory if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)

    def write_error(self, error_detail: dict[str, Any]) -> None:
        """Write single error to stream.

        Args:
            error_detail: Error details dictionary
        """
        # Write to disk
        with open(self.output_file, "a") as f:
            f.write(json.dumps(error_detail) + "\n")

        # Keep sample for quick access
        if len(self.error_samples) < self.keep_samples:
            self.error_samples.append(error_detail)

        self.total_errors += 1

        # Log every 1000 errors to show progress
        if self.total_errors % 1000 == 0:
            print(f"Validation: {self.total_errors} errors written to {self.output_file}")

    def write_errors(self, errors: list[dict[str, Any]]) -> None:
        """Write multiple errors in batch.

        Args:
            errors: List of error details
        """
        for error in errors:
            self.write_error(error)

    def get_samples(self) -> list[dict[str, Any]]:
        """Get sample errors for quick feedback."""
        return self.error_samples

    def iter_all_errors(self) -> Iterator[dict[str, Any]]:
        """Iterate over all errors from disk.

        Yields:
            Error detail dictionaries
        """
        if not self.output_file.exists():
            return

        with open(self.output_file) as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)

    def cleanup(self) -> None:
        """Remove error file."""
        if self.output_file.exists():
            self.output_file.unlink()


def update_validator_to_use_streams(validator_module_path: Path) -> None:
    """
    Reference for updating existing validator to use error streams.

    Example update to ParquetValidator.validate():

        # Create error stream instead of list
        error_stream = ErrorStream(
            output_file=Path("/tmp/validation_errors.jsonl"),
            keep_samples=100
        )

        # During validation loops:
        error_stream.write_error(error_detail)  # Instead of errors.append()

        # Return samples instead of all errors
        return ValidationResult(
            total_rows=len(df),
            valid_rows=valid_count,
            critical_failures=error_stream.total_errors,
            warnings=warning_count,
            error_details=error_stream.get_samples(),  # Only samples
            error_file=str(error_stream.output_file),   # Path to all errors
        )

    Benefits:
    - Memory usage is constant regardless of file size
    - Can process 1GB+ files without OOM
    - Still get sample errors for debugging
    - Full error audit trail on disk
    """
    pass
