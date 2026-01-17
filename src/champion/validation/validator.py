"""Core validation utilities for Parquet datasets."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import structlog
from jsonschema import Draft7Validator

logger = structlog.get_logger()


@dataclass
class ValidationResult:
    """Result of validation operation."""

    total_rows: int
    valid_rows: int
    critical_failures: int
    warnings: int
    error_details: list[dict[str, Any]]


class ParquetValidator:
    """Validates Parquet files against JSON schemas."""

    def __init__(self, schema_dir: Path):
        """Initialize validator with schema directory.

        Args:
            schema_dir: Directory containing JSON schema files
        """
        self.schema_dir = Path(schema_dir)
        self.schemas: dict[str, dict] = {}
        self._load_schemas()

    def _load_schemas(self) -> None:
        """Load all JSON schemas from schema directory."""
        if not self.schema_dir.exists():
            raise ValueError(f"Schema directory does not exist: {self.schema_dir}")

        for schema_file in self.schema_dir.glob("*.json"):
            schema_name = schema_file.stem
            with open(schema_file) as f:
                self.schemas[schema_name] = json.load(f)

            logger.info(
                "loaded_schema",
                schema_name=schema_name,
                schema_file=str(schema_file),
            )

    def validate_dataframe(
        self,
        df: pl.DataFrame,
        schema_name: str,
        strict: bool = True,
        batch_size: int = 10000,
    ) -> ValidationResult:
        """Validate a Polars DataFrame against a JSON schema.

        Args:
            df: DataFrame to validate
            schema_name: Name of the schema to validate against
            strict: If True, fail on any validation error. If False, collect all errors.
            batch_size: Number of rows to process in each batch (default: 10000)

        Returns:
            ValidationResult with validation statistics and error details
        """
        if schema_name not in self.schemas:
            raise ValueError(
                f"Schema '{schema_name}' not found. Available schemas: {list(self.schemas.keys())}"
            )

        schema = self.schemas[schema_name]
        validator = Draft7Validator(schema)

        total_rows = len(df)
        error_details = []

        logger.info(
            "validating_dataframe",
            schema_name=schema_name,
            total_rows=total_rows,
            columns=df.columns,
            batch_size=batch_size,
        )

        # Use streaming validation with iter_slices for memory efficiency
        for batch_idx, batch in enumerate(df.iter_slices(batch_size)):
            # Convert only current batch to dicts
            records = batch.to_dicts()
            for local_idx, record in enumerate(records):
                row_idx = batch_idx * batch_size + local_idx
                errors = list(validator.iter_errors(record))
                if errors:
                    for error in errors:
                        error_detail = {
                            "row_index": row_idx,
                            "error_type": "critical",
                            "field": ".".join(str(p) for p in error.path) or "root",
                            "message": error.message,
                            "validator": error.validator,
                            "record": record,
                        }
                        error_details.append(error_detail)

                        logger.warning(
                            "validation_error",
                            row_index=row_idx,
                            field=error_detail["field"],
                            message=error.message,
                        )

        # Perform additional business logic validations
        # Note: Business logic uses Polars operations which are memory-efficient
        # as they don't materialize data until needed (e.g., only violations)
        business_errors = self._validate_business_logic(df, schema_name)
        error_details.extend(business_errors)

        critical_failures = len(error_details)
        valid_rows = total_rows - len({e["row_index"] for e in error_details})

        result = ValidationResult(
            total_rows=total_rows,
            valid_rows=valid_rows,
            critical_failures=critical_failures,
            warnings=0,
            error_details=error_details,
        )

        logger.info(
            "validation_complete",
            schema_name=schema_name,
            total_rows=total_rows,
            valid_rows=valid_rows,
            critical_failures=critical_failures,
        )

        return result

    def _validate_business_logic(self, df: pl.DataFrame, schema_name: str) -> list[dict[str, Any]]:
        """Apply business logic validations specific to schema type.

        Args:
            df: DataFrame to validate
            schema_name: Schema name for determining which rules to apply

        Returns:
            List of error details for business logic violations
        """
        errors = []

        # OHLC-specific validations
        if "ohlc" in schema_name:
            errors.extend(self._validate_ohlc_consistency(df))

        return errors

    def _validate_ohlc_consistency(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate OHLC price consistency (high >= low).

        Args:
            df: DataFrame with OHLC data

        Returns:
            List of error details for OHLC violations
        """
        errors: list[dict[str, Any]] = []

        # Check if required columns exist
        required_cols = ["high", "low"]
        if not all(col in df.columns for col in required_cols):
            return errors

        # Find rows where high < low
        violations = df.filter(
            (pl.col("high").is_not_null())
            & (pl.col("low").is_not_null())
            & (pl.col("high") < pl.col("low"))
        )

        for _idx, row in enumerate(violations.iter_rows(named=True)):
            # Find original index in the full dataframe
            original_idx = (
                df.with_row_index("__idx__")
                .filter((pl.col("high") == row["high"]) & (pl.col("low") == row["low"]))
                .select("__idx__")
                .to_series()[0]
            )

            errors.append(
                {
                    "row_index": original_idx,
                    "error_type": "critical",
                    "field": "high,low",
                    "message": f"OHLC violation: high ({row['high']}) < low ({row['low']})",
                    "validator": "business_logic",
                    "record": dict(row),
                }
            )

            logger.warning(
                "ohlc_violation",
                row_index=original_idx,
                high=row["high"],
                low=row["low"],
            )

        return errors

    def validate_file(
        self,
        file_path: Path,
        schema_name: str,
        quarantine_dir: Path | None = None,
    ) -> ValidationResult:
        """Validate a Parquet file against a JSON schema.

        Args:
            file_path: Path to Parquet file to validate
            schema_name: Name of the schema to validate against
            quarantine_dir: Directory to write quarantined records (optional)

        Returns:
            ValidationResult with validation statistics
        """
        logger.info("validating_file", file_path=str(file_path), schema_name=schema_name)

        # Read Parquet file
        df = pl.read_parquet(file_path)

        # Validate
        result = self.validate_dataframe(df, schema_name)

        # Quarantine failed records if requested
        if quarantine_dir and result.critical_failures > 0:
            self._quarantine_failures(df, result, quarantine_dir, schema_name)

        return result

    def quarantine_failures(
        self,
        df: pl.DataFrame,
        result: ValidationResult,
        quarantine_dir: Path,
        schema_name: str,
    ) -> None:
        """Write failed records to quarantine directory.

        Public method to quarantine validation failures.

        Args:
            df: Original DataFrame
            result: Validation result with error details
            quarantine_dir: Directory to write quarantined records
            schema_name: Schema name for organizing quarantine files
        """
        self._quarantine_failures(df, result, quarantine_dir, schema_name)

    def _quarantine_failures(
        self,
        df: pl.DataFrame,
        result: ValidationResult,
        quarantine_dir: Path,
        schema_name: str,
    ) -> None:
        """Write failed records to quarantine directory.

        Private implementation of quarantine functionality.

        Args:
            df: Original DataFrame
            result: Validation result with error details
            quarantine_dir: Directory to write quarantined records
            schema_name: Schema name for organizing quarantine files
        """
        quarantine_dir = Path(quarantine_dir)
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        # Get indices of failed rows
        failed_indices = list({e["row_index"] for e in result.error_details})

        if not failed_indices:
            return

        # Extract failed rows
        failed_df = df[failed_indices]

        # Add error information
        error_map: dict[int, list[str]] = {}
        for error in result.error_details:
            idx = error["row_index"]
            if idx not in error_map:
                error_map[idx] = []
            error_map[idx].append(f"{error['field']}: {error['message']}")

        error_messages = [
            "; ".join(error_map.get(orig_idx, ["Unknown error"])) for orig_idx in failed_indices
        ]

        failed_df = failed_df.with_columns(
            [
                pl.lit(error_messages).alias("validation_errors"),
                pl.lit(schema_name).alias("schema_name"),
            ]
        )

        # Write to quarantine
        quarantine_file = quarantine_dir / f"{schema_name}_failures.parquet"
        failed_df.write_parquet(quarantine_file)

        logger.info(
            "quarantined_failures",
            quarantine_file=str(quarantine_file),
            failed_rows=len(failed_indices),
        )
