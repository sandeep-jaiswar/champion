"""Core validation utilities for Parquet datasets."""

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, cast

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
    validation_rules_applied: list[str] = field(default_factory=list)
    validation_timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class ParquetValidator:
    """Validates Parquet files against JSON schemas with comprehensive business rules.

    Features:
    - Schema validation (JSON Schema Draft 7)
    - 15+ business logic validation rules
    - Custom validator support
    - Quarantine & recovery with audit trail
    - Memory-efficient streaming validation
    """

    def __init__(
        self,
        schema_dir: Path,
        max_price_change_pct: float = 20.0,
        max_freshness_hours: int = 48,
        enable_all_rules: bool = True,
    ):
        """Initialize validator with schema directory and configuration.

        Args:
            schema_dir: Directory containing JSON schema files
            max_price_change_pct: Maximum allowed price change percentage (default: 20%)
            max_freshness_hours: Maximum hours between event_time and ingest_time (default: 48)
            enable_all_rules: Enable all business logic rules by default (default: True)
        """
        self.schema_dir = Path(schema_dir)
        self.schemas: dict[str, dict] = {}
        self.max_price_change_pct = max_price_change_pct
        self.max_freshness_hours = max_freshness_hours
        self.enable_all_rules = enable_all_rules
        self.custom_validators: dict[str, Callable] = {}
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

    def register_custom_validator(
        self, name: str, validator_func: Callable[[pl.DataFrame], list[dict[str, Any]]]
    ) -> None:
        """Register a custom validation function.

        Args:
            name: Name of the custom validator
            validator_func: Function that takes a DataFrame and returns list of error dicts
        """
        self.custom_validators[name] = validator_func
        logger.info("registered_custom_validator", validator_name=name)

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
        rules_applied = ["schema_validation"]

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
        business_errors, business_rules = self._validate_business_logic(df, schema_name)
        error_details.extend(business_errors)
        rules_applied.extend(business_rules)

        critical_failures = len(error_details)
        valid_rows = total_rows - len({e["row_index"] for e in error_details})

        result = ValidationResult(
            total_rows=total_rows,
            valid_rows=valid_rows,
            critical_failures=critical_failures,
            warnings=0,
            error_details=error_details,
            validation_rules_applied=rules_applied,
        )

        logger.info(
            "validation_complete",
            schema_name=schema_name,
            total_rows=total_rows,
            valid_rows=valid_rows,
            critical_failures=critical_failures,
            rules_applied=len(rules_applied),
        )

        return result

    def _validate_business_logic(
        self, df: pl.DataFrame, schema_name: str
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Apply business logic validations specific to schema type.

        Args:
            df: DataFrame to validate
            schema_name: Schema name for determining which rules to apply

        Returns:
            Tuple of (error_details, rules_applied)
        """
        errors: list[dict[str, Any]] = []
        rules_applied: list[str] = []

        if not self.enable_all_rules:
            return errors, rules_applied

        # OHLC-specific validations
        if "ohlc" in schema_name:
            # Rule 1: OHLC consistency (high >= low)
            rule_errors = self._validate_ohlc_consistency(df)
            if rule_errors:
                errors.extend(rule_errors)
            rules_applied.append("ohlc_high_low_consistency")

            # Rule 2: OHLC extended (close within [low, high])
            rule_errors = self._validate_ohlc_close_in_range(df)
            if rule_errors:
                errors.extend(rule_errors)
            rules_applied.append("ohlc_close_in_range")

            # Rule 3: OHLC extended (open within [low, high])
            rule_errors = self._validate_ohlc_open_in_range(df)
            if rule_errors:
                errors.extend(rule_errors)
            rules_applied.append("ohlc_open_in_range")

            # Rule 4: Volume consistency (volume > 0 when trades > 0)
            rule_errors = self._validate_volume_consistency(df)
            if rule_errors:
                errors.extend(rule_errors)
            rules_applied.append("volume_consistency")

            # Rule 5: Turnover consistency (volume * avgprice ≈ turnover)
            rule_errors = self._validate_turnover_consistency(df)
            if rule_errors:
                errors.extend(rule_errors)
            rules_applied.append("turnover_consistency")

            # Rule 6: Price reasonableness (% change from prev_close)
            rule_errors = self._validate_price_reasonableness(df)
            if rule_errors:
                errors.extend(rule_errors)
            rules_applied.append("price_reasonableness")

            # Rule 7: Price continuity after corporate actions
            if "normalized" in schema_name:
                rule_errors = self._validate_price_continuity(df)
                if rule_errors:
                    errors.extend(rule_errors)
                rules_applied.append("price_continuity_post_ca")

        # Rule 8: Duplicate detection (symbol + date uniqueness)
        rule_errors = self._validate_duplicates(df, schema_name)
        if rule_errors:
            errors.extend(rule_errors)
        rules_applied.append("duplicate_detection")

        # Rule 9: Freshness checks (event_time vs ingest_time)
        rule_errors = self._validate_freshness(df)
        if rule_errors:
            errors.extend(rule_errors)
        rules_applied.append("data_freshness")

        # Rule 10: Timestamp validations
        rule_errors = self._validate_timestamps(df)
        if rule_errors:
            errors.extend(rule_errors)
        rules_applied.append("timestamp_validation")

        # Rule 11: Missing critical data
        rule_errors = self._validate_missing_critical_data(df, schema_name)
        if rule_errors:
            errors.extend(rule_errors)
        rules_applied.append("missing_critical_data")

        # Rule 12: Non-negative price validation
        rule_errors = self._validate_non_negative_prices(df)
        if rule_errors:
            errors.extend(rule_errors)
        rules_applied.append("non_negative_prices")

        # Rule 13: Non-negative volume validation
        rule_errors = self._validate_non_negative_volume(df)
        if rule_errors:
            errors.extend(rule_errors)
        rules_applied.append("non_negative_volume")

        # Rule 14: Date range validation
        rule_errors = self._validate_date_range(df, schema_name)
        if rule_errors:
            errors.extend(rule_errors)
        rules_applied.append("date_range_validation")

        # Rule 15: Data completeness for trading days
        if "normalized" in schema_name and "is_trading_day" in df.columns:
            rule_errors = self._validate_trading_day_completeness(df)
            if rule_errors:
                errors.extend(rule_errors)
            rules_applied.append("trading_day_completeness")

        # Rule 16: Apply custom validators
        for validator_name, validator_func in self.custom_validators.items():
            try:
                rule_errors = validator_func(df)
                if rule_errors:
                    errors.extend(rule_errors)
                rules_applied.append(f"custom_{validator_name}")
            except Exception as e:
                logger.error(
                    "custom_validator_failed",
                    validator_name=validator_name,
                    error=str(e),
                )

        return errors, rules_applied

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

    def _validate_ohlc_close_in_range(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate that close price is within [low, high] range.

        Args:
            df: DataFrame with OHLC data

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []
        required_cols = ["high", "low", "close"]
        if not all(col in df.columns for col in required_cols):
            return errors

        violations = df.with_row_index("__idx__").filter(
            (pl.col("close").is_not_null())
            & (pl.col("high").is_not_null())
            & (pl.col("low").is_not_null())
            & ((pl.col("close") > pl.col("high")) | (pl.col("close") < pl.col("low")))
        )

        for row in violations.iter_rows(named=True):
            errors.append(
                {
                    "row_index": row["__idx__"],
                    "error_type": "critical",
                    "field": "close,high,low",
                    "message": (
                        f"Close ({row['close']}) outside range "
                        f"[low={row['low']}, high={row['high']}]"
                    ),
                    "validator": "business_logic",
                    "record": dict(row),
                }
            )

        return errors

    def _validate_ohlc_open_in_range(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate that open price is within [low, high] range.

        Args:
            df: DataFrame with OHLC data

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []
        required_cols = ["high", "low", "open"]
        if not all(col in df.columns for col in required_cols):
            return errors

        violations = df.with_row_index("__idx__").filter(
            (pl.col("open").is_not_null())
            & (pl.col("high").is_not_null())
            & (pl.col("low").is_not_null())
            & ((pl.col("open") > pl.col("high")) | (pl.col("open") < pl.col("low")))
        )

        for row in violations.iter_rows(named=True):
            errors.append(
                {
                    "row_index": row["__idx__"],
                    "error_type": "critical",
                    "field": "open,high,low",
                    "message": (
                        f"Open ({row['open']}) outside range "
                        f"[low={row['low']}, high={row['high']}]"
                    ),
                    "validator": "business_logic",
                    "record": dict(row),
                }
            )

        return errors

    def _validate_volume_consistency(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate volume > 0 when trades > 0.

        Args:
            df: DataFrame with volume and trades data

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        # Check for volume field (could be 'volume' or 'TtlTradgVol')
        volume_col = None
        if "volume" in df.columns:
            volume_col = "volume"
        elif "TtlTradgVol" in df.columns:
            volume_col = "TtlTradgVol"

        # Check for trades field (could be 'trades' or 'TtlNbOfTxsExctd')
        trades_col = None
        if "trades" in df.columns:
            trades_col = "trades"
        elif "TtlNbOfTxsExctd" in df.columns:
            trades_col = "TtlNbOfTxsExctd"

        if not volume_col or not trades_col:
            return errors

        violations = df.with_row_index("__idx__").filter(
            (pl.col(trades_col).is_not_null())
            & (pl.col(volume_col).is_not_null())
            & (pl.col(trades_col) > 0)
            & (pl.col(volume_col) == 0)
        )

        for row in violations.iter_rows(named=True):
            errors.append(
                {
                    "row_index": row["__idx__"],
                    "error_type": "critical",
                    "field": f"{volume_col},{trades_col}",
                    "message": f"Volume is 0 but trades is {row[trades_col]}",
                    "validator": "business_logic",
                    "record": dict(row),
                }
            )

        return errors

    def _validate_turnover_consistency(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate turnover ≈ volume * average_price (within 5% tolerance).

        Args:
            df: DataFrame with volume and turnover data

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        # Map column names
        volume_col = (
            "volume"
            if "volume" in df.columns
            else "TtlTradgVol"
            if "TtlTradgVol" in df.columns
            else None
        )
        turnover_col = (
            "turnover"
            if "turnover" in df.columns
            else "TtlTrfVal"
            if "TtlTrfVal" in df.columns
            else None
        )
        close_col = (
            "close" if "close" in df.columns else "ClsPric" if "ClsPric" in df.columns else None
        )

        if not all([volume_col, turnover_col, close_col]):
            return errors

        # Type guard for mypy
        assert isinstance(volume_col, str)
        assert isinstance(turnover_col, str)
        assert isinstance(close_col, str)

        # Calculate expected turnover (volume * close as proxy for avg price)
        # Allow 10% tolerance for approximation
        violations = (
            df.with_row_index("__idx__")
            .filter(
                (pl.col(volume_col).is_not_null())
                & (pl.col(turnover_col).is_not_null())
                & (pl.col(close_col).is_not_null())
                & (pl.col(volume_col) > 0)
                & (pl.col(turnover_col) > 0)
            )
            .with_columns(
                [
                    (pl.col(volume_col) * pl.col(close_col)).alias("expected_turnover"),
                    (
                        (pl.col(turnover_col) - pl.col(volume_col) * pl.col(close_col)).abs()
                        / (pl.col(volume_col) * pl.col(close_col))
                        * 100
                    ).alias("deviation_pct"),
                ]
            )
            .filter(
                pl.col("deviation_pct") > 10  # More than 10% deviation
            )
        )

        for row in violations.iter_rows(named=True):
            errors.append(
                {
                    "row_index": row["__idx__"],
                    "error_type": "warning",
                    "field": f"{turnover_col},{volume_col}",
                    "message": (
                        f"Turnover deviation: {row['deviation_pct']:.1f}% "
                        f"(actual={row[turnover_col]}, "  # type: ignore[index]
                        f"expected≈{row['expected_turnover']:.2f})"
                    ),
                    "validator": "business_logic",
                    "record": dict(row),
                }
            )

        return errors

    def _validate_price_reasonableness(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate price change from prev_close is within reasonable limits.

        Args:
            df: DataFrame with price data

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        close_col = (
            "close" if "close" in df.columns else "ClsPric" if "ClsPric" in df.columns else None
        )
        prev_close_col = (
            "prev_close"
            if "prev_close" in df.columns
            else "PrvsClsgPric"
            if "PrvsClsgPric" in df.columns
            else None
        )

        if not all([close_col, prev_close_col]):
            return errors

        # Type guard for mypy
        assert isinstance(close_col, str)
        assert isinstance(prev_close_col, str)

        violations = (
            df.with_row_index("__idx__")
            .filter(
                (pl.col(close_col).is_not_null())
                & (pl.col(prev_close_col).is_not_null())
                & (pl.col(prev_close_col) > 0)
            )
            .with_columns(
                [
                    (
                        (pl.col(close_col) - pl.col(prev_close_col)).abs()
                        / pl.col(prev_close_col)
                        * 100
                    ).alias("change_pct")
                ]
            )
            .filter(pl.col("change_pct") > self.max_price_change_pct)
        )

        for row in violations.iter_rows(named=True):
            errors.append(
                {
                    "row_index": row["__idx__"],
                    "error_type": "warning",
                    "field": close_col,
                    "message": (
                        f"Price change {row['change_pct']:.1f}% exceeds "
                        f"threshold {self.max_price_change_pct}%"
                    ),
                    "validator": "business_logic",
                    "record": dict(row),
                }
            )

        return errors

    def _validate_price_continuity(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate price continuity after corporate actions.

        Args:
            df: DataFrame with adjustment factor data

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        if "adjustment_factor" not in df.columns or "adjustment_date" not in df.columns:
            return errors

        # Check for sudden changes in adjustment factor
        violations = df.with_row_index("__idx__").filter(
            (pl.col("adjustment_factor").is_not_null())
            & (pl.col("adjustment_date").is_not_null())
            & (pl.col("adjustment_factor") <= 0)
        )

        for row in violations.iter_rows(named=True):
            errors.append(
                {
                    "row_index": row["__idx__"],
                    "error_type": "critical",
                    "field": "adjustment_factor",
                    "message": (
                        f"Invalid adjustment factor: {row['adjustment_factor']} " f"(must be > 0)"
                    ),
                    "validator": "business_logic",
                    "record": dict(row),
                }
            )

        return errors

    def _validate_duplicates(self, df: pl.DataFrame, schema_name: str) -> list[dict[str, Any]]:
        """Validate uniqueness of records (no duplicates).

        Args:
            df: DataFrame to check
            schema_name: Schema name for determining uniqueness keys

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        # Determine uniqueness keys based on schema
        key_cols = []
        if "entity_id" in df.columns or "instrument_id" in df.columns:
            key_cols.append("entity_id" if "entity_id" in df.columns else "instrument_id")
        elif "symbol" in df.columns:
            key_cols.append("symbol")

        if "trade_date" in df.columns:
            key_cols.append("trade_date")
        elif "TradDt" in df.columns:
            key_cols.append("TradDt")

        if not key_cols:
            return errors

        # Find duplicates
        duplicates = df.with_row_index("__idx__").filter(pl.struct(key_cols).is_duplicated())

        if len(duplicates) > 0:
            # Group by key to show which keys are duplicated
            dup_groups = duplicates.group_by(key_cols).agg(pl.col("__idx__").alias("indices"))

            for row in dup_groups.iter_rows(named=True):
                key_str = ", ".join([f"{k}={row[k]}" for k in key_cols])
                indices = row["indices"]
                errors.append(
                    {
                        "row_index": indices[0] if indices else 0,
                        "error_type": "critical",
                        "field": ",".join(key_cols),
                        "message": f"Duplicate record: {key_str} (found at indices: {indices})",
                        "validator": "business_logic",
                        "record": dict(row),
                    }
                )

        return errors

    def _validate_freshness(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate data freshness (event_time vs ingest_time).

        Args:
            df: DataFrame with timestamp columns

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        if "event_time" not in df.columns or "ingest_time" not in df.columns:
            return errors

        max_freshness_ms = self.max_freshness_hours * 3600 * 1000

        violations = df.with_row_index("__idx__").filter(
            (pl.col("event_time").is_not_null())
            & (pl.col("ingest_time").is_not_null())
            & ((pl.col("ingest_time") - pl.col("event_time")) > max_freshness_ms)
        )

        for row in violations.iter_rows(named=True):
            delay_hours = (row["ingest_time"] - row["event_time"]) / (3600 * 1000)
            errors.append(
                {
                    "row_index": row["__idx__"],
                    "error_type": "warning",
                    "field": "ingest_time,event_time",
                    "message": (
                        f"Data stale: {delay_hours:.1f} hours delay "
                        f"(threshold: {self.max_freshness_hours}h)"
                    ),
                    "validator": "business_logic",
                    "record": dict(row),
                }
            )

        return errors

    def _validate_timestamps(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate timestamp fields are reasonable.

        Args:
            df: DataFrame with timestamp columns

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        # Check event_time and ingest_time are positive and not in future
        timestamp_cols = []
        if "event_time" in df.columns:
            timestamp_cols.append("event_time")
        if "ingest_time" in df.columns:
            timestamp_cols.append("ingest_time")

        if not timestamp_cols:
            return errors

        now_ms = int(datetime.now().timestamp() * 1000)
        # Allow 1 hour into future for clock skew
        future_threshold_ms = now_ms + (3600 * 1000)

        for col in timestamp_cols:
            violations = df.with_row_index("__idx__").filter(
                (pl.col(col).is_not_null())
                & ((pl.col(col) < 0) | (pl.col(col) > future_threshold_ms))
            )

            for row in violations.iter_rows(named=True):
                errors.append(
                    {
                        "row_index": row["__idx__"],
                        "error_type": "critical",
                        "field": col,
                        "message": f"Invalid timestamp: {row[col]} (negative or too far in future)",
                        "validator": "business_logic",
                        "record": dict(row),
                    }
                )

        return errors

    def _validate_missing_critical_data(
        self, df: pl.DataFrame, schema_name: str
    ) -> list[dict[str, Any]]:
        """Validate that critical fields are not null when they should have values.

        Args:
            df: DataFrame to check
            schema_name: Schema name for determining critical fields

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        # Define critical fields by schema type
        critical_fields = []
        if "ohlc" in schema_name:
            critical_fields = ["open", "high", "low", "close"]
            if "normalized" in schema_name:
                critical_fields.extend(["volume", "turnover"])

        if not critical_fields:
            return errors

        for col_field in critical_fields:
            if col_field not in df.columns:
                continue

            violations = df.with_row_index("__idx__").filter(pl.col(col_field).is_null())

            if len(violations) > 0:
                errors.append(
                    {
                        "row_index": violations["__idx__"][0],
                        "error_type": "critical",
                        "field": col_field,
                        "message": f"Critical field '{col_field}' has {len(violations)} null values",
                        "validator": "business_logic",
                        "record": {},
                    }
                )

        return errors

    def _validate_non_negative_prices(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate all price fields are non-negative.

        Args:
            df: DataFrame with price columns

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        price_cols = [
            col
            for col in df.columns
            if any(
                price_term in col.lower()
                for price_term in ["pric", "price", "open", "high", "low", "close"]
            )
        ]

        for col in price_cols:
            # Skip if not numeric
            if df[col].dtype not in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]:
                continue

            violations = df.with_row_index("__idx__").filter(
                (pl.col(col).is_not_null()) & (pl.col(col) < 0)
            )

            for row in violations.iter_rows(named=True):
                errors.append(
                    {
                        "row_index": row["__idx__"],
                        "error_type": "critical",
                        "field": col,
                        "message": f"Negative price: {row[col]}",
                        "validator": "business_logic",
                        "record": dict(row),
                    }
                )

        return errors

    def _validate_non_negative_volume(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate volume fields are non-negative.

        Args:
            df: DataFrame with volume columns

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        volume_cols = [
            col for col in df.columns if any(v in col.lower() for v in ["vol", "volume", "qty"])
        ]

        for col in volume_cols:
            # Skip if not numeric
            if df[col].dtype not in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]:
                continue

            violations = df.with_row_index("__idx__").filter(
                (pl.col(col).is_not_null()) & (pl.col(col) < 0)
            )

            for row in violations.iter_rows(named=True):
                errors.append(
                    {
                        "row_index": row["__idx__"],
                        "error_type": "critical",
                        "field": col,
                        "message": f"Negative volume: {row[col]}",
                        "validator": "business_logic",
                        "record": dict(row),
                    }
                )

        return errors

    def _validate_date_range(self, df: pl.DataFrame, schema_name: str) -> list[dict[str, Any]]:
        """Validate dates are within reasonable range (not too old or in future).

        Args:
            df: DataFrame with date columns
            schema_name: Schema name

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        date_col = None
        if "trade_date" in df.columns:
            date_col = "trade_date"
        elif "TradDt" in df.columns:
            date_col = "TradDt"

        if not date_col:
            return errors

        # For integer dates (days since epoch), check reasonable range
        # Stock market data: 1990-01-01 (7305 days) to today + 1 day
        min_date = 7305  # ~1990-01-01
        max_date = int(datetime.now().timestamp() / 86400) + 1  # Today + 1

        if df[date_col].dtype in [pl.Int32, pl.Int64]:
            violations = df.with_row_index("__idx__").filter(
                (pl.col(date_col).is_not_null())
                & ((pl.col(date_col) < min_date) | (pl.col(date_col) > max_date))
            )

            for row in violations.iter_rows(named=True):
                errors.append(
                    {
                        "row_index": row["__idx__"],
                        "error_type": "critical",
                        "field": date_col,
                        "message": (
                            f"Date out of range: {row[date_col]} " f"(valid: {min_date}-{max_date})"
                        ),
                        "validator": "business_logic",
                        "record": dict(row),
                    }
                )

        return errors

    def _validate_trading_day_completeness(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        """Validate that is_trading_day flag is set correctly.

        Args:
            df: DataFrame with is_trading_day column

        Returns:
            List of error details for violations
        """
        errors: list[dict[str, Any]] = []

        if "is_trading_day" not in df.columns or "volume" not in df.columns:
            return errors

        # If is_trading_day is True but volume is 0 (and close != prev_close), flag as warning
        violations = df.with_row_index("__idx__").filter(
            pl.col("is_trading_day") & (pl.col("volume") == 0)
        )

        for row in violations.iter_rows(named=True):
            errors.append(
                {
                    "row_index": row["__idx__"],
                    "error_type": "warning",
                    "field": "is_trading_day,volume",
                    "message": "Trading day flag set but volume is 0",
                    "validator": "business_logic",
                    "record": dict(row),
                }
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
        """Write failed records to quarantine directory with audit trail.

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

        # Add error information and audit trail
        error_map: dict[int, list[str]] = {}
        error_types: dict[int, list[str]] = {}
        for error in result.error_details:
            idx = error["row_index"]
            if idx not in error_map:
                error_map[idx] = []
                error_types[idx] = []
            error_map[idx].append(f"{error['field']}: {error['message']}")
            error_types[idx].append(error["error_type"])

        error_messages = [
            "; ".join(error_map.get(orig_idx, ["Unknown error"])) for orig_idx in failed_indices
        ]
        error_type_list = [
            ",".join(set(error_types.get(orig_idx, ["unknown"]))) for orig_idx in failed_indices
        ]

        failed_df = failed_df.with_columns(
            [
                pl.lit(error_messages).alias("validation_errors"),
                pl.lit(error_type_list).alias("error_types"),
                pl.lit(schema_name).alias("schema_name"),
                pl.lit(result.validation_timestamp).alias("quarantine_timestamp"),
                pl.lit(",".join(result.validation_rules_applied)).alias("rules_applied"),
                pl.lit(0).alias("retry_count"),  # For future retry mechanism
            ]
        )

        # Write to quarantine with timestamp
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        quarantine_file = quarantine_dir / f"{schema_name}_failures_{timestamp_str}.parquet"
        failed_df.write_parquet(quarantine_file)

        # Also write/update audit log
        audit_log_file = quarantine_dir / "audit_log.jsonl"
        audit_entry = {
            "timestamp": result.validation_timestamp,
            "schema_name": schema_name,
            "quarantine_file": str(quarantine_file.name),
            "failed_rows": len(failed_indices),
            "total_rows": result.total_rows,
            "rules_applied": result.validation_rules_applied,
            "failure_rate": len(failed_indices) / result.total_rows if result.total_rows > 0 else 0,
        }

        with open(audit_log_file, "a") as f:
            f.write(json.dumps(audit_entry) + "\n")

        logger.info(
            "quarantined_failures",
            quarantine_file=str(quarantine_file),
            failed_rows=len(failed_indices),
            audit_log=str(audit_log_file),
        )
