"""Prefect flow integration for data validation."""

from pathlib import Path

import structlog
from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook

from validation.validator import ParquetValidator, ValidationResult

logger = structlog.get_logger()


@task(name="validate-parquet-file", retries=2)
def validate_parquet_file(
    file_path: str,
    schema_name: str,
    schema_dir: str,
    quarantine_dir: str | None = None,
) -> ValidationResult:
    """Validate a Parquet file against a JSON schema.

    Args:
        file_path: Path to Parquet file to validate
        schema_name: Name of the schema to validate against
        schema_dir: Directory containing JSON schema files
        quarantine_dir: Directory to write quarantined records (optional)

    Returns:
        ValidationResult with validation statistics

    Raises:
        ValueError: If validation fails critically
    """
    logger.info(
        "starting_validation",
        file_path=file_path,
        schema_name=schema_name,
    )

    validator = ParquetValidator(schema_dir=Path(schema_dir))

    result = validator.validate_file(
        file_path=Path(file_path),
        schema_name=schema_name,
        quarantine_dir=Path(quarantine_dir) if quarantine_dir else None,
    )

    logger.info(
        "validation_complete",
        file_path=file_path,
        total_rows=result.total_rows,
        valid_rows=result.valid_rows,
        critical_failures=result.critical_failures,
        warnings=result.warnings,
    )

    return result


@task(name="check-validation-result")
def check_validation_result(
    result: ValidationResult,
    fail_on_errors: bool = True,
    max_failure_rate: float = 0.05,
) -> bool:
    """Check validation result and determine if pipeline should fail.

    Args:
        result: Validation result to check
        fail_on_errors: If True, fail on any critical errors
        max_failure_rate: Maximum acceptable failure rate (0.0-1.0)

    Returns:
        True if validation passed, False otherwise

    Raises:
        ValueError: If validation failed and fail_on_errors is True
    """
    failure_rate = result.critical_failures / result.total_rows if result.total_rows > 0 else 0

    logger.info(
        "checking_validation_result",
        total_rows=result.total_rows,
        valid_rows=result.valid_rows,
        critical_failures=result.critical_failures,
        failure_rate=failure_rate,
        max_failure_rate=max_failure_rate,
    )

    if result.critical_failures == 0:
        logger.info("validation_passed", message="No validation errors detected")
        return True

    if fail_on_errors:
        error_msg = (
            f"Validation failed: {result.critical_failures} critical errors "
            f"({failure_rate:.2%} failure rate)"
        )
        logger.error("validation_failed", message=error_msg)
        raise ValueError(error_msg)

    if failure_rate > max_failure_rate:
        error_msg = (
            f"Validation failure rate ({failure_rate:.2%}) exceeds "
            f"threshold ({max_failure_rate:.2%})"
        )
        logger.error("validation_threshold_exceeded", message=error_msg)
        raise ValueError(error_msg)

    logger.warning(
        "validation_passed_with_warnings",
        critical_failures=result.critical_failures,
        failure_rate=failure_rate,
    )
    return True


@task(name="send-validation-alert")
async def send_validation_alert(
    result: ValidationResult,
    file_path: str,
    schema_name: str,
    slack_webhook_block: str | None = None,
) -> None:
    """Send alert about validation failures.

    Args:
        result: Validation result
        file_path: Path to file that was validated
        schema_name: Schema name used for validation
        slack_webhook_block: Name of Prefect Slack webhook block (optional)
    """
    if result.critical_failures == 0:
        return

    message = (
        f"⚠️ *Data Validation Alert*\n\n"
        f"File: `{file_path}`\n"
        f"Schema: `{schema_name}`\n"
        f"Total rows: {result.total_rows}\n"
        f"Valid rows: {result.valid_rows}\n"
        f"Critical failures: {result.critical_failures}\n"
        f"Failure rate: {result.critical_failures / result.total_rows:.2%}\n\n"
    )

    if result.error_details:
        # Show first 5 errors
        message += "Sample errors:\n"
        for error in result.error_details[:5]:
            message += f"- Row {error['row_index']}: {error['message']}\n"

    logger.warning("sending_validation_alert", message=message)

    if slack_webhook_block:
        try:
            slack = await SlackWebhook.load(slack_webhook_block)
            await slack.notify(message)
            logger.info("slack_alert_sent", webhook_block=slack_webhook_block)
        except Exception as e:
            logger.error(
                "failed_to_send_slack_alert",
                error=str(e),
                webhook_block=slack_webhook_block,
            )
    else:
        logger.warning("no_slack_webhook_configured", message="Skipping Slack notification")


@flow(name="validate-parquet-dataset")
async def validate_parquet_dataset(
    file_path: str,
    schema_name: str,
    schema_dir: str = "./schemas/parquet",
    quarantine_dir: str = "./data/lake/quarantine",
    fail_on_errors: bool = True,
    max_failure_rate: float = 0.05,
    slack_webhook_block: str | None = None,
) -> ValidationResult:
    """Prefect flow for validating Parquet datasets.

    This flow:
    1. Validates a Parquet file against a JSON schema
    2. Quarantines failed records
    3. Checks validation results against thresholds
    4. Sends alerts on failures

    Args:
        file_path: Path to Parquet file to validate
        schema_name: Name of the schema to validate against
        schema_dir: Directory containing JSON schema files
        quarantine_dir: Directory to write quarantined records
        fail_on_errors: If True, fail flow on any critical errors
        max_failure_rate: Maximum acceptable failure rate (0.0-1.0)
        slack_webhook_block: Name of Prefect Slack webhook block (optional)

    Returns:
        ValidationResult with validation statistics

    Raises:
        ValueError: If validation fails and fail_on_errors is True
    """
    logger.info(
        "starting_validation_flow",
        file_path=file_path,
        schema_name=schema_name,
    )

    # Step 1: Validate the file
    result = validate_parquet_file(
        file_path=file_path,
        schema_name=schema_name,
        schema_dir=schema_dir,
        quarantine_dir=quarantine_dir,
    )

    # Step 2: Check validation result
    passed = check_validation_result(
        result=result,
        fail_on_errors=fail_on_errors,
        max_failure_rate=max_failure_rate,
    )

    # Step 3: Send alerts if there are failures
    if result.critical_failures > 0:
        await send_validation_alert(
            result=result,
            file_path=file_path,
            schema_name=schema_name,
            slack_webhook_block=slack_webhook_block,
        )

    logger.info(
        "validation_flow_complete",
        file_path=file_path,
        passed=passed,
        critical_failures=result.critical_failures,
    )

    return result


@flow(name="validate-parquet-batch")
async def validate_parquet_batch(
    file_paths: list[str],
    schema_name: str,
    schema_dir: str = "./schemas/parquet",
    quarantine_dir: str = "./data/lake/quarantine",
    fail_on_errors: bool = True,
    max_failure_rate: float = 0.05,
    slack_webhook_block: str | None = None,
) -> list[ValidationResult]:
    """Validate multiple Parquet files in batch.

    Args:
        file_paths: List of paths to Parquet files to validate
        schema_name: Name of the schema to validate against
        schema_dir: Directory containing JSON schema files
        quarantine_dir: Directory to write quarantined records
        fail_on_errors: If True, fail flow on any critical errors
        max_failure_rate: Maximum acceptable failure rate (0.0-1.0)
        slack_webhook_block: Name of Prefect Slack webhook block (optional)

    Returns:
        List of ValidationResult objects, one per file
    """
    logger.info(
        "starting_batch_validation",
        file_count=len(file_paths),
        schema_name=schema_name,
    )

    results = []
    for file_path in file_paths:
        result = await validate_parquet_dataset(
            file_path=file_path,
            schema_name=schema_name,
            schema_dir=schema_dir,
            quarantine_dir=quarantine_dir,
            fail_on_errors=fail_on_errors,
            max_failure_rate=max_failure_rate,
            slack_webhook_block=slack_webhook_block,
        )
        results.append(result)

    total_rows = sum(r.total_rows for r in results)
    total_failures = sum(r.critical_failures for r in results)

    logger.info(
        "batch_validation_complete",
        file_count=len(file_paths),
        total_rows=total_rows,
        total_failures=total_failures,
    )

    return results
