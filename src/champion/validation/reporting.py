"""Validation reporting and analytics module.

Provides:
- Daily validation summary reports
- Trend analysis over time
- Anomaly detection in validation metrics
- Historical validation metrics
"""

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()


@dataclass
class ValidationTrend:
    """Trend analysis result for validation metrics."""

    metric_name: str
    current_value: float
    previous_value: float
    change_pct: float
    trend: str  # "increasing", "decreasing", "stable"
    is_anomaly: bool


@dataclass
class ValidationReport:
    """Daily validation report with aggregated metrics."""

    report_date: str
    total_validations: int
    total_rows_validated: int
    total_failures: int
    failure_rate: float
    schemas_validated: list[str]
    rules_applied: list[str]
    trends: list[ValidationTrend]
    anomalies: list[str]


class ValidationReporter:
    """Generate validation reports, trends, and detect anomalies."""

    def __init__(self, quarantine_dir: Path):
        """Initialize reporter with quarantine directory.

        Args:
            quarantine_dir: Directory containing quarantine and audit logs
        """
        self.quarantine_dir = Path(quarantine_dir)
        self.audit_log_file = self.quarantine_dir / "audit_log.jsonl"

    def load_audit_log(self, days: int = 30) -> list[dict[str, Any]]:
        """Load audit log entries from the last N days.

        Args:
            days: Number of days to look back (default: 30)

        Returns:
            List of audit log entries
        """
        if not self.audit_log_file.exists():
            logger.warning("audit_log_not_found", file=str(self.audit_log_file))
            return []

        entries = []
        cutoff_date = datetime.now() - timedelta(days=days)

        with open(self.audit_log_file) as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    entry_date = datetime.fromisoformat(entry["timestamp"])
                    if entry_date >= cutoff_date:
                        entries.append(entry)
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    logger.warning("invalid_audit_entry", error=str(e), line=line[:100])
                    continue

        logger.info("loaded_audit_log", entries=len(entries), days=days)
        return entries

    def generate_daily_report(
        self, date: str | None = None, include_trends: bool = True
    ) -> ValidationReport:
        """Generate daily validation report.

        Args:
            date: Date for report (YYYY-MM-DD format), defaults to today
            include_trends: Include trend analysis (default: True)

        Returns:
            ValidationReport with aggregated metrics
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # Load audit log for the day
        entries = self.load_audit_log(days=1)
        date_entries = [e for e in entries if e["timestamp"].startswith(date)]

        if not date_entries:
            logger.warning("no_validation_data", date=date)
            return ValidationReport(
                report_date=date,
                total_validations=0,
                total_rows_validated=0,
                total_failures=0,
                failure_rate=0.0,
                schemas_validated=[],
                rules_applied=[],
                trends=[],
                anomalies=[],
            )

        # Aggregate metrics
        total_validations = len(date_entries)
        total_rows = sum(e["total_rows"] for e in date_entries)
        total_failures = sum(e["failed_rows"] for e in date_entries)
        failure_rate = total_failures / total_rows if total_rows > 0 else 0.0

        schemas = list({e["schema_name"] for e in date_entries})

        # Collect all rules applied
        all_rules = set()
        for e in date_entries:
            all_rules.update(e.get("rules_applied", []))
        rules = sorted(all_rules)

        # Calculate trends if requested
        trends = []
        anomalies = []
        if include_trends:
            trends = self._calculate_trends(date_entries)
            anomalies = self._detect_anomalies(date_entries)

        report = ValidationReport(
            report_date=date,
            total_validations=total_validations,
            total_rows_validated=total_rows,
            total_failures=total_failures,
            failure_rate=failure_rate,
            schemas_validated=schemas,
            rules_applied=rules,
            trends=trends,
            anomalies=anomalies,
        )

        logger.info(
            "generated_daily_report",
            date=date,
            validations=total_validations,
            failure_rate=f"{failure_rate:.2%}",
        )

        return report

    def _calculate_trends(self, current_entries: list[dict[str, Any]]) -> list[ValidationTrend]:
        """Calculate trends by comparing with previous period.

        Args:
            current_entries: Current period audit entries

        Returns:
            List of ValidationTrend objects
        """
        trends: list[ValidationTrend] = []

        if not current_entries:
            return trends

        # Load previous period (same length as current)
        all_entries = self.load_audit_log(days=60)
        current_date = current_entries[0]["timestamp"][:10]

        # Calculate metrics for current period
        current_total_rows = sum(e["total_rows"] for e in current_entries)
        current_failures = sum(e["failed_rows"] for e in current_entries)
        current_failure_rate = (
            current_failures / current_total_rows if current_total_rows > 0 else 0.0
        )

        # Find previous period entries (same duration before current period)
        prev_date = (datetime.fromisoformat(current_date) - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_entries = [e for e in all_entries if e["timestamp"].startswith(prev_date)]

        if not prev_entries:
            return trends

        prev_total_rows = sum(e["total_rows"] for e in prev_entries)
        prev_failures = sum(e["failed_rows"] for e in prev_entries)
        prev_failure_rate = prev_failures / prev_total_rows if prev_total_rows > 0 else 0.0

        # Calculate failure rate trend
        if prev_failure_rate > 0:
            change_pct = ((current_failure_rate - prev_failure_rate) / prev_failure_rate) * 100
        else:
            change_pct = 0.0

        trend_direction = "stable"
        if abs(change_pct) > 10:  # 10% threshold for trend detection
            trend_direction = "increasing" if change_pct > 0 else "decreasing"

        # Anomaly detection: >50% increase in failure rate
        is_anomaly = change_pct > 50

        trends.append(
            ValidationTrend(
                metric_name="failure_rate",
                current_value=current_failure_rate,
                previous_value=prev_failure_rate,
                change_pct=change_pct,
                trend=trend_direction,
                is_anomaly=is_anomaly,
            )
        )

        # Calculate volume trend
        if prev_total_rows > 0:
            volume_change_pct = ((current_total_rows - prev_total_rows) / prev_total_rows) * 100
        else:
            volume_change_pct = 0.0

        volume_trend = "stable"
        if abs(volume_change_pct) > 20:  # 20% threshold for volume trend
            volume_trend = "increasing" if volume_change_pct > 0 else "decreasing"

        trends.append(
            ValidationTrend(
                metric_name="validation_volume",
                current_value=float(current_total_rows),
                previous_value=float(prev_total_rows),
                change_pct=volume_change_pct,
                trend=volume_trend,
                is_anomaly=abs(volume_change_pct) > 50,  # >50% change is anomaly
            )
        )

        return trends

    def _detect_anomalies(self, entries: list[dict[str, Any]]) -> list[str]:
        """Detect anomalies in validation metrics.

        Args:
            entries: Audit log entries to analyze

        Returns:
            List of anomaly descriptions
        """
        anomalies: list[str] = []

        if not entries:
            return anomalies

        # Anomaly 1: High failure rate (>5%)
        total_rows = sum(e["total_rows"] for e in entries)
        total_failures = sum(e["failed_rows"] for e in entries)
        failure_rate = total_failures / total_rows if total_rows > 0 else 0.0

        if failure_rate > 0.05:
            anomalies.append(f"High failure rate detected: {failure_rate:.2%} (threshold: 5%)")

        # Anomaly 2: Schema-specific failures
        schema_failures: dict[str, tuple[int, int]] = defaultdict(lambda: (0, 0))
        for e in entries:
            schema = e["schema_name"]
            schema_failures[schema] = (
                schema_failures[schema][0] + e["failed_rows"],
                schema_failures[schema][1] + e["total_rows"],
            )

        for schema, (failures, total) in schema_failures.items():
            schema_rate = failures / total if total > 0 else 0.0
            if schema_rate > 0.1:  # 10% threshold per schema
                anomalies.append(f"High failure rate for schema '{schema}': {schema_rate:.2%}")

        # Anomaly 3: Sudden spike in validations
        historical = self.load_audit_log(days=7)
        if len(historical) > len(entries):
            avg_daily = len(historical) / 7
            if len(entries) > avg_daily * 2:  # More than 2x average
                anomalies.append(
                    f"Validation volume spike: {len(entries)} (7-day avg: {avg_daily:.0f})"
                )

        return anomalies

    def format_report(self, report: ValidationReport) -> str:
        """Format validation report as human-readable string.

        Args:
            report: ValidationReport to format

        Returns:
            Formatted report string
        """
        lines = []
        lines.append("=" * 80)
        lines.append(f"Validation Report - {report.report_date}")
        lines.append("=" * 80)
        lines.append("")

        # Summary
        lines.append("Summary:")
        lines.append(f"  Total Validations:    {report.total_validations}")
        lines.append(f"  Rows Validated:       {report.total_rows_validated:,}")
        lines.append(f"  Failures:             {report.total_failures:,}")
        lines.append(f"  Failure Rate:         {report.failure_rate:.2%}")
        lines.append("")

        # Schemas
        lines.append(f"Schemas Validated ({len(report.schemas_validated)}):")
        for schema in sorted(report.schemas_validated):
            lines.append(f"  - {schema}")
        lines.append("")

        # Rules
        lines.append(f"Validation Rules Applied ({len(report.rules_applied)}):")
        for i, rule in enumerate(sorted(report.rules_applied)[:10], 1):
            lines.append(f"  {i}. {rule}")
        if len(report.rules_applied) > 10:
            lines.append(f"  ... and {len(report.rules_applied) - 10} more")
        lines.append("")

        # Trends
        if report.trends:
            lines.append("Trends:")
            for trend in report.trends:
                symbol = (
                    "📈"
                    if trend.trend == "increasing"
                    else "📉"
                    if trend.trend == "decreasing"
                    else "➡️"
                )
                anomaly_flag = " ⚠️ ANOMALY" if trend.is_anomaly else ""
                lines.append(
                    f"  {symbol} {trend.metric_name}: {trend.current_value:.4f} "
                    f"({trend.change_pct:+.1f}% vs previous){anomaly_flag}"
                )
            lines.append("")

        # Anomalies
        if report.anomalies:
            lines.append("⚠️  Anomalies Detected:")
            for anomaly in report.anomalies:
                lines.append(f"  - {anomaly}")
            lines.append("")

        lines.append("=" * 80)

        return "\n".join(lines)

    def save_report(self, report: ValidationReport, output_dir: Path) -> Path:
        """Save report to file.

        Args:
            report: ValidationReport to save
            output_dir: Directory to save report

        Returns:
            Path to saved report file
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        report_file = output_dir / f"validation_report_{report.report_date}.txt"
        report_text = self.format_report(report)

        with open(report_file, "w") as f:
            f.write(report_text)

        # Also save JSON version for programmatic access
        json_file = output_dir / f"validation_report_{report.report_date}.json"
        report_dict = {
            "report_date": report.report_date,
            "total_validations": report.total_validations,
            "total_rows_validated": report.total_rows_validated,
            "total_failures": report.total_failures,
            "failure_rate": report.failure_rate,
            "schemas_validated": report.schemas_validated,
            "rules_applied": report.rules_applied,
            "trends": [
                {
                    "metric_name": t.metric_name,
                    "current_value": t.current_value,
                    "previous_value": t.previous_value,
                    "change_pct": t.change_pct,
                    "trend": t.trend,
                    "is_anomaly": t.is_anomaly,
                }
                for t in report.trends
            ],
            "anomalies": report.anomalies,
        }

        with open(json_file, "w") as f:
            json.dump(report_dict, f, indent=2)

        logger.info(
            "saved_validation_report",
            text_file=str(report_file),
            json_file=str(json_file),
        )

        return report_file

    def generate_trend_chart_data(self, days: int = 30) -> dict[str, list[Any]]:
        """Generate data for trend visualization.

        Args:
            days: Number of days to include (default: 30)

        Returns:
            Dictionary with dates and metrics for charting
        """
        entries = self.load_audit_log(days=days)

        if not entries:
            return {"dates": [], "failure_rates": [], "volumes": []}

        # Group by date
        daily_metrics: dict[str, dict[str, int]] = defaultdict(
            lambda: {"total_rows": 0, "failed_rows": 0}
        )

        for entry in entries:
            date = entry["timestamp"][:10]
            daily_metrics[date]["total_rows"] += entry["total_rows"]
            daily_metrics[date]["failed_rows"] += entry["failed_rows"]

        # Sort by date
        sorted_dates = sorted(daily_metrics.keys())

        dates = []
        failure_rates = []
        volumes = []

        for date in sorted_dates:
            metrics = daily_metrics[date]
            dates.append(date)
            volumes.append(metrics["total_rows"])
            failure_rate = (
                metrics["failed_rows"] / metrics["total_rows"] if metrics["total_rows"] > 0 else 0.0
            )
            failure_rates.append(failure_rate)

        return {
            "dates": dates,
            "failure_rates": failure_rates,
            "volumes": volumes,
        }
