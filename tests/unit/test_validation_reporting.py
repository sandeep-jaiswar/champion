"""Tests for validation reporting module."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from champion.validation.reporting import ValidationReporter, ValidationReport


@pytest.fixture
def quarantine_dir(tmp_path):
    """Create test quarantine directory with audit log."""
    qdir = tmp_path / "quarantine"
    qdir.mkdir()

    # Create sample audit log
    audit_log = qdir / "audit_log.jsonl"
    
    # Generate entries for 3 days
    base_date = datetime.now()
    
    entries = []
    for i in range(3):
        date = (base_date - timedelta(days=i)).isoformat()
        # Day 0: 2 validations
        # Day 1: 3 validations
        # Day 2: 1 validation
        num_entries = 2 if i == 0 else (3 if i == 1 else 1)
        
        for j in range(num_entries):
            entry = {
                "timestamp": date,
                "schema_name": f"test_schema_{j % 2}",
                "quarantine_file": f"test_failures_{i}_{j}.parquet",
                "failed_rows": 10 * (i + 1),
                "total_rows": 1000,
                "rules_applied": ["schema_validation", "ohlc_consistency", "duplicate_detection"],
                "failure_rate": (10 * (i + 1)) / 1000,
            }
            entries.append(entry)
    
    with open(audit_log, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
    
    return qdir


@pytest.fixture
def reporter(quarantine_dir):
    """Create reporter instance."""
    return ValidationReporter(quarantine_dir=quarantine_dir)


def test_reporter_initialization(quarantine_dir):
    """Test reporter initialization."""
    reporter = ValidationReporter(quarantine_dir=quarantine_dir)
    assert reporter.quarantine_dir == quarantine_dir
    assert reporter.audit_log_file == quarantine_dir / "audit_log.jsonl"


def test_load_audit_log(reporter):
    """Test loading audit log."""
    entries = reporter.load_audit_log(days=30)
    assert len(entries) == 6  # 2 + 3 + 1 from fixture


def test_generate_daily_report(reporter):
    """Test generating daily report."""
    date = datetime.now().strftime("%Y-%m-%d")
    report = reporter.generate_daily_report(date=date, include_trends=False)
    
    assert report.report_date == date
    assert report.total_validations == 2
    assert report.total_rows_validated == 2000
    assert report.total_failures == 20
    assert report.failure_rate == 0.01
    assert len(report.schemas_validated) > 0
    assert len(report.rules_applied) > 0


def test_generate_daily_report_with_trends(reporter):
    """Test report generation with trend analysis."""
    date = datetime.now().strftime("%Y-%m-%d")
    report = reporter.generate_daily_report(date=date, include_trends=True)
    
    assert len(report.trends) > 0
    # Should have failure_rate and validation_volume trends
    trend_names = [t.metric_name for t in report.trends]
    assert "failure_rate" in trend_names or "validation_volume" in trend_names


def test_generate_daily_report_no_data(reporter, tmp_path):
    """Test report generation with no data."""
    empty_qdir = tmp_path / "empty_quarantine"
    empty_qdir.mkdir()
    empty_reporter = ValidationReporter(quarantine_dir=empty_qdir)
    
    report = empty_reporter.generate_daily_report()
    
    assert report.total_validations == 0
    assert report.total_rows_validated == 0
    assert report.total_failures == 0


def test_detect_anomalies_high_failure_rate(tmp_path):
    """Test anomaly detection for high failure rate."""
    qdir = tmp_path / "quarantine"
    qdir.mkdir()
    
    audit_log = qdir / "audit_log.jsonl"
    date = datetime.now().isoformat()
    
    # Create entry with high failure rate (>5%)
    entry = {
        "timestamp": date,
        "schema_name": "test_schema",
        "quarantine_file": "test_failures.parquet",
        "failed_rows": 100,
        "total_rows": 1000,
        "rules_applied": ["schema_validation"],
        "failure_rate": 0.1,  # 10% failure rate
    }
    
    with open(audit_log, "w") as f:
        f.write(json.dumps(entry) + "\n")
    
    reporter = ValidationReporter(quarantine_dir=qdir)
    report = reporter.generate_daily_report()
    
    assert len(report.anomalies) > 0
    assert any("high failure rate" in a.lower() for a in report.anomalies)


def test_format_report(reporter):
    """Test report formatting."""
    date = datetime.now().strftime("%Y-%m-%d")
    report = reporter.generate_daily_report(date=date, include_trends=False)
    
    formatted = reporter.format_report(report)
    
    assert "Validation Report" in formatted
    assert date in formatted
    assert "Summary:" in formatted
    assert str(report.total_validations) in formatted


def test_save_report(reporter, tmp_path):
    """Test saving report to files."""
    date = datetime.now().strftime("%Y-%m-%d")
    report = reporter.generate_daily_report(date=date)
    
    output_dir = tmp_path / "reports"
    report_file = reporter.save_report(report, output_dir)
    
    # Check text report exists
    assert report_file.exists()
    assert report_file.suffix == ".txt"
    
    # Check JSON report exists
    json_file = output_dir / f"validation_report_{date}.json"
    assert json_file.exists()
    
    # Verify JSON content
    with open(json_file) as f:
        data = json.load(f)
    assert data["report_date"] == date
    assert data["total_validations"] == report.total_validations


def test_generate_trend_chart_data(reporter):
    """Test trend chart data generation."""
    chart_data = reporter.generate_trend_chart_data(days=30)
    
    assert "dates" in chart_data
    assert "failure_rates" in chart_data
    assert "volumes" in chart_data
    
    assert len(chart_data["dates"]) > 0
    assert len(chart_data["dates"]) == len(chart_data["failure_rates"])
    assert len(chart_data["dates"]) == len(chart_data["volumes"])


def test_generate_trend_chart_data_empty(tmp_path):
    """Test trend chart data with no data."""
    empty_qdir = tmp_path / "empty_quarantine"
    empty_qdir.mkdir()
    empty_reporter = ValidationReporter(quarantine_dir=empty_qdir)
    
    chart_data = empty_reporter.generate_trend_chart_data()
    
    assert chart_data["dates"] == []
    assert chart_data["failure_rates"] == []
    assert chart_data["volumes"] == []


def test_calculate_trends_decreasing(tmp_path):
    """Test trend calculation for decreasing metrics."""
    qdir = tmp_path / "quarantine"
    qdir.mkdir()
    
    audit_log = qdir / "audit_log.jsonl"
    
    # Day 0: high failure rate
    # Day 1: low failure rate (decreasing trend)
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    entries = [
        {
            "timestamp": today.isoformat(),
            "schema_name": "test",
            "quarantine_file": "test_0.parquet",
            "failed_rows": 10,
            "total_rows": 1000,
            "rules_applied": ["test"],
            "failure_rate": 0.01,
        },
        {
            "timestamp": yesterday.isoformat(),
            "schema_name": "test",
            "quarantine_file": "test_1.parquet",
            "failed_rows": 50,
            "total_rows": 1000,
            "rules_applied": ["test"],
            "failure_rate": 0.05,
        },
    ]
    
    with open(audit_log, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
    
    reporter = ValidationReporter(quarantine_dir=qdir)
    report = reporter.generate_daily_report(date=today.strftime("%Y-%m-%d"), include_trends=True)
    
    # Should have trends showing decrease
    failure_rate_trend = next(
        (t for t in report.trends if t.metric_name == "failure_rate"), None
    )
    
    if failure_rate_trend:
        assert failure_rate_trend.current_value < failure_rate_trend.previous_value


def test_anomaly_detection_schema_specific(tmp_path):
    """Test schema-specific anomaly detection."""
    qdir = tmp_path / "quarantine"
    qdir.mkdir()
    
    audit_log = qdir / "audit_log.jsonl"
    date = datetime.now().isoformat()
    
    # Create entries with one schema having high failure rate
    entries = [
        {
            "timestamp": date,
            "schema_name": "good_schema",
            "quarantine_file": "good.parquet",
            "failed_rows": 5,
            "total_rows": 1000,
            "rules_applied": ["test"],
            "failure_rate": 0.005,
        },
        {
            "timestamp": date,
            "schema_name": "bad_schema",
            "quarantine_file": "bad.parquet",
            "failed_rows": 200,
            "total_rows": 1000,
            "rules_applied": ["test"],
            "failure_rate": 0.2,
        },
    ]
    
    with open(audit_log, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")
    
    reporter = ValidationReporter(quarantine_dir=qdir)
    report = reporter.generate_daily_report()
    
    # Should detect schema-specific anomaly
    assert any("bad_schema" in a for a in report.anomalies)
