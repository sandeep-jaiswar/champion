"""Tests for CLI date validation."""

from datetime import date, timedelta

import pytest
import typer

from champion.cli import validate_date_format


class TestValidateDateFormat:
    """Tests for validate_date_format function."""

    def test_valid_date_format(self):
        """Test that valid date formats are accepted."""
        result = validate_date_format("2024-01-15")
        assert result == date(2024, 1, 15)

    def test_valid_date_format_different_date(self):
        """Test another valid date format."""
        result = validate_date_format("2023-12-31")
        assert result == date(2023, 12, 31)

    def test_invalid_date_format_dashes(self):
        """Test that invalid date format with wrong separators is rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            validate_date_format("2024/01/15")
        assert exc_info.value.exit_code == 1

    def test_valid_date_format_no_separators(self):
        """Test that date without separators is accepted (ISO 8601 basic format)."""
        result = validate_date_format("20240115")
        assert result == date(2024, 1, 15)

    def test_invalid_date_format_wrong_order(self):
        """Test that date in wrong order (MM-DD-YYYY) is rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            validate_date_format("01-15-2024")
        assert exc_info.value.exit_code == 1

    def test_invalid_date_format_text(self):
        """Test that text instead of date is rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            validate_date_format("not-a-date")
        assert exc_info.value.exit_code == 1

    def test_invalid_date_values(self):
        """Test that invalid date values (e.g., month 13) are rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            validate_date_format("2024-13-01")
        assert exc_info.value.exit_code == 1

    def test_invalid_day_values(self):
        """Test that invalid day values are rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            validate_date_format("2024-02-30")
        assert exc_info.value.exit_code == 1

    def test_leap_year_valid(self):
        """Test that leap year dates are accepted."""
        result = validate_date_format("2024-02-29")
        assert result == date(2024, 2, 29)

    def test_non_leap_year_invalid(self):
        """Test that Feb 29 in non-leap year is rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            validate_date_format("2023-02-29")
        assert exc_info.value.exit_code == 1

    def test_future_date_rejected_by_default(self):
        """Test that future dates are rejected by default."""
        future_date = (date.today() + timedelta(days=30)).isoformat()
        with pytest.raises(typer.Exit) as exc_info:
            validate_date_format(future_date)
        assert exc_info.value.exit_code == 1

    def test_future_date_allowed_when_specified(self):
        """Test that future dates are allowed when allow_future=True."""
        future_date = (date.today() + timedelta(days=30)).isoformat()
        result = validate_date_format(future_date, allow_future=True)
        assert result == date.today() + timedelta(days=30)

    def test_today_is_valid(self):
        """Test that today's date is valid."""
        result = validate_date_format(date.today().isoformat())
        assert result == date.today()

    def test_past_date_is_valid(self):
        """Test that past dates are valid."""
        past_date = (date.today() - timedelta(days=30)).isoformat()
        result = validate_date_format(past_date)
        assert result == date.today() - timedelta(days=30)

    def test_compact_format_yyyymmdd(self):
        """Test that YYYYMMDD format is accepted."""
        result = validate_date_format("20231215")
        assert result == date(2023, 12, 15)

    def test_compact_format_invalid(self):
        """Test that invalid compact format is rejected."""
        with pytest.raises(typer.Exit) as exc_info:
            validate_date_format("2023121")  # Only 7 digits
        assert exc_info.value.exit_code == 1
