"""Unit tests for BSE bhavcopy scraper."""

from datetime import date
from unittest.mock import Mock, patch

import pytest

from src.scrapers.bse_bhavcopy import BseBhavcopyScraper


@pytest.fixture
def scraper():
    """Create a scraper instance."""
    return BseBhavcopyScraper()


@pytest.fixture
def sample_date():
    """Sample trading date."""
    return date(2026, 1, 9)


def test_scraper_initialization(scraper):
    """Test that scraper initializes correctly."""
    assert scraper.name == "bse_bhavcopy"
    assert scraper.logger is not None


def test_scrape_url_format(scraper, sample_date):
    """Test that the URL is formatted correctly for BSE."""
    # BSE format: DDMMYY (e.g., 090126 for 09-Jan-2026)
    expected_date_str = "090126"

    with patch.object(scraper, "_download_and_extract_zip") as mock_download:
        mock_download.return_value = True

        try:
            scraper.scrape(sample_date)
        except Exception:
            pass  # We just want to check the URL format

        # Verify the URL was called with correct format
        call_args = mock_download.call_args
        assert call_args is not None
        url = call_args[0][0]
        assert expected_date_str in url
        assert "EQ" in url
        assert "CSV.ZIP" in url


def test_scrape_creates_correct_paths(scraper, sample_date, tmp_path):
    """Test that scrape creates correct file paths."""
    with patch("src.scrapers.bse_bhavcopy.config") as mock_config:
        mock_config.storage.data_dir = tmp_path
        mock_config.bse.bhavcopy_url = "https://example.com/EQ{date}_CSV.ZIP"
        mock_config.scraper.user_agent = "test"
        mock_config.scraper.timeout = 30

        with patch.object(scraper, "_download_and_extract_zip") as mock_download:
            mock_download.return_value = True

            result = scraper.scrape(sample_date)

            # Check that CSV path is in the expected location
            assert "BhavCopy_BSE_EQ_20260109.csv" in str(result)


def test_scrape_failure_raises_error(scraper, sample_date):
    """Test that scrape raises error on download failure."""
    with patch.object(scraper, "_download_and_extract_zip") as mock_download:
        mock_download.return_value = False

        with pytest.raises(RuntimeError) as exc_info:
            scraper.scrape(sample_date)

        assert "Failed to download BSE bhavcopy" in str(exc_info.value)
        assert str(sample_date) in str(exc_info.value)


def test_download_and_extract_zip_success(scraper, tmp_path):
    """Test successful ZIP download and extraction."""
    import zipfile
    from io import BytesIO

    # Create a mock ZIP file with CSV content
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("test.csv", "SC_CODE,SC_NAME,CLOSE\n500325,RELIANCE,2750.00")
    zip_content = zip_buffer.getvalue()

    csv_path = tmp_path / "test.csv"

    with patch("httpx.get") as mock_get:
        mock_response = Mock()
        mock_response.content = zip_content
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = scraper._download_and_extract_zip(
            "https://example.com/test.zip",
            str(tmp_path / "test.zip"),
            str(csv_path),
        )

        assert result is True
        assert csv_path.exists()
        content = csv_path.read_text()
        assert "RELIANCE" in content


def test_download_and_extract_zip_no_csv(scraper, tmp_path):
    """Test ZIP extraction fails when no CSV found."""
    import zipfile
    from io import BytesIO

    # Create a mock ZIP file with no CSV
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("test.txt", "not a csv")
    zip_content = zip_buffer.getvalue()

    csv_path = tmp_path / "test.csv"

    with patch("httpx.get") as mock_get:
        mock_response = Mock()
        mock_response.content = zip_content
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = scraper._download_and_extract_zip(
            "https://example.com/test.zip",
            str(tmp_path / "test.zip"),
            str(csv_path),
        )

        assert result is False
        assert not csv_path.exists()


def test_download_and_extract_zip_network_error(scraper, tmp_path):
    """Test download handles network errors gracefully."""
    import httpx

    csv_path = tmp_path / "test.csv"

    with patch("httpx.get") as mock_get:
        mock_get.side_effect = httpx.HTTPError("Network error")

        result = scraper._download_and_extract_zip(
            "https://example.com/test.zip",
            str(tmp_path / "test.zip"),
            str(csv_path),
        )

        assert result is False
        assert not csv_path.exists()
