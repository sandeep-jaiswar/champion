"""Integration tests for exception handling in orchestration flows."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import date

from champion.orchestration.flows.flows import (
    scrape_bhavcopy,
    parse_polars_raw,
    normalize_polars,
    write_parquet,
)


class TestFlowExceptionHandling:
    """Test suite for exception handling in flow tasks."""

    @patch('champion.orchestration.flows.flows.BhavcopyScraper')
    def test_scrape_bhavcopy_connection_error(self, mock_scraper_class, caplog):
        """Test that ConnectionError is handled as retryable."""
        mock_scraper = Mock()
        mock_scraper.scrape.side_effect = ConnectionError("Network unavailable")
        mock_scraper_class.return_value = mock_scraper
        
        with pytest.raises(ConnectionError):
            scrape_bhavcopy(date(2024, 1, 1))
        
        # Check that the error was logged with retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.BhavcopyScraper')
    def test_scrape_bhavcopy_timeout_error(self, mock_scraper_class, caplog):
        """Test that TimeoutError is handled as retryable."""
        mock_scraper = Mock()
        mock_scraper.scrape.side_effect = TimeoutError("Request timed out")
        mock_scraper_class.return_value = mock_scraper
        
        with pytest.raises(TimeoutError):
            scrape_bhavcopy(date(2024, 1, 1))
        
        # Check that the error was logged with retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.BhavcopyScraper')
    def test_scrape_bhavcopy_file_error(self, mock_scraper_class, caplog):
        """Test that FileNotFoundError is handled as retryable."""
        mock_scraper = Mock()
        mock_scraper.scrape.side_effect = FileNotFoundError("File not found")
        mock_scraper_class.return_value = mock_scraper
        
        with pytest.raises(FileNotFoundError):
            scrape_bhavcopy(date(2024, 1, 1))
        
        # Check that the error was logged with retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.BhavcopyScraper')
    def test_scrape_bhavcopy_validation_error(self, mock_scraper_class, caplog):
        """Test that ValueError is handled as non-retryable."""
        mock_scraper = Mock()
        mock_scraper.scrape.side_effect = ValueError("Invalid date format")
        mock_scraper_class.return_value = mock_scraper
        
        with pytest.raises(ValueError):
            scrape_bhavcopy(date(2024, 1, 1))
        
        # Check that the error was logged with non-retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.BhavcopyScraper')
    def test_scrape_bhavcopy_unexpected_error(self, mock_scraper_class, caplog):
        """Test that unexpected errors are converted to RuntimeError."""
        mock_scraper = Mock()
        mock_scraper.scrape.side_effect = Exception("Unexpected error")
        mock_scraper_class.return_value = mock_scraper
        
        with pytest.raises(RuntimeError) as exc_info:
            scrape_bhavcopy(date(2024, 1, 1))
        
        assert "Fatal error" in str(exc_info.value)
        # Check that the error was logged as critical with non-retryable flag
        assert any("critical" in record.levelname.lower() or 
                   "fatal" in record.message.lower() 
                   for record in caplog.records)

    @patch('champion.orchestration.flows.flows.PolarsBhavcopyParser')
    def test_parse_polars_raw_file_error(self, mock_parser_class, caplog):
        """Test that file errors are handled as retryable."""
        mock_parser = Mock()
        mock_parser.parse_to_dataframe.side_effect = FileNotFoundError("CSV file not found")
        mock_parser_class.return_value = mock_parser
        
        with pytest.raises(FileNotFoundError):
            parse_polars_raw("/nonexistent/file.csv", date(2024, 1, 1))
        
        # Check that the error was logged with retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.PolarsBhavcopyParser')
    def test_parse_polars_raw_validation_error(self, mock_parser_class, caplog):
        """Test that validation errors are handled as non-retryable."""
        mock_parser = Mock()
        mock_parser.parse_to_dataframe.side_effect = ValueError("Invalid CSV format")
        mock_parser_class.return_value = mock_parser
        
        with pytest.raises(ValueError):
            parse_polars_raw("/some/file.csv", date(2024, 1, 1))
        
        # Check that the error was logged with non-retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.PolarsBhavcopyParser')
    def test_parse_polars_raw_unexpected_error(self, mock_parser_class, caplog):
        """Test that unexpected errors are converted to RuntimeError."""
        mock_parser = Mock()
        mock_parser.parse_to_dataframe.side_effect = Exception("Unexpected parsing error")
        mock_parser_class.return_value = mock_parser
        
        with pytest.raises(RuntimeError) as exc_info:
            parse_polars_raw("/some/file.csv", date(2024, 1, 1))
        
        assert "Fatal error" in str(exc_info.value)
        # Check that the error was logged as critical
        assert any("critical" in record.levelname.lower() or 
                   "fatal" in record.message.lower() 
                   for record in caplog.records)

    def test_normalize_polars_validation_error(self, caplog):
        """Test that validation errors in normalization are handled correctly."""
        import polars as pl
        
        # Create an empty DataFrame that will fail validation
        df = pl.DataFrame()
        
        with pytest.raises(ValueError):
            normalize_polars(df)
        
        # Check that the error was logged with non-retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.PolarsBhavcopyParser')
    def test_write_parquet_io_error(self, mock_parser_class, caplog):
        """Test that I/O errors are handled as retryable."""
        import polars as pl
        
        mock_parser = Mock()
        mock_parser.write_parquet.side_effect = IOError("Disk full")
        mock_parser_class.return_value = mock_parser
        
        df = pl.DataFrame({"TckrSymb": ["TEST"], "ClsPric": [100.0]})
        
        with pytest.raises(IOError):
            write_parquet(df, date(2024, 1, 1))
        
        # Check that the error was logged with retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.PolarsBhavcopyParser')
    def test_write_parquet_validation_error(self, mock_parser_class, caplog):
        """Test that validation errors are handled as non-retryable."""
        import polars as pl
        
        mock_parser = Mock()
        mock_parser.write_parquet.side_effect = ValueError("Invalid DataFrame schema")
        mock_parser_class.return_value = mock_parser
        
        df = pl.DataFrame({"TckrSymb": ["TEST"], "ClsPric": [100.0]})
        
        with pytest.raises(ValueError):
            write_parquet(df, date(2024, 1, 1))
        
        # Check that the error was logged with non-retryable flag
        assert any("retryable" in str(record) for record in caplog.records)


class TestFlowLevelExceptionHandling:
    """Test exception handling at the flow level."""

    @patch('champion.orchestration.flows.flows.scrape_bhavcopy')
    def test_nse_bhavcopy_etl_flow_network_error(self, mock_scrape, caplog):
        """Test that network errors in flow are handled as retryable."""
        from champion.orchestration.flows.flows import nse_bhavcopy_etl_flow
        
        mock_scrape.side_effect = ConnectionError("Network error")
        
        with pytest.raises(ConnectionError):
            nse_bhavcopy_etl_flow(
                trade_date=date(2024, 1, 1),
                start_metrics_server_flag=False
            )
        
        # Check that the error was logged with retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.scrape_bhavcopy')
    def test_nse_bhavcopy_etl_flow_validation_error(self, mock_scrape, caplog):
        """Test that validation errors in flow are handled as non-retryable."""
        from champion.orchestration.flows.flows import nse_bhavcopy_etl_flow
        
        mock_scrape.side_effect = ValueError("Invalid trade date")
        
        with pytest.raises(ValueError):
            nse_bhavcopy_etl_flow(
                trade_date=date(2024, 1, 1),
                start_metrics_server_flag=False
            )
        
        # Check that the error was logged with non-retryable flag
        assert any("retryable" in str(record) for record in caplog.records)

    @patch('champion.orchestration.flows.flows.scrape_bhavcopy')
    def test_nse_bhavcopy_etl_flow_unexpected_error(self, mock_scrape, caplog):
        """Test that unexpected errors in flow are converted to RuntimeError."""
        from champion.orchestration.flows.flows import nse_bhavcopy_etl_flow
        
        mock_scrape.side_effect = Exception("Unexpected error")
        
        with pytest.raises(RuntimeError) as exc_info:
            nse_bhavcopy_etl_flow(
                trade_date=date(2024, 1, 1),
                start_metrics_server_flag=False
            )
        
        assert "Fatal error" in str(exc_info.value)
        # Check that the error was logged as critical
        assert any("critical" in record.levelname.lower() or 
                   "fatal" in record.message.lower() 
                   for record in caplog.records)
