"""Test Prefect flows and tasks.

This module tests the orchestration flows to ensure they work correctly
with sample data.
"""

import os
import tempfile
from datetime import date
from pathlib import Path

import pytest

from src.orchestration.flows import (
    load_clickhouse,
    nse_bhavcopy_etl_flow,
    normalize_polars,
    parse_polars_raw,
    scrape_bhavcopy,
    write_parquet,
)


@pytest.fixture
def sample_csv_file():
    """Create a sample CSV file for testing."""
    test_dir = Path(tempfile.mkdtemp())
    csv_file = test_dir / "test_bhavcopy.csv"

    csv_content = """TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4
2024-01-02,2024-01-02,CM,NSE,STK,2885,INE002A01018,RELIANCE,EQ,-,-,-,-,RELIANCE INDUSTRIES LTD,2750.00,2780.50,2740.00,2765.25,2765.00,2750.00,-,2765.25,-,-,5000000,13826250000.00,50000,F1,1,-,-,-,-,-
2024-01-02,2024-01-02,CM,NSE,STK,11536,INE467B01029,TCS,EQ,-,-,-,-,TATA CONSULTANCY SERVICES LTD,3550.00,3575.00,3545.00,3560.75,3560.50,3550.00,-,3560.75,-,-,3500000,12462625000.00,45000,F1,1,-,-,-,-,-
2024-01-02,2024-01-02,CM,NSE,STK,5258,INE040A01034,INFY,EQ,-,-,-,-,INFOSYS LTD,1450.00,1465.00,1448.00,1460.50,1460.25,1450.00,-,1460.50,-,-,4200000,6134100000.00,42000,F1,1,-,-,-,-,-"""

    csv_file.write_text(csv_content)
    yield csv_file

    # Cleanup
    import shutil

    shutil.rmtree(test_dir)


@pytest.fixture
def test_output_dir():
    """Create temporary output directory."""
    test_dir = Path(tempfile.mkdtemp())
    yield test_dir

    # Cleanup
    import shutil

    shutil.rmtree(test_dir)


def test_parse_polars_raw_task(sample_csv_file):
    """Test parse_polars_raw task."""
    trade_date = date(2024, 1, 2)

    df = parse_polars_raw(str(sample_csv_file), trade_date)

    assert df is not None
    assert len(df) == 3
    assert "TckrSymb" in df.columns
    assert "ClsPric" in df.columns
    assert "event_id" in df.columns


def test_normalize_polars_task(sample_csv_file):
    """Test normalize_polars task."""
    trade_date = date(2024, 1, 2)

    # First parse
    df = parse_polars_raw(str(sample_csv_file), trade_date)

    # Then normalize
    normalized_df = normalize_polars(df)

    assert normalized_df is not None
    assert len(normalized_df) == 3  # All rows should be valid
    assert "TckrSymb" in normalized_df.columns
    assert "ClsPric" in normalized_df.columns


def test_write_parquet_task(sample_csv_file, test_output_dir):
    """Test write_parquet task."""
    trade_date = date(2024, 1, 2)

    # Parse and normalize
    df = parse_polars_raw(str(sample_csv_file), trade_date)
    normalized_df = normalize_polars(df)

    # Write to Parquet
    output_file = write_parquet(normalized_df, trade_date, str(test_output_dir))

    assert output_file is not None
    assert Path(output_file).exists()
    assert Path(output_file).suffix == ".parquet"


def test_load_clickhouse_task_without_connection(sample_csv_file, test_output_dir):
    """Test load_clickhouse task behavior when ClickHouse is unavailable."""
    trade_date = date(2024, 1, 2)

    # Parse, normalize, and write
    df = parse_polars_raw(str(sample_csv_file), trade_date)
    normalized_df = normalize_polars(df)
    output_file = write_parquet(normalized_df, trade_date, str(test_output_dir))

    # Try to load (should handle gracefully when ClickHouse unavailable)
    result = load_clickhouse(
        parquet_file=output_file,
        table="normalized_equity_ohlc",
        host="nonexistent-host",  # This will fail
        port=8123,
    )

    # Should return error result but not raise exception
    assert result is not None
    assert "error" in result or "rows_loaded" in result


@pytest.mark.skipif(
    os.getenv("SKIP_INTEGRATION_TESTS") == "true",
    reason="Skipping integration test - requires dependencies",
)
def test_nse_bhavcopy_etl_flow_with_mock_scraper(sample_csv_file, test_output_dir, monkeypatch):
    """Test the complete ETL flow with mocked scraper."""
    trade_date = date(2024, 1, 2)

    # Mock the scrape_bhavcopy task to return our sample CSV
    def mock_scrape(trade_date):
        return str(sample_csv_file)

    monkeypatch.setattr("src.orchestration.flows.scrape_bhavcopy", mock_scrape)

    # Run the flow (without ClickHouse load)
    result = nse_bhavcopy_etl_flow(
        trade_date=trade_date,
        output_base_path=str(test_output_dir),
        load_to_clickhouse=False,  # Skip ClickHouse
    )

    assert result is not None
    assert result["status"] == "success"
    assert result["rows_processed"] == 3
    assert "parquet_file" in result
    assert Path(result["parquet_file"]).exists()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
