"""Unit tests for ClickHouse batch loader schema validation and column mapping."""

import uuid
from datetime import datetime, date
from pathlib import Path

import polars as pl
import pytest

# Add parent directory to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from warehouse.loader.batch_loader import ClickHouseLoader


@pytest.fixture
def loader():
    """Create a loader instance (without connecting to ClickHouse)."""
    return ClickHouseLoader()


@pytest.fixture
def sample_normalized_df_nse_names():
    """Create a sample DataFrame with NSE column names."""
    records = [
        {
            'event_id': str(uuid.uuid4()),
            'event_time': int(datetime.now().timestamp() * 1000),
            'ingest_time': int(datetime.now().timestamp() * 1000),
            'source': 'test',
            'schema_version': 'v1',
            'entity_id': 'TEST001:12345:NSE',
            'TradDt': '2024-01-02',
            'BizDt': '2024-01-02',
            'Sgmt': 'CM',
            'Src': 'NSE',
            'FinInstrmTp': 'STK',
            'FinInstrmId': 12345,
            'ISIN': 'INE000001018',
            'TckrSymb': 'TEST001',
            'SctySrs': 'EQ',
            'OpnPric': 100.0,
            'HghPric': 105.0,
            'LwPric': 99.0,
            'ClsPric': 102.5,
            'LastPric': 102.5,
            'PrvsClsgPric': 100.0,
            'TtlTradgVol': 1000000,
            'TtlTrfVal': 102500000.0,
            'TtlNbOfTxsExctd': 10000,
        }
    ]
    return pl.DataFrame(records)


@pytest.fixture
def sample_normalized_df_friendly_names():
    """Create a sample DataFrame with friendly column names."""
    records = [
        {
            'event_id': str(uuid.uuid4()),
            'event_time': int(datetime.now().timestamp() * 1000),
            'ingest_time': int(datetime.now().timestamp() * 1000),
            'source': 'test',
            'schema_version': 'v1',
            'entity_id': 'TEST001:12345:NSE',
            'trade_date': date(2024, 1, 2),
            'symbol': 'TEST001',
            'instrument_id': 12345,
            'isin': 'INE000001018',
            'open': 100.0,
            'high': 105.0,
            'low': 99.0,
            'close': 102.5,
            'last_price': 102.5,
            'prev_close': 100.0,
            'volume': 1000000,
            'turnover': 102500000.0,
            'trades': 10000,
        }
    ]
    return pl.DataFrame(records)


@pytest.fixture
def sample_features_df():
    """Create a sample features DataFrame."""
    records = [
        {
            'symbol': 'TEST001',
            'trade_date': date(2024, 1, 2),
            'feature_timestamp': int(datetime.now().timestamp() * 1000),
            'feature_version': 'v1',
            'sma_20': 100.5,
            'rsi_14': 55.0,
            'macd': 1.5,
        }
    ]
    return pl.DataFrame(records)


class TestSchemaValidation:
    """Tests for schema validation logic."""

    def test_validate_schema_normalized_with_nse_names(self, loader, sample_normalized_df_nse_names):
        """Test that validation passes for DataFrame with NSE column names."""
        # Should not raise an exception
        loader._validate_schema(sample_normalized_df_nse_names, 'normalized_equity_ohlc')

    def test_validate_schema_normalized_missing_required_columns(self, loader):
        """Test that validation fails when required columns are missing."""
        # DataFrame missing TckrSymb
        df = pl.DataFrame([{
            'event_time': int(datetime.now().timestamp() * 1000),
            'TradDt': '2024-01-02',
            'FinInstrmId': 12345,
        }])
        
        with pytest.raises(ValueError) as exc_info:
            loader._validate_schema(df, 'normalized_equity_ohlc')
        
        assert 'Missing required columns' in str(exc_info.value)
        assert 'TckrSymb' in str(exc_info.value)

    def test_validate_schema_features_valid(self, loader, sample_features_df):
        """Test that validation passes for valid features DataFrame."""
        # Should not raise an exception
        loader._validate_schema(sample_features_df, 'features_equity_indicators')

    def test_validate_schema_features_missing_columns(self, loader):
        """Test that validation fails for features with missing columns."""
        df = pl.DataFrame([{
            'symbol': 'TEST001',
            'trade_date': date(2024, 1, 2),
            # Missing feature_timestamp
        }])
        
        with pytest.raises(ValueError) as exc_info:
            loader._validate_schema(df, 'features_equity_indicators')
        
        assert 'Missing required columns' in str(exc_info.value)
        assert 'feature_timestamp' in str(exc_info.value)


class TestColumnMapping:
    """Tests for column name mapping logic."""

    def test_column_mapping_normalized_friendly_to_nse(self, loader, sample_normalized_df_friendly_names):
        """Test that friendly column names are mapped to NSE names."""
        result = loader._prepare_dataframe_for_insert(
            sample_normalized_df_friendly_names,
            'normalized_equity_ohlc'
        )
        
        # Check that columns were renamed
        assert 'TradDt' in result.columns
        assert 'TckrSymb' in result.columns
        assert 'FinInstrmId' in result.columns
        assert 'OpnPric' in result.columns
        assert 'ClsPric' in result.columns
        assert 'TtlTradgVol' in result.columns
        
        # Original friendly names should be gone
        assert 'trade_date' not in result.columns
        assert 'symbol' not in result.columns
        assert 'volume' not in result.columns

    def test_column_mapping_normalized_nse_names_unchanged(self, loader, sample_normalized_df_nse_names):
        """Test that NSE names are kept as-is (no unnecessary mapping)."""
        result = loader._prepare_dataframe_for_insert(
            sample_normalized_df_nse_names,
            'normalized_equity_ohlc'
        )
        
        # NSE names should still be present
        assert 'TradDt' in result.columns
        assert 'TckrSymb' in result.columns
        assert 'FinInstrmId' in result.columns

    def test_date_string_conversion(self, loader):
        """Test that date strings are converted to Date type."""
        df = pl.DataFrame([{
            'event_id': str(uuid.uuid4()),
            'event_time': int(datetime.now().timestamp() * 1000),
            'ingest_time': int(datetime.now().timestamp() * 1000),
            'source': 'test',
            'schema_version': 'v1',
            'entity_id': 'TEST:12345:NSE',
            'TradDt': '2024-01-02',  # String date
            'TckrSymb': 'TEST001',
            'FinInstrmId': 12345,
        }])
        
        result = loader._prepare_dataframe_for_insert(df, 'normalized_equity_ohlc')
        
        # Check that TradDt was converted to Date type
        assert result['TradDt'].dtype == pl.Date

    def test_timestamp_millisecond_conversion(self, loader):
        """Test that integer timestamps are converted to DateTime."""
        df = pl.DataFrame([{
            'event_id': str(uuid.uuid4()),
            'event_time': 1704153600000,  # Integer milliseconds
            'ingest_time': 1704153600000,
            'source': 'test',
            'schema_version': 'v1',
            'entity_id': 'TEST:12345:NSE',
            'TradDt': date(2024, 1, 2),
            'TckrSymb': 'TEST001',
            'FinInstrmId': 12345,
        }])
        
        result = loader._prepare_dataframe_for_insert(df, 'normalized_equity_ohlc')
        
        # Check that timestamps were converted to DateTime
        assert result['event_time'].dtype in [pl.Datetime, pl.Datetime('ms'), pl.Datetime('ns'), pl.Datetime('us')]
        assert result['ingest_time'].dtype in [pl.Datetime, pl.Datetime('ms'), pl.Datetime('ns'), pl.Datetime('us')]


class TestTableSupport:
    """Tests for supported tables configuration."""

    def test_supported_tables_list(self, loader):
        """Test that all expected tables are supported."""
        assert 'raw_equity_ohlc' in loader.SUPPORTED_TABLES
        assert 'normalized_equity_ohlc' in loader.SUPPORTED_TABLES
        assert 'features_equity_indicators' in loader.SUPPORTED_TABLES

    def test_column_mappings_exist_for_normalized(self, loader):
        """Test that column mappings are defined for normalized table."""
        assert 'normalized_equity_ohlc' in loader.COLUMN_MAPPINGS
        mappings = loader.COLUMN_MAPPINGS['normalized_equity_ohlc']
        
        # Check key mappings exist
        assert 'trade_date' in mappings
        assert 'symbol' in mappings
        assert 'volume' in mappings
        assert mappings['trade_date'] == 'TradDt'
        assert mappings['symbol'] == 'TckrSymb'
        assert mappings['volume'] == 'TtlTradgVol'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
