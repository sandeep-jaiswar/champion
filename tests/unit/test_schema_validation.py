"""Tests for schema validation in parsers."""

from datetime import date

import polars as pl
import pytest
from champion.parsers.polars_bhavcopy_parser import BHAVCOPY_SCHEMA, PolarsBhavcopyParser
from champion.parsers.polars_bse_parser import BSE_BHAVCOPY_SCHEMA, PolarsBseParser
from champion.parsers.symbol_master_parser import SYMBOL_MASTER_SCHEMA, SymbolMasterParser


class TestPolarsBhavcopyParserSchemaValidation:
    """Test schema validation for PolarsBhavcopyParser."""

    def test_schema_version_attribute_exists(self):
        """Test that parser has SCHEMA_VERSION attribute."""
        parser = PolarsBhavcopyParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"

    def test_validate_schema_with_correct_columns(self):
        """Test that validation passes with correct columns."""
        parser = PolarsBhavcopyParser()

        # Create a DataFrame with correct columns
        data = {col: [] for col in BHAVCOPY_SCHEMA.keys()}
        df = pl.DataFrame(data)

        # Should not raise
        parser._validate_schema(df, BHAVCOPY_SCHEMA)

    def test_validate_schema_with_missing_columns(self):
        """Test that validation fails with missing columns."""
        parser = PolarsBhavcopyParser()

        # Create a DataFrame missing some columns
        data = {"TckrSymb": [], "ClsPric": []}
        df = pl.DataFrame(data)

        with pytest.raises(ValueError) as exc_info:
            parser._validate_schema(df, BHAVCOPY_SCHEMA)

        assert "Schema mismatch" in str(exc_info.value)
        assert "v1.0" in str(exc_info.value)
        assert "missing columns=" in str(exc_info.value)

    def test_validate_schema_with_extra_columns(self):
        """Test that validation fails with extra columns."""
        parser = PolarsBhavcopyParser()

        # Create a DataFrame with all required columns plus extras
        data = {col: [] for col in BHAVCOPY_SCHEMA.keys()}
        data["EXTRA_COL1"] = []
        data["EXTRA_COL2"] = []
        df = pl.DataFrame(data)

        with pytest.raises(ValueError) as exc_info:
            parser._validate_schema(df, BHAVCOPY_SCHEMA)

        assert "Schema mismatch" in str(exc_info.value)
        assert "v1.0" in str(exc_info.value)
        assert "extra columns=" in str(exc_info.value)

    def test_validate_schema_with_both_missing_and_extra(self):
        """Test that validation fails with both missing and extra columns."""
        parser = PolarsBhavcopyParser()

        # Create a DataFrame with only some columns and extras
        data = {"TckrSymb": [], "ClsPric": [], "EXTRA_COL": []}
        df = pl.DataFrame(data)

        with pytest.raises(ValueError) as exc_info:
            parser._validate_schema(df, BHAVCOPY_SCHEMA)

        error_msg = str(exc_info.value)
        assert "Schema mismatch" in error_msg
        assert "v1.0" in error_msg
        assert "missing columns=" in error_msg
        assert "extra columns=" in error_msg

    def test_parse_with_schema_mismatch_raises_error(self, tmp_path):
        """Test that parse raises error when CSV has schema mismatch."""
        parser = PolarsBhavcopyParser()

        # Create a CSV file with wrong columns
        csv_file = tmp_path / "test_bhavcopy.csv"
        csv_file.write_text("WRONG_COL1,WRONG_COL2\nvalue1,value2\n")

        with pytest.raises(ValueError) as exc_info:
            parser.parse(csv_file, date(2024, 1, 15))

        assert "Schema mismatch" in str(exc_info.value)


class TestPolarsBseParserSchemaValidation:
    """Test schema validation for PolarsBseParser."""

    def test_schema_version_attribute_exists(self):
        """Test that parser has SCHEMA_VERSION attribute."""
        parser = PolarsBseParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"

    def test_validate_schema_with_correct_columns(self):
        """Test that validation passes with correct columns."""
        parser = PolarsBseParser()

        # Create a DataFrame with correct columns
        data = {col: [] for col in BSE_BHAVCOPY_SCHEMA.keys()}
        df = pl.DataFrame(data)

        # Should not raise
        parser._validate_schema(df, BSE_BHAVCOPY_SCHEMA)

    def test_validate_schema_with_missing_columns(self):
        """Test that validation fails with missing columns."""
        parser = PolarsBseParser()

        # Create a DataFrame missing some columns
        data = {"SC_CODE": [], "SC_NAME": []}
        df = pl.DataFrame(data)

        with pytest.raises(ValueError) as exc_info:
            parser._validate_schema(df, BSE_BHAVCOPY_SCHEMA)

        assert "Schema mismatch" in str(exc_info.value)
        assert "v1.0" in str(exc_info.value)
        assert "missing columns=" in str(exc_info.value)


class TestSymbolMasterParserSchemaValidation:
    """Test schema validation for SymbolMasterParser."""

    def test_schema_version_attribute_exists(self):
        """Test that parser has SCHEMA_VERSION attribute."""
        parser = SymbolMasterParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"

    def test_validate_schema_with_correct_columns(self):
        """Test that validation passes with correct columns."""
        parser = SymbolMasterParser()

        # Create a DataFrame with correct columns
        data = {col: [] for col in SYMBOL_MASTER_SCHEMA.keys()}
        df = pl.DataFrame(data)

        # Should not raise
        parser._validate_schema(df, SYMBOL_MASTER_SCHEMA)

    def test_validate_schema_with_missing_columns(self):
        """Test that validation fails with missing columns."""
        parser = SymbolMasterParser()

        # Create a DataFrame missing some columns
        data = {"SYMBOL": [], "ISIN NUMBER": []}
        df = pl.DataFrame(data)

        with pytest.raises(ValueError) as exc_info:
            parser._validate_schema(df, SYMBOL_MASTER_SCHEMA)

        assert "Schema mismatch" in str(exc_info.value)
        assert "v1.0" in str(exc_info.value)
        assert "missing columns=" in str(exc_info.value)


class TestAllParsersHaveSchemaVersion:
    """Test that all parsers have SCHEMA_VERSION attribute."""

    def test_bhavcopy_parser_has_schema_version(self):
        """Test BhavcopyParser has SCHEMA_VERSION."""
        from champion.parsers.bhavcopy_parser import BhavcopyParser

        parser = BhavcopyParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"

    def test_bulk_block_deals_parser_has_schema_version(self):
        """Test BulkBlockDealsParser has SCHEMA_VERSION."""
        from champion.parsers.bulk_block_deals_parser import BulkBlockDealsParser

        parser = BulkBlockDealsParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"

    def test_ca_parser_has_schema_version(self):
        """Test CorporateActionsParser has SCHEMA_VERSION."""
        from champion.parsers.ca_parser import CorporateActionsParser

        # CA parser is abstract, check class attribute directly
        assert hasattr(CorporateActionsParser, "SCHEMA_VERSION")
        assert CorporateActionsParser.SCHEMA_VERSION == "v1.0"

    def test_index_constituent_parser_has_schema_version(self):
        """Test IndexConstituentParser has SCHEMA_VERSION."""
        from champion.parsers.index_constituent_parser import IndexConstituentParser

        parser = IndexConstituentParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"

    def test_macro_indicator_parser_has_schema_version(self):
        """Test MacroIndicatorParser has SCHEMA_VERSION."""
        from champion.parsers.macro_indicator_parser import MacroIndicatorParser

        parser = MacroIndicatorParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"

    def test_quarterly_financials_parser_has_schema_version(self):
        """Test QuarterlyFinancialsParser has SCHEMA_VERSION."""
        from champion.parsers.quarterly_financials_parser import QuarterlyFinancialsParser

        parser = QuarterlyFinancialsParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"

    def test_shareholding_parser_has_schema_version(self):
        """Test ShareholdingPatternParser has SCHEMA_VERSION."""
        from champion.parsers.shareholding_parser import ShareholdingPatternParser

        parser = ShareholdingPatternParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"

    def test_symbol_enrichment_has_schema_version(self):
        """Test SymbolEnrichment has SCHEMA_VERSION."""
        from champion.parsers.symbol_enrichment import SymbolEnrichment

        enrichment = SymbolEnrichment()
        assert hasattr(enrichment, "SCHEMA_VERSION")
        assert enrichment.SCHEMA_VERSION == "v1.0"

    def test_trading_calendar_parser_has_schema_version(self):
        """Test TradingCalendarParser has SCHEMA_VERSION."""
        from champion.parsers.trading_calendar_parser import TradingCalendarParser

        parser = TradingCalendarParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v1.0"
