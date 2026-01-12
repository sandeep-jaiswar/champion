"""Tests for base Parser class."""

from pathlib import Path
from datetime import datetime

import polars as pl
import pytest

from champion.parsers.base_parser import Parser


class ConcreteParser(Parser):
    """Concrete implementation of Parser for testing."""

    SCHEMA_VERSION = "v2.0"

    def parse(self, file_path: Path, *args, **kwargs) -> pl.DataFrame:
        """Simple parse implementation that returns a DataFrame."""
        # Return a simple DataFrame for testing
        return pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})


class TestBaseParser:
    """Test suite for base Parser class."""

    def test_parser_cannot_be_instantiated(self):
        """Test that Parser is abstract and cannot be instantiated directly."""
        with pytest.raises(TypeError):
            Parser()

    def test_concrete_parser_can_be_instantiated(self):
        """Test that a concrete implementation can be instantiated."""
        parser = ConcreteParser()
        assert isinstance(parser, Parser)
        assert parser.SCHEMA_VERSION == "v2.0"

    def test_concrete_parser_has_schema_version(self):
        """Test that concrete parser has SCHEMA_VERSION attribute."""
        parser = ConcreteParser()
        assert hasattr(parser, "SCHEMA_VERSION")
        assert parser.SCHEMA_VERSION == "v2.0"

    def test_concrete_parser_parse_method(self, tmp_path):
        """Test that parse method can be called on concrete implementation."""
        parser = ConcreteParser()
        test_file = tmp_path / "test.csv"
        test_file.write_text("dummy data")

        result = parser.parse(test_file)
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3
        assert result.columns == ["col1", "col2"]

    def test_validate_schema_not_implemented_by_default(self):
        """Test that validate_schema raises NotImplementedError by default."""
        parser = ConcreteParser()
        df = pl.DataFrame({"col1": [1, 2, 3]})

        with pytest.raises(NotImplementedError) as exc_info:
            parser.validate_schema(df)

        assert "ConcreteParser" in str(exc_info.value)
        assert "does not implement schema validation" in str(exc_info.value)

    def test_add_metadata_adds_standard_columns(self):
        """Test that add_metadata adds _schema_version and _parsed_at columns."""
        parser = ConcreteParser()
        df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        result = parser.add_metadata(df)

        # Check that new columns were added
        assert "_schema_version" in result.columns
        assert "_parsed_at" in result.columns

        # Check that original columns are preserved
        assert "col1" in result.columns
        assert "col2" in result.columns

        # Check that schema version is correct
        assert result["_schema_version"][0] == "v2.0"

        # Check that parsed_at is a datetime
        assert result.schema["_parsed_at"] == pl.Datetime

    def test_add_metadata_preserves_original_data(self):
        """Test that add_metadata doesn't modify original data."""
        parser = ConcreteParser()
        df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        result = parser.add_metadata(df)

        # Check that original data is preserved
        assert result["col1"].to_list() == [1, 2, 3]
        assert result["col2"].to_list() == ["a", "b", "c"]

        # Check that row count is preserved
        assert len(result) == len(df)

    def test_schema_version_default(self):
        """Test that Parser has a default SCHEMA_VERSION."""
        assert hasattr(Parser, "SCHEMA_VERSION")
        assert Parser.SCHEMA_VERSION == "v1.0"

    def test_parser_inheritance(self):
        """Test that concrete parser properly inherits from Parser."""
        parser = ConcreteParser()
        assert isinstance(parser, Parser)
        assert isinstance(parser, ConcreteParser)


class CustomParser(Parser):
    """Custom parser with overridden validate_schema."""

    def parse(self, file_path: Path, *args, **kwargs) -> pl.DataFrame:
        """Simple parse implementation."""
        return pl.DataFrame({"value": [1, 2, 3]})

    def validate_schema(self, df: pl.DataFrame) -> None:
        """Custom validation implementation."""
        if "value" not in df.columns:
            raise ValueError("Missing 'value' column")


class TestCustomValidation:
    """Test custom validation implementation."""

    def test_custom_validate_schema_can_be_implemented(self):
        """Test that validate_schema can be implemented by subclasses."""
        parser = CustomParser()
        df = pl.DataFrame({"value": [1, 2, 3]})

        # Should not raise
        parser.validate_schema(df)

    def test_custom_validate_schema_raises_on_invalid(self):
        """Test that custom validation can detect invalid data."""
        parser = CustomParser()
        df = pl.DataFrame({"other": [1, 2, 3]})

        with pytest.raises(ValueError) as exc_info:
            parser.validate_schema(df)

        assert "Missing 'value' column" in str(exc_info.value)
