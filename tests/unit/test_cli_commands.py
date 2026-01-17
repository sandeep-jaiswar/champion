"""Tests for unified CLI command structure."""

from typer.testing import CliRunner

from champion.cli import app

runner = CliRunner()


class TestCLIStructure:
    """Test the CLI command group structure."""

    def test_cli_help(self):
        """Test that main CLI help shows command groups."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "etl" in result.stdout
        assert "warehouse" in result.stdout
        assert "validate" in result.stdout
        assert "orchestrate" in result.stdout
        assert "admin" in result.stdout

    def test_etl_group_help(self):
        """Test that ETL group shows available commands."""
        result = runner.invoke(app, ["etl", "--help"])
        assert result.exit_code == 0
        assert "index" in result.stdout
        assert "macro" in result.stdout
        assert "ohlc" in result.stdout
        assert "scrape" in result.stdout

    def test_warehouse_group_help(self):
        """Test that warehouse group shows available commands."""
        result = runner.invoke(app, ["warehouse", "--help"])
        assert result.exit_code == 0
        assert "load-equity-list" in result.stdout

    def test_validate_group_help(self):
        """Test that validate group shows available commands."""
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0
        assert "file" in result.stdout

    def test_orchestrate_group_help(self):
        """Test that orchestrate group shows available commands."""
        result = runner.invoke(app, ["orchestrate", "--help"])
        assert result.exit_code == 0
        assert "backfill" in result.stdout

    def test_admin_group_help(self):
        """Test that admin group shows available commands."""
        result = runner.invoke(app, ["admin", "--help"])
        assert result.exit_code == 0
        assert "config" in result.stdout
        assert "health" in result.stdout


class TestETLCommands:
    """Test ETL command invocation (without execution)."""

    def test_etl_index_help(self):
        """Test ETL index command help."""
        result = runner.invoke(app, ["etl", "index", "--help"])
        assert result.exit_code == 0
        assert "Index Constituent ETL" in result.stdout
        assert "--index" in result.stdout

    def test_etl_macro_help(self):
        """Test ETL macro command help."""
        result = runner.invoke(app, ["etl", "macro", "--help"])
        assert result.exit_code == 0
        assert "macro indicators" in result.stdout

    def test_etl_ohlc_help(self):
        """Test ETL OHLC command help."""
        result = runner.invoke(app, ["etl", "ohlc", "--help"])
        assert result.exit_code == 0
        assert "bhavcopy" in result.stdout

    def test_etl_scrape_help(self):
        """Test ETL scrape command help."""
        result = runner.invoke(app, ["etl", "scrape", "--help"])
        assert result.exit_code == 0
        assert "Scrape NSE data" in result.stdout
        assert "--scraper" in result.stdout


class TestAdminCommands:
    """Test admin command invocation."""

    def test_admin_config_help(self):
        """Test admin config command help."""
        result = runner.invoke(app, ["admin", "config", "--help"])
        assert result.exit_code == 0
        assert "configuration" in result.stdout

    def test_admin_health_help(self):
        """Test admin health command help."""
        result = runner.invoke(app, ["admin", "health", "--help"])
        assert result.exit_code == 0
        assert "health" in result.stdout


class TestOrchestrateCommands:
    """Test orchestrate command invocation."""

    def test_orchestrate_backfill_help(self):
        """Test orchestrate backfill command help."""
        result = runner.invoke(app, ["orchestrate", "backfill", "--help"])
        assert result.exit_code == 0
        assert "Backfill" in result.stdout
        assert "--start" in result.stdout
        assert "--end" in result.stdout
