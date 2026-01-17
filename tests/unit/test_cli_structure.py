"""Minimal tests for CLI structure validation."""

import ast
import re
from pathlib import Path


def test_cli_syntax():
    """Test that CLI file has valid Python syntax."""
    cli_path = Path(__file__).parent.parent.parent / "src" / "champion" / "cli.py"
    with open(cli_path) as f:
        code = f.read()
        # This will raise SyntaxError if invalid
        ast.parse(code)


def test_cli_has_command_groups():
    """Test that CLI defines expected command groups."""
    cli_path = Path(__file__).parent.parent.parent / "src" / "champion" / "cli.py"
    with open(cli_path) as f:
        code = f.read()

    # Check for command group definitions
    assert "etl_app = typer.Typer" in code
    assert "warehouse_app = typer.Typer" in code
    assert "validate_app = typer.Typer" in code
    assert "orchestrate_app = typer.Typer" in code
    assert "admin_app = typer.Typer" in code

    # Check groups are registered
    assert 'app.add_typer(etl_app, name="etl")' in code
    assert 'app.add_typer(warehouse_app, name="warehouse")' in code
    assert 'app.add_typer(validate_app, name="validate")' in code
    assert 'app.add_typer(orchestrate_app, name="orchestrate")' in code
    assert 'app.add_typer(admin_app, name="admin")' in code


def test_cli_has_expected_commands():
    """Test that CLI has all expected commands."""
    cli_path = Path(__file__).parent.parent.parent / "src" / "champion" / "cli.py"
    with open(cli_path) as f:
        code = f.read()

    # Extract commands
    commands = re.findall(r'@(\w+_app)\.command\(["\']([^"\']+)["\']', code)
    
    # Group commands
    groups = {}
    for app, cmd in commands:
        group = app.replace('_app', '')
        if group not in groups:
            groups[group] = []
        groups[group].append(cmd)

    # Verify expected commands exist
    expected_groups = {
        'etl': ['index', 'macro', 'ohlc', 'scrape', 'bulk-deals', 'corporate-actions',
                'combined-equity', 'quarterly-financials', 'trading-calendar'],
        'warehouse': ['load-equity-list'],
        'validate': ['file'],
        'orchestrate': ['backfill'],
        'admin': ['config', 'health'],
    }

    for group, expected_cmds in expected_groups.items():
        assert group in groups, f"Command group '{group}' not found"
        for cmd in expected_cmds:
            assert cmd in groups[group], f"Command '{cmd}' not found in group '{group}'"


def test_cli_has_verbose_flags():
    """Test that commands have verbose flags."""
    cli_path = Path(__file__).parent.parent.parent / "src" / "champion" / "cli.py"
    with open(cli_path) as f:
        code = f.read()

    # Check for verbose flag pattern
    verbose_pattern = r'verbose.*=.*typer\.Option.*--verbose.*-v'
    matches = re.findall(verbose_pattern, code, re.IGNORECASE)

    # Should have multiple verbose flags
    assert len(matches) > 5, "Expected multiple commands with verbose flags"


def test_cli_has_help_text():
    """Test that commands have help text with examples."""
    cli_path = Path(__file__).parent.parent.parent / "src" / "champion" / "cli.py"
    with open(cli_path) as f:
        code = f.read()

    # Check for rich formatting in docstrings
    assert '[bold]Example' in code or '[bold]Examples' in code

    # Check that commands have docstrings
    command_functions = re.findall(r'def (etl_\w+|validate_\w+|orchestrate_\w+|show_\w+|health_\w+)\(', code)
    assert len(command_functions) > 5, "Expected multiple command functions"


def test_cli_no_duplicate_commands():
    """Test that there are no duplicate command names within groups."""
    cli_path = Path(__file__).parent.parent.parent / "src" / "champion" / "cli.py"
    with open(cli_path) as f:
        code = f.read()

    commands = re.findall(r'@(\w+_app)\.command\(["\']([^"\']+)["\']', code)

    # Group commands
    groups = {}
    for app, cmd in commands:
        group = app.replace('_app', '')
        if group not in groups:
            groups[group] = []
        groups[group].append(cmd)

    # Check for duplicates within each group
    for group, cmds in groups.items():
        assert len(cmds) == len(set(cmds)), f"Duplicate commands found in group '{group}'"


def test_cli_has_auto_completion():
    """Test that CLI has auto-completion enabled."""
    cli_path = Path(__file__).parent.parent.parent / "src" / "champion" / "cli.py"
    with open(cli_path) as f:
        code = f.read()

    assert 'add_completion=True' in code, "Auto-completion should be enabled"
