# Champion CLI Reference

The Champion CLI provides a unified interface for all platform operations, organized into logical command groups.

## Installation

Once you have the project installed via Poetry:

```bash
# The CLI is available as the 'champion' command
champion --help
```

## Auto-completion

Enable shell auto-completion for a better experience:

```bash
# For Bash
champion --install-completion bash

# For Zsh
champion --install-completion zsh

# For Fish
champion --install-completion fish
```

## Command Groups

The CLI is organized into five main command groups:

### 1. ETL Commands (`champion etl`)

Data ingestion and extraction commands.

#### `champion etl index`

Run Index Constituent ETL flow.

```bash
# Process NIFTY50 index for today
champion etl index --index NIFTY50

# Process for a specific date
champion etl index --index NIFTY50 --date 2024-01-15
```

#### `champion etl macro`

Run macro indicators ETL flow.

```bash
# Fetch last 90 days of macro data
champion etl macro --days 90

# Fetch data for a specific date range
champion etl macro --start 2024-01-01 --end 2024-01-31
```

#### `champion etl ohlc`

Run NSE OHLC (bhavcopy) ETL flow.

```bash
# Fetch for a specific date
champion etl ohlc --date 2024-01-15

# Fetch for a date range
champion etl ohlc --start 2024-01-01 --end 2024-01-31

# Skip loading to ClickHouse
champion etl ohlc --date 2024-01-15 --no-load
```

#### `champion etl scrape`

Scrape NSE data for a specific date.

```bash
# Scrape bhavcopy for yesterday (default)
champion etl scrape --scraper bhavcopy

# Scrape for a specific date
champion etl scrape --scraper bhavcopy --date 2024-01-15

# Scrape symbol master
champion etl scrape --scraper symbol-master

# Dry run (parse without producing to Kafka)
champion etl scrape --scraper bhavcopy --dry-run
```

#### Other ETL Commands

```bash
# Bulk/block deals
champion etl bulk-deals --start 2024-01-01 --end 2024-01-31

# Corporate actions
champion etl corporate-actions

# Combined equity (NSE + BSE)
champion etl combined-equity --date 2024-01-15

# Quarterly financials
champion etl quarterly-financials --start 2024-01-01 --end 2024-03-31
champion etl quarterly-financials --symbol TCS --load

# Trading calendar
champion etl trading-calendar
```

### 2. Warehouse Commands (`champion warehouse`)

Data warehouse loading and management commands.

#### `champion warehouse load-equity-list`

Download NSE equity list and load into ClickHouse.

```bash
# Download and load to ClickHouse
champion warehouse load-equity-list

# Download only (skip ClickHouse load)
champion warehouse load-equity-list --no-load
```

### 3. Validate Commands (`champion validate`)

Data validation and quality checks.

#### `champion validate file`

Validate a downloaded NSE file.

```bash
# Validate a bhavcopy file
champion validate file --file data/bhavcopy.csv --type bhavcopy

# Validate symbol master
champion validate file --file data/symbols.csv --type symbol-master
```

### 4. Orchestrate Commands (`champion orchestrate`)

Workflow orchestration and scheduling commands.

#### `champion orchestrate backfill`

Backfill NSE data for a date range.

```bash
# Backfill bhavcopy data
champion orchestrate backfill --start 2024-01-01 --end 2024-01-31

# Dry run
champion orchestrate backfill --start 2024-01-01 --end 2024-01-31 --dry-run
```

### 5. Admin Commands (`champion admin`)

Administration and configuration commands.

#### `champion admin config`

Display current configuration values.

```bash
champion admin config
```

#### `champion admin health`

Check system health and dependencies.

```bash
champion admin health
```

## Global Options

Most commands support these global options:

- `--verbose`, `-v`: Enable verbose output for debugging
- `--help`: Show help for any command
- `--dry-run`: (where applicable) Run without making changes

## Examples

### Daily Data Pipeline

```bash
# 1. Check system health
champion admin health

# 2. Fetch trading calendar
champion etl trading-calendar

# 3. Fetch OHLC data
champion etl ohlc --date 2024-01-15

# 4. Load equity list
champion warehouse load-equity-list
```

### Historical Backfill

```bash
# Backfill a month of data
champion orchestrate backfill --start 2024-01-01 --end 2024-01-31
```

### Development Workflow

```bash
# Use verbose mode for debugging
champion etl ohlc --date 2024-01-15 --verbose

# Use dry-run to test without side effects
champion etl scrape --scraper bhavcopy --date 2024-01-15 --dry-run
```

## Migration from Old CLI

If you were using the old CLI structure:

| Old Command | New Command |
|------------|-------------|
| `champion etl-index` | `champion etl index` |
| `champion etl-macro` | `champion etl macro` |
| `champion etl-ohlc` | `champion etl ohlc` |
| `champion show-config` | `champion admin config` |
| `champion equity-list` | `champion warehouse load-equity-list` |

## Troubleshooting

### Command Not Found

If you get "command not found" errors:

```bash
# Ensure the package is installed
poetry install

# Activate the virtual environment
poetry shell

# Or run via poetry
poetry run champion --help
```

### Import Errors

If you see import errors, ensure all dependencies are installed:

```bash
poetry install
```

### Configuration Issues

Check your configuration:

```bash
champion admin config
champion admin health
```

## Development

### Adding New Commands

Commands are defined in `src/champion/cli.py` using the Typer framework:

```python
@etl_app.command("my-command")
def my_command(
    param: str = typer.Option(..., help="Parameter description"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Command description.
    
    [bold]Example:[/bold]
        champion etl my-command --param value
    """
    # Command implementation
```

### Testing Commands

Run the test suite:

```bash
# Run all CLI tests
pytest tests/unit/test_cli.py tests/unit/test_cli_commands.py -v

# Run specific test
pytest tests/unit/test_cli_commands.py::TestCLIStructure::test_cli_help -v
```
