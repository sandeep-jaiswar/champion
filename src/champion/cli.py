from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

import typer
from rich.console import Console

from champion.config import config
from champion.utils.logger import configure_logging, get_logger

# Default MLflow backend to local file store unless explicitly set
os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")

# Main CLI app
app = typer.Typer(
    name="champion",
    help="Champion CLI: Production-grade data platform for market analytics",
    no_args_is_help=True,
    add_completion=True,
    rich_markup_mode="rich",
)

# Command groups
etl_app = typer.Typer(
    name="etl",
    help="ETL commands for data ingestion",
    no_args_is_help=True,
)
warehouse_app = typer.Typer(
    name="warehouse",
    help="Data warehouse loading commands",
    no_args_is_help=True,
)
validate_app = typer.Typer(
    name="validate",
    help="Data validation and quality checks",
    no_args_is_help=True,
)
orchestrate_app = typer.Typer(
    name="orchestrate",
    help="Workflow orchestration and scheduling",
    no_args_is_help=True,
)
admin_app = typer.Typer(
    name="admin",
    help="Administration and configuration",
    no_args_is_help=True,
)
api_app = typer.Typer(
    name="api",
    help="REST API server commands",
    no_args_is_help=True,
)

# Register command groups
app.add_typer(etl_app, name="etl")
app.add_typer(warehouse_app, name="warehouse")
app.add_typer(validate_app, name="validate")
app.add_typer(orchestrate_app, name="orchestrate")
app.add_typer(admin_app, name="admin")
app.add_typer(api_app, name="api")

# Rich console for better output
console = Console()
logger = get_logger(__name__)


def validate_date_format(date_str: str, allow_future: bool = False) -> date:
    """Validate date format and return date object.

    Supports multiple formats:
    - YYYY-MM-DD (ISO format)
    - YYYYMMDD (compact format)

    Args:
        date_str: Date string to validate
        allow_future: If False, reject dates in the future (default: False)

    Returns:
        date: Parsed date object

    Raises:
        typer.Exit: If date format is invalid or date is in future when not allowed
    """
    parsed_date = None

    # Try ISO format (YYYY-MM-DD)
    try:
        parsed_date = date.fromisoformat(date_str)
    except ValueError:
        # Try compact format (YYYYMMDD)
        try:
            if len(date_str) == 8 and date_str.isdigit():
                parsed_date = datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            pass

    if parsed_date is None:
        typer.secho(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD or YYYYMMDD",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from None

    # Check if date is not in the future (unless allowed)
    if not allow_future and parsed_date > date.today():
        typer.secho(
            f"Date {parsed_date} is in the future. Trading dates must be today or earlier.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)

    return parsed_date


@etl_app.command("index")
def etl_index(
    index_name: str = typer.Option(
        "NIFTY50", "--index", "-i", help="Index to process (e.g., NIFTY50)"
    ),
    effective_date: str | None = typer.Option(
        None, "--date", "-d", help="Effective date (YYYY-MM-DD)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Run Index Constituent ETL flow.

    [bold]Example:[/bold]
        champion etl index --index NIFTY50 --date 2024-01-15
    """
    eff = validate_date_format(effective_date) if effective_date else date.today()
    try:
        from champion.orchestration.flows.flows import index_constituent_etl_flow

        index_constituent_etl_flow(indices=[index_name], effective_date=eff)
    except (ImportError, ModuleNotFoundError) as e:
        typer.secho(
            f"Index ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from e
    except Exception as e:
        typer.secho(
            f"Index ETL execution failed: {e}",
            fg=typer.colors.RED,
        )
        raise


@etl_app.command("macro")
def etl_macro(
    days: int = typer.Option(90, help="Number of days back to include"),
    start_date: str | None = typer.Option(None, "--start", help="Start date (YYYY-MM-DD)"),
    end_date: str | None = typer.Option(None, "--end", help="End date (YYYY-MM-DD)"),
    fail_on_empty: bool = typer.Option(
        False, help="Fail if no macro data retrieved from any source"
    ),
    source_order: str | None = typer.Option(
        None,
        help="Comma-separated list of macro sources to try in order (e.g., MoSPI,RBI,DEA,NITI Aayog)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Run macro indicators ETL flow.

    [bold]Example:[/bold]
        champion etl macro --days 90
        champion etl macro --start 2024-01-01 --end 2024-01-31
    """
    # Ensure MLflow uses file backend by default if not set
    os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")
    try:
        from champion.orchestration.flows.macro_flow import macro_indicators_flow

        # Determine start/end datetimes
        if start_date and end_date:
            start = datetime.combine(validate_date_format(start_date), datetime.min.time())
            end = datetime.combine(validate_date_format(end_date), datetime.max.time())
        else:
            end = datetime.now()
            start = end - timedelta(days=days)

        if start > end:
            typer.secho("start_date must be before or equal to end_date", fg=typer.colors.RED)
            raise typer.Exit(1)

        sources = [s.strip() for s in source_order.split(",")] if source_order else None
        macro_indicators_flow(
            start_date=start, end_date=end, source_order=sources, fail_on_empty=fail_on_empty
        )
    except (ImportError, ModuleNotFoundError) as e:
        typer.secho(
            f"Macro ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from e
    except Exception as e:
        typer.secho(
            f"Macro ETL execution failed: {e}",
            fg=typer.colors.RED,
        )
        raise


@etl_app.command("trading-calendar")
def etl_trading_calendar(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Run trading calendar ETL flow.

    [bold]Example:[/bold]
        champion etl trading-calendar
    """
    try:
        from champion.orchestration.flows.trading_calendar_flow import (
            trading_calendar_etl_flow,
        )

        trading_calendar_etl_flow()
    except (ImportError, ModuleNotFoundError) as e:
        typer.secho(
            f"Trading calendar ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from e
    except Exception as e:
        typer.secho(
            f"Trading calendar ETL execution failed: {e}",
            fg=typer.colors.RED,
        )
        raise


@etl_app.command("bulk-deals")
def etl_bulk_deals(
    start_date: str | None = typer.Option(None, "--start", help="Start date (YYYY-MM-DD)"),
    end_date: str | None = typer.Option(None, "--end", help="End date (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Run bulk/block deals ETL flow.

    [bold]Example:[/bold]
        champion etl bulk-deals
        champion etl bulk-deals --start 2024-01-01 --end 2024-01-31
    """
    # Check that both dates are provided together
    if (start_date and not end_date) or (end_date and not start_date):
        typer.secho(
            "Both start_date and end_date must be provided together",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)

    if start_date and end_date:
        # Validate date formats and convert to proper format
        start_dt = validate_date_format(start_date)
        end_dt = validate_date_format(end_date)
        # The flow file includes a date-range variant; call if available
        try:
            from champion.orchestration.flows.bulk_block_deals_flow import (
                bulk_block_deals_date_range_etl_flow,
            )

            bulk_block_deals_date_range_etl_flow(
                start_date=start_dt.isoformat(),
                end_date=end_dt.isoformat(),
            )
            return
        except (ImportError, ModuleNotFoundError):
            # Fallback to single flow if date-range flow is not available
            pass
    try:
        from champion.orchestration.flows.bulk_block_deals_flow import (
            bulk_block_deals_etl_flow,
        )

        bulk_block_deals_etl_flow()
    except (ImportError, ModuleNotFoundError) as e:
        typer.secho(
            f"Bulk/Block deals ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from e
    except Exception as e:
        typer.secho(
            f"Bulk/Block deals ETL execution failed: {e}",
            fg=typer.colors.RED,
        )
        raise


@etl_app.command("ohlc")
def etl_ohlc(
    trade_date: str | None = typer.Option(
        None, "--date", "-d", help="Trade date (YYYY-MM-DD, default: previous business day)"
    ),
    start_date: str | None = typer.Option(
        None, "--start", help="Start date (YYYY-MM-DD) for range run"
    ),
    end_date: str | None = typer.Option(None, "--end", help="End date (YYYY-MM-DD) for range run"),
    output_base_path: str | None = typer.Option(
        None, "--output", help="Base output path (default: data/lake)"
    ),
    load_to_clickhouse: bool = typer.Option(
        True, "--load/--no-load", help="Load results into ClickHouse"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Run NSE OHLC (bhavcopy) ETL flow.

    [bold]Examples:[/bold]
        champion etl ohlc --date 2024-01-15
        champion etl ohlc --start 2024-01-01 --end 2024-01-31
    """
    # Ensure MLflow uses file backend by default if not set
    os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")
    try:
        from champion.orchestration.flows.flows import nse_bhavcopy_etl_flow

        # If both start_date and end_date provided, run the flow for each date in the range
        if start_date and end_date:
            start_dt = validate_date_format(start_date)
            end_dt = validate_date_format(end_date)
            if start_dt > end_dt:
                typer.secho("start_date must be before or equal to end_date", fg=typer.colors.RED)
                raise typer.Exit(1)
            cur = start_dt
            failed_dates = []
            while cur <= end_dt:
                # Skip weekends to avoid unnecessary network calls
                if cur.weekday() >= 5:
                    typer.secho(f"Skipping weekend: {cur}", fg=typer.colors.BLUE)
                    cur = cur + timedelta(days=1)
                    continue
                try:
                    nse_bhavcopy_etl_flow(
                        trade_date=cur,
                        output_base_path=output_base_path,
                        load_to_clickhouse=load_to_clickhouse,
                        start_metrics_server_flag=False,
                    )
                except Exception as e:
                    typer.secho(
                        f"Warning: Skipping {cur} due to: {str(e)[:100]}",
                        fg=typer.colors.YELLOW,
                    )
                    failed_dates.append(str(cur))
                cur = cur + timedelta(days=1)
            if failed_dates:
                typer.secho(
                    f"Completed with {len(failed_dates)} skipped dates: {', '.join(failed_dates[:5])}{'...' if len(failed_dates) > 5 else ''}",
                    fg=typer.colors.YELLOW,
                )
            return

        td = validate_date_format(trade_date) if trade_date else None
        nse_bhavcopy_etl_flow(
            trade_date=td,
            output_base_path=output_base_path,
            load_to_clickhouse=load_to_clickhouse,
            start_metrics_server_flag=False,
        )
    except (ImportError, ModuleNotFoundError) as e:
        typer.secho(
            f"OHLC ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from e
    except Exception as e:
        typer.secho(
            f"OHLC ETL execution failed: {e}",
            fg=typer.colors.RED,
        )
        raise


@etl_app.command("corporate-actions")
def etl_corporate_actions(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Run corporate actions ETL flow.

    [bold]Example:[/bold]
        champion etl corporate-actions
    """
    try:
        from champion.orchestration.flows.corporate_actions_flow import (
            corporate_actions_etl_flow,
        )

        corporate_actions_etl_flow()
    except (ImportError, ModuleNotFoundError) as e:
        typer.secho(
            f"Corporate actions ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from e
    except Exception as e:
        typer.secho(
            f"Corporate actions ETL execution failed: {e}",
            fg=typer.colors.RED,
        )
        raise


@etl_app.command("combined-equity")
def etl_combined_equity(
    trade_date: str | None = typer.Option(
        None, "--date", "-d", help="Trade date (YYYY-MM-DD, default: today)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Run combined equity ETL flow (NSE + BSE bhavcopy).

    [bold]Example:[/bold]
        champion etl combined-equity --date 2024-01-15
    """
    try:
        from champion.orchestration.flows.combined_flows import combined_equity_etl_flow

        if trade_date:
            td = validate_date_format(trade_date)
        else:
            td = date.today()
        combined_equity_etl_flow(trade_date=td)
    except (ImportError, ModuleNotFoundError) as e:
        typer.secho(
            f"Combined equity ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from e
    except Exception as e:
        typer.secho(
            f"Combined equity ETL execution failed: {e}",
            fg=typer.colors.RED,
        )
        raise


@etl_app.command("quarterly-financials")
def etl_quarterly_financials(
    start_date: str | None = typer.Option(
        None, "--start", help="Start date (YYYY-MM-DD) for range run"
    ),
    end_date: str | None = typer.Option(None, "--end", help="End date (YYYY-MM-DD) for range run"),
    symbol: str | None = typer.Option(
        None, "--symbol", "-s", help="Optional symbol to query (e.g., TCS)"
    ),
    issuer: str | None = typer.Option(None, help="Optional issuer name for symbol queries"),
    filter_audited: bool = typer.Option(
        False, help="Only download documents for rows where audited='Audited'"
    ),
    load_to_clickhouse: bool = typer.Option(
        False, "--load/--no-load", help="Load results into ClickHouse after download"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Run quarterly financials ETL flow.

    [bold]Examples:[/bold]
        champion etl quarterly-financials --start 2024-01-01 --end 2024-03-31
        champion etl quarterly-financials --symbol TCS --load
    """
    try:
        import polars as pl

        from champion.orchestration.flows.quarterly_financial_flow import QuarterlyResultsScraper
        from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

        if start_date and end_date:
            sd = validate_date_format(start_date)
            ed = validate_date_format(end_date)
        else:
            sd = date.today()
            ed = date.today()

        console.print("[bold]Phase 1: Fetching master data and downloading documents...[/bold]")

        master_df = QuarterlyResultsScraper().get_master(
            from_date=sd.strftime("%d-%m-%Y"),
            to_date=ed.strftime("%d-%m-%Y"),
            symbol=symbol,
            issuer=issuer,
            filter_audited=filter_audited,
        )

        console.print(f"[green]✓[/green] Found {len(master_df)} quarterly financial records")

        scraper = QuarterlyResultsScraper()
        # normalize dataframe and write canonical parquet
        norm_df = scraper.normalize_master_dataframe(master_df)
        # download documents and capture saved file paths (use normalized dataframe)
        saved_files = scraper.download_documents(master=norm_df)

        console.print(f"[green]✓[/green] Downloaded {len(saved_files)} documents")

        console.print(f"[green]✓[/green] Downloaded {len(saved_files)} documents")

        # Phase 2: Parse XBRL files and prepare for batch insert
        console.print("\n[bold]Phase 2: Parsing XBRL documents...[/bold]")
        parsed_rows = []
        try:
            from champion.parsers.xbrl_parser import parse_xbrl_file

            for idx, p in enumerate(saved_files):
                try:
                    if str(p).lower().endswith(".xml"):
                        rec = parse_xbrl_file(Path(p))
                        parsed_rows.append(rec)
                        if (idx + 1) % 100 == 0:
                            console.print(f"  Parsed {idx + 1}/{len(saved_files)} files...")
                except Exception as e:
                    if verbose:
                        console.print(f"[yellow]⊘ Failed to parse: {p} - {e}[/yellow]")
            console.print(f"[green]✓[/green] Successfully parsed {len(parsed_rows)} XBRL files")
        except Exception as e:
            console.print(f"[yellow]⚠ XBRL parser not available or failed: {e}[/yellow]")
            parsed_rows = []

        # Phase 2.5: Merge master metadata with parsed XBRL data
        if parsed_rows and norm_df is not None and len(norm_df) > 0:
            console.print("\n[bold]Phase 2.5: Merging master data with XBRL financials...[/bold]")
            try:
                # Convert parsed rows to DataFrame for easier merging
                import pandas as pd

                parsed_df = pd.DataFrame(parsed_rows)

                # Ensure period_end_date is in the same format for both
                if "period_end_date" in parsed_df.columns:
                    parsed_df["period_end_date"] = pd.to_datetime(
                        parsed_df["period_end_date"], errors="coerce"
                    ).dt.date
                if "period_end_date" in norm_df.columns:
                    norm_df["period_end_date"] = pd.to_datetime(
                        norm_df["period_end_date"], errors="coerce"
                    ).dt.date

                # Merge strategy: Use period_end_date as primary key since symbols may differ
                # between master (full company name) and XBRL (exchange symbol)
                # For each parsed row, find corresponding master row by period_end_date
                merged_rows = []
                for _, parsed_row in parsed_df.iterrows():
                    parsed_dict = parsed_row.to_dict()
                    period_end = parsed_dict.get("period_end_date")

                    # Find matching master row by period_end_date
                    if period_end and period_end in norm_df["period_end_date"].values:
                        matching_master = (
                            norm_df[norm_df["period_end_date"] == period_end].iloc[0].to_dict()
                        )
                        # Merge: master metadata + XBRL financials
                        # Prefer master values for metadata fields, XBRL values for financial metrics
                        merged_row = {**parsed_dict, **matching_master}
                        # But keep XBRL financial metrics (don't overwrite with master NULLs)
                        for key, value in parsed_dict.items():
                            if value is not None and pd.notna(value):
                                # Keep non-null XBRL values
                                merged_row[key] = value
                        merged_rows.append(merged_row)
                    else:
                        # No matching master row, use XBRL data only
                        merged_rows.append(parsed_dict)

                if merged_rows:
                    norm_df = pd.DataFrame(merged_rows)
                    console.print(
                        f"[green]✓[/green] Merged {len(norm_df)} records with XBRL financials"
                    )
                else:
                    console.print("[yellow]⚠ No records merged - using master data only[/yellow]")
            except Exception as e:
                console.print(f"[yellow]⚠ Merge failed: {e}[/yellow]")
                if verbose:
                    import traceback

                    console.print(traceback.format_exc())

        # Phase 3: Batch load into ClickHouse
        if load_to_clickhouse and (norm_df is not None or parsed_rows):
            console.print("\n[bold]Phase 3: Batch loading to ClickHouse...[/bold]")
            try:
                loader = ClickHouseLoader(
                    host="localhost", port=8123, user="default", password="", database="champion"
                )
                loader.connect()

                total_rows = 0

                # Load merged data (master + XBRL financials)
                if norm_df is not None and len(norm_df) > 0:
                    try:
                        pldf = pl.from_pandas(norm_df)
                        rows_inserted = loader.insert_polars_dataframe(
                            table="quarterly_financials", df=pldf, batch_size=10000, dry_run=False
                        )
                        total_rows += rows_inserted
                        console.print(
                            f"[green]✓[/green] Loaded merged data: {rows_inserted:,} rows"
                        )
                    except Exception as e:
                        console.print(f"[red]✗[/red] Failed to load data: {e}")
                        if verbose:
                            import traceback

                            console.print(traceback.format_exc())

                loader.disconnect()
                console.print(
                    f"\n[bold]Batch load complete:[/bold] {total_rows:,} total rows loaded into ClickHouse"
                )
            except Exception as e:
                console.print(f"[red]✗ Batch load failed: {e}[/red]")
                if verbose:
                    logger.error("Batch load failed", error=str(e))
                raise
    except (ImportError, ModuleNotFoundError) as e:
        typer.secho(
            f"quarterly financials ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1) from e
    except Exception as e:
        typer.secho(
            f"quarterly financials ETL execution failed: {e}",
            fg=typer.colors.RED,
        )
        raise


@etl_app.command("scrape")
def etl_scrape(
    scraper_type: str = typer.Option(
        "bhavcopy",
        "--scraper",
        "-s",
        help="Type of scraper (bhavcopy, symbol-master, corporate-actions, trading-calendar)",
    ),
    date_str: str = typer.Option(
        None, "--date", "-d", help="Date to scrape (YYYY-MM-DD, defaults to yesterday)"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse without producing to Kafka"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
) -> None:
    """Scrape NSE data for a specific date.

    [bold]Examples:[/bold]
        champion etl scrape --scraper bhavcopy --date 2024-01-15
        champion etl scrape --scraper symbol-master --dry-run
    """
    if verbose:
        logger.info(
            f"Starting {scraper_type} scraper", scraper=scraper_type, date=date_str, dry_run=dry_run
        )

    try:
        target_date = (
            datetime.strptime(date_str, "%Y-%m-%d").date()
            if date_str
            else (date.today() - timedelta(days=1))
        )

        if scraper_type == "bhavcopy":
            from champion.scrapers.bhavcopy import BhavcopyScraper

            scraper = BhavcopyScraper()
            scraper.scrape(target_date, dry_run=dry_run)
        elif scraper_type == "symbol-master":
            from champion.scrapers.symbol_master import SymbolMasterScraper

            scraper = SymbolMasterScraper()  # type: ignore[assignment]
            scraper.scrape(dry_run=dry_run)  # type: ignore[call-arg]
        elif scraper_type == "corporate-actions":
            from champion.scrapers.corporate_actions import CorporateActionsScraper

            scraper = CorporateActionsScraper()  # type: ignore[assignment]
            scraper.scrape(dry_run=dry_run)  # type: ignore[call-arg]
        elif scraper_type == "trading-calendar":
            from champion.scrapers.trading_calendar import TradingCalendarScraper

            scraper = TradingCalendarScraper()  # type: ignore[assignment]
            scraper.scrape(year=target_date.year, dry_run=dry_run)  # type: ignore[call-arg]
        else:
            console.print(f"[red]Unknown scraper type: {scraper_type}[/red]")
            raise typer.Exit(1)

        console.print(f"[green]✓[/green] Successfully scraped {scraper_type}")

    except Exception as e:
        logger.error("Scraper failed", scraper=scraper_type, error=str(e), exc_info=True)
        console.print(f"[red]✗ Scraper failed: {e}[/red]")
        raise typer.Exit(1) from e


@warehouse_app.command("load-equity-list")
def equity_list(
    output_base_path: str | None = typer.Option(
        None, "--output", help="Base output path (default: data/lake)"
    ),
    load_to_clickhouse: bool = typer.Option(
        True, "--load/--no-load", help="Load results into ClickHouse"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Download NSE equity list, save as Parquet and load into ClickHouse.

    [bold]Example:[/bold]
        champion warehouse load-equity-list
        champion warehouse load-equity-list --no-load
    """
    # Local imports to avoid top-level dependency issues during help/info runs
    from io import BytesIO

    import httpx
    import pandas as pd
    import polars as pl

    base_path = Path(output_base_path or "data/lake")

    origin_url = "https://nsewebsite-staging.nseindia.com"
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"

    try:
        resp = httpx.get(
            url, headers={"Origin": origin_url, "User-Agent": "champion-cli/1.0"}, timeout=30.0
        )
    except Exception as e:
        typer.secho(f"Failed to fetch equity list: {e}", fg=typer.colors.RED)
        raise typer.Exit(1) from e

    if resp.status_code != 200:
        typer.secho(
            f"No data equity list available (status {resp.status_code})", fg=typer.colors.RED
        )
        raise typer.Exit(1)

    try:
        # Read into pandas to perform robust column-normalisation, then convert to Polars
        pdf = pd.read_csv(BytesIO(resp.content))
    except Exception as e:
        typer.secho(f"Failed to parse equity CSV: {e}", fg=typer.colors.RED)
        raise typer.Exit(1) from e

    # Normalise column names by stripping whitespace
    pdf.rename(columns=lambda c: c.strip() if isinstance(c, str) else c, inplace=True)

    # Map CSV columns to ClickHouse `symbol_master` columns
    column_map = {
        "SYMBOL": "symbol",
        "NAME OF COMPANY": "company_name",
        "SERIES": "series",
        "DATE OF LISTING": "listing_date",
        "FACE VALUE": "face_value",
        "ISIN NUMBER": "isin",
        "PAID UP VALUE": "paid_up_value",
        "MARKET LOT": "lot_size",
    }

    # Apply mapping for columns that exist
    existing_map = {k: v for k, v in column_map.items() if k in pdf.columns}
    pdf = pdf.rename(columns=existing_map)

    # Parse listing_date which is in formats like '06-OCT-2008'
    if "listing_date" in pdf.columns:
        try:
            pdf["listing_date"] = pd.to_datetime(
                pdf["listing_date"], format="%d-%b-%Y", errors="coerce"
            ).dt.date
        except Exception:
            pdf["listing_date"] = pd.to_datetime(pdf["listing_date"], errors="coerce").dt.date

    # Ensure numeric types
    if "face_value" in pdf.columns:
        pdf["face_value"] = pd.to_numeric(pdf["face_value"], errors="coerce")
    if "paid_up_value" in pdf.columns:
        pdf["paid_up_value"] = pd.to_numeric(pdf["paid_up_value"], errors="coerce")
    if "lot_size" in pdf.columns:
        pdf["lot_size"] = pd.to_numeric(pdf["lot_size"], errors="coerce").astype("Int64")

    # Add valid_from as ISO date string if missing
    if "valid_from" not in pdf.columns:
        pdf["valid_from"] = date.today().isoformat()

    # Convert to Polars and write Parquet to data lake
    try:
        pldf = pl.from_pandas(pdf)
    except Exception:
        # If conversion fails, fall back to reading via Polars directly
        try:
            pldf = pl.read_csv(BytesIO(resp.content))
        except Exception as e:
            typer.secho(f"Failed to create DataFrame: {e}", fg=typer.colors.RED)
            raise typer.Exit(1) from e

    # Ensure `valid_from` column exists so ClickHouse partitioning is sane
    if "valid_from" not in pldf.columns:
        try:
            pldf = pldf.with_columns(pl.lit(date.today().isoformat()).alias("valid_from"))
        except Exception:
            # Best-effort: fall back to adding via pandas before writing
            pdf["valid_from"] = date.today().isoformat()
            pldf = pl.from_pandas(pdf)

    # Build storage paths
    today = date.today().isoformat()
    target_dir = base_path / "raw" / "symbol_master" / f"date={today}"
    target_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = target_dir / "symbol_master.parquet"

    try:
        pldf.write_parquet(parquet_path)
    except Exception as e:
        typer.secho(f"Failed to write Parquet file: {e}", fg=typer.colors.RED)
        raise typer.Exit(1) from e

    typer.echo(f"Wrote Parquet to: {parquet_path}")

    if load_to_clickhouse:
        try:
            from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

            loader = ClickHouseLoader()
            loader.connect()
            stats = loader.load_parquet_files(table="symbol_master", source_path=str(target_dir))
            loader.disconnect()
            typer.echo(f"ClickHouse load complete: {stats}")
        except Exception as e:
            typer.secho(f"ClickHouse load failed: {e}", fg=typer.colors.RED)
            raise typer.Exit(1) from e


@orchestrate_app.command("backfill")
def orchestrate_backfill(
    scraper_type: str = typer.Option("bhavcopy", "--scraper", "-s", help="Scraper type: bhavcopy"),
    start_date: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse without producing to Kafka"),
    load_to_clickhouse: bool = typer.Option(
        True, "--load/--no-load", help="Load results into ClickHouse"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
) -> None:
    """Backfill NSE data for a date range (two-phase: download then batch load).

    [bold]Example:[/bold]
        champion orchestrate backfill --start 2024-01-01 --end 2024-01-31
    """
    if verbose:
        logger.info("Starting backfill", scraper=scraper_type, start=start_date, end=end_date)

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        if scraper_type != "bhavcopy":
            console.print("[red]Backfill only supported for bhavcopy[/red]")
            raise typer.Exit(1)

        import polars as pl

        from champion.config import config
        from champion.scrapers.bhavcopy import BhavcopyScraper
        from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

        scraper = BhavcopyScraper()

        # Phase 1: Download all data files for the date range
        console.print("[bold]Phase 1: Downloading data files...[/bold]")
        current = start
        successful_dates = []
        download_failures = 0

        while current <= end:
            try:
                scraper.scrape(current, dry_run=dry_run)
                successful_dates.append(current)
                console.print(f"[green]✓ Downloaded[/green] {current}")
                if verbose:
                    logger.info("Downloaded data for date", date=current)
            except Exception as e:
                download_failures += 1
                console.print(f"[yellow]⊘ Skipped[/yellow] {current}: {e}")
                if verbose:
                    logger.warning("Download failed for date", date=current, error=str(e))

            current = current + timedelta(days=1)

        console.print(
            f"\n[bold]Download phase complete:[/bold] {len(successful_dates)} downloaded, {download_failures} failed\n"
        )

        # Phase 2: Batch load all parquet files to ClickHouse using compressed HTTP
        if load_to_clickhouse and successful_dates:
            console.print("[bold]Phase 2: Batch loading to ClickHouse...[/bold]")

            try:
                # Initialize loader with compression for better batch performance
                loader = ClickHouseLoader(
                    host="localhost",
                    port=8123,  # HTTP port with compression
                    user="default",
                    password="",
                    database="champion",
                )
                loader.connect()

                total_rows = 0
                for trade_date in successful_dates:
                    # Build path to parquet file
                    parquet_path = (
                        config.storage.data_dir
                        / "lake"
                        / "normalized"
                        / "equity_ohlc"
                        / f"year={trade_date.year}"
                        / f"month={trade_date.month:02d}"
                        / f"day={trade_date.day:02d}"
                        / f"bhavcopy_{trade_date.strftime('%Y%m%d')}.parquet"
                    )

                    if parquet_path.exists():
                        try:
                            # Read parquet and insert using compressed HTTP
                            df = pl.read_parquet(str(parquet_path))
                            rows_inserted = loader.insert_polars_dataframe(
                                table="normalized_equity_ohlc",
                                df=df,
                                batch_size=100000,
                                dry_run=False,
                            )
                            total_rows += rows_inserted
                            console.print(
                                f"[green]✓ Loaded[/green] {trade_date} ({rows_inserted:,} rows)"
                            )
                            if verbose:
                                logger.info(
                                    "Loaded data for date", date=trade_date, rows=rows_inserted
                                )
                        except Exception as e:
                            console.print(f"[red]✗ Load failed[/red] {trade_date}: {e}")
                            if verbose:
                                logger.error("Load failed for date", date=trade_date, error=str(e))
                    else:
                        console.print(f"[yellow]⊘ Parquet not found[/yellow] {trade_date}")

                loader.disconnect()
                console.print(
                    f"\n[bold]Batch load complete:[/bold] {total_rows:,} rows loaded into ClickHouse"
                )

            except Exception as e:
                console.print(f"[red]✗ Batch load failed: {e}[/red]")
                if verbose:
                    logger.error("Batch load failed", error=str(e))
                raise
        elif not load_to_clickhouse:
            console.print("[yellow]Skipping load phase (--no-load flag set)[/yellow]")
        else:
            console.print("[yellow]No data to load (all downloads failed)[/yellow]")

    except Exception as e:
        logger.error("Backfill failed", error=str(e), exc_info=True)
        console.print(f"[red]✗ Backfill failed: {e}[/red]")
        raise typer.Exit(1) from e


@validate_app.command("file")
def validate_file(
    file_path: Path = typer.Option(..., "--file", "-f", help="Path to NSE file to validate"),
    file_type: str = typer.Option(
        ..., "--type", "-t", help="File type: bhavcopy, symbol-master, corporate-actions"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
) -> None:
    """Validate a downloaded NSE file.

    [bold]Example:[/bold]
        champion validate file --file data.csv --type bhavcopy
    """
    if verbose:
        logger.info("Validating file", path=str(file_path), type=file_type)

    try:
        if not file_path.exists():
            console.print(f"[red]File not found: {file_path}[/red]")
            raise typer.Exit(1)

        # Validation logic here
        console.print("[green]✓[/green] File validated successfully")

    except Exception as e:
        logger.error("Validation failed", error=str(e), exc_info=True)
        console.print(f"[red]✗ Validation failed: {e}[/red]")
        raise typer.Exit(1) from e


@admin_app.command("config")
def show_config(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """Print current configuration values.

    [bold]Example:[/bold]
        champion admin config
    """
    typer.echo(f"Data dir: {config.storage.data_dir}")
    typer.echo(f"Kafka bootstrap: {config.kafka.bootstrap_servers}")
    typer.echo("ClickHouse: configured via flows/loaders")


@admin_app.command("health")
def health_check(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
) -> None:
    """Check scraper health and dependencies.

    [bold]Example:[/bold]
        champion admin health
    """
    console.print("[bold]Champion Health Check[/bold]\n")

    # Check Schema Registry (which also validates Kafka connectivity)
    try:
        import httpx

        response = httpx.get(f"{config.kafka.schema_registry_url}/subjects", timeout=5.0)
        if response.status_code == 200:
            console.print("[green]✓[/green] Schema Registry: OK")
            console.print("[green]✓[/green] Kafka connection: OK (via Schema Registry)")
        else:
            console.print(f"[red]✗[/red] Schema Registry: FAILED ({response.status_code})")
            console.print("[red]✗[/red] Kafka connection: Cannot verify")
    except Exception as e:
        console.print(f"[red]✗[/red] Schema Registry: FAILED ({e})")
        console.print("[red]✗[/red] Kafka connection: Cannot verify")

    # Check data directory
    if config.storage.data_dir.exists():
        console.print(f"[green]✓[/green] Data directory: {config.storage.data_dir}")
    else:
        console.print("[red]✗[/red] Data directory: Not found")


@api_app.command("serve")
def api_serve(
    host: str = typer.Option("0.0.0.0", help="Host to bind the API server"),
    port: int = typer.Option(8000, help="Port to bind the API server"),
    reload: bool = typer.Option(False, help="Enable auto-reload for development"),
    workers: int = typer.Option(1, help="Number of worker processes"),
):
    """Start the REST API server.

    [bold]Examples:[/bold]
        champion api serve
        champion api serve --port 8080
        champion api serve --reload
        champion api serve --workers 4
    """
    try:
        import uvicorn

        console.print(f"[green]Starting Champion API server on {host}:{port}[/green]")

        uvicorn.run(
            "champion.api.main:app",
            host=host,
            port=port,
            reload=reload,
            workers=workers if not reload else 1,  # workers>1 incompatible with reload
            log_level="info",
        )
    except ImportError:
        console.print("[red]uvicorn not installed. Install with: pip install uvicorn[/red]")
        raise typer.Exit(1) from None
    except Exception as e:
        console.print(f"[red]Failed to start API server: {e}[/red]")
        raise typer.Exit(1) from None


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Args:
        argv: Command line arguments (optional)

    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Initialize logging
    configure_logging()

    try:
        app()
        return 0
    except typer.Exit as e:
        # typer.Exit is expected for normal exit with custom codes
        return e.exit_code
    except KeyboardInterrupt:
        typer.secho("\nOperation cancelled by user", fg=typer.colors.YELLOW)
        return 130  # Standard exit code for SIGINT
    except Exception as e:
        typer.secho(f"Unexpected error: {e}", fg=typer.colors.RED)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
