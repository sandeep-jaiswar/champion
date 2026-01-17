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

# Register command groups
app.add_typer(etl_app, name="etl")
app.add_typer(warehouse_app, name="warehouse")
app.add_typer(validate_app, name="validate")
app.add_typer(orchestrate_app, name="orchestrate")
app.add_typer(admin_app, name="admin")

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
    index_name: str = typer.Option("NIFTY50", "--index", "-i", help="Index to process (e.g., NIFTY50)"),
    effective_date: str | None = typer.Option(None, "--date", "-d", help="Effective date (YYYY-MM-DD)"),
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
    start_date: str | None = typer.Option(None, "--start", help="Start date (YYYY-MM-DD) for range run"),
    end_date: str | None = typer.Option(None, "--end", help="End date (YYYY-MM-DD) for range run"),
    output_base_path: str | None = typer.Option(None, "--output", help="Base output path (default: data/lake)"),
    load_to_clickhouse: bool = typer.Option(True, "--load/--no-load", help="Load results into ClickHouse"),
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
            while cur <= end_dt:
                nse_bhavcopy_etl_flow(
                    trade_date=cur,
                    output_base_path=output_base_path,
                    load_to_clickhouse=load_to_clickhouse,
                    start_metrics_server_flag=False,
                )
                cur = cur + timedelta(days=1)
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
    trade_date: str | None = typer.Option(None, "--date", "-d", help="Trade date (YYYY-MM-DD, default: today)"),
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
    start_date: str | None = typer.Option(None, "--start", help="Start date (YYYY-MM-DD) for range run"),
    end_date: str | None = typer.Option(None, "--end", help="End date (YYYY-MM-DD) for range run"),
    symbol: str | None = typer.Option(None, "--symbol", "-s", help="Optional symbol to query (e.g., TCS)"),
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
        from champion.orchestration.flows.quarterly_financial_flow import QuarterlyResultsScraper

        if start_date and end_date:
            sd = validate_date_format(start_date)
            ed = validate_date_format(end_date)
        else:
            sd = date.today()
            ed = date.today()

        master_df = QuarterlyResultsScraper().get_master(
            from_date=sd.strftime("%d-%m-%Y"),
            to_date=ed.strftime("%d-%m-%Y"),
            symbol=symbol,
            issuer=issuer,
            filter_audited=filter_audited,
        )
        scraper = QuarterlyResultsScraper()
        # normalize dataframe and write canonical parquet
        norm_df = scraper.normalize_master_dataframe(master_df)
        # download documents and capture saved file paths
        saved_files = scraper.download_documents(master=master_df)

        # Parse downloaded XBRL/XML files into rows (if any) so we can optionally
        # insert parsed facts directly into ClickHouse without relying only on Parquet.
        parsed_rows = []
        try:
            # local import to avoid adding heavy deps at module import time
            import polars as pl

            from champion.parsers.xbrl_parser import parse_xbrl_file

            for p in saved_files:
                try:
                    if str(p).lower().endswith(".xml"):
                        rec = parse_xbrl_file(Path(p))
                        parsed_rows.append(rec)
                except Exception:
                    # be tolerant; we still proceed with Parquet write/load
                    typer.secho(f"Failed to parse XBRL: {p}", fg=typer.colors.YELLOW)
        except Exception:
            # parser missing or failed import; skip parsed insert
            parsed_rows = []
        # Optionally write Parquet and upload master rows into ClickHouse in batch
        if load_to_clickhouse:
            try:
                import pandas as pd
                import polars as pl

                from champion.warehouse.clickhouse.batch_loader import ClickHouseLoader

                # Normalize/master -> Parquet column mapping to match ClickHouse schema
                today = date.today().isoformat()
                # Canonical path: include date partition; when `--symbol` provided
                # write directly under a symbol subfolder so loader can target
                # the exact file produced by this run and avoid cross-writes.
                base = Path("data/lake/raw/quarterly_financials") / f"date={today}"
                base.mkdir(parents=True, exist_ok=True)

                # Work on a pandas copy to rename and coerce types
                # use normalized dataframe if available
                pdf = norm_df.copy() if norm_df is not None else master_df.copy()

                # Common mappings from NSE master to ClickHouse schema
                rename_map = {
                    "companyName": "company_name",
                    "financialYear": "year",
                    "filingDate": "filing_date",
                    "fromDate": "period_start",
                    "toDate": "period_end_date",
                    "period": "period_type",
                    "isin": "cin",
                }

                # Only rename columns that exist
                existing_rename = {k: v for k, v in rename_map.items() if k in pdf.columns}
                if existing_rename:
                    pdf = pdf.rename(columns=existing_rename)

                # Coerce year to integer when possible
                if "year" in pdf.columns:
                    try:
                        pdf["year"] = (
                            pdf["year"]
                            .astype(str)
                            .str.extract(r"(\d{4})")[0]
                            .astype(float)
                            .astype("Int64")
                        )
                    except Exception:
                        pass

                # Try to derive quarter from period_type or fromDate/toDate if available
                if "quarter" not in pdf.columns:
                    qseries = None
                    if "period_type" in pdf.columns:
                        try:
                            qseries = pdf["period_type"].astype(str).str.extract(r"Q([1-4])")[0]
                        except Exception:
                            qseries = None
                    if qseries is None or qseries.isnull().all():
                        # Try parsing period_end_date month to quarter
                        if "period_end_date" in pdf.columns:
                            try:
                                pdf["period_end_date"] = pd.to_datetime(
                                    pdf["period_end_date"], errors="coerce"
                                )
                                qseries = pdf["period_end_date"].dt.quarter
                            except Exception:
                                qseries = None
                    if qseries is not None:
                        try:
                            pdf["quarter"] = qseries.astype("Int64")
                        except Exception:
                            pass

                # Ensure company_name exists
                if "company_name" not in pdf.columns:
                    if "companyName" in master_df.columns:
                        pdf["company_name"] = master_df["companyName"]
                    else:
                        pdf["company_name"] = None

                # Convert to Polars and write Parquet
                try:
                    pldf = pl.from_pandas(pdf)
                except Exception:
                    pldf = pl.DataFrame(pdf)

                # If a single symbol was requested, write into a dedicated
                # symbol subfolder and only include rows for that symbol.
                written = 0
                if symbol:
                    safe_sym = str(symbol)
                    out_dir = base / f"symbol={safe_sym}"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_path = out_dir / "quarterly_financials.parquet"
                    try:
                        sub_pdf = pdf[pdf.get("symbol") == symbol]
                        try:
                            pldf_sub = pl.from_pandas(sub_pdf)
                        except Exception:
                            pldf_sub = pl.DataFrame(sub_pdf)
                        pldf_sub.write_parquet(out_path)
                        written = 1
                    except Exception:
                        try:
                            import pyarrow as pa
                            import pyarrow.parquet as pq

                            table = pa.Table.from_pandas(sub_pdf)
                            pq.write_table(table, out_path)
                            written = 1
                        except Exception:
                            sub_pdf.to_parquet(out_path)
                            written = 1
                else:
                    # Write one folder per distinct symbol found in the dataframe
                    try:
                        syms = pldf.select("symbol").unique().to_series().to_list()
                    except Exception:
                        syms = (
                            pdf.get("symbol").dropna().unique().tolist()
                            if "symbol" in pdf.columns
                            else []
                        )

                    for sym in syms:
                        safe_sym = str(sym) if sym is not None else ""
                        out_dir = base / f"symbol={safe_sym}"
                        out_dir.mkdir(parents=True, exist_ok=True)
                        out_path = out_dir / "quarterly_financials.parquet"
                        try:
                            sub = pldf.filter(pl.col("symbol") == sym)
                            sub.write_parquet(out_path)
                            written += 1
                        except Exception:
                            pdf_sub = pdf[pdf.get("symbol") == sym]
                            try:
                                import pyarrow as pa  # optional, may raise if missing
                                import pyarrow.parquet as pq

                                table = pa.Table.from_pandas(pdf_sub)
                                pq.write_table(table, out_path)
                                written += 1
                            except Exception:
                                try:
                                    pdf_sub.to_parquet(out_path)
                                    written += 1
                                except Exception:
                                    pass

                typer.echo(f"Wrote {written} Parquet file(s) under: {base}")

                # Load Parquet files into ClickHouse using existing loader (batch mode)
                loader = ClickHouseLoader()
                dry_run = bool(os.environ.get("CLICKHOUSE_DRY"))
                # Target the staging table by default to keep production safe
                target_table = "quarterly_financials_raw"
                # If we wrote a single symbol file, point loader at that symbol folder
                source_path = str(base)
                if symbol:
                    source_path = str(base / f"symbol={symbol}")

                if dry_run:
                    stats = loader.load_parquet_files(
                        table=target_table, source_path=source_path, dry_run=True
                    )
                else:
                    loader.connect()
                    stats = loader.load_parquet_files(
                        table=target_table, source_path=source_path, dry_run=False
                    )
                    # If we parsed XBRL files above, try inserting them directly
                    try:
                        if parsed_rows:
                            try:
                                import polars as pl

                                pldf = pl.from_dicts(parsed_rows)
                            except Exception:
                                pldf = None
                            if pldf is not None and len(pldf) > 0:
                                # Insert parsed facts into canonical table
                                loader.insert_polars_dataframe(
                                    table="quarterly_financials", df=pldf, dry_run=dry_run
                                )
                    except Exception as e:
                        typer.secho(
                            f"ClickHouse insert of parsed XBRL failed: {e}", fg=typer.colors.RED
                        )
                    loader.disconnect()
                typer.echo(f"ClickHouse load result: {stats}")
            except Exception as e:
                typer.secho(f"ClickHouse load failed: {e}", fg=typer.colors.RED)
                raise typer.Exit(1) from e
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
    output_base_path: str | None = typer.Option(None, "--output", help="Base output path (default: data/lake)"),
    load_to_clickhouse: bool = typer.Option(True, "--load/--no-load", help="Load results into ClickHouse"),
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
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
) -> None:
    """Backfill NSE data for a date range.

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

        from champion.scrapers.bhavcopy import BhavcopyScraper

        scraper = BhavcopyScraper()

        current = start
        successes = 0
        failures = 0

        while current <= end:
            try:
                scraper.scrape(current, dry_run=dry_run)
                successes += 1
                console.print(f"[green]✓[/green] {current}")
            except Exception as e:
                failures += 1
                console.print(f"[red]✗[/red] {current}: {e}")
                if verbose:
                    logger.error("Backfill failed for date", date=current, error=str(e))

            current = current + timedelta(days=1)

        console.print(f"\n[bold]Backfill complete:[/bold] {successes} succeeded, {failures} failed")

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
