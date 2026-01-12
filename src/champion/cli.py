from __future__ import annotations

import os
from datetime import date

import typer

from champion.config import config

# Default MLflow backend to local file store unless explicitly set
os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")

app = typer.Typer(help="Champion CLI: run common ETL flows and utilities")


@app.command("etl-index")
def etl_index(
    index_name: str = typer.Option("NIFTY50", help="Index to process"),
    effective_date: str | None = typer.Option(None, help="YYYY-MM-DD effective date"),
):
    """Run the Index Constituent ETL flow."""
    eff = date.fromisoformat(effective_date) if effective_date else date.today()
    try:
        from champion.orchestration.flows.flows import index_constituent_etl_flow

        index_constituent_etl_flow(indices=[index_name], effective_date=eff)
    except Exception as e:
        typer.secho(
            f"Index ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise


@app.command("etl-macro")
def etl_macro(
    days: int = typer.Option(90, help="Number of days back to include"),
    fail_on_empty: bool = typer.Option(
        False, help="Fail if no macro data retrieved from any source"
    ),
    source_order: str | None = typer.Option(
        None,
        help="Comma-separated list of macro sources to try in order (e.g., MoSPI,RBI,DEA,NITI Aayog)",
    ),
):
    """Run macro indicators ETL flow for a recent window."""
    # Ensure MLflow uses file backend by default if not set
    os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")
    try:
        from datetime import datetime, timedelta

        from champion.orchestration.flows.macro_flow import macro_indicators_flow

        end = datetime.now()
        start = end - timedelta(days=days)
        sources = [s.strip() for s in source_order.split(",")] if source_order else None
        macro_indicators_flow(
            start_date=start, end_date=end, source_order=sources, fail_on_empty=fail_on_empty
        )
    except Exception as e:
        typer.secho(
            f"Macro ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise


@app.command("etl-trading-calendar")
def etl_trading_calendar():
    """Run trading calendar ETL flow."""
    try:
        from champion.orchestration.flows.trading_calendar_flow import (
            trading_calendar_etl_flow,
        )

        trading_calendar_etl_flow()
    except Exception as e:
        typer.secho(
            f"Trading calendar ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise


@app.command("etl-bulk-deals")
def etl_bulk_deals(
    start_date: str | None = typer.Option(None, help="Start date YYYY-MM-DD"),
    end_date: str | None = typer.Option(None, help="End date YYYY-MM-DD"),
):
    """Run bulk/block deals ETL flow (optionally for a date range)."""
    if start_date and end_date:
        # The flow file includes a date-range variant; call if available
        try:
            from champion.orchestration.flows.bulk_block_deals_flow import (
                bulk_block_deals_date_range_etl_flow,
            )

            bulk_block_deals_date_range_etl_flow(
                start_date=start_date,
                end_date=end_date,
            )
            return
        except Exception:
            # Fallback to single flow
            pass
    try:
        from champion.orchestration.flows.bulk_block_deals_flow import (
            bulk_block_deals_etl_flow,
        )

        bulk_block_deals_etl_flow()
    except Exception as e:
        typer.secho(
            f"Bulk/Block deals ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise


@app.command("etl-ohlc")
def etl_ohlc(
    trade_date: str | None = typer.Option(
        None, help="Trade date YYYY-MM-DD (default: previous business day)"
    ),
    output_base_path: str | None = typer.Option(None, help="Base output path (default: data/lake)"),
    load_to_clickhouse: bool = typer.Option(True, help="Load results into ClickHouse"),
):
    """Run NSE OHLC (bhavcopy) ETL flow."""
    # Ensure MLflow uses file backend by default if not set
    os.environ.setdefault("MLFLOW_TRACKING_URI", "file:./mlruns")
    try:
        from champion.orchestration.flows.flows import nse_bhavcopy_etl_flow

        td = date.fromisoformat(trade_date) if trade_date else None
        nse_bhavcopy_etl_flow(
            trade_date=td,
            output_base_path=output_base_path,
            load_to_clickhouse=load_to_clickhouse,
            start_metrics_server_flag=False,
        )
    except Exception as e:
        typer.secho(
            f"OHLC ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise


@app.command("etl-corporate-actions")
def etl_corporate_actions():
    """Run corporate actions ETL flow."""
    try:
        from champion.orchestration.flows.corporate_actions_flow import (
            corporate_actions_etl_flow,
        )

        corporate_actions_etl_flow()
    except Exception as e:
        typer.secho(
            f"Corporate actions ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise


@app.command("etl-combined-equity")
def etl_combined_equity(
    trade_date: str | None = typer.Option(None, help="Trade date YYYY-MM-DD (default: today)"),
):
    """Run combined equity ETL flow (NSE + BSE bhavcopy)."""
    try:
        from champion.orchestration.flows.combined_flows import combined_equity_etl_flow

        if trade_date:
            td = date.fromisoformat(trade_date)
        else:
            td = date.today()
        combined_equity_etl_flow(trade_date=td)
    except Exception as e:
        typer.secho(
            f"Combined equity ETL failed to start: {e}. Did tasks migrate to champion.*?",
            fg=typer.colors.RED,
        )
        raise


@app.command("show-config")
def show_config():
    """Print current configuration values."""
    typer.echo(f"Data dir: {config.storage.data_dir}")
    typer.echo(f"Kafka bootstrap: {config.kafka.bootstrap_servers}")
    typer.echo("ClickHouse: configured via flows/loaders")


def main(argv: list[str] | None = None) -> int:
    try:
        app()
        return 0
    except Exception as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
