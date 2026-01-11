from __future__ import annotations

import sys
from datetime import date
from typing import Optional

import typer

from champion.config import config

app = typer.Typer(help="Champion CLI: run common ETL flows and utilities")


@app.command("etl-index")
def etl_index(
    index_name: str = typer.Option("NIFTY50", help="Index to process"),
    effective_date: Optional[str] = typer.Option(None, help="YYYY-MM-DD effective date"),
):
    """Run the Index Constituent ETL flow."""
    eff = (date.fromisoformat(effective_date) if effective_date else date.today())
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
):
    """Run macro indicators ETL flow for a recent window."""
    try:
        from datetime import datetime, timedelta
        from champion.orchestration.flows.macro_flow import macro_indicators_flow

        end = datetime.now()
        start = end - timedelta(days=days)
        macro_indicators_flow(start_date=start, end_date=end)
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
    start_date: Optional[str] = typer.Option(None, help="Start date YYYY-MM-DD"),
    end_date: Optional[str] = typer.Option(None, help="End date YYYY-MM-DD"),
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


@app.command("show-config")
def show_config():
    """Print current configuration values."""
    typer.echo(f"Data dir: {config.storage.data_dir}")
    typer.echo(f"Kafka bootstrap: {config.kafka.bootstrap_servers}")
    typer.echo(f"ClickHouse: configured via flows/loaders")


def main(argv: Optional[list[str]] = None) -> int:
    try:
        app()
        return 0
    except Exception as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
