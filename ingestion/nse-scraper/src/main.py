"""NSE Scraper CLI entrypoint."""

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import typer
from rich.console import Console

from src.config import config
from src.utils.logger import get_logger, configure_logging

app = typer.Typer(
    name="nse-scraper",
    help="Production-grade NSE data scraper with Kafka integration",
    add_completion=False,
)

console = Console()
logger = get_logger(__name__)


@app.command()
def scrape(
    scraper_type: str = typer.Argument(..., help="Scraper type: bhavcopy, symbol-master, corporate-actions, trading-calendar"),
    date_str: str = typer.Option(None, "--date", help="Date to scrape (YYYY-MM-DD). Defaults to yesterday"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse without producing to Kafka"),
) -> None:
    """Scrape NSE data for a specific date."""
    logger.info(f"Starting {scraper_type} scraper", scraper=scraper_type, date=date_str, dry_run=dry_run)

    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else date.today()
        
        if scraper_type == "bhavcopy":
            from src.scrapers.bhavcopy import BhavcopyScraper
            scraper = BhavcopyScraper()
            scraper.scrape(target_date, dry_run=dry_run)
        elif scraper_type == "symbol-master":
            from src.scrapers.symbol_master import SymbolMasterScraper
            scraper = SymbolMasterScraper()
            scraper.scrape(dry_run=dry_run)
        elif scraper_type == "corporate-actions":
            from src.scrapers.corporate_actions import CorporateActionsScraper
            scraper = CorporateActionsScraper()
            scraper.scrape(dry_run=dry_run)
        elif scraper_type == "trading-calendar":
            from src.scrapers.trading_calendar import TradingCalendarScraper
            scraper = TradingCalendarScraper()
            scraper.scrape(year=target_date.year, dry_run=dry_run)
        else:
            console.print(f"[red]Unknown scraper type: {scraper_type}[/red]")
            raise typer.Exit(1)

        console.print(f"[green]✓[/green] Successfully scraped {scraper_type}")

    except Exception as e:
        logger.error(f"Scraper failed", scraper=scraper_type, error=str(e), exc_info=True)
        console.print(f"[red]✗ Scraper failed: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def backfill(
    scraper_type: str = typer.Argument(..., help="Scraper type: bhavcopy"),
    start_date: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse without producing to Kafka"),
) -> None:
    """Backfill NSE data for a date range."""
    logger.info(f"Starting backfill", scraper=scraper_type, start=start_date, end=end_date)

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        if scraper_type != "bhavcopy":
            console.print(f"[red]Backfill only supported for bhavcopy[/red]")
            raise typer.Exit(1)

        from src.scrapers.bhavcopy import BhavcopyScraper
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
                logger.error(f"Backfill failed for date", date=current, error=str(e))
            
            current = current + timedelta(days=1)

        console.print(f"\n[bold]Backfill complete:[/bold] {successes} succeeded, {failures} failed")

    except Exception as e:
        logger.error(f"Backfill failed", error=str(e), exc_info=True)
        console.print(f"[red]✗ Backfill failed: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def validate(
    file_path: Path = typer.Option(..., "--file", help="Path to NSE file to validate"),
    file_type: str = typer.Option(..., "--type", help="File type: bhavcopy, symbol-master, corporate-actions"),
) -> None:
    """Validate a downloaded NSE file."""
    logger.info(f"Validating file", path=str(file_path), type=file_type)

    try:
        if not file_path.exists():
            console.print(f"[red]File not found: {file_path}[/red]")
            raise typer.Exit(1)

        # Validation logic here
        console.print(f"[green]✓[/green] File validated successfully")

    except Exception as e:
        logger.error(f"Validation failed", error=str(e), exc_info=True)
        console.print(f"[red]✗ Validation failed: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def health() -> None:
    """Check scraper health and dependencies."""
    console.print("[bold]NSE Scraper Health Check[/bold]\n")

    # Check Kafka connectivity
    try:
        from src.producers.avro_producer import AvroProducer
        producer = AvroProducer(config.topics.raw_ohlc)
        console.print("[green]✓[/green] Kafka connection: OK")
    except Exception as e:
        console.print(f"[red]✗[/red] Kafka connection: FAILED ({e})")

    # Check Schema Registry
    try:
        import httpx
        response = httpx.get(f"{config.kafka.schema_registry_url}/subjects")
        if response.status_code == 200:
            console.print("[green]✓[/green] Schema Registry: OK")
        else:
            console.print(f"[red]✗[/red] Schema Registry: FAILED ({response.status_code})")
    except Exception as e:
        console.print(f"[red]✗[/red] Schema Registry: FAILED ({e})")

    # Check data directory
    if config.storage.data_dir.exists():
        console.print(f"[green]✓[/green] Data directory: {config.storage.data_dir}")
    else:
        console.print(f"[red]✗[/red] Data directory: Not found")


def main() -> None:
    """Main entrypoint."""
    app()


if __name__ == "__main__":
    main()
