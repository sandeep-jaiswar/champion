"""NSE CM Bhavcopy scraper."""

from datetime import date

from src.config import config
from src.scrapers.base import BaseScraper
from src.utils.logger import get_logger
from src.utils.metrics import files_downloaded, scrape_duration

logger = get_logger(__name__)


class BhavcopyScraper(BaseScraper):
    """Scraper for NSE CM Bhavcopy files."""

    def __init__(self):
        """Initialize bhavcopy scraper."""
        super().__init__("bhavcopy")

    def scrape(self, target_date: date, dry_run: bool = False) -> None:
        """Scrape bhavcopy for a specific date.

        Args:
            target_date: Date to scrape
            dry_run: If True, parse without producing to Kafka
        """
        with scrape_duration.labels(scraper=self.name).time():
            self.logger.info("Starting bhavcopy scrape", date=str(target_date), dry_run=dry_run)

            # Format date for NSE URL (YYYYMMDD)
            date_str = target_date.strftime("%Y%m%d")
            url = config.nse.bhavcopy_url.format(date=date_str)

            # Download file
            local_path = config.storage.data_dir / f"BhavCopy_NSE_CM_{date_str}.csv"

            if not self.download_file(url, str(local_path)):
                raise RuntimeError(f"Failed to download bhavcopy for {target_date}")

            files_downloaded.labels(scraper=self.name).inc()

            # Parse file
            from src.parsers.bhavcopy_parser import BhavcopyParser

            parser = BhavcopyParser()
            events = parser.parse(local_path, target_date)

            self.logger.info(
                f"Parsed {len(events)} events", date=str(target_date), count=len(events)
            )

            if not dry_run:
                # Produce to Kafka
                from src.producers.avro_producer import AvroProducer

                producer = AvroProducer(
                    topic=config.topics.raw_ohlc,
                    schema_type="raw_equity_ohlc"
                )

                success_count = 0
                failed_count = 0

                for event in events:
                    try:
                        producer.produce(event)
                        success_count += 1
                    except Exception as e:
                        failed_count += 1
                        self.logger.error(
                            "Failed to produce event", symbol=event.get("entity_id"), error=str(e)
                        )

                producer.flush()
                self.logger.info(
                    "Produced events to Kafka",
                    success=success_count,
                    failed=failed_count,
                    topic=config.topics.raw_ohlc,
                )
            else:
                self.logger.info("Dry run - skipped Kafka production", count=len(events))

            import time

            from src.utils.metrics import last_successful_scrape

            last_successful_scrape.labels(scraper=self.name).set(time.time())
