"""NSE Option Chain scraper with CLI interface."""

import argparse
import json
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import polars as pl
from tenacity import retry, stop_after_attempt, wait_exponential

from champion.config import config
from champion.scrapers.base import BaseScraper
from champion.utils.logger import get_logger

logger = get_logger(__name__)


class OptionChainScraper(BaseScraper):
    """Scraper for NSE Option Chain data with rate limiting and retry logic."""

    # Symbol to NSE instrument mapping
    SYMBOL_INSTRUMENT_MAP = {
        "NIFTY": "indices",
        "BANKNIFTY": "indices",
        "FINNIFTY": "indices",
        "MIDCPNIFTY": "indices",
        # Equity symbols default to "equities"
    }

    def __init__(self) -> None:
        """Initialize option chain scraper."""
        super().__init__("option_chain")
        self._session: httpx.Client | None = None

    def _get_session(self) -> httpx.Client:
        """Get or create HTTP session with proper headers for NSE.

        Returns:
            HTTP client session
        """
        if self._session is None:
            headers = {
                "User-Agent": config.scraper.user_agent,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://www.nseindia.com/option-chain",
                "X-Requested-With": "XMLHttpRequest",
            }
            self._session = httpx.Client(
                headers=headers,
                timeout=config.scraper.timeout,
                follow_redirects=True,
            )
            # Initialize cookies by visiting the main page
            try:
                self._session.get("https://www.nseindia.com")
            except (httpx.RequestError, httpx.HTTPError) as e:
                self.logger.warning("Failed to initialize session cookies", error=str(e))

        return self._session

    def _determine_instrument_type(self, symbol: str) -> str:
        """Determine NSE instrument type from symbol.

        Args:
            symbol: Symbol name (e.g., NIFTY, RELIANCE)

        Returns:
            Instrument type (indices or equities)
        """
        return self.SYMBOL_INSTRUMENT_MAP.get(symbol.upper(), "equities")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def _fetch_option_chain(self, symbol: str) -> dict[str, Any]:
        """Fetch option chain data from NSE API.

        Args:
            symbol: Underlying symbol (e.g., NIFTY, BANKNIFTY, RELIANCE)

        Returns:
            Option chain data as dictionary

        Raises:
            httpx.HTTPError: If request fails
        """
        instrument_type = self._determine_instrument_type(symbol)
        url = config.nse.option_chain_url.format(instrument=instrument_type)

        session = self._get_session()
        params = {"symbol": symbol.upper()}

        self.logger.info(
            "Fetching option chain",
            symbol=symbol,
            instrument=instrument_type,
            url=url,
        )

        response = session.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        self.logger.info(
            "Option chain fetched successfully",
            symbol=symbol,
            records=len(data.get("records", {}).get("data", [])),
        )

        return data

    def scrape(  # type: ignore[override]
        self,
        symbol: str,
        output_dir: Path | None = None,
    ) -> pl.DataFrame:
        """Scrape option chain data for a symbol.

        Args:
            symbol: Underlying symbol
            output_dir: Optional directory to save raw JSON

        Returns:
            Polars DataFrame with parsed option chain data
        """
        self.logger.info("Starting option chain scrape", symbol=symbol)

        # Fetch data from NSE
        data = self._fetch_option_chain(symbol)

        # Save raw JSON if output directory provided
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_path = output_dir / f"option_chain_{symbol}_{timestamp}.json"

            with open(json_path, "w") as f:
                json.dump(data, f, indent=2)

            self.logger.info("Raw JSON saved", path=str(json_path))

        # Parse to DataFrame
        df = self._parse_to_dataframe(data, symbol)

        self.logger.info(
            "Option chain parsing complete",
            symbol=symbol,
            rows=len(df),
        )

        return df

    def _parse_to_dataframe(self, data: dict[str, Any], symbol: str) -> pl.DataFrame:
        """Parse NSE option chain JSON to Polars DataFrame.

        Args:
            data: Option chain JSON data from NSE
            symbol: Underlying symbol

        Returns:
            Polars DataFrame with option chain records
        """
        records = data.get("records", {})
        option_data = records.get("data", [])
        underlying_value = records.get("underlyingValue")
        timestamp = datetime.now()

        # Extract both CE and PE options
        rows = []

        for item in option_data:
            expiry_date = item.get("expiryDate", "")
            strike_price = item.get("strikePrice", 0)

            # Process Call (CE) option
            if "CE" in item:
                ce = item["CE"]
                rows.append(
                    self._create_option_record(
                        symbol=symbol,
                        underlying_value=underlying_value,
                        timestamp=timestamp,
                        expiry_date=expiry_date,
                        strike_price=strike_price,
                        option_type="CE",
                        option_data=ce,
                    )
                )

            # Process Put (PE) option
            if "PE" in item:
                pe = item["PE"]
                rows.append(
                    self._create_option_record(
                        symbol=symbol,
                        underlying_value=underlying_value,
                        timestamp=timestamp,
                        expiry_date=expiry_date,
                        strike_price=strike_price,
                        option_type="PE",
                        option_data=pe,
                    )
                )

        # Create DataFrame
        if not rows:
            self.logger.warning("No option chain records found", symbol=symbol)
            return pl.DataFrame()

        df = pl.DataFrame(rows)

        return df

    def _create_option_record(
        self,
        symbol: str,
        underlying_value: float | None,
        timestamp: datetime,
        expiry_date: str,
        strike_price: float,
        option_type: str,
        option_data: dict[str, Any],
    ) -> dict[str, Any]:
        """Create a single option record.

        Args:
            symbol: Underlying symbol
            underlying_value: Current underlying value
            timestamp: Snapshot timestamp
            expiry_date: Option expiry date
            strike_price: Strike price
            option_type: CE or PE
            option_data: Option data from NSE API

        Returns:
            Dictionary with option record
        """
        # Convert expiry date format (DD-MMM-YYYY to YYYY-MM-DD)
        try:
            expiry_dt = datetime.strptime(expiry_date, "%d-%b-%Y")
            expiry_formatted = expiry_dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError) as e:
            self.logger.warning(
                "Failed to parse expiry date, using original format",
                expiry_date=expiry_date,
                error=str(e),
            )
            expiry_formatted = expiry_date

        return {
            "event_id": str(uuid.uuid4()),
            "event_time": int(timestamp.timestamp() * 1000),
            "ingest_time": int(datetime.now().timestamp() * 1000),
            "source": "nse_option_chain",
            "schema_version": "v1",
            "entity_id": symbol,
            "underlying": symbol,
            "underlying_value": underlying_value,
            "timestamp": int(timestamp.timestamp() * 1000),
            "expiry_date": expiry_formatted,
            "strike_price": strike_price,
            "option_type": option_type,
            "bid_price": option_data.get("bidprice"),
            "ask_price": option_data.get("askPrice"),
            "last_price": option_data.get("lastPrice"),
            "volume": option_data.get("totalTradedVolume"),
            "open_interest": option_data.get("openInterest"),
            "change_in_oi": option_data.get("changeinOpenInterest"),
            "implied_volatility": option_data.get("impliedVolatility"),
            "delta": option_data.get("delta"),
            "theta": option_data.get("theta"),
            "gamma": option_data.get("gamma"),
            "vega": option_data.get("vega"),
        }

    def scrape_continuous(
        self,
        symbol: str,
        interval_minutes: int,
        duration_minutes: int,
        output_dir: Path,
    ) -> None:
        """Scrape option chain continuously at intervals.

        Args:
            symbol: Underlying symbol
            interval_minutes: Interval between scrapes in minutes
            duration_minutes: Total duration to run in minutes
            output_dir: Directory to save Parquet files
        """
        self.logger.info(
            "Starting continuous option chain scraping",
            symbol=symbol,
            interval_min=interval_minutes,
            duration_min=duration_minutes,
        )

        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        iteration = 0

        while datetime.now() < end_time:
            iteration += 1
            self.logger.info(
                "Scraping iteration",
                iteration=iteration,
                symbol=symbol,
            )

            try:
                # Scrape data
                df = self.scrape(symbol, output_dir=output_dir)

                # Write to Parquet with partitioning
                if len(df) > 0:
                    self._write_parquet(df, output_dir, symbol)
                else:
                    self.logger.warning("No data to write", symbol=symbol, iteration=iteration)

            except Exception as e:
                self.logger.error(
                    "Scraping iteration failed",
                    symbol=symbol,
                    iteration=iteration,
                    error=str(e),
                )

            # Sleep until next interval
            remaining_time = (end_time - datetime.now()).total_seconds()
            if remaining_time <= 0:
                break

            sleep_seconds = min(interval_minutes * 60, remaining_time)
            self.logger.info(
                "Sleeping until next iteration",
                seconds=sleep_seconds,
            )
            time.sleep(sleep_seconds)

        self.logger.info(
            "Continuous scraping complete",
            symbol=symbol,
            iterations=iteration,
        )

    def _write_parquet(
        self,
        df: pl.DataFrame,
        output_dir: Path,
        symbol: str,
    ) -> None:
        """Write DataFrame to Parquet with date/symbol partitioning.

        Args:
            df: DataFrame to write
            output_dir: Output directory
            symbol: Underlying symbol for partitioning
        """
        # Create partition path: output_dir/date=YYYY-MM-DD/symbol=XXX/
        date_str = datetime.now().strftime("%Y-%m-%d")
        partition_dir = output_dir / f"date={date_str}" / f"symbol={symbol}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"option_chain_{timestamp}.parquet"
        file_path = partition_dir / filename

        # Write Parquet file
        df.write_parquet(file_path, compression="snappy")

        self.logger.info(
            "Parquet file written",
            path=str(file_path),
            rows=len(df),
        )

    def close(self) -> None:
        """Close HTTP session."""
        if self._session:
            self._session.close()
            self._session = None


def parse_interval(interval_str: str) -> int:
    """Parse interval string like '5m', '1h' to minutes.

    Args:
        interval_str: Interval string (e.g., '5m', '1h', '30s')

    Returns:
        Interval in minutes

    Raises:
        ValueError: If interval format is invalid
    """
    interval_str = interval_str.strip().lower()

    try:
        if interval_str.endswith("s"):
            seconds = int(interval_str[:-1])
            return max(1, seconds // 60)
        elif interval_str.endswith("m"):
            return int(interval_str[:-1])
        elif interval_str.endswith("h"):
            return int(interval_str[:-1]) * 60
        else:
            raise ValueError(f"Invalid interval format: {interval_str}. Use '5m', '1h', etc.")
    except ValueError as e:
        # Re-raise with more context if it's our error, otherwise add context for parsing errors
        if "Invalid interval format" in str(e):
            raise
        raise ValueError(
            f"Invalid interval value in '{interval_str}': must be a number followed by s/m/h"
        ) from e


def main() -> None:
    """CLI entry point for option chain scraper."""
    parser = argparse.ArgumentParser(
        description="NSE Option Chain Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape NIFTY every 5 minutes for 1 hour
  python src/scrapers/option_chain.py --symbol NIFTY --interval 5m --duration 1h

  # Scrape BANKNIFTY every 15 minutes for 2 hours
  python src/scrapers/option_chain.py --symbol BANKNIFTY --interval 15m --duration 2h

  # Single snapshot of RELIANCE
  python src/scrapers/option_chain.py --symbol RELIANCE --interval 1m --duration 1m
        """,
    )

    parser.add_argument(
        "--symbol",
        type=str,
        required=True,
        help="Underlying symbol (e.g., NIFTY, BANKNIFTY, RELIANCE)",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default="5m",
        help="Scraping interval (e.g., 5m, 15m, 1h). Default: 5m",
    )
    parser.add_argument(
        "--duration",
        type=str,
        default="1h",
        help="Total duration to run (e.g., 30m, 1h, 2h). Default: 1h",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./data/option_chain"),
        help="Output directory for Parquet files. Default: ./data/option_chain",
    )

    args = parser.parse_args()

    # Parse interval and duration
    try:
        interval_minutes = parse_interval(args.interval)
        duration_minutes = parse_interval(args.duration)
    except ValueError as e:
        logger.error("Invalid interval or duration", error=str(e))
        parser.print_help()
        return

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Starting option chain scraper",
        symbol=args.symbol,
        interval_minutes=interval_minutes,
        duration_minutes=duration_minutes,
        output_dir=str(args.output_dir),
    )

    # Run scraper
    scraper = OptionChainScraper()
    try:
        scraper.scrape_continuous(
            symbol=args.symbol,
            interval_minutes=interval_minutes,
            duration_minutes=duration_minutes,
            output_dir=args.output_dir,
        )
    finally:
        scraper.close()

    logger.info("Scraper completed successfully")


if __name__ == "__main__":
    main()
