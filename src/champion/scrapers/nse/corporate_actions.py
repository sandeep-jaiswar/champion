"""Corporate Actions scraper."""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import httpx

from champion.config import config
from champion.scrapers.base import BaseScraper


class CorporateActionsScraper(BaseScraper):
    """Scraper for NSE Corporate Actions data.

    This scraper calls the NSE corporate actions API endpoint and stores
    the returned JSON into `data/lake/reference/corporate_actions`.
    """

    def __init__(self) -> None:
        """Initialize corporate actions scraper."""
        super().__init__("corporate_actions")
        self.session: Optional[httpx.Client] = None

    def _establish_session(self) -> httpx.Client:
        """Establish an httpx session with NSE (visit landing page first).

        NSE requires visiting the main page to get cookies. Reuse a session
        across calls to avoid repeated handshakes.
        """
        if self.session is None:
            self.session = httpx.Client(follow_redirects=True, timeout=config.scraper.timeout)
            try:
                self.logger.info("Establishing session with NSE")
                # Hit the landing page to get cookies set
                self.session.get("https://www.nseindia.com/", headers={"User-Agent": config.scraper.user_agent})
                time.sleep(1)
            except Exception as e:
                self.logger.warning("Failed to establish NSE session", error=str(e))

        return self.session

    def scrape(
        self,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        fno_only: bool = False,
        dry_run: bool = False,
    ) -> Path:
        """Scrape corporate actions data from NSE API and save JSON.

        Args:
            from_date: start date string in 'dd-mm-YYYY' (defaults to 90 days ago)
            to_date: end date string in 'dd-mm-YYYY' (defaults to today)
            fno_only: if True, request only F&O securities
            dry_run: if True, don't persist files

        Returns:
            Path to saved JSON directory (directory containing file)
        """
        self.logger.info("Starting corporate actions scrape", from_date=from_date, to_date=to_date, dry_run=dry_run)

        # Default date range: last 90 days
        today = date.today()
        default_from = (today - timedelta(days=90)).strftime("%d-%m-%Y")
        default_to = today.strftime("%d-%m-%Y")

        from_date = from_date or default_from
        to_date = to_date or default_to

        # Prepare API URL and params
        base_url = config.nse.ca_url  # e.g., https://www.nseindia.com/api/corporates-corporateActions
        params = {"index": "equities", "from_date": from_date, "to_date": to_date}
        if fno_only:
            params["fo_sec"] = "true"

        session = self._establish_session()

        headers = {
            "User-Agent": config.scraper.user_agent,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-actions",
        }

        try:
            self.logger.info("Downloading corporate actions", url=base_url, params=params)
            resp = session.get(base_url, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

            # Prepare output path
            output_dir = config.storage.data_dir / "lake" / "reference" / "corporate_actions"
            output_dir.mkdir(parents=True, exist_ok=True)
            out_file = output_dir / f"corporate_actions_{from_date.replace('-','')}_to_{to_date.replace('-','')}.json"

            if not dry_run:
                with open(out_file, "w") as f:
                    json.dump(data, f, indent=2)
                self.logger.info("Corporate actions downloaded", path=str(out_file), records=len(data) if isinstance(data, list) else None)

            return output_dir

        except Exception as e:
            self.logger.error("Failed to download corporate actions", url=base_url, error=str(e))
            raise

    def close(self) -> None:
        """Close HTTP session."""
        if self.session:
            self.session.close()
            self.session = None
