"""Data ingestion layer for external data sources.

Scrapers extract data from NSE, BSE, and other data providers.
All scrapers implement the champion.core.Scraper interface for consistency.

## Submodules

- `nse/`: National Stock Exchange (NSE) scrapers
- `bse/`: Bombay Stock Exchange (BSE) scrapers
- `adapters.py`: Domain adapters for scraper patterns
"""

from .adapters import EquityScraper, ReferenceDataScraper, ScraperWithRetry

__all__ = [
    "EquityScraper",
    "ReferenceDataScraper",
    "ScraperWithRetry",
]
