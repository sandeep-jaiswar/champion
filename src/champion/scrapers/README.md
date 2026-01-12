# Scrapers

Domain scope: data collection for exchanges and macro sources. Centralizes scraper interfaces and per-provider implementations.

Current contents:

- Base helpers (`base.py`), provider packages (e.g., `nse/`, `bse/`).
- NSE coverage: bhavcopy, bulk/block deals, corporate actions, index constituents, macro (MoSPI/RBI), option chain, trading calendar, symbol master.

Migration notes:

- Consolidate any remaining ingestion/nse-scraper code into `champion.scrapers`.
- Keep provider-specific subpackages (`champion.scrapers.nse`, `champion.scrapers.bse`).
- Prefer typed HTTP clients and shared retry/backoff utilities.
