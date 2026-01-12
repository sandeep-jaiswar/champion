# Parsers

Domain scope: transform raw scraper outputs to typed data frames/events.

Current contents:

- Polars-based parsers for bhavcopy, bulk/block deals, corporate actions, index constituents, macro indicators, symbol master, trading calendar.
- Specialized fundamentals parsers (quarterly financials, shareholding) and enrichment utilities.

Migration notes:

- Extract any remaining parser logic from ingestion and validation packages here.
- Standardize schemas, validation, and date/time casting.
- Keep I/O-free pure parsing to ease testing.
