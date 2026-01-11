"""Parser for quarterly financial statements.

Parses financial data from MCA/BSE sources into structured format.
Computes key financial ratios (ROE, ROA, debt ratios, margins).
"""

from datetime import date, datetime
from pathlib import Path
from typing import Any
import uuid

import polars as pl
from bs4 import BeautifulSoup

from src.utils.logger import get_logger

logger = get_logger(__name__)


class QuarterlyFinancialsParser:
    """Parser for quarterly financial statements."""

    def __init__(self) -> None:
        """Initialize parser."""
        self.logger = get_logger(__name__)

    def parse(self, file_path: Path, symbol: str, cin: str | None = None) -> pl.DataFrame:
        """Parse financial statements file into structured DataFrame.

        Args:
            file_path: Path to data file (HTML, CSV, or JSON)
            symbol: Trading symbol
            cin: Corporate Identification Number (optional)

        Returns:
            Polars DataFrame with financial data
        """
        self.logger.info("Parsing financial file", file_path=str(file_path))

        try:
            # Determine file type and parse accordingly
            if file_path.suffix.lower() == ".html":
                return self._parse_html(file_path, symbol, cin)
            elif file_path.suffix.lower() == ".csv":
                return self._parse_csv(file_path, symbol, cin)
            elif file_path.suffix.lower() == ".json":
                return self._parse_json(file_path, symbol, cin)
            else:
                raise ValueError(f"Unsupported file type: {file_path.suffix}")

        except Exception as e:
            self.logger.error("Failed to parse financial file", file_path=str(file_path), error=str(e))
            raise

    def _parse_html(self, file_path: Path, symbol: str, cin: str | None) -> pl.DataFrame:
        """Parse HTML financial statement.

        Args:
            file_path: Path to HTML file
            symbol: Trading symbol
            cin: Corporate Identification Number

        Returns:
            Polars DataFrame
        """
        with open(file_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, "html.parser")
        financial_data = self._extract_financial_data(soup, symbol, cin)

        if not financial_data:
            self.logger.warning("No financial data found", file_path=str(file_path))
            return pl.DataFrame()

        # Compute ratios
        for record in financial_data:
            self._compute_ratios(record)

        df = pl.DataFrame(financial_data)
        self.logger.info("Parsed financial data", rows=len(df))
        return df

    def _parse_csv(self, file_path: Path, symbol: str, cin: str | None) -> pl.DataFrame:
        """Parse CSV financial statement.

        Args:
            file_path: Path to CSV file
            symbol: Trading symbol
            cin: Corporate Identification Number

        Returns:
            Polars DataFrame
        """
        # Placeholder for CSV parsing logic
        self.logger.info("CSV parsing not yet implemented")
        return pl.DataFrame()

    def _parse_json(self, file_path: Path, symbol: str, cin: str | None) -> pl.DataFrame:
        """Parse JSON financial statement.

        Args:
            file_path: Path to JSON file
            symbol: Trading symbol
            cin: Corporate Identification Number

        Returns:
            Polars DataFrame
        """
        # Placeholder for JSON parsing logic
        self.logger.info("JSON parsing not yet implemented")
        return pl.DataFrame()

    def _extract_financial_data(
        self, soup: BeautifulSoup, symbol: str, cin: str | None
    ) -> list[dict[str, Any]]:
        """Extract financial data from HTML.

        Args:
            soup: BeautifulSoup parsed HTML
            symbol: Trading symbol
            cin: Corporate Identification Number

        Returns:
            List of financial records
        """
        # This is a simplified placeholder. Actual parsing will depend on BSE/MCA HTML structure
        now = datetime.utcnow()
        
        record = {
            "event_id": str(uuid.uuid4()),
            "event_time": now,
            "ingest_time": now,
            "source": "BSE",
            "schema_version": "1.0.0",
            "entity_id": cin or symbol,
            "symbol": symbol,
            "company_name": None,
            "cin": cin,
            "period_end_date": date.today(),
            "period_type": "QUARTERLY",
            "statement_type": "STANDALONE",
            "filing_date": None,
            "revenue": None,
            "operating_profit": None,
            "net_profit": None,
            "total_assets": None,
            "total_liabilities": None,
            "equity": None,
            "total_debt": None,
            "current_assets": None,
            "current_liabilities": None,
            "cash_and_equivalents": None,
            "inventories": None,
            "depreciation": None,
            "interest_expense": None,
            "tax_expense": None,
            "eps": None,
            "book_value_per_share": None,
            "roe": None,
            "roa": None,
            "debt_to_equity": None,
            "current_ratio": None,
            "operating_margin": None,
            "net_margin": None,
            "year": date.today().year,
            "quarter": (date.today().month - 1) // 3 + 1,
            "metadata": {},
        }

        return [record]

    def _compute_ratios(self, record: dict[str, Any]) -> None:
        """Compute financial ratios and add to record.

        Args:
            record: Financial record (modified in place)
        """
        try:
            # ROE = (Net Profit / Equity) * 100
            if record["net_profit"] and record["equity"] and record["equity"] != 0:
                record["roe"] = (record["net_profit"] / record["equity"]) * 100

            # ROA = (Net Profit / Total Assets) * 100
            if record["net_profit"] and record["total_assets"] and record["total_assets"] != 0:
                record["roa"] = (record["net_profit"] / record["total_assets"]) * 100

            # Debt to Equity = Total Debt / Equity
            if record["total_debt"] and record["equity"] and record["equity"] != 0:
                record["debt_to_equity"] = record["total_debt"] / record["equity"]

            # Current Ratio = Current Assets / Current Liabilities
            if (
                record["current_assets"]
                and record["current_liabilities"]
                and record["current_liabilities"] != 0
            ):
                record["current_ratio"] = record["current_assets"] / record["current_liabilities"]

            # Operating Margin = (Operating Profit / Revenue) * 100
            if record["operating_profit"] and record["revenue"] and record["revenue"] != 0:
                record["operating_margin"] = (record["operating_profit"] / record["revenue"]) * 100

            # Net Margin = (Net Profit / Revenue) * 100
            if record["net_profit"] and record["revenue"] and record["revenue"] != 0:
                record["net_margin"] = (record["net_profit"] / record["revenue"]) * 100

        except Exception as e:
            self.logger.warning("Error computing ratios", error=str(e))

    def parse_batch(
        self, file_paths: list[Path], symbols: list[str], cins: list[str | None] | None = None
    ) -> pl.DataFrame:
        """Parse multiple financial statement files.

        Args:
            file_paths: List of file paths
            symbols: List of trading symbols (must match file_paths length)
            cins: Optional list of CINs (must match file_paths length)

        Returns:
            Combined DataFrame
        """
        if cins is None:
            cins = [None] * len(file_paths)

        dfs = []
        for file_path, symbol, cin in zip(file_paths, symbols, cins):
            try:
                df = self.parse(file_path, symbol, cin)
                if len(df) > 0:
                    dfs.append(df)
            except Exception as e:
                self.logger.error(
                    "Failed to parse file",
                    file_path=str(file_path),
                    error=str(e),
                )

        if not dfs:
            return pl.DataFrame()

        return pl.concat(dfs)


def compute_pe_ratio(
    financials_df: pl.DataFrame,
    ohlc_df: pl.DataFrame,
) -> pl.DataFrame:
    """Compute P/E ratio by joining financial and market data.

    Args:
        financials_df: DataFrame with quarterly financials (must have 'symbol', 'period_end_date', 'eps')
        ohlc_df: DataFrame with OHLC data (must have 'symbol', 'trade_date', 'close')

    Returns:
        DataFrame with P/E ratios
    """
    logger.info("Computing P/E ratios")

    # Join financials with closest market price
    # PE = Price / EPS
    result = (
        financials_df.join_asof(
            ohlc_df.rename({"TradDt": "trade_date", "ClsPric": "close"}),
            left_on="period_end_date",
            right_on="trade_date",
            by="symbol",
            strategy="nearest",
        )
        .with_columns(
            [
                (pl.col("close") / pl.col("eps")).alias("pe_ratio"),
            ]
        )
        .select(["symbol", "period_end_date", "eps", "close", "pe_ratio"])
    )

    return result
