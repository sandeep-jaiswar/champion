"""Parser for BSE shareholding pattern data.

Parses HTML/data from BSE shareholding disclosures into structured format.
"""

import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from bs4 import BeautifulSoup

from champion.parsers.base_parser import Parser
from champion.utils.logger import get_logger

logger = get_logger(__name__)


class ShareholdingPatternParser(Parser):
    """Parser for BSE shareholding pattern data.

    Attributes:
        SCHEMA_VERSION: Parser schema version for tracking compatibility.
    """

    SCHEMA_VERSION = "v1.0"

    def __init__(self) -> None:
        """Initialize parser."""
        self.logger = get_logger(__name__)

    def parse(self, file_path: Path, symbol: str, scrip_code: str) -> pl.DataFrame:
        """Parse shareholding pattern file into structured DataFrame.

        Args:
            file_path: Path to HTML file
            symbol: Trading symbol
            scrip_code: BSE scrip code

        Returns:
            Polars DataFrame with shareholding data
        """
        self.logger.info("Parsing shareholding file", file_path=str(file_path))

        try:
            with open(file_path, encoding="utf-8") as f:
                html_content = f.read()

            soup = BeautifulSoup(html_content, "html.parser")

            # Extract shareholding data from tables
            # Note: This is a simplified parser. Actual BSE HTML structure may vary.
            shareholding_data = self._extract_shareholding_data(soup, symbol, scrip_code)

            if not shareholding_data:
                self.logger.warning("No shareholding data found", file_path=str(file_path))
                return pl.DataFrame()

            # Convert to DataFrame
            df = pl.DataFrame(shareholding_data)
            self.logger.info("Parsed shareholding data", rows=len(df))
            return df

        except Exception as e:
            self.logger.error(
                "Failed to parse shareholding file", file_path=str(file_path), error=str(e)
            )
            raise

    def _extract_shareholding_data(
        self, soup: BeautifulSoup, symbol: str, scrip_code: str
    ) -> list[dict[str, Any]]:
        """Extract shareholding data from BeautifulSoup object.

        Args:
            soup: BeautifulSoup parsed HTML
            symbol: Trading symbol
            scrip_code: BSE scrip code

        Returns:
            List of shareholding records
        """
        records = []

        # Find tables containing shareholding data
        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                # Try to extract shareholding categories and percentages
                category = cells[0].get_text(strip=True)
                try:
                    percentage = float(cells[1].get_text(strip=True).replace("%", "").strip())
                except (ValueError, IndexError):
                    continue

                # Map categories to schema fields
                record = self._map_category_to_record(category, percentage, symbol, scrip_code)
                if record:
                    records.append(record)

        # Consolidate records
        if records:
            return [self._consolidate_records(records)]
        return []

    def _map_category_to_record(
        self, category: str, percentage: float, symbol: str, scrip_code: str
    ) -> dict[str, Any] | None:
        """Map shareholding category to record field.

        Args:
            category: Shareholding category name
            percentage: Shareholding percentage
            symbol: Trading symbol
            scrip_code: BSE scrip code

        Returns:
            Partial record or None
        """
        category_lower = category.lower()

        field_mapping = {
            "promoter": "promoter_shareholding_percent",
            "public": "public_shareholding_percent",
            "institutional": "institutional_shareholding_percent",
            "fii": "fii_shareholding_percent",
            "dii": "dii_shareholding_percent",
            "mutual fund": "mutual_fund_shareholding_percent",
            "insurance": "insurance_companies_percent",
            "bank": "banks_shareholding_percent",
            "employee": "employee_shareholding_percent",
        }

        for key, field in field_mapping.items():
            if key in category_lower:
                return {"category": category, "field": field, "value": percentage}

        return None

    def _consolidate_records(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Consolidate multiple category records into single shareholding record.

        Args:
            records: List of category records

        Returns:
            Consolidated shareholding record
        """
        now = datetime.utcnow()
        consolidated = {
            "event_id": str(uuid.uuid4()),
            "event_time": now,
            "ingest_time": now,
            "source": "BSE",
            "schema_version": "1.0.0",
            "entity_id": "",
            "symbol": "",
            "company_name": None,
            "scrip_code": None,
            "isin": None,
            "quarter_end_date": date.today(),
            "filing_date": None,
            "promoter_shareholding_percent": None,
            "promoter_shares": None,
            "public_shareholding_percent": None,
            "public_shares": None,
            "institutional_shareholding_percent": None,
            "institutional_shares": None,
            "fii_shareholding_percent": None,
            "fii_shares": None,
            "dii_shareholding_percent": None,
            "dii_shares": None,
            "mutual_fund_shareholding_percent": None,
            "mutual_fund_shares": None,
            "insurance_companies_percent": None,
            "insurance_companies_shares": None,
            "banks_shareholding_percent": None,
            "banks_shares": None,
            "employee_shareholding_percent": None,
            "employee_shares": None,
            "total_shares_outstanding": None,
            "pledged_promoter_shares_percent": None,
            "pledged_promoter_shares": None,
            "year": date.today().year,
            "quarter": (date.today().month - 1) // 3 + 1,
            "metadata": {},
        }

        # Fill in values from records
        for record in records:
            field = record.get("field")
            value = record.get("value")
            if field and value is not None:
                consolidated[field] = value

        return consolidated

    def parse_batch(
        self, file_paths: list[Path], symbols: list[str], scrip_codes: list[str]
    ) -> pl.DataFrame:
        """Parse multiple shareholding files.

        Args:
            file_paths: List of file paths
            symbols: List of trading symbols (must match file_paths length)
            scrip_codes: List of BSE scrip codes (must match file_paths length)

        Returns:
            Combined DataFrame
        """
        dfs = []
        for file_path, symbol, scrip_code in zip(file_paths, symbols, scrip_codes, strict=False):
            try:
                df = self.parse(file_path, symbol, scrip_code)
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
