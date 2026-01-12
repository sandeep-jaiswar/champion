"""Parser for NSE Corporate Actions CSV files.

NSE provides corporate actions data in CSV format with columns like:
- SYMBOL, COMPANY, SERIES, FACE VALUE, PURPOSE, EX-DATE, RECORD DATE, etc.

This parser handles the NSE CA format and produces structured events.
"""

import re
from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import polars as pl

from champion.utils.logger import get_logger

logger = get_logger(__name__)

# NSE CA CSV schema (based on typical format)
CA_SCHEMA = {
    "SYMBOL": pl.Utf8,
    "COMPANY": pl.Utf8,
    "SERIES": pl.Utf8,
    "FACE VALUE": pl.Utf8,
    "PURPOSE": pl.Utf8,
    "EX-DATE": pl.Utf8,
    "RECORD DATE": pl.Utf8,
    "BC START DATE": pl.Utf8,
    "BC END DATE": pl.Utf8,
    "ND START DATE": pl.Utf8,
    "ND END DATE": pl.Utf8,
    "ACTUAL PAYMENT DATE": pl.Utf8,
}


class CorporateActionsParser:
    """Parser for NSE Corporate Actions CSV files."""

    def __init__(self):
        """Initialize parser."""
        self.action_type_patterns = {
            "SPLIT": [r"split", r"sub-division", r"subdivision"],
            "BONUS": [r"bonus", r"capitalisation"],
            "DIVIDEND": [r"dividend", r"div"],
            "RIGHTS": [r"rights"],
            "INTEREST_PAYMENT": [r"interest"],
            "EGMMEETING": [r"egm", r"agm", r"meeting"],
            "DEMERGER": [r"demerger", r"de-merger"],
            "MERGER": [r"merger", r"amalgamation"],
            "BUYBACK": [r"buy-back", r"buyback"],
        }

    def parse_action_type(self, purpose: str) -> str:
        """Determine action type from PURPOSE field.

        Args:
            purpose: PURPOSE field from NSE CA

        Returns:
            Action type enum value
        """
        purpose_lower = purpose.lower()

        for action_type, patterns in self.action_type_patterns.items():
            for pattern in patterns:
                if re.search(pattern, purpose_lower):
                    return action_type

        return "OTHER"

    def parse_split_ratio(self, purpose: str) -> dict[str, int] | None:
        """Extract split ratio from PURPOSE field.

        Examples:
            "Stock Split From Rs 10/- to Rs 2/- Per Share" -> (10, 2) -> (5, 1)
            "Sub-Division from Face Value of Rs. 10/- to Face Value of Rs. 2/-" -> (10, 2) -> (5, 1)

        Args:
            purpose: PURPOSE field

        Returns:
            Dict with old_shares and new_shares, or None
        """
        # Pattern: "Rs X to Rs Y" or "Rs. X/- to Rs. Y/-"
        match = re.search(
            r"rs\.?\s*(\d+(?:\.\d+)?)\s*/?\-?\s*to\s*rs\.?\s*(\d+(?:\.\d+)?)",
            purpose.lower(),
        )
        if match:
            old_fv = float(match.group(1))
            new_fv = float(match.group(2))
            # Split ratio = old_fv / new_fv
            # e.g., Rs 10 to Rs 2 means 1 share becomes 5 shares
            # old_shares = 1, new_shares = old_fv / new_fv = 5
            ratio = old_fv / new_fv
            return {"old_shares": 1, "new_shares": int(ratio)}

        return None

    def parse_bonus_ratio(self, purpose: str) -> dict[str, int] | None:
        """Extract bonus ratio from PURPOSE field.

        Examples:
            "Bonus issue 1:2" -> (1, 2)
            "1:1 Bonus" -> (1, 1)

        Args:
            purpose: PURPOSE field

        Returns:
            Dict with new_shares and existing_shares, or None
        """
        # Pattern: "X:Y" or "X for Y"
        match = re.search(r"(\d+)\s*(?:[:]\s*|for\s+)(\d+)", purpose.lower())
        if match:
            new_shares = int(match.group(1))
            existing_shares = int(match.group(2))
            return {
                "new_shares": new_shares,
                "existing_shares": existing_shares,
            }

        return None

    def parse_dividend_amount(self, purpose: str) -> float | None:
        """Extract dividend amount from PURPOSE field.

        Examples:
            "Dividend Rs. 15/- Per Share" -> 15.0
            "Final Dividend - Rs 10 per share" -> 10.0

        Args:
            purpose: PURPOSE field

        Returns:
            Dividend amount, or None
        """
        # Pattern: "Rs. X" or "Rs X"
        match = re.search(r"rs\.?\s*(\d+(?:\.\d+)?)\s*/?\-?\s*per\s*share", purpose.lower())
        if match:
            return float(match.group(1))

        return None

    def parse_date(self, date_str: str | None) -> date | None:
        """Parse date string to date object.

        Args:
            date_str: Date string in various formats

        Returns:
            Date object, or None
        """
        if not date_str or date_str.strip() == "" or date_str.strip() == "-":
            return None

        # Try common date formats
        formats = ["%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue

        logger.warning(f"Could not parse date: {date_str}")
        return None

    def compute_adjustment_factor(self, action_type: str, purpose: str) -> float:
        """Compute adjustment factor from CA details.

        Args:
            action_type: Type of corporate action
            purpose: PURPOSE field with ratio/amount details

        Returns:
            Adjustment factor (1.0 = no adjustment)
        """
        if action_type == "SPLIT":
            ratio = self.parse_split_ratio(purpose)
            if ratio:
                return ratio["new_shares"] / ratio["old_shares"]

        elif action_type == "BONUS":
            ratio = self.parse_bonus_ratio(purpose)
            if ratio:
                return (ratio["existing_shares"] + ratio["new_shares"]) / ratio["existing_shares"]

        # Dividend adjustment requires close price, so default to 1.0
        return 1.0

    def parse_to_dataframe(
        self, file_path: Path, source: str = "nse_corporate_actions"
    ) -> pl.DataFrame:
        """Parse NSE CA CSV file to Polars DataFrame.

        Args:
            file_path: Path to CSV file
            source: Source identifier

        Returns:
            DataFrame with parsed CA events
        """
        logger.info(f"Parsing CA file: {file_path}")

        # Read CSV with schema
        try:
            df = pl.read_csv(
                file_path,
                schema_overrides=CA_SCHEMA,
                ignore_errors=True,
            )
        except Exception as e:
            logger.error(f"Failed to read CSV: {e}")
            raise

        # Parse dates
        df = df.with_columns(
            [
                pl.col("EX-DATE")
                .map_elements(self.parse_date, return_dtype=pl.Date)
                .alias("ex_date"),
                pl.col("RECORD DATE")
                .map_elements(self.parse_date, return_dtype=pl.Date)
                .alias("record_date"),
                pl.col("BC START DATE")
                .map_elements(self.parse_date, return_dtype=pl.Date)
                .alias("bc_start_date"),
                pl.col("BC END DATE")
                .map_elements(self.parse_date, return_dtype=pl.Date)
                .alias("bc_end_date"),
            ]
        )

        # Parse action type
        df = df.with_columns(
            [
                pl.col("PURPOSE")
                .map_elements(self.parse_action_type, return_dtype=pl.Utf8)
                .alias("action_type"),
            ]
        )

        # Compute adjustment factors
        df = df.with_columns(
            [
                pl.struct(["action_type", "PURPOSE"])
                .map_elements(
                    lambda row: self.compute_adjustment_factor(row["action_type"], row["PURPOSE"]),
                    return_dtype=pl.Float64,
                )
                .alias("adjustment_factor"),
            ]
        )

        # Add metadata
        ingest_time = datetime.now()
        df = df.with_columns(
            [
                pl.lit(str(uuid4())).alias("event_id"),
                pl.lit(int(ingest_time.timestamp() * 1000)).alias("event_time"),
                pl.lit(int(ingest_time.timestamp() * 1000)).alias("ingest_time"),
                pl.lit(source).alias("source"),
                pl.lit("v1").alias("schema_version"),
                (pl.col("SYMBOL") + pl.lit(":NSE")).alias("entity_id"),
            ]
        )

        logger.info(f"Parsed {len(df)} CA events")
        return df

    def write_parquet(
        self, df: pl.DataFrame, output_path: Path, partition_by_year: bool = True
    ) -> Path:
        """Write CA data to Parquet.

        Args:
            df: DataFrame with CA events
            output_path: Base output path
            partition_by_year: Whether to partition by year

        Returns:
            Path to written file(s)
        """
        if partition_by_year:
            # Partition by year
            for year in df["ex_date"].dt.year().unique().sort():
                year_df = df.filter(pl.col("ex_date").dt.year() == year)
                partition_path = output_path / "raw" / "corporate_actions" / f"year={year}"
                partition_path.mkdir(parents=True, exist_ok=True)

                output_file = partition_path / "data.parquet"
                year_df.write_parquet(output_file, compression="snappy", use_pyarrow=True)
                logger.info(f"Wrote {len(year_df)} CA events to {output_file}")

            return output_path
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            output_file = output_path / "corporate_actions.parquet"
            df.write_parquet(output_file, compression="snappy", use_pyarrow=True)
            logger.info(f"Wrote {len(df)} CA events to {output_file}")
            return output_file
