"""Symbol Master Enrichment - Map FinInstrmId from bhavcopy to symbol master.

This module provides functionality to:
1. Parse bhavcopy data to extract unique instrument identifiers
2. Create enriched symbol master with canonical instrument IDs
3. Handle one-to-many ticker cases (e.g., IBULHSGFIN with EQ + multiple NCDs)
4. Map TckrSymb + SctySrs + FinInstrmId + ISIN to canonical instrument IDs

The enriched symbol master resolves ambiguity in ticker symbols by including
FinInstrmId as part of the canonical instrument ID.
"""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from src.utils.logger import get_logger

logger = get_logger(__name__)


class SymbolEnrichment:
    """Enriches symbol master with FinInstrmId and series information from bhavcopy."""

    def __init__(self) -> None:
        """Initialize the enrichment processor."""
        self.logger = get_logger(__name__)

    def enrich_from_bhavcopy(
        self, symbol_master_df: pl.DataFrame, bhavcopy_paths: list[Path]
    ) -> pl.DataFrame:
        """Enrich symbol master with FinInstrmId from bhavcopy data.

        Args:
            symbol_master_df: Symbol master DataFrame (from EQUITY_L)
            bhavcopy_paths: List of paths to bhavcopy Parquet files

        Returns:
            Enriched DataFrame with canonical instrument IDs including FinInstrmId

        This function:
        1. Reads bhavcopy data to get unique (TckrSymb, SctySrs, FinInstrmId, ISIN) combinations
        2. Joins with symbol master on (SYMBOL, ISIN) or just SYMBOL
        3. Creates canonical instrument IDs: symbol:fiid:exchange
        4. Handles one-to-many cases where one ticker has multiple instruments
        """
        self.logger.info(
            "Starting symbol master enrichment",
            bhavcopy_files=len(bhavcopy_paths),
            symbol_master_rows=len(symbol_master_df),
        )

        # Read all bhavcopy files to extract unique instruments
        bhavcopy_instruments = self._extract_unique_instruments(bhavcopy_paths)

        if len(bhavcopy_instruments) == 0:
            self.logger.warning("No instruments found in bhavcopy data, skipping enrichment")
            return symbol_master_df

        self.logger.info(
            "Extracted unique instruments from bhavcopy", instruments=len(bhavcopy_instruments)
        )

        # Rename symbol master columns to match bhavcopy for join
        symbol_master_normalized = symbol_master_df.rename(
            {
                "SYMBOL": "TckrSymb",
                "ISIN NUMBER": "ISIN",
                "SERIES": "SctySrs",
                "NAME OF COMPANY": "CompanyName",
                "FACE VALUE": "FaceValue",
                "PAID UP VALUE": "PaidUpValue",
                "MARKET LOT": "LotSize",
                "DATE OF LISTING": "ListingDate",
            }
        )

        # Join with bhavcopy instruments on TckrSymb and ISIN (where available)
        enriched = bhavcopy_instruments.join(
            symbol_master_normalized,
            on=["TckrSymb", "ISIN"],
            how="left",
            suffix="_master",
            coalesce=True,
        )

        # For instruments without ISIN match, try joining on just TckrSymb
        # This handles cases where ISIN might differ slightly or be missing
        missing_isin = enriched.filter(pl.col("CompanyName").is_null())
        if len(missing_isin) > 0:
            self.logger.info(
                "Attempting symbol-only join for instruments without ISIN match",
                count=len(missing_isin),
            )

            # Get the base columns from missing_isin (excluding the master columns that failed to match)
            base_cols = ["TckrSymb", "SctySrs", "FinInstrmId", "ISIN", "FinInstrmNm", "FinInstrmTp"]
            missing_base = missing_isin.select(base_cols)
            
            # Join on TckrSymb only for missing records
            symbol_only_join = missing_base.join(
                symbol_master_normalized.select(
                    ["TckrSymb", "CompanyName", "FaceValue", "PaidUpValue", "LotSize", "ListingDate"]
                ),
                on="TckrSymb",
                how="left",
                suffix="_sym",
                coalesce=True,
            )

            # Get the matched records with the same schema as enriched
            matched = enriched.filter(pl.col("CompanyName").is_not_null())
            
            # Ensure symbol_only_join has the same columns as matched
            # Add any missing columns with null values
            for col in matched.columns:
                if col not in symbol_only_join.columns:
                    symbol_only_join = symbol_only_join.with_columns(pl.lit(None).alias(col))
            
            # Select columns in the same order as matched
            symbol_only_join = symbol_only_join.select(matched.columns)
            
            # Merge back into enriched dataframe
            enriched = pl.concat([matched, symbol_only_join], how="vertical")

        # Create canonical instrument IDs: symbol:fiid:exchange
        enriched = enriched.with_columns(
            [
                (
                    pl.col("TckrSymb").cast(str)
                    + ":"
                    + pl.col("FinInstrmId").cast(str)
                    + ":NSE"
                ).alias("instrument_id"),
            ]
        )

        # Generate event metadata for each enriched record
        event_time = int(datetime.now().timestamp() * 1000)
        enriched = enriched.with_columns(
            [
                pl.lit(event_time).alias("event_time"),
                pl.lit(event_time).alias("ingest_time"),
                pl.lit("nse_symbol_master_enriched").alias("source"),
                pl.lit("v1").alias("schema_version"),
                pl.col("instrument_id").alias("entity_id"),
            ]
        )

        # Generate event IDs
        enriched = enriched.with_columns(
            [
                pl.col("instrument_id")
                .map_elements(
                    lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, f"nse_symbol_enriched:{x}")),
                    return_dtype=pl.Utf8,
                )
                .alias("event_id"),
            ]
        )

        self.logger.info("Symbol master enrichment complete", enriched_rows=len(enriched))

        return enriched

    def _extract_unique_instruments(self, bhavcopy_paths: list[Path]) -> pl.DataFrame:
        """Extract unique instrument combinations from bhavcopy data.

        Args:
            bhavcopy_paths: List of paths to bhavcopy Parquet files

        Returns:
            DataFrame with unique (TckrSymb, SctySrs, FinInstrmId, ISIN, FinInstrmNm) combinations
        """
        all_instruments = []

        for path in bhavcopy_paths:
            try:
                # Read bhavcopy file
                df = pl.read_parquet(path)

                # Select relevant columns for instrument identification
                instruments = df.select(
                    [
                        "TckrSymb",
                        "SctySrs",
                        "FinInstrmId",
                        "ISIN",
                        "FinInstrmNm",
                        "FinInstrmTp",
                    ]
                ).filter(
                    # Filter out null or empty symbols
                    pl.col("TckrSymb").is_not_null()
                    & (pl.col("TckrSymb") != "")
                    # Filter out null FinInstrmId (default to 0)
                    & pl.col("FinInstrmId").is_not_null()
                )

                all_instruments.append(instruments)

            except Exception as e:
                self.logger.error("Failed to read bhavcopy file", path=str(path), error=str(e))
                continue

        if not all_instruments:
            return pl.DataFrame()

        # Combine all instruments and get unique combinations
        combined = pl.concat(all_instruments)

        # Get unique combinations based on all key fields
        unique_instruments = combined.unique(
            subset=["TckrSymb", "SctySrs", "FinInstrmId", "ISIN"]
        )

        self.logger.info(
            "Extracted unique instruments",
            total_records=len(combined),
            unique_instruments=len(unique_instruments),
        )

        return unique_instruments

    def create_canonical_mapping(self, enriched_df: pl.DataFrame) -> pl.DataFrame:
        """Create canonical mapping table for joining with OHLC data.

        Args:
            enriched_df: Enriched symbol master DataFrame

        Returns:
            DataFrame with canonical mappings suitable for joining with OHLC

        The mapping includes:
        - TckrSymb + FinInstrmId -> instrument_id (for joining with bhavcopy)
        - ISIN -> instrument_id (for cross-reference)
        - Company name, sector, series metadata
        """
        mapping = enriched_df.select(
            [
                "instrument_id",
                "TckrSymb",
                "FinInstrmId",
                "SctySrs",
                "ISIN",
                "FinInstrmNm",
                "FinInstrmTp",
                "CompanyName",
                "FaceValue",
                "PaidUpValue",
                "LotSize",
                "ListingDate",
            ]
        )

        # Sort by symbol and FinInstrmId for easier verification
        mapping = mapping.sort(["TckrSymb", "FinInstrmId"])

        self.logger.info("Created canonical mapping", mappings=len(mapping))

        return mapping

    def verify_one_to_many_cases(self, enriched_df: pl.DataFrame) -> dict[str, int]:
        """Verify handling of one-to-many ticker cases.

        Args:
            enriched_df: Enriched symbol master DataFrame

        Returns:
            Dictionary with verification statistics

        This function identifies symbols like IBULHSGFIN that have multiple
        instruments (EQ + multiple NCD tranches) to verify they're all captured.
        """
        # Group by TckrSymb and count distinct FinInstrmIds
        symbol_counts = (
            enriched_df.group_by("TckrSymb")
            .agg(
                [
                    pl.col("FinInstrmId").n_unique().alias("instrument_count"),
                    pl.col("SctySrs").unique().alias("series_list"),
                    pl.col("FinInstrmNm").unique().alias("instrument_names"),
                ]
            )
            .sort("instrument_count", descending=True)
        )

        # Find symbols with multiple instruments
        multi_instrument_symbols = symbol_counts.filter(pl.col("instrument_count") > 1)

        self.logger.info(
            "One-to-many verification",
            total_symbols=len(symbol_counts),
            multi_instrument_symbols=len(multi_instrument_symbols),
        )

        # Log top cases for verification
        if len(multi_instrument_symbols) > 0:
            self.logger.info(
                "Top multi-instrument symbols",
                top_10=multi_instrument_symbols.head(10).to_dicts(),
            )

        return {
            "total_symbols": len(symbol_counts),
            "multi_instrument_symbols": len(multi_instrument_symbols),
            "total_instruments": len(enriched_df),
        }
