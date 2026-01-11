"""Integration test for symbol master enrichment workflow.

This test demonstrates the full enrichment pipeline:
1. Parse symbol master (EQUITY_L.csv)
2. Generate sample bhavcopy data
3. Enrich symbol master with FinInstrmId
4. Verify one-to-many cases
5. Create canonical mappings
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from src.parsers.symbol_enrichment import SymbolEnrichment


class TestSymbolMasterEnrichmentIntegration:
    """Integration tests for symbol master enrichment."""

    @pytest.fixture
    def sample_equity_l_csv(self) -> Path:
        """Create a sample EQUITY_L.csv file for testing."""
        csv_content = """SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
RELIANCE,Reliance Industries Limited,EQ,29-Nov-1977,10,1,INE002A01018,10
IBULHSGFIN,Indiabulls Housing Finance Limited,EQ,10-Jul-2013,2,1,INE148I01020,2
HDFCBANK,HDFC Bank Limited,EQ,08-Nov-1995,1,1,INE040A01034,1
TCS,Tata Consultancy Services Limited,EQ,25-Aug-2004,1,1,INE467B01029,1
INFY,Infosys Limited,EQ,08-Feb-1995,5,1,INE009A01021,5
"""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        temp_file.write(csv_content)
        temp_file.flush()
        return Path(temp_file.name)

    @pytest.fixture
    def sample_bhavcopy_parquets(self, tmp_path) -> list[Path]:
        """Create sample bhavcopy Parquet files for testing."""
        # Create sample data representing different instruments
        data1 = {
            "TckrSymb": ["RELIANCE", "IBULHSGFIN", "IBULHSGFIN", "HDFCBANK"],
            "SctySrs": ["EQ", "EQ", "D1", "EQ"],
            "FinInstrmId": [2885, 30125, 14678, 1333],
            "ISIN": ["INE002A01018", "INE148I01020", "INE148I08023", "INE040A01034"],
            "FinInstrmNm": [
                "Reliance Industries Limited",
                "Indiabulls Housing Finance Limited",
                "Indiabulls Housing Finance - NCD SR.I",
                "HDFC Bank Limited",
            ],
            "FinInstrmTp": ["EQ", "EQ", "D1", "EQ"],
        }

        data2 = {
            "TckrSymb": ["TCS", "INFY", "IBULHSGFIN", "IBULHSGFIN"],
            "SctySrs": ["EQ", "EQ", "D2", "D3"],
            "FinInstrmId": [11536, 1594, 17505, 18901],
            "ISIN": ["INE467B01029", "INE009A01021", "INE148I08031", "INE148I08049"],
            "FinInstrmNm": [
                "Tata Consultancy Services Limited",
                "Infosys Limited",
                "Indiabulls Housing Finance - NCD SR.II",
                "Indiabulls Housing Finance - NCD SR.III",
            ],
            "FinInstrmTp": ["EQ", "EQ", "D2", "D3"],
        }

        # Write to Parquet files
        paths = []
        for i, data in enumerate([data1, data2]):
            df = pl.DataFrame(data)
            path = tmp_path / f"bhavcopy_{i}.parquet"
            df.write_parquet(path)
            paths.append(path)

        return paths

    def test_full_enrichment_workflow(self, sample_equity_l_csv, sample_bhavcopy_parquets):
        """Test the full enrichment workflow from EQUITY_L to enriched master."""
        # Step 1: Parse symbol master
        df_symbol_master = pl.read_csv(
            sample_equity_l_csv,
            null_values=["-", "", "null", "NULL", "N/A", "NA"],
        )

        assert len(df_symbol_master) == 5
        assert "SYMBOL" in df_symbol_master.columns
        assert "ISIN NUMBER" in df_symbol_master.columns

        # Step 2: Enrich with bhavcopy data
        enricher = SymbolEnrichment()
        enriched_df = enricher.enrich_from_bhavcopy(df_symbol_master, sample_bhavcopy_parquets)

        # Verify enrichment succeeded
        assert len(enriched_df) > 0
        assert "instrument_id" in enriched_df.columns
        assert "TckrSymb" in enriched_df.columns
        assert "FinInstrmId" in enriched_df.columns

    def test_ibulhsgfin_multiple_instruments(self, sample_equity_l_csv, sample_bhavcopy_parquets):
        """Test that IBULHSGFIN correctly shows multiple instruments."""
        # Parse and enrich
        df_symbol_master = pl.read_csv(sample_equity_l_csv, null_values=["-", "", "null"])
        enricher = SymbolEnrichment()
        enriched_df = enricher.enrich_from_bhavcopy(df_symbol_master, sample_bhavcopy_parquets)

        # Filter for IBULHSGFIN
        ibulhsgfin_instruments = enriched_df.filter(pl.col("TckrSymb") == "IBULHSGFIN")

        # Should have 4 instruments: EQ, D1, D2, D3
        assert len(ibulhsgfin_instruments) == 4

        # Verify series are distinct
        series_list = ibulhsgfin_instruments["SctySrs"].to_list()
        assert "EQ" in series_list
        assert "D1" in series_list
        assert "D2" in series_list
        assert "D3" in series_list

        # Verify FinInstrmIds are distinct
        fiids = ibulhsgfin_instruments["FinInstrmId"].to_list()
        assert len(set(fiids)) == 4
        assert 30125 in fiids  # EQ
        assert 14678 in fiids  # D1
        assert 17505 in fiids  # D2
        assert 18901 in fiids  # D3

        # Verify instrument_ids are unique
        instrument_ids = ibulhsgfin_instruments["instrument_id"].to_list()
        assert len(set(instrument_ids)) == 4
        assert "IBULHSGFIN:30125:NSE" in instrument_ids
        assert "IBULHSGFIN:14678:NSE" in instrument_ids
        assert "IBULHSGFIN:17505:NSE" in instrument_ids
        assert "IBULHSGFIN:18901:NSE" in instrument_ids

    def test_canonical_mapping_creation(self, sample_equity_l_csv, sample_bhavcopy_parquets):
        """Test creation of canonical mapping table."""
        # Parse and enrich
        df_symbol_master = pl.read_csv(sample_equity_l_csv, null_values=["-", "", "null"])
        enricher = SymbolEnrichment()
        enriched_df = enricher.enrich_from_bhavcopy(df_symbol_master, sample_bhavcopy_parquets)

        # Create canonical mapping
        mapping = enricher.create_canonical_mapping(enriched_df)

        # Verify mapping structure
        assert "instrument_id" in mapping.columns
        assert "TckrSymb" in mapping.columns
        assert "FinInstrmId" in mapping.columns
        assert "ISIN" in mapping.columns
        assert "CompanyName" in mapping.columns

        # Verify all instruments are present
        assert len(mapping) == len(enriched_df)

        # Verify instrument_id uniqueness
        unique_ids = mapping["instrument_id"].n_unique()
        assert unique_ids == len(mapping)

    def test_one_to_many_verification(self, sample_equity_l_csv, sample_bhavcopy_parquets):
        """Test verification of one-to-many ticker cases."""
        # Parse and enrich
        df_symbol_master = pl.read_csv(sample_equity_l_csv, null_values=["-", "", "null"])
        enricher = SymbolEnrichment()
        enriched_df = enricher.enrich_from_bhavcopy(df_symbol_master, sample_bhavcopy_parquets)

        # Verify one-to-many cases
        stats = enricher.verify_one_to_many_cases(enriched_df)

        # Should have statistics
        assert "total_symbols" in stats
        assert "multi_instrument_symbols" in stats
        assert "total_instruments" in stats

        # IBULHSGFIN should be counted as multi-instrument
        assert stats["multi_instrument_symbols"] >= 1

        # Total instruments should be more than total symbols
        assert stats["total_instruments"] >= stats["total_symbols"]

    def test_join_with_ohlc_data(self, sample_equity_l_csv, sample_bhavcopy_parquets):
        """Test that enriched master can be joined with OHLC data."""
        # Parse and enrich
        df_symbol_master = pl.read_csv(sample_equity_l_csv, null_values=["-", "", "null"])
        enricher = SymbolEnrichment()
        enriched_df = enricher.enrich_from_bhavcopy(df_symbol_master, sample_bhavcopy_parquets)

        # Create sample OHLC data
        ohlc_data = {
            "TradDt": ["2024-01-15"] * 4,
            "TckrSymb": ["RELIANCE", "IBULHSGFIN", "IBULHSGFIN", "HDFCBANK"],
            "FinInstrmId": [2885, 30125, 14678, 1333],
            "ClsPric": [2800.50, 150.25, 9.65, 1550.75],
            "TtlTradgVol": [1000000, 50000, 100, 750000],
        }
        df_ohlc = pl.DataFrame(ohlc_data)

        # Create computed instrument_id in OHLC
        df_ohlc = df_ohlc.with_columns(
            [
                (
                    pl.col("TckrSymb").cast(str) + ":" + pl.col("FinInstrmId").cast(str) + ":NSE"
                ).alias("computed_instrument_id")
            ]
        )

        # Join with enriched master
        joined = df_ohlc.join(
            enriched_df.select(["instrument_id", "CompanyName", "SctySrs", "FinInstrmNm"]),
            left_on="computed_instrument_id",
            right_on="instrument_id",
            how="left",
            coalesce=True,
        )

        # Verify all rows matched
        assert len(joined) == len(df_ohlc)
        assert joined["CompanyName"].null_count() == 0

        # Verify IBULHSGFIN instruments have different instrument names
        ibulhsgfin_joined = joined.filter(pl.col("TckrSymb") == "IBULHSGFIN")
        # Should have 2 different FinInstrmNm values
        fin_instrm_names = ibulhsgfin_joined["FinInstrmNm"].to_list()
        assert len(set(fin_instrm_names)) == 2  # EQ and NCD have different instrument names

        # Verify one is equity and one is debt
        series_list = ibulhsgfin_joined["SctySrs"].to_list()
        assert "EQ" in series_list
        assert "D1" in series_list

    def test_enrichment_handles_missing_isin(self, tmp_path):
        """Test enrichment handles cases where ISIN doesn't match."""
        # Create symbol master with one ISIN
        equity_l_content = """SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE
TESTCO,Test Company Limited,EQ,01-Jan-2000,10,1,INE000000001,10
"""
        equity_l_path = tmp_path / "EQUITY_L.csv"
        equity_l_path.write_text(equity_l_content)

        # Create bhavcopy with different ISIN (should still match on symbol)
        bhavcopy_data = {
            "TckrSymb": ["TESTCO"],
            "SctySrs": ["EQ"],
            "FinInstrmId": [12345],
            "ISIN": ["INE999999999"],  # Different ISIN
            "FinInstrmNm": ["Test Company Limited"],
            "FinInstrmTp": ["EQ"],
        }
        bhavcopy_path = tmp_path / "bhavcopy.parquet"
        pl.DataFrame(bhavcopy_data).write_parquet(bhavcopy_path)

        # Parse and enrich
        df_symbol_master = pl.read_csv(equity_l_path, null_values=["-", "", "null"])
        enricher = SymbolEnrichment()
        enriched_df = enricher.enrich_from_bhavcopy(df_symbol_master, [bhavcopy_path])

        # Should still enrich via symbol fallback
        assert len(enriched_df) == 1
        assert enriched_df["TckrSymb"][0] == "TESTCO"
        assert enriched_df["FinInstrmId"][0] == 12345
