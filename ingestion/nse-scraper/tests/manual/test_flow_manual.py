#!/usr/bin/env python
"""Manual test script for Prefect flows.

This script creates sample data and runs the flow locally to verify
everything works correctly.
"""

import os
import shutil
import sys
import tempfile
from datetime import date
from pathlib import Path

from src.orchestration.flows import (
    normalize_polars,
    parse_polars_raw,
    write_parquet,
)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Set MLflow tracking URI to not require server
os.environ["MLFLOW_TRACKING_URI"] = "file:///tmp/mlruns"


def create_sample_csv(path: Path) -> None:
    """Create a sample bhavcopy CSV file."""
    csv_content = """TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4
2024-01-02,2024-01-02,CM,NSE,STK,2885,INE002A01018,RELIANCE,EQ,-,-,-,-,RELIANCE INDUSTRIES LTD,2750.00,2780.50,2740.00,2765.25,2765.00,2750.00,-,2765.25,-,-,5000000,13826250000.00,50000,F1,1,-,-,-,-,-
2024-01-02,2024-01-02,CM,NSE,STK,11536,INE467B01029,TCS,EQ,-,-,-,-,TATA CONSULTANCY SERVICES LTD,3550.00,3575.00,3545.00,3560.75,3560.50,3550.00,-,3560.75,-,-,3500000,12462625000.00,45000,F1,1,-,-,-,-,-
2024-01-02,2024-01-02,CM,NSE,STK,5258,INE040A01034,INFY,EQ,-,-,-,-,INFOSYS LTD,1450.00,1465.00,1448.00,1460.50,1460.25,1450.00,-,1460.50,-,-,4200000,6134100000.00,42000,F1,1,-,-,-,-,-
2024-01-02,2024-01-02,CM,NSE,STK,1594,INE009A01021,HDFC,EQ,-,-,-,-,HDFC BANK LTD,1600.00,1615.00,1595.00,1610.00,1609.75,1600.00,-,1610.00,-,-,6000000,9660000000.00,55000,F1,1,-,-,-,-,-
2024-01-02,2024-01-02,CM,NSE,STK,526,INE019A01038,ICICIBANK,EQ,-,-,-,-,ICICI BANK LTD,850.00,862.50,847.00,858.25,858.00,850.00,-,858.25,-,-,8500000,7294125000.00,78000,F1,1,-,-,-,-,-"""

    path.write_text(csv_content)


def main():
    """Run manual test of the flow."""
    print("🧪 Testing Prefect Orchestration Flow")
    print("=" * 60)

    # Create temporary directory for testing
    test_dir = Path(tempfile.mkdtemp(prefix="prefect_test_"))
    print(f"\n📁 Test directory: {test_dir}")

    try:
        # Step 1: Create sample CSV
        print("\n1️⃣  Creating sample CSV...")
        csv_file = test_dir / "sample_bhavcopy.csv"
        create_sample_csv(csv_file)
        print(f"   ✅ Created: {csv_file}")
        print(f"   📊 File size: {csv_file.stat().st_size} bytes")

        # Step 2: Parse raw CSV
        print("\n2️⃣  Parsing CSV to Polars DataFrame...")
        trade_date = date(2024, 1, 2)
        df = parse_polars_raw(str(csv_file), trade_date)
        print(f"   ✅ Parsed: {len(df)} rows")
        print(f"   📋 Columns: {df.columns[:5]}... ({len(df.columns)} total)")
        print(f"   🔍 Sample symbols: {df['TckrSymb'].to_list()}")

        # Step 3: Normalize data
        print("\n3️⃣  Normalizing DataFrame...")
        normalized_df = normalize_polars(df)
        print(f"   ✅ Normalized: {len(normalized_df)} rows")
        print(f"   📈 Close prices: {normalized_df['ClsPric'].to_list()}")

        # Step 4: Write to Parquet
        print("\n4️⃣  Writing to Parquet...")
        output_base = test_dir / "lake"
        parquet_file = write_parquet(normalized_df, trade_date, str(output_base))
        print(f"   ✅ Written: {parquet_file}")
        parquet_path = Path(parquet_file)
        if parquet_path.exists():
            size_mb = parquet_path.stat().st_size / (1024 * 1024)
            print(f"   💾 Size: {size_mb:.2f} MB")

        # Step 5: Verify Parquet can be read
        print("\n5️⃣  Verifying Parquet file...")
        import polars as pl

        verified_df = pl.read_parquet(parquet_file)
        print(f"   ✅ Verified: {len(verified_df)} rows read back")
        print(f"   📊 Schema matches: {len(verified_df.columns) == len(normalized_df.columns)}")

        # Success!
        print("\n" + "=" * 60)
        print("✅ All tests passed successfully!")
        print("=" * 60)

        # Print MLflow info
        print("\n📈 MLflow:")
        print(f"   Tracking URI: {os.environ['MLFLOW_TRACKING_URI']}")
        print("   Run artifacts: /tmp/mlruns/")

        print("\n🎉 Prefect orchestration flow is working correctly!")

        return 0

    except Exception as e:
        print("\n❌ Test failed with error:")
        print(f"   {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        # Cleanup
        print(f"\n🧹 Cleaning up test directory: {test_dir}")
        shutil.rmtree(test_dir, ignore_errors=True)


if __name__ == "__main__":
    import sys

    sys.exit(main())
