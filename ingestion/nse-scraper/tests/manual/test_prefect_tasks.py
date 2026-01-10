"""Quick test to verify Prefect tasks work correctly."""

from datetime import date
from pathlib import Path
import tempfile

# Configure environment before importing tasks
import os
os.environ.setdefault("DATA_DIR", "./data")

from src.tasks.bhavcopy_tasks import parse_bhavcopy_to_parquet, parse_bhavcopy_to_events


def test_prefect_task_stub():
    """Test that Prefect task can be called successfully."""
    print("🧪 Testing Prefect task stub...")

    # Create test CSV
    test_dir = Path(tempfile.mkdtemp())
    csv_file = test_dir / "test_bhavcopy.csv"

    csv_content = """TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4
2024-01-02,2024-01-02,CM,NSE,STK,2885,INE002A01018,RELIANCE,EQ,-,-,-,-,RELIANCE INDUSTRIES LTD,2750.00,2780.50,2740.00,2765.25,2765.00,2750.00,-,2765.25,-,-,5000000,13826250000.00,50000,F1,1,-,-,-,-,-
2024-01-02,2024-01-02,CM,NSE,STK,11536,INE467B01029,TCS,EQ,-,-,-,-,TATA CONSULTANCY SERVICES LTD,3550.00,3575.00,3545.00,3560.75,3560.50,3550.00,-,3560.75,-,-,3500000,12462625000.00,45000,F1,1,-,-,-,-,-"""

    csv_file.write_text(csv_content)

    try:
        # Test parse to Parquet task
        print("\n📦 Testing parse_bhavcopy_to_parquet task...")
        output_file = parse_bhavcopy_to_parquet(
            csv_file_path=str(csv_file),
            trade_date="2024-01-02",
            output_base_path=str(test_dir),
        )
        print(f"✅ Parquet written to: {output_file}")
        assert Path(output_file).exists()

        # Test parse to events task
        print("\n📤 Testing parse_bhavcopy_to_events task...")
        events = parse_bhavcopy_to_events(
            csv_file_path=str(csv_file),
            trade_date="2024-01-02",
        )
        print(f"✅ Parsed {len(events)} events")
        assert len(events) == 2

        # Verify event structure
        assert "event_id" in events[0]
        assert "payload" in events[0]
        assert events[0]["source"] == "nse_cm_bhavcopy"

        print("\n✅ All Prefect tasks work correctly!")

    finally:
        # Cleanup
        import shutil
        shutil.rmtree(test_dir)


if __name__ == "__main__":
    test_prefect_task_stub()
