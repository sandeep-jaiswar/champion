#!/usr/bin/env python
"""Run the NSE bhavcopy ETL pipeline."""

import os
from datetime import date, timedelta

# Set MLflow tracking URI
os.environ["MLFLOW_TRACKING_URI"] = "http://localhost:5000"

from src.orchestration.flows import nse_bhavcopy_etl_flow


def main():
    """Execute the ETL pipeline."""
    # Use yesterday's date (today - 1 day)
    trade_date = date.today() - timedelta(days=1)

    print("🚀 Starting NSE Bhavcopy ETL Pipeline")
    print(f"📅 Trade Date: {trade_date}")
    print(f"📊 MLflow Tracking: {os.environ['MLFLOW_TRACKING_URI']}")
    print("-" * 60)

    # Run the flow
    result = nse_bhavcopy_etl_flow(
        trade_date=trade_date,
        load_to_clickhouse=False,  # Set to True if ClickHouse is ready
        start_metrics_server_flag=True,
    )

    print("\n" + "=" * 60)
    print(f"✅ Pipeline Status: {result['status']}")
    print(f"📈 Rows Processed: {result.get('rows_processed', 'N/A')}")
    print(f"📁 Parquet File: {result.get('parquet_file', 'N/A')}")
    print("=" * 60)

    if result["status"] == "success":
        print("\n🎉 ETL Pipeline completed successfully!")
        print("\n📊 View metrics at: http://localhost:9090/metrics")
        print("🔬 View MLflow UI at: http://localhost:5000")
        return 0
    else:
        print("\n❌ Pipeline failed")
        return 1


if __name__ == "__main__":
    exit(main())
