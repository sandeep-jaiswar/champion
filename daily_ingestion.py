#!/usr/bin/env python3
"""
Daily ingestion script for bulk/block deals.
Run this daily after market close (e.g., via cron at 22:00 IST).
Usage: poetry run python daily_ingestion.py [--date YYYY-MM-DD]
"""
import os
import sys
from datetime import datetime, timedelta
import subprocess

def run_daily_ingestion(target_date: str | None = None):
    """Run daily ingestion for bulk/block deals."""
    
    if target_date is None:
        # Run for yesterday (since market closes EOD)
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"Starting daily ingestion for {target_date}...")
    print("=" * 80)
    
    # Set environment
    env = os.environ.copy()
    env["MLFLOW_TRACKING_URI"] = "file:/media/sandeep-jaiswar/DataDrive/champion/mlruns"
    
    # Run single-day ETL
    cmd = [
        "poetry", "run", "champion",
        "etl-bulk-deals",
        "--start-date", target_date,
        "--end-date", target_date
    ]
    
    result = subprocess.run(cmd, env=env, cwd="/media/sandeep-jaiswar/DataDrive/champion")
    
    if result.returncode == 0:
        print("=" * 80)
        print(f"✅ Daily ingestion completed for {target_date}")
        return True
    else:
        print("=" * 80)
        print(f"❌ Daily ingestion failed for {target_date}")
        return False

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    if not run_daily_ingestion(date_arg):
        sys.exit(1)
