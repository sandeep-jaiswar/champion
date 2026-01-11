#!/usr/bin/env python
"""Example script demonstrating Prefect deployment and scheduling.

This script shows how to:
1. Create a Prefect deployment with scheduling
2. Trigger manual runs
3. View flow runs and results

Usage:
    python examples/run_prefect_deployment.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def show_usage():
    """Show usage instructions."""
    print("\n" + "=" * 60)
    print("📚 How to Use Prefect Orchestration")
    print("=" * 60)

    print("\n1️⃣  Start Prefect Server (in separate terminal):")
    print("   $ prefect server start")
    print("   → Access UI at http://localhost:4200")

    print("\n2️⃣  Create Deployment:")
    print("   $ cd ingestion/nse-scraper")
    print("   $ python -m src.orchestration.flows deploy")
    print("   → Creates scheduled deployment (weekdays 6pm IST)")

    print("\n3️⃣  Start Agent (in separate terminal):")
    print("   $ prefect agent start -q default")
    print("   → Agent will pick up scheduled and manual runs")

    print("\n4️⃣  Trigger Manual Run:")
    print("   $ python -m src.orchestration.flows")
    print("   → Runs flow for yesterday's date")

    print("\n5️⃣  View Results:")
    print("   → Prefect UI: http://localhost:4200")
    print("   → MLflow UI: http://localhost:5000 (if running)")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    show_usage()
