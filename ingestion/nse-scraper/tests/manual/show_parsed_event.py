"""Sample event structure viewer.

Shows the parsed event structure from a bhavcopy file.

Usage:
    python -m tests.manual.show_parsed_event [--file PATH] [--date YYYY-MM-DD]
"""

import json
from pathlib import Path
from datetime import date
import argparse

from src.parsers.bhavcopy_parser import BhavcopyParser
from src.utils.logger import configure_logging

configure_logging()


def main():
    """Display parsed event structure."""
    parser = argparse.ArgumentParser(description="View parsed event structure")
    parser.add_argument(
        "--file",
        type=Path,
        default=Path("data/BhavCopy_NSE_CM_20240102.csv"),
        help="Path to bhavcopy CSV file"
    )
    parser.add_argument(
        "--date",
        type=str,
        default="2024-01-02",
        help="Trade date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of sample events to show"
    )
    args = parser.parse_args()
    
    # Parse date
    trade_date = date.fromisoformat(args.date)
    
    # Parse file
    bhavcopy_parser = BhavcopyParser()
    events = bhavcopy_parser.parse(args.file, trade_date)
    
    print("\n" + "=" * 80)
    print(f"PARSED EVENT STRUCTURE - {args.file.name}")
    print("=" * 80)
    
    # Show sample events
    for i in range(min(args.count, len(events))):
        print(f"\n[Event {i+1}/{args.count}]")
        print(json.dumps(events[i], indent=2, default=str))
    
    print("\n" + "=" * 80)
    print("EVENT STATISTICS")
    print("=" * 80)
    print(f"Total events parsed: {len(events)}")
    print(f"Event ID format: UUID5 (deterministic)")
    print(f"Entity ID format: SYMBOL:EXCHANGE")
    print(f"Source: nse_cm_bhavcopy")
    print(f"Schema version: v1")
    
    # Show symbol distribution
    symbols = [e['entity_id'].split(':')[0] for e in events[:10]]
    print(f"\nFirst 10 symbols: {', '.join(symbols)}")
    
    # Payload field statistics
    payload = events[0]['payload']
    non_null_fields = sum(1 for v in payload.values() if v is not None)
    print(f"\nPayload fields: {len(payload)} total, {non_null_fields} non-null in sample")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
