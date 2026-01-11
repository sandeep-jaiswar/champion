"""Quick test script to verify the pipeline works."""

# Configure before importing anything else
import os
from datetime import datetime
from pathlib import Path

os.environ.setdefault("DATA_DIR", "./data")

from src.config import config
from src.parsers.bhavcopy_parser import BhavcopyParser
from src.producers.avro_producer import AvroProducer


def test_pipeline():
    """Test the full pipeline with sample data."""
    print("🚀 Testing NSE Scraper Pipeline...")

    # Use existing sample data
    data_file = Path("data/BhavCopy_NSE_CM_20240102.csv")
    if not data_file.exists():
        print(f"❌ Sample data file not found: {data_file}")
        return

    print(f"📁 Using sample data: {data_file}")

    # Parse the file
    print("📊 Parsing bhavcopy file...")
    parser = BhavcopyParser()
    trade_date = datetime(2024, 1, 2).date()
    events = parser.parse(data_file, trade_date)

    print(f"✅ Parsed {len(events)} events")

    if events:
        print("\n📋 Sample event:")
        print(f"   Entity ID: {events[0]['entity_id']}")
        print(f"   Source: {events[0]['source']}")
        print(f"   Symbol: {events[0]['payload']['TckrSymb']}")
        print(f"   Close Price: {events[0]['payload']['ClsPric']}")

    # Test Kafka producer
    print(f"\n📤 Producing to Kafka topic: {config.topics.raw_ohlc}")
    producer = AvroProducer(topic=config.topics.raw_ohlc, schema_type="raw_equity_ohlc")

    success_count = 0
    for event in events:
        try:
            producer.produce(event)
            success_count += 1
        except Exception as e:
            print(f"❌ Failed to produce event: {e}")
            break

    print(f"✅ Produced {success_count} events")

    # Flush producer
    print("⏳ Flushing producer...")
    remaining = producer.flush()

    if remaining == 0:
        print("✅ All events delivered successfully!")
        print(f"\n🎉 Pipeline test PASSED! {success_count} events sent to Kafka.")
    else:
        print(f"⚠️  {remaining} events still pending after flush timeout")


if __name__ == "__main__":
    test_pipeline()
