#!/bin/bash
set -e

# Default to yesterday if no date provided (portable across GNU and BSD date)
if [ -z "$SCRAPE_DATE" ]; then
    SCRAPE_DATE=$(python -c "from datetime import date, timedelta; print((date.today() - timedelta(days=1)).isoformat())")
fi

echo "Starting NSE Scraper"
echo "   Date: $SCRAPE_DATE"

# Wait for Schema Registry (and transitively Kafka) to become reachable
for i in $(seq 1 30); do
    if curl -sf "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; then
        echo "Schema Registry is up"
        break
    fi
    echo "Waiting for Schema Registry at ${SCHEMA_REGISTRY_URL} (attempt ${i}/30)"
    sleep 2
done

# If still not reachable after retries, exit early
if ! curl -sf "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; then
    echo "Schema Registry is not reachable at ${SCHEMA_REGISTRY_URL}"
    exit 1
fi

# Run the scraper without Typer
python run_scraper.py
