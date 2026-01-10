#!/bin/bash
# Check Champion System Status

echo "🔍 Champion System Status Check"
echo "=================================================="
echo ""

echo "📦 Docker Services:"
echo "--------------------------------------------------"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "❌ Docker not running or not accessible"
echo ""

echo "🔗 Service Health Checks:"
echo "--------------------------------------------------"

# MLflow
if curl -s http://localhost:5000/health > /dev/null 2>&1; then
    echo "✅ MLflow UI       : http://localhost:5000 (Running)"
else
    echo "❌ MLflow UI       : http://localhost:5000 (Not accessible)"
fi

# ClickHouse HTTP
if curl -s http://localhost:8123/ping > /dev/null 2>&1; then
    echo "✅ ClickHouse HTTP : http://localhost:8123 (Running)"
else
    echo "❌ ClickHouse HTTP : http://localhost:8123 (Not accessible)"
fi

# ClickHouse Native
if timeout 2 bash -c "echo 'SELECT 1' | clickhouse-client --host localhost --port 9000" > /dev/null 2>&1; then
    echo "✅ ClickHouse CLI  : localhost:9000 (Running)"
else
    echo "❌ ClickHouse CLI  : localhost:9000 (Not accessible)"
fi

# Kafka
if timeout 2 bash -c "</dev/tcp/localhost/9092" > /dev/null 2>&1; then
    echo "✅ Kafka           : localhost:9092 (Running)"
else
    echo "❌ Kafka           : localhost:9092 (Not accessible)"
fi

# Schema Registry
if curl -s http://localhost:8081/subjects > /dev/null 2>&1; then
    echo "✅ Schema Registry : http://localhost:8081 (Running)"
else
    echo "❌ Schema Registry : http://localhost:8081 (Not accessible)"
fi

echo ""
echo "💾 Data Lake Status:"
echo "--------------------------------------------------"

DATA_LAKE_PATH="../../data/lake"

if [ -d "$DATA_LAKE_PATH" ]; then
    echo "📁 Data Lake: $DATA_LAKE_PATH"
    
    # Raw data
    if [ -d "$DATA_LAKE_PATH/raw/equity_ohlc" ]; then
        RAW_COUNT=$(find "$DATA_LAKE_PATH/raw/equity_ohlc" -name "*.parquet" 2>/dev/null | wc -l)
        echo "   Raw (Bronze)      : $RAW_COUNT parquet files"
    else
        echo "   Raw (Bronze)      : No data"
    fi
    
    # Normalized data
    if [ -d "$DATA_LAKE_PATH/normalized/equity_ohlc" ]; then
        NORM_COUNT=$(find "$DATA_LAKE_PATH/normalized/equity_ohlc" -name "*.parquet" 2>/dev/null | wc -l)
        echo "   Normalized (Silver): $NORM_COUNT parquet files"
    else
        echo "   Normalized (Silver): No data"
    fi
    
    # Features
    if [ -d "$DATA_LAKE_PATH/features" ]; then
        FEAT_COUNT=$(find "$DATA_LAKE_PATH/features" -name "*.parquet" 2>/dev/null | wc -l)
        echo "   Features (Gold)   : $FEAT_COUNT parquet files"
    else
        echo "   Features (Gold)   : No data"
    fi
else
    echo "❌ Data Lake not found at $DATA_LAKE_PATH"
fi

echo ""
echo "📊 ClickHouse Data (if accessible):"
echo "--------------------------------------------------"

if command -v clickhouse-client &> /dev/null; then
    if timeout 2 bash -c "echo 'SELECT 1' | clickhouse-client --host localhost --port 9000" > /dev/null 2>&1; then
        echo "Databases:"
        clickhouse-client --host localhost --port 9000 --query "SHOW DATABASES" 2>/dev/null | grep -v "^system$" | grep -v "^information_schema$" | while read db; do
            echo "  📚 $db"
            clickhouse-client --host localhost --port 9000 --database "$db" --query "SHOW TABLES" 2>/dev/null | while read table; do
                count=$(clickhouse-client --host localhost --port 9000 --database "$db" --query "SELECT COUNT(*) FROM $table" 2>/dev/null)
                echo "     └─ $table: $count rows"
            done
        done
    else
        echo "❌ Cannot connect to ClickHouse"
    fi
else
    echo "⚠️  clickhouse-client not installed"
fi

echo ""
echo "🎯 Next Steps:"
echo "--------------------------------------------------"
echo "1. Start missing services:"
echo "   docker-compose up -d"
echo ""
echo "2. Run date range ingestion:"
echo "   poetry run python run_date_range.py --start-date 2024-01-02 --end-date 2024-01-05 --skip-weekends"
echo ""
echo "3. With ClickHouse loading:"
echo "   poetry run python run_date_range.py --start-date 2024-01-02 --end-date 2024-01-05 --skip-weekends --load-clickhouse"
echo ""
echo "4. View metrics:"
echo "   curl http://localhost:9090/metrics"
echo ""
echo "5. Access UIs:"
echo "   - MLflow: http://localhost:5000"
echo "   - Kafka topics: kafka-console-consumer --bootstrap-server localhost:9092 --topic <topic> --from-beginning"
echo ""
