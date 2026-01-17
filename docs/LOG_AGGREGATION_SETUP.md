# Log Aggregation Setup Guide

This guide explains how to set up log aggregation for the Champion platform using either ELK Stack or Grafana Loki.

## Table of Contents

- [Overview](#overview)
- [Structured Logging](#structured-logging)
- [Option 1: ELK Stack](#option-1-elk-stack-elasticsearch-logstash-kibana)
- [Option 2: Grafana Loki](#option-2-grafana-loki)
- [Log Query Examples](#log-query-examples)
- [Best Practices](#best-practices)

## Overview

The Champion platform uses structured logging with JSON output format and trace ID support for distributed tracing. All logs include:

- **timestamp**: ISO 8601 format with UTC timezone
- **level**: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- **event**: Log message
- **trace_id**: Unique identifier for tracking requests across components
- **context fields**: Additional structured data (scraper name, table name, etc.)

## Structured Logging

### Using Trace IDs

Trace IDs help correlate logs across different components and requests:

```python
from champion.utils.logger import get_logger, set_trace_id, clear_trace_id

logger = get_logger(__name__)

# Set a trace ID for the current operation
trace_id = set_trace_id()  # Generates a UUID
logger.info("operation_started", operation="scrape", date="2024-01-15")

# All subsequent logs in this context will include the trace_id
logger.info("data_downloaded", rows=1000)
logger.info("data_parsed", valid_rows=950, invalid_rows=50)

# Clear trace ID when operation completes
clear_trace_id()
```

### Log Output Format

Example JSON log output:

```json
{
  "event": "operation_started",
  "operation": "scrape",
  "date": "2024-01-15",
  "level": "info",
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "trace_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "logger": "champion.scrapers.bhavcopy"
}
```

## Option 1: ELK Stack (Elasticsearch, Logstash, Kibana)

### Architecture

```
Champion App → Filebeat → Logstash → Elasticsearch → Kibana
```

### Setup Steps

#### 1. Add Filebeat to Docker Compose

```yaml
services:
  filebeat:
    image: docker.elastic.co/beats/filebeat:8.11.0
    user: root
    volumes:
      - ./logs:/logs:ro
      - ./monitoring/filebeat/filebeat.yml:/usr/share/filebeat/filebeat.yml:ro
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks:
      - monitoring
    depends_on:
      - logstash
```

#### 2. Configure Filebeat

Create `monitoring/filebeat/filebeat.yml`:

```yaml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /logs/*.log
    json.keys_under_root: true
    json.add_error_key: true
    fields:
      service: champion
      environment: production

output.logstash:
  hosts: ["logstash:5044"]
```

#### 3. Add Logstash to Docker Compose

```yaml
services:
  logstash:
    image: docker.elastic.co/logstash/logstash:8.11.0
    volumes:
      - ./monitoring/logstash/pipeline:/usr/share/logstash/pipeline:ro
    networks:
      - monitoring
    ports:
      - "5044:5044"
    depends_on:
      - elasticsearch
```

#### 4. Configure Logstash Pipeline

Create `monitoring/logstash/pipeline/champion.conf`:

```
input {
  beats {
    port => 5044
  }
}

filter {
  json {
    source => "message"
  }
  
  # Add geoip if needed
  if [client_ip] {
    geoip {
      source => "client_ip"
    }
  }
}

output {
  elasticsearch {
    hosts => ["elasticsearch:9200"]
    index => "champion-logs-%{+YYYY.MM.dd}"
  }
}
```

#### 5. Add Elasticsearch and Kibana

```yaml
services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - xpack.security.enabled=false
    volumes:
      - elasticsearch_data:/usr/share/elasticsearch/data
    networks:
      - monitoring
    ports:
      - "9200:9200"

  kibana:
    image: docker.elastic.co/kibana/kibana:8.11.0
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
    networks:
      - monitoring
      - frontend
    ports:
      - "5601:5601"
    depends_on:
      - elasticsearch

volumes:
  elasticsearch_data:
    driver: local
```

#### 6. Create Kibana Index Pattern

1. Access Kibana at `http://localhost:5601`
2. Navigate to Management → Index Patterns
3. Create pattern: `champion-logs-*`
4. Set time field: `timestamp`

## Option 2: Grafana Loki

### Architecture

```
Champion App → Promtail → Loki → Grafana
```

### Setup Steps

#### 1. Add Loki to Docker Compose

```yaml
services:
  loki:
    image: grafana/loki:2.9.0
    ports:
      - "3100:3100"
    volumes:
      - ./monitoring/loki/loki-config.yml:/etc/loki/local-config.yaml:ro
      - loki_data:/loki
    command: -config.file=/etc/loki/local-config.yaml
    networks:
      - monitoring

volumes:
  loki_data:
    driver: local
```

#### 2. Configure Loki

Create `monitoring/loki/loki-config.yml`:

```yaml
auth_enabled: false

server:
  http_listen_port: 3100
  grpc_listen_port: 9096

common:
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory

schema_config:
  configs:
    - from: 2020-10-24
      store: boltdb-shipper
      object_store: filesystem
      schema: v11
      index:
        prefix: index_
        period: 24h

ruler:
  alertmanager_url: http://localhost:9093

limits_config:
  retention_period: 744h  # 31 days
```

#### 3. Add Promtail to Docker Compose

```yaml
services:
  promtail:
    image: grafana/promtail:2.9.0
    volumes:
      - ./logs:/logs:ro
      - ./monitoring/promtail/promtail-config.yml:/etc/promtail/config.yml:ro
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    command: -config.file=/etc/promtail/config.yml
    networks:
      - monitoring
    depends_on:
      - loki
```

#### 4. Configure Promtail

Create `monitoring/promtail/promtail-config.yml`:

```yaml
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: champion
    static_configs:
      - targets:
          - localhost
        labels:
          job: champion
          service: champion
          environment: production
          __path__: /logs/*.log
    
    pipeline_stages:
      # Parse JSON logs
      - json:
          expressions:
            level: level
            timestamp: timestamp
            trace_id: trace_id
            event: event
            logger: logger
      
      # Extract additional labels
      - labels:
          level:
          trace_id:
          logger:
      
      # Set timestamp
      - timestamp:
          source: timestamp
          format: RFC3339
```

#### 5. Add Loki Data Source in Grafana

1. Access Grafana at `http://localhost:3000`
2. Navigate to Configuration → Data Sources
3. Add Loki data source:
   - URL: `http://loki:3100`
   - Access: Server (default)
4. Save & Test

## Log Query Examples

### ELK (Kibana)

```
# Find all errors
level: "error"

# Find logs by trace ID
trace_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# Find scraper failures
event: "scrape_failed" AND logger: champion.scrapers.*

# Find validation errors
event: "validation_failed" AND table: "raw_equity_ohlc"

# Complex query
level: "error" AND (event: "validation_failed" OR event: "load_failed") AND timestamp: [now-1h TO now]
```

### Loki (Grafana)

```
# Find all error logs
{service="champion"} |= "error"

# Find logs by trace ID
{service="champion"} | json | trace_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# Find logs from specific logger
{service="champion", logger=~"champion.scrapers.*"}

# Find validation failures
{service="champion"} | json | event="validation_failed"

# Rate of errors per minute
rate({service="champion"} |= "error" [1m])

# Count errors by level
sum(count_over_time({service="champion"} | json [5m])) by (level)
```

## Best Practices

### 1. Log Retention

- **Development**: 7 days
- **Staging**: 30 days
- **Production**: 90 days (or per compliance requirements)

### 2. Log Levels

- **DEBUG**: Detailed diagnostic information (disable in production)
- **INFO**: General informational messages (operations, milestones)
- **WARNING**: Warning messages (recoverable issues)
- **ERROR**: Error messages (failures that need attention)
- **CRITICAL**: Critical errors (system-level failures)

### 3. Structured Fields

Always include context-specific fields:

```python
logger.info(
    "data_loaded",
    table="raw_equity_ohlc",
    rows=1000,
    duration_ms=250,
    date="2024-01-15"
)
```

### 4. Trace ID Usage

- Set trace ID at the entry point of each operation
- Pass trace ID to downstream components
- Include trace ID in metrics labels when possible
- Clear trace ID when operation completes

### 5. Log Sampling

For high-volume logs, consider sampling:

```python
import random

if random.random() < 0.1:  # Sample 10%
    logger.debug("high_frequency_event", detail="...")
```

### 6. Sensitive Data

Never log sensitive information:
- Passwords or API keys
- Personal identifiable information (PII)
- Financial account numbers
- Authentication tokens

Use masking when logging potentially sensitive data:

```python
logger.info("user_action", user_id=hash(user_email))
```

### 7. Performance

- Use appropriate log levels
- Avoid expensive computations in log statements
- Use lazy evaluation for debug logs
- Batch log writes when possible

### 8. Alerting

Configure alerts based on log patterns:

- Error rate exceeds threshold
- Specific error patterns appear
- Missing expected log messages
- Unusual trace ID patterns (indicating system issues)

## Monitoring Log Pipeline Health

### Prometheus Metrics

Add metrics to monitor log pipeline:

```python
from prometheus_client import Counter, Gauge

logs_written = Counter(
    "champion_logs_written_total",
    "Total log messages written",
    ["level", "logger"]
)

logs_dropped = Counter(
    "champion_logs_dropped_total",
    "Total log messages dropped",
    ["reason"]
)
```

### Health Checks

Monitor:
- Filebeat/Promtail status
- Logstash/Loki ingestion rate
- Elasticsearch/Loki storage usage
- Query performance
- Index/label cardinality

## Troubleshooting

### Logs Not Appearing

1. Check log file permissions
2. Verify Filebeat/Promtail configuration
3. Check network connectivity
4. Verify index patterns/labels
5. Check log format (valid JSON)

### High Cardinality

If experiencing performance issues:
1. Limit number of labels/fields
2. Use log sampling
3. Increase retention deletion frequency
4. Optimize queries

### Disk Space

Monitor and manage disk usage:
1. Set appropriate retention policies
2. Use log compression
3. Archive old logs to cold storage
4. Set up automated cleanup

## References

- [Structlog Documentation](https://www.structlog.org/)
- [ELK Stack Guide](https://www.elastic.co/guide/index.html)
- [Grafana Loki Documentation](https://grafana.com/docs/loki/latest/)
- [Filebeat Reference](https://www.elastic.co/guide/en/beats/filebeat/current/index.html)
- [Promtail Documentation](https://grafana.com/docs/loki/latest/clients/promtail/)
