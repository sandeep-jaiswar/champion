# Orchestration

Domain scope: Prefect flows, tasks, and orchestration config.

Current contents:
- Flow/task packages plus CLI/main entry glue.

Migration notes:
- Move remaining ingestion flow code here; keep tasks thin wrappers over scrapers/parsers/storage.
- Ensure flows log MLflow tracking URIs and handle empty data paths gracefully.
