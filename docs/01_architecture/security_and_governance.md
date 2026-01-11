# Security and Compliance

## Objectives

- Protect market and reference data, ensure least-privilege access, and maintain auditability.
- Keep the platform replayable and observable while enforcing controls.

## Identity and Access

- Auth: SSO/OIDC for humans; workload identities for services.
- AuthZ: domain-scoped roles; producers/consumers limited to owned topics; table-level ACLs.
- Secrets: stored in managed vault/KMS (e.g., HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager); never in code or configs; short-lived service tokens (< 1 hour TTL) issued on-demand with automatic refresh; keys rotated every 90 days.

## Data Protection

- Encryption in transit (TLS everywhere) and at rest (KMS-backed keys for Kafka, Hudi, ClickHouse, object storage).
- Data classification: tag datasets (raw, normalized, analytics, sensitive); restrict sensitive access.
- Backups and snapshots: periodic for ClickHouse and critical metadata; tested restores.

## Network and Perimeter

- Private networking for brokers, lake, OLAP clusters; ingress via API gateway with WAF.
- Narrow egress for ingestion connectors; outbound allowlists for exchanges/APIs.

## Logging and Audit

- Access logs for Kafka, Hudi, ClickHouse, Pinot, caches, and vault.
- Change logs for schemas (via Technical Design Reviews or TDRs), topic configs, ACLs, and CI/CD deploys.
- Tamper-evident storage for audit trails; retention aligned with compliance.

## Governance and Compliance

- Schema changes require Technical Design Review (TDR) and versioning; breaking changes flagged by schema registry and reviewed by data governance + consuming teams before promotion; no breaking changes on existing topics.
- DQ gates at domain boundaries; quarantine and replay processes documented.
- Incident response playbooks for data leaks, key rotation, and backfill misuse.

## Recovery and Resilience

- Replay-first: recover by reprocessing from Kafka/Hudi Bronze; avoid mutable state in serving (see [compute-strategy.md](./compute-strategy.md) for deterministic replay patterns).
- **RTO/RPO targets:** < 4 hours RTO for critical paths; < 1 hour RPO (within Kafka retention window).
- **DR testing:** Quarterly cross-region restores for object storage, metadata, and ClickHouse; results documented and reviewed.
- **Incident playbooks:** Documented and tested procedures for data leaks, key rotation, and replay misuse.
- Rate limits and circuit breakers on serving APIs to isolate abuse.
