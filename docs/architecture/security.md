# Security and Compliance

## Objectives

- Protect market and reference data, ensure least-privilege access, and maintain auditability.
- Keep the platform replayable and observable while enforcing controls.

## Identity and Access

- Auth: SSO/OIDC for humans; workload identities for services.
- AuthZ: domain-scoped roles; producers/consumers limited to owned topics; table-level ACLs.
- Secrets: stored in managed vault/KMS; never in code or configs; short-lived tokens for services.

## Data Protection

- Encryption in transit (TLS everywhere) and at rest (KMS-backed keys for Kafka, Hudi, ClickHouse, object storage).
- Data classification: tag datasets (raw, normalized, analytics, sensitive); restrict sensitive access.
- Backups and snapshots: periodic for ClickHouse and critical metadata; tested restores.

## Network and Perimeter

- Private networking for brokers, lake, OLAP clusters; ingress via API gateway with WAF.
- Narrow egress for ingestion connectors; outbound allowlists for exchanges/APIs.

## Logging and Audit

- Access logs for Kafka, Hudi, ClickHouse, Pinot, caches, and vault.
- Change logs for schemas (TDRs), topic configs, ACLs, and CI/CD deploys.
- Tamper-evident storage for audit trails; retention aligned with compliance.

## Governance and Compliance

- Schema changes require review and versioning; no breaking changes on existing topics.
- DQ gates at domain boundaries; quarantine and replay processes documented.
- Incident response playbooks for data leaks, key rotation, and backfill misuse.

## Recovery and Resilience

- Replay-first: recover by reprocessing from Kafka/Hudi Bronze; avoid mutable state in serving.
- DR: replicate object storage and metadata; test cross-region restores for critical paths.
- Rate limits and circuit breakers on serving APIs to isolate abuse.
