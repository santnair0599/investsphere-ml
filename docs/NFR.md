# InvestSphere — Non-Functional Requirements (HA · Security · Performance)

Summarises how the platform addresses the JD's "**ensure high availability, security
and performance**" duty. Status is honest: **implemented** (in code), **platform**
(provided by Databricks), or **designed** (documented, not yet executed/measured).

## Reliability & availability
**Posture (precise):** *Workload resilience* is implemented — task retries, timeouts,
failure/duration notifications, readiness gating, idempotent processing, replayable
Bronze data and controlled reruns/backfills. *In-region platform high availability*
is provided by **Azure Databricks managed infrastructure** where supported (zone
redundancy for control-plane components in supported regions; automatic zone
distribution / replacement of compute; serverless compute is Databricks-managed for
zone selection and VM replacement). *Regional outages* require a **separate,
customer-designed disaster-recovery** approach (documented below + in `DEPLOYMENT.md`).
Retries/timeouts/alerts/gating give resilience and recoverability — they do **not**
themselves constitute platform HA.

| Concern | Approach | Status | Target |
|---|---|---|---|
| Transient job failures | Task **retries** (`max_retries: 2`, 60s interval, retry-on-timeout) | implemented (`databricks.yml`) | auto-recover transient errors |
| Stuck / slow runs | `timeout_seconds` + **health rule** (warn > 30 min) + duration alert | implemented | detect within one run |
| Failure visibility | jobs: `email_notifications.on_failure`; Lakeflow pipeline: `notifications` (on-update-failure / on-update-fatal-failure / on-flow-failure) — both to `${var.oncall_email}` + ops dashboard + runbook | implemented | alert on every failure |
| Bad-data runs | EOD **condition task** on `feeds_ready` (skip Gold if feeds missing) | implemented | no analytics on incomplete data |
| Re-run / backfill safety | **Idempotent** MERGE / `replaceWhere`; Auto Loader checkpoints | implemented | safe, no duplicates |
| Recoverability | Delta **time travel**; immutable Bronze landing zone (replayable) | implemented + platform | restore/rebuild any date |
| Compute redundancy | Databricks managed multi-AZ / serverless | platform | provider SLA |
| **Disaster recovery** | Delta **deep clone** to secondary region; bundle/Terraform redeploy | designed (`DEPLOYMENT.md`) | **RPO ≤ 24h, RTO ≤ 4h**, quarterly drill |

## Security
| Concern | Approach | Status |
|---|---|---|
| Access control | Unity Catalog **group-based RBAC** (engineers/analysts/PMs) | implemented (`governance/`) |
| Row-level security | PM sees only mapped portfolios (`portfolio_row_filter`) | implemented |
| Column protection | counterparty name **masking** for non-engineers | implemented |
| AI security boundary | structured data only via governed UC functions; AI Search corpus = **approved docs only** (no row/column-restricted content) | implemented |
| Lineage & audit | UC automatic lineage + audit columns (`_ingest_ts/_source_file/_batch_id`) | implemented + platform |
| Secrets | `dbutils.secrets` / Key Vault / Secrets Manager | designed (`DEPLOYMENT.md`) |
| Network isolation | Private Link / VNet injection / IP access lists | **gap** — enterprise hardening, not in scope of this build |
| Encryption / CMK, data classification | platform encryption at rest/in transit; UC data classification | platform / designed (not run) |

## Performance
| Concern | Approach | Status |
|---|---|---|
| Incremental ingestion | **Auto Loader** (new files only) + idempotent writes | implemented |
| Data layout | **Liquid clustering** on `(portfolio_id, as_of_date)` (evaluate `CLUSTER BY AUTO`) | implemented |
| Join efficiency | **Broadcast** small dims; no driver `.collect()` | implemented |
| Engine | **Serverless** compute (jobs); **Photon** on the Lakeflow pipeline; AQE on by default | implemented |
| File maintenance | OPTIMIZE/VACUUM (demo) → **Predictive Optimization** in prod | implemented + designed |
| Storage cost / tiering | **ADLS lifecycle** (delete staging 30d; raw → Cool 30d → Archive 180d; **Delta excluded**) + redundancy ZRS/GZRS | designed (`storage_lifecycle/`) |
| Schema efficiency | explicit schemas (no `inferSchema`) | implemented |
| **Measured gains** | full-vs-incremental timing, files-scanned, DBU/run | **to be benchmarked** (`benchmarks/` harness) — do not quote numbers until measured |

## How I'd describe it in interview
> *"Resilience and recoverability are built in — task retries, timeouts, failure
> alerts, a readiness condition gate, idempotent writes, Delta time travel and an
> immutable replayable landing zone — with a documented deep-clone DR plan (RPO ≤ 24h
> / RTO ≤ 4h). Security is Unity-Catalog-centric: RBAC, row-level security, masking,
> lineage, and an explicit AI-search boundary. Performance uses Auto Loader,
> liquid clustering, broadcast joins and Photon — as implemented patterns I would
> then benchmark to quote real numbers. Network isolation and a live DR drill are the
> enterprise-hardening items I'd add next."*
