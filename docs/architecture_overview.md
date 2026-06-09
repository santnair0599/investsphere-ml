# InvestSphere — End-to-End Architecture

A governed investment-data platform on Databricks (Lakehouse), with a medallion
pipeline, reusable platform library, governance, monitoring, CI/CD, and an AI
research/policy copilot.

## End-to-end flow
```
                         ┌─────────────────────────────────────────┐
   DATA SOURCES          │  snapshots CSV + txns JSON + docs PDF    │
                         └─────────────────────────────────────────┘
                                          │
                 INGEST (PySpark / Auto Loader, + audit columns)
                                          ▼
   BRONZE  (raw Delta)        investsphere.bronze.raw_*
                                          │
                 CLEAN + VALIDATE (DQ rules → quarantine, dedupe, SCD2)
                                          ▼
   SILVER  (conformed Delta)  investsphere.silver.silver_* / quarantine_*
                                          │
                 MODEL + CALCULATE (dimensional star; exposure / P&L / breach)
                                          ▼
   GOLD    (analytics Delta)  investsphere.gold.dim_* / fact_*
                                          │
            ┌─────────────────────────────┼───────────────────────────────┐
            ▼                              ▼                               ▼
   AI/BI DASHBOARDS             SQL / BI tools                  AI COPILOT
   exposure, breaches, P&L      (governed SELECT)               AI Search RAG +
                                                                MCP tools + LLM

   CROSS-CUTTING:  Governance & Security (Unity Catalog) · Data Quality Monitoring
                   · Operational Monitoring · CI/CD (Bundles + Terraform + Git)
```

---

## 1. Data sources
Organised under `data/` into four source families (`generate_data.py` +
`generate_documents.py`):

**`reference_data/` (CSV)** — master / slowly-changing data
`portfolio_master`, `investment_asset_master` (listed + private + funds),
`issuer_master`, `counterparty_master`, `investment_limits`,
`benchmark_or_target_allocation`, `currency_rates`

**`transaction_data/` (JSON)** — flows of activity
`investment_transactions`, `cashflows`, `corporate_actions`

**`valuation_data/` (CSV)** — prices, holdings, valuations
`listed_market_prices`, `investment_holdings_snapshot`,
`private_valuation_snapshot`, `fund_nav_snapshot`

**`documents/` (PDF)** — unstructured, for RAG
`investment_policy_statement`, `listed_equity_research_note`,
`private_investment_committee_memo`, `portfolio_risk_guidelines`

**Counts:** 14 structured datasets (7 reference CSV + 3 transaction JSON + 4
valuation CSV) + 4 PDF documents. The asset business key is `investment_asset_id`;
`isin` is a nullable attribute populated only for listed securities. In production
these arrive as files in cloud storage (custodian / market-data feeds); reference
data is slowly changing. The core exposure/breach marts use listed holdings +
prices; privates, funds, cashflows, corporate actions and FX rates are landed for
richer marts (total NAV, income, multi-currency). Folder → landing-path mapping:
`docs/SOURCES.md`.

## 2. Ingestion layer → BRONZE
- Script: `pipelines/bronze_ingest.py`
- Reads each source and writes a raw Delta table `investsphere.bronze.raw_<name>`.
- **Audit columns** added to every row (`investsphere_platform.ingestion`):
  `_ingest_ts`, `_source_file`, `_batch_id`.
- Production pattern: **Auto Loader** for incremental file ingestion; **Lakeflow
  Spark Declarative Pipelines** (formerly DLT) to orchestrate.
- **Schema drift = rescue, not evolve**: Auto Loader runs with
  `schemaEvolutionMode="rescue"` + `rescuedDataColumn="_rescued_data"` against the
  explicit `schemas.py` contract. New/renamed/mis-typed columns land in `_rescued_data`
  (no auto-promotion into Silver/Gold, no stream crash); an engineer reviews and edits
  `schemas.py` by hand. Raw data is kept as-is for replayability.

## 3. Transformations
### SILVER — clean & conform (`pipelines/silver_conform.py`)
- **Tiered DQ policy** (`investsphere_platform.quality.dq_policy` / `dq_rules`):
  **FAIL** the job on a missing primary key (NULL `transaction_id` — unauditable);
  **QUARANTINE** bad FK / quantity / type / schema-drift rows; **WARN** on missing
  optional fields.
- **Quarantine** (append-only audit trail): failing rows →
  `investsphere.silver.quarantine_transaction` with `quarantine_reason`, `source_table`,
  `source_file`, `business_date`, `pipeline_run_id`, `job_run_id`, `check_timestamp` and
  full `raw_payload` (replayable); good rows → `investsphere.silver.silver_transaction`.
- **Deduplication**: keep the latest transaction per `transaction_id` by ingestion time.
- **SCD Type-2**: deployed via native **AUTO CDC FROM SNAPSHOT** in the Lakeflow
  pipeline (`lakeflow_pipeline.py`) for the asset/issuer/limit history dimensions;
  the pure-Python MERGE (`transformations/scd2.py`) is the unit-tested fallback.

### GOLD — model & calculate (`pipelines/gold_marts.py`)
- **Dimensional star schema** (Kimball): `dim_portfolio`, `dim_investment_asset`,
  `dim_issuer`, `dim_counterparty`, `dim_date`.
- **Facts**: `fact_investment_transaction`, `fact_daily_holding`, `fact_daily_pnl`,
  `fact_portfolio_exposure`, `fact_limit_breach`.
- **Business calculations** (`transformations/exposure_calculator.py`,
  `pnl_calculator.py`): market value = qty × price; sector & issuer exposure %;
  daily mark-to-market P&L; concentration-limit breaches.
- Key design: **pure Python reference functions** define and unit-test the expected
  exposure/P&L/breach outcomes locally; the **production PySpark gold job implements
  equivalent logic with Spark-native column expressions and SQL** (not row-by-row
  Python UDFs), and **reconciliation tests** confirm matching results on controlled
  datasets.

## 4. Target
- **Storage:** Delta Lake tables in Unity Catalog, catalog `investsphere`, schemas
  `bronze` / `silver` / `gold` / `governance` / `ai`.
- **Serving model:** the Gold star schema is the single source of truth for
  analytics, dashboards, and the AI copilot. `fact_limit_breach` answers the
  headline compliance question directly.
- **ML loop (this variant):** Gold also feeds a UC **feature store** →
  training-set prep → MLflow model (registered as `investsphere.ml.breach_classifier`)
  → **offline batch inference** (`investsphere.ml.breach_predictions`) → **monitoring**
  (predicted-vs-actual + PSI drift written to `governance.dq_results`). Orchestrated by the
  `investsphere_ml_pipeline` job, chained after Gold. Offline scoring only — **no paid
  Model Serving endpoint** (that's an optional paid extension). See `FEATURE_STORE_AND_ML.md`.
- **Release scope:** **V1 (implemented)** = listed holdings market value, daily P&L,
  exposure, concentration breaches. **V2 (designed, not yet built)** = private
  valuation, fund NAV, cashflows, total portfolio NAV, multi-currency. **V3** =
  policy-grounded AI assistant + evaluation.

## 5. Data quality & alerting (Observable → Alertable → Actionable)
- **In-pipeline (Silver):** tiered fail / quarantine / warn validation (above).
- **Gold integrity gate** (`pipelines/gold_marts.py`): the Gold job **fails** on a
  data-integrity violation — holdings without a price, NULL `market_value`, or a
  single-sector `exposure_pct > 100`. A limit **breach** is NOT a failure — it is valid
  business output.
- **Custom DQ checks** (`quality_monitoring/custom_dq_checks.sql`): holdings without
  price, **per-run** quarantine count, stale-price detection, breach count, **schema
  drift / rescued rows**, **freshness**, **Lakeflow expectation failures** → appended to
  `investsphere.governance.dq_results`. The authoritative **per-run quarantine rate** is
  written by `silver_conform.py` (the quarantine table is append-only, so the rate is
  computed from the run's exact counts and tagged with `pipeline_run_id` — never a
  cumulative count).
- **SQL Alerts** (`quality_monitoring/dq_alerts.sql`, provisioned via
  `terraform/dq_alerts.tf`): scheduled queries over `dq_results` page on-call on schema
  drift, quarantine rate >2%, missing feed, stale feed, expectation failures and breach
  spikes. Runs in parallel with the Prometheus/Grafana push path (`docs/MONITORING_EXTERNAL.md`).
- **Native Data Quality Monitoring** (`quality_monitoring/create_data_quality_monitor.py`):
  freshness, completeness, profiling and anomaly/drift on Gold tables (its data-
  profiling capability was formerly called Lakehouse Monitoring).

## 6. Governance & security (Unity Catalog — `governance/`)
- **Structure:** one catalog, schema-per-layer (`01_catalog_and_schemas.sql`).
- **RBAC by group** (`02_grants.sql`): engineers (full), analysts & PMs (read Gold).
- **Row-level security** (`03_row_and_column_security.sql`): a portfolio manager
  sees only the portfolios mapped to them in `pm_portfolio_map`.
- **Column masking:** counterparty names masked for non-engineers.
- **Lineage & auditing:** Unity Catalog captures lineage automatically for supported
  Databricks workloads and provides audit visibility. **Data classification** (AI
  tagging of sensitive columns) can be enabled where available — not assumed to have
  already run.

## 7. Monitoring
- **Data health:** the DQ results table + native Data Quality Monitoring metrics.
- **Pipeline/operational metrics** (`investsphere_platform.monitoring`): rows in /
  valid / quarantined, quarantine rate, run duration — emitted per run.
- **Dashboards/alerting:** native AI/BI dashboards (business + ops).
- **External monitoring (implemented):** a custom exporter
  (`investsphere_platform.monitoring.prometheus_exporter`) pushes pipeline metrics to a
  **Prometheus Pushgateway** via `pipelines/export_pipeline_metrics.py` (last EOD
  task); **Grafana** visualises them (`dashboards/grafana_pipeline_dashboard.json`).
  See `docs/MONITORING_EXTERNAL.md`. (Requires running Pushgateway/Prometheus/Grafana.)

## 8. CI/CD & Infrastructure as Code
- **Version control:** Git (GitHub).
- **Tests:** `pytest` on the pure-Python library (`tests/`, 21 passing) — runs in
  GitHub Actions CI on every push, no cluster needed.
- **Deployment:** **Declarative Automation Bundles** (`databricks.yml`, formerly
  Databricks Asset Bundles) deploy the medallion as one job with ordered tasks
  (bronze → silver → gold) across `dev` / `prod` targets. Compute is **serverless**
  (Databricks Free Edition): each job declares an `environments:` block and tasks
  reference it via `environment_key: default` — no classic job clusters.
- **Library packaging:** `investsphere_platform` is built into a **wheel**
  (`pyproject.toml` `[build-system]`; `python -m pip wheel . --no-deps -w dist`),
  built and uploaded by the bundle's `artifacts:` block and attached to each job via
  its serverless `environments.spec.dependencies` (plus `prometheus_client` on the EOD
  job and `pypdf` on the docs/RAG job).
- **IaC:** **Terraform** (`terraform/`) provisions the catalog, schemas and grants
  reproducibly.
- **Parameterization & conditional flow:** jobs read **job parameters** (`run_date`,
  `catalog`, `mode`) via `dbutils.widgets`/argv (`pipelines/job_params.py`). The EOD
  job uses a Databricks **condition task** on a `feeds_ready` task value set by the
  readiness check — true → Silver → Gold, false → notify (Gold skipped).
- **Orchestration cadence** (`databricks.yml`): file-arrival triggers (ingest, docs),
  **daily** EOD with the readiness condition task, **monthly** private/fund valuations,
  **weekly** maintenance (demo; prod uses Predictive Optimization), plus the Lakeflow
  pipeline. Scale modes sample/dev/demo/realistic(~11.2M, prices+holdings dominant)/
  perftest(50–100M capital-markets stress test) and cost are in `docs/COST_AND_SCALE.md`.

## 9. How end users consume the platform
| User | How they consume | What they see |
|---|---|---|
| **Portfolio managers** | Dashboards + AI copilot | Their portfolios only (row-level security): exposure, P&L, breaches |
| **Analysts** | SQL / BI on Gold + AI copilot | Read-only analytics; counterparty names masked |
| **Compliance** | `fact_limit_breach` + copilot | Breaches with the policy clause that explains them |
| **Data engineers** | Pipelines, full catalog | Build/operate the platform |
| **Apps / services** | Governed SQL / Delta | Programmatic access under UC permissions |

**AI Research & Policy Assistant** (`ai/`): a user asks, e.g., *"Does GULF_EQ
breach the banking limit, by how much, and which policy clause explains it?"* The
assistant (1) calls a governed UC function tool (exposed via a **managed MCP
server**) to read the breach from Gold — **this path enforces Unity Catalog row/
column security**; (2) retrieves the matching passage from **Databricks AI Search**
over an **approved, non-restricted** corpus (IPS, risk guidelines, general research);
(3) a foundation model writes a grounded, cited answer. Quality is checked with
**MLflow GenAI evaluation**.

**Security boundary (important):** Databricks AI Search does **not** support row/
column-level permissions, and you cannot index a table that has row filters or masks.
So structured exposure/breach access is enforced through **governed UC functions**,
and the AI Search corpus contains only **documents suitable for the copilot
audience**. Confidential or portfolio-specific documents would need an explicit
application-level retrieval-access design and are **not** assumed to inherit
Gold-table row/column policies.
