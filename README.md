# InvestSphere — Governed Investment Data Platform & AI Research Copilot on Databricks

A portfolio project that demonstrates a **governed, end-to-end investment-data
platform** on Databricks: Bronze/Silver/Gold medallion architecture in PySpark, a
reusable Python platform library, data-quality + quarantine, dimensional modelling,
concentration-limit breach detection, governance, monitoring, and (as an
extension) an AI research/policy copilot.

**Domain:** capital markets / investment data — portfolios, investment assets
(listed + private + funds), transactions, holdings, prices, counterparties,
benchmarks and investment-policy limits.

> Built with current Databricks terminology (June 2026): **Lakeflow Spark
> Declarative Pipelines** (formerly Delta Live Tables), **Databricks AI Search**
> (formerly Vector Search), **Declarative Automation Bundles** (formerly Asset
> Bundles), **Data Quality Monitoring** (its data-profiling capability was formerly
> Lakehouse Monitoring).

---

## Business problem & why
A diversified investment organisation runs many portfolios (listed + private + funds,
multi-currency) and must track **exposure, P&L and investment-policy-limit breaches**
daily. Today that data is **fragmented across feeds**, compliance breaches are caught
**late/manually**, reporting isn't **reproducible or governed**, and answering *"does
this portfolio breach a limit, and which policy clause applies?"* means digging through
both data **and** policy PDFs. InvestSphere solves this with a **governed lakehouse**
(one trusted, lineage-tracked source), **automated breach detection**, **data-quality +
access controls**, and a **policy-grounded AI copilot** — on Databricks, because a
lakehouse unifies BI **and** AI under one governance model. Full write-up:
**`docs/BUSINESS_CONTEXT.md`**.

## What it does (headline demo)
> *"Which portfolios breach the UAE banking-sector concentration limit, by how much,
> and which policy clause explains it?"*

The sample data is engineered so the **GULF_EQ** portfolio breaches the 20% Banking
limit (`LIM_001`). The Gold layer produces a `fact_limit_breach` table answering the
structured half; the optional AI copilot retrieves the matching policy clause.

## Repo layout
```
investsphere/
├── data/                       # source catalog + generators
│   ├── reference_data/         # 7 master CSVs (portfolio/asset/issuer/counterparty/limits/benchmark/fx)
│   ├── transaction_data/       # 3 JSON feeds (transactions, cashflows, corporate_actions)
│   ├── valuation_data/         # 4 CSVs (prices, holdings, private valuations, fund NAV)
│   ├── documents/              # 4 policy/research PDFs (RAG corpus)
│   ├── generate_data.py        # structured sample generator   generate_documents.py (PDFs)
├── src/investsphere_platform/  # reusable Python library (plain functions, no classes)
│   ├── ingestion/  quality/  transformations/  monitoring/
├── pipelines/                  # PySpark + Lakeflow scripts (run on Databricks)
│   ├── schemas.py  job_params.py  bronze_ingest.py  silver_conform.py  gold_marts.py
│   ├── lakeflow_pipeline.py (pyspark.pipelines)  silver_reference_scd2.py  gold_private_valuations.py
│   ├── eod_readiness_check.py  notify_not_ready.py  reconcile_gold.py  maintenance.py
│   ├── generate_synthetic_data.py  export_pipeline_metrics.py
├── features/                   # UC feature store — build_portfolio_features.py
├── ml/                         # ML loop — prepare_training_set, train_breach_model,
│                               #   batch_inference, model_monitoring
├── ai/                         # RAG + copilot: AI Search index/refresh, UC tools, agent, MLflow eval
├── governance/                 # Unity Catalog SQL: catalog/schemas, grants, RLS, masking
├── quality_monitoring/         # custom DQ checks (custom_dq_checks.sql) + DQ alert
│                               #   queries (dq_alerts.sql) + native Data Quality Monitor
├── dashboards/                 # AI/BI SQL (business + ops) + Grafana dashboard JSON
├── terraform/                  # IaC: catalog, schemas, grants + DQ SQL alerts (dq_alerts.tf)
├── storage_lifecycle/          # ADLS lifecycle policy (JSON + Terraform) + redundancy
├── tests/                      # pytest unit tests (21; run locally, no Spark)
├── examples/run_local_demo.py  # end-to-end demo, no Spark
├── .github/workflows/ci.yml    # CI: pytest on push
├── databricks.yml              # Declarative Automation Bundle (jobs + Lakeflow pipeline)
├── docs/                       # see "Documentation" below
├── pyproject.toml
└── requirements.txt
```

## Documentation (`docs/`)
| File | Covers |
|---|---|
| `BUSINESS_CONTEXT.md` | the business problem, why this solution, value, why Databricks |
| `architecture_overview.md` | end-to-end architecture (sources → bronze/silver/gold → consumption) |
| `architecture.md` | one-page ASCII diagram |
| `SOURCES.md` | the 14 datasets + 4 PDFs, folder → bronze-table → landing-path mapping |
| `OPTIMIZATIONS.md` | optimisation patterns + measurement template |
| `COST_AND_SCALE.md` | scale modes, ~11.2M row model, cost posture |
| `ORCHESTRATION.md` | parameterization (job params / widgets) + conditional logic (condition task, expectations) |
| `NFR.md` | HA / security / performance posture + targets |
| `DEPLOYMENT.md` | AWS/Azure mapping + disaster recovery (deep clone, RPO/RTO) |
| `RUNBOOK.md` | ops runbook: deploy, on-call checklist, incident playbooks |
| `MONITORING_EXTERNAL.md` | Prometheus Pushgateway + Grafana setup |
| `IMPLEMENTATION_GUIDE.md` | **from-scratch build sequence on Azure PAYG + Databricks Premium** (phased, cost-guarded) |

## Design idea worth knowing (and saying in interviews)
**Pure Python reference functions** (`src/investsphere_platform/transformations`,
`/quality`) define and **unit-test** the expected exposure, P&L and breach outcomes
locally in milliseconds (no Spark dependency). The **production PySpark pipeline
implements equivalent logic using Spark-native column expressions and SQL
operations** — not row-by-row Python UDFs — and a **reconciliation check**
(`pipelines/reconcile_gold.py`) confirms matching results on controlled datasets.
→ testable reference logic + scalable Spark-native implementation + reconciliation
for correctness.

## Run it locally (no Spark / no cloud needed)
```bash
# 1. generate the structured datasets (reference / transaction / valuation)
python data/generate_data.py

# 2. (optional) generate the PDF documents for RAG
pip install fpdf2
python data/generate_documents.py

# 3. run the unit tests
pip install -r requirements.txt
pytest -q                      # 21 tests pass

# 4. (optional) see exposures + breaches locally, no Spark
python examples/run_local_demo.py
```

## Run the platform on Databricks
1. Create a free Databricks workspace and a Unity Catalog volume.
2. Upload the datasets from `data/` into **per-source subfolders** on the volume
   (Auto Loader reads a folder per source), e.g.
   `.../raw/investment_transactions/investment_transactions.json`,
   `.../raw/investment_asset_master/investment_asset_master.csv`. Reference data is
   CSV; transaction feeds are JSON. Use **immutable file names** (file-arrival
   triggers fire on new files, not overwrites).
3. The library is built as a wheel (bundle `artifacts` block) and attached via each job's serverless environment `dependencies` — no sys.path hacks.
4. Run `pipelines/bronze_ingest.py` → `silver_conform.py` → `gold_marts.py`.
5. Inspect `investsphere.gold.fact_limit_breach`.

## Governance, monitoring & deployment (Stronger version)
Run on Databricks in this order:
1. **Governance** — `governance/01_catalog_and_schemas.sql` → `02_grants.sql` →
   `03_row_and_column_security.sql` (create groups `investsphere_engineers/analysts/pms`
   first, and fill `pm_portfolio_map` with real user emails).
2. **Data quality & alerting** — `quality_monitoring/custom_dq_checks.sql` computes
   metrics into `governance.dq_results`; `quality_monitoring/dq_alerts.sql` +
   `terraform/dq_alerts.tf` provision the Databricks **SQL Alerts** that page on-call;
   optionally `create_data_quality_monitor.py` for the native monitor. (DQ design,
   alert thresholds and the rescued-data flow: **`docs/RUNBOOK.md`**.)
3. **Dashboard** — build an AI/BI dashboard (formerly Lakeview) from `dashboards/dashboard_queries.sql`.
4. **Infra as code** — `terraform/` creates the catalog/schemas/grants reproducibly.
5. **CI/CD** — `databricks bundle deploy` then `databricks bundle run investsphere_eod`
   using `databricks.yml`.

## Feature Store & ML loop (this variant)
On top of the medallion, `investsphere_ml` adds the full **MLOps loop** — and orchestrates it:
```
Gold → features/build_portfolio_features.py → UC feature table
     → ml/prepare_training_set.py            → labelled training set (FeatureLookup)
     → ml/train_breach_model.py              → MLflow + registered UC model
     → ml/batch_inference.py                 → investsphere.ml.breach_predictions
     → ml/model_monitoring.py                → ml_* metrics in governance.dq_results
```
The `investsphere_ml_pipeline` job in `databricks.yml` runs these five steps and is **chained
after Gold** by the EOD job (a `run_job_task`); it's also runnable on its own with
`databricks bundle run investsphere_ml_pipeline -t dev`. Monitoring reuses the existing
`governance.dq_results` + SQL-alert pattern (predicted-vs-actual accuracy, missing-feature
coverage, and PSI drift). **Scoring is offline batch only — no paid Model Serving endpoint is
created;** real-time serving is an optional paid future enhancement. Pure-Python monitoring
maths is unit-tested (`tests/test_ml_monitoring.py`). Full detail: **`docs/FEATURE_STORE_AND_ML.md`**.

## AI Research Copilot (Differentiating version)
Run on Databricks (needs serverless + Mosaic AI / Foundation Models), after the
platform and governance are in place:
1. Upload `data/documents/*` to the volume `/Volumes/investsphere/ai/documents/`.
2. `ai/01_build_ai_search_index.py` — chunk the docs and build the **Databricks AI
   Search** index (RAG knowledge base).
3. `ai/02_agent_tools.sql` — create the governed **UC function tools**
   (`get_portfolio_breaches`, `get_sector_exposure`); register them on a managed
   **MCP server** for production agents.
4. `ai/agent_exposure_policy.py` — the assistant: it calls the breach tool, retrieves
   the matching policy clause from AI Search, and a foundation model writes the
   grounded answer (the headline demo).
5. `ai/evaluate_agent.py` — score the assistant with **MLflow GenAI evaluation**.

Why it's built last: managed MCP servers are in **Public Preview**, so nothing in
the core platform depends on them. The assistant is an additive layer.

## Performance & best practices (implemented)
Explicit schemas with **Auto Loader rescue mode** (drift → `_rescued_data`, never
auto-promoted), incremental ingestion, mixed CSV+JSON feeds,
**idempotent MERGE / replaceWhere** writes, **broadcast joins** (no driver collect),
**liquid clustering** on facts, **OPTIMIZE/VACUUM + Predictive Optimization**, a
**Lakeflow Declarative Pipeline with quality expectations**, **native SCD2 via
AUTO CDC FROM SNAPSHOT** (custom MERGE kept only as a unit-tested fallback),
**serverless compute (Photon on the Lakeflow pipeline)**, GitHub Actions **CI**, and cost tags + secrets guidance.
**Storage cost optimisation** — ADLS lifecycle tiering (delete staging after 30d;
raw archive → Cool 30d → Archive 180d; **Delta tables excluded**) + redundancy
(ZRS/GZRS): **`storage_lifecycle/`**.
Full before/after rationale: **`docs/OPTIMIZATIONS.md`**.

## Orchestration, scale & cost
Cadence-based orchestration in `databricks.yml`: **file-arrival** triggers (ingest +
doc upload), a **parameterized daily EOD** job (`run_date`/`catalog` job parameters
via `dbutils.widgets`/argv) with a **Workflows condition task** that branches on a
`feeds_ready` task value (true → Silver → Gold, false → notify; Gold skipped),
**monthly** private/fund valuations, **weekly** maintenance (demo; production prefers
Predictive Optimization), plus the Lakeflow pipeline. **Full parameterization &
conditional-logic detail: `docs/ORCHESTRATION.md`.** Scale modes
(`pipelines/generate_synthetic_data.py`): `sample` (tests) · `dev` (~50–100k) ·
`demo` (~500–800k) · **`realistic` ~11.2M** where **daily prices + holdings dominate
and transactions are low-volume** · `perftest` 50–100M (capital-markets stress test
only). Free Edition ≈ AED 0 for dev; paid costs are **measured after a timed run,
not quoted**. Details: **`docs/COST_AND_SCALE.md`**.

## Operations
Platform-ops dashboard (`dashboards/platform_ops_dashboard.sql`): job-run status,
spend, DQ pass/fail, quarantine trend, freshness, open breaches. Operations
**runbook** (`docs/RUNBOOK.md`): deploy, daily on-call checklist, the DQ model &
alerting loop, incident playbooks (readiness-gate fail, quarantine alert, schema
drift, Gold integrity failure, job failure, AI-Search cost), backfill, teardown. Source catalog + landing-zone paths: **`docs/SOURCES.md`**.
Non-functional posture — **HA / security / performance + targets**: **`docs/NFR.md`**;
disaster recovery (deep-clone, RPO/RTO): **`docs/DEPLOYMENT.md`**. **Reliability &
availability posture:** *workload resilience* (task retries, timeouts,
failure/duration alerts, readiness gating, idempotent processing, replayable Bronze,
controlled reruns/backfills) is implemented in `databricks.yml`; *in-region platform
high availability* is provided by Databricks managed infrastructure where supported;
*cross-region disaster recovery* is a separate documented secondary-region design.
**External pipeline-performance monitoring** — a custom
exporter pushes metrics to a **Prometheus Pushgateway** and a **Grafana** dashboard
visualises them: **`docs/MONITORING_EXTERNAL.md`**.

## Build roadmap (priority order)
**Minimum strong version (this scaffold):**
- [x] 14 structured datasets (7 reference CSV + 3 transaction JSON + 4 valuation CSV) + 4 PDF docs + generators
- [x] Bronze → Silver → Gold PySpark pipelines
- [x] Exposure, sector concentration and limit-breach Gold tables
- [x] DQ rules + tiered policy (fail / quarantine / warn) + per-run quarantine rate
- [x] `investsphere_platform` reusable package with unit tests
- [x] README + architecture diagram

**Stronger version (built):**
- [x] Unity Catalog governance — `governance/`: catalog + schemas, RBAC grants,
      row-level security (PM sees only their portfolios), counterparty column masking
- [x] Data Quality Monitoring & alerting — `quality_monitoring/`: native monitor +
      custom SQL checks (holdings-without-price, per-run quarantine rate, stale prices,
      breach count, **schema-drift/rescued rows**, **freshness**, **expectation
      failures**) → `governance.dq_results` → **Databricks SQL Alerts**
      (`dq_alerts.sql` + `terraform/dq_alerts.tf`), plus parallel Prometheus/Grafana
- [x] Declarative Automation Bundles (`databricks.yml`) + Terraform (`terraform/`)
- [x] Dashboard queries — `dashboards/dashboard_queries.sql`

**Differentiating version (built — MCP is in Public Preview, so the core does not depend on it):**
- [x] Databricks AI Search over investment-policy & research documents (RAG) —
      `data/documents/` + `ai/01_build_ai_search_index.py`
- [x] Investment Exposure & Policy Assistant — `ai/agent_exposure_policy.py`:
      governed UC function tools (`ai/02_agent_tools.sql`, exposed via managed MCP
      server in production) + AI Search retrieval + a foundation model
- [x] MLflow GenAI evaluation — `ai/evaluate_agent.py` (correctness, relevance,
      cites-policy)

**ML / MLOps loop (built — `investsphere_ml` variant):**
- [x] UC **feature store** — `features/build_portfolio_features.py`
- [x] **Training-set prep** (leakage-controlled FeatureLookup) — `ml/prepare_training_set.py`
- [x] **Model training + UC registry** (MLflow) — `ml/train_breach_model.py`
- [x] **Batch inference** → `investsphere.ml.breach_predictions` — `ml/batch_inference.py`
- [x] **Model monitoring** (predicted-vs-actual + PSI drift → `governance.dq_results`) —
      `ml/model_monitoring.py` + unit-tested `investsphere_platform.ml.monitoring`
- [x] **Orchestration** — `investsphere_ml_pipeline` job chained after Gold (`run_job_task`)
- [ ] Real-time **Model Serving** endpoint — *optional, paid; intentionally not built*

## One-line interview summary
> "I built InvestSphere, a governed investment-data platform on Databricks using
> Lakeflow Spark Declarative Pipelines (formerly DLT), with Bronze/Silver/Gold Delta
> tables, PySpark transformations, dimensional modelling for transactions and holdings,
> reusable Python platform utilities, Unity Catalog governance, Data Quality
> Monitoring and deployment through Declarative Automation Bundles. As an extension
> I integrated Databricks AI Search with governed business-rule functions to support
> policy-grounded portfolio exposure analysis."
