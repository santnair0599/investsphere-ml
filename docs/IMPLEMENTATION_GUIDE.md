# InvestSphere — Implementation Guide (Azure PAYG + Databricks Premium, from scratch)

End-to-end build sequence with verification checkpoints and **cost guardrails in
every phase**. Replace `<...>` placeholders. Architecture detail: `architecture_overview.md`.

> Cost reality (see `COST_AND_SCALE.md`): disciplined full build ≈ **$100–250 one-off**.
> The two killers are an **AI Search endpoint left running (~$200/mo)** and a
> **cluster without auto-terminate**. Phase 0 sets the guardrails first.

---

## Architecture (recap)
```
Sources (reference CSV + transaction JSON + valuation CSV + policy PDFs)
  → BRONZE (Auto Loader, audit cols)
  → SILVER (DQ rules + quarantine, dedupe, SCD2 history)
  → GOLD (dim_* / fact_daily_holding / fact_portfolio_exposure / fact_limit_breach)
  → Consumption: AI/BI dashboards · governed SQL · AI copilot (AI Search + UC tools)
  → ML loop: feature store → train/register (MLflow+UC) → batch inference → monitoring
Cross-cutting: Unity Catalog governance · Data Quality + ops monitoring · CI/CD ·
               orchestration (params + condition task + run_job_task) · storage lifecycle
```

---

## Phase 0 — Cost guardrails FIRST (do not skip)
```bash
az login
az account set --subscription "<subscription-id>"
# Budget + email alert so you can't be surprised
az consumption budget create --budget-name investsphere-budget --amount 100 \
  --time-grain Monthly --category Cost \
  --start-date 2026-07-01 --end-date 2027-07-01 || echo "set budget in Portal > Cost Management if CLI differs"
```
Rules for the whole build: **auto-terminate clusters (10–20 min)**, prefer
**serverless scale-to-zero**, and **delete the AI Search index** the moment a demo ends.

## Prerequisites (tools on your laptop)
- Azure subscription (PAYG); **Azure CLI**, **Databricks CLI v0.2+** (for bundles),
  **Terraform**, **Python 3.11**, **Git**, VS Code.
- Pick a region with **Foundation Models + serverless** (e.g. East US / West Europe;
  confirm for UAE North).

---

## Phase 1 — Azure foundation (storage + identity)
```bash
RG=investsphere-rg ; LOC=eastus ; SA=investspherelake
az group create -n $RG -l $LOC

# ADLS Gen2 (hierarchical namespace), ZRS for in-region availability
az storage account create -n $SA -g $RG -l $LOC \
  --sku Standard_ZRS --kind StorageV2 --hns true --min-tls-version TLS1_2

# Containers: UC managed data + raw landing
az storage container create --account-name $SA -n unity-catalog
az storage container create --account-name $SA -n landing

# Access Connector (managed identity) so Unity Catalog can reach the storage
az databricks access-connector create -n investsphere-ac -g $RG -l $LOC
# Grant it Storage Blob Data Contributor on the storage account
AC_ID=$(az databricks access-connector show -n investsphere-ac -g $RG --query identity.principalId -o tsv)
SA_ID=$(az storage account show -n $SA -g $RG --query id -o tsv)
az role assignment create --assignee-object-id $AC_ID --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope $SA_ID
```
**Checkpoint:** storage account exists with HNS=true; access connector has the role.

## Phase 2 — Databricks workspace + Unity Catalog
```bash
az databricks workspace create -n investsphere-ws -g $RG -l $LOC --sku premium
```
In the **workspace UI / account console**:
1. **Unity Catalog metastore** — most Azure regions auto-create one; else create it
   and assign the workspace (account console → Data → Metastores).
2. **Storage credential** — from the access connector (`investsphere-ac`).
3. **External location** — `abfss://unity-catalog@<SA>.dfs.core.windows.net/` using that credential.
4. **Enable serverless** (Account console → Settings → Feature enablement) for
   Lakeflow pipelines + AI Search.
5. **Enable system schemas** (so the ops/usage dashboards work):
   ```bash
   databricks auth login --host https://<workspace-url>
   databricks system-schemas enable <metastore-id> billing
   databricks system-schemas enable <metastore-id> lakeflow
   databricks system-schemas enable <metastore-id> access
   databricks system-schemas enable <metastore-id> query
   ```
**Checkpoint:** you can `databricks catalogs list` and see the metastore.

## Phase 3 — Local repo, sample data, tests
```bash
git clone <your-investsphere-repo> && cd investsphere
python -m venv .venv && .venv\Scripts\activate   # (or source on mac/linux)
pip install -r requirements.txt
python data/generate_data.py
pip install fpdf2 && python data/generate_documents.py
pytest -q                         # 21 tests pass
python examples/run_local_demo.py # see the GULF_EQ breach locally, no Spark
```
**Checkpoint:** 21 tests pass; demo prints the Banking breach.

## Phase 4 — Create catalog/schemas/volumes + upload data
Run `governance/01_catalog_and_schemas.sql` (SQL editor), **or** Terraform:
```bash
cd terraform && terraform init && terraform apply -var databricks_host=https://<workspace-url> ; cd ..
```
Create UC **volumes** and upload (one subfolder per source — Auto Loader reads a folder):
```sql
CREATE VOLUME IF NOT EXISTS investsphere.bronze.raw;
CREATE VOLUME IF NOT EXISTS investsphere.ai.documents;
-- REQUIRED: Auto Loader checkpoint + schema-tracking dirs must be UC Volumes.
-- Without these, bronze_ingest cannot checkpoint and silently writes NO tables
-- (error tell: UC_VOLUME_NOT_FOUND — the job may even report success).
CREATE VOLUME IF NOT EXISTS investsphere.bronze.`_checkpoints`;
CREATE VOLUME IF NOT EXISTS investsphere.bronze.`_schemas`;
```
```bash
# upload each dataset into its own subfolder + the PDFs (use IMMUTABLE filenames)
databricks fs cp -r data/reference_data   dbfs:/Volumes/investsphere/bronze/raw/  # then arrange per-table subfolders
databricks fs cp -r data/transaction_data dbfs:/Volumes/investsphere/bronze/raw/
databricks fs cp -r data/valuation_data   dbfs:/Volumes/investsphere/bronze/raw/
databricks fs cp -r data/documents        dbfs:/Volumes/investsphere/ai/documents/
```
(Each source must land in `/Volumes/investsphere/bronze/raw/<table_name>/`.)
**Checkpoint:** `SELECT * FROM ...` / `LIST` shows files in the volumes.

## Phase 5 — Governance (RBAC, RLS, masking)
1. Create account groups: `investsphere_engineers`, `investsphere_analysts`, `investsphere_pms`.
2. Run `governance/02_grants.sql`, then `governance/03_row_and_column_security.sql`.
3. Fill `investsphere.governance.pm_portfolio_map` with real user emails.
**Checkpoint:** a PM user sees only their portfolios; analyst sees masked counterparty.

## Phase 6 — Medallion run + verify + benchmark
Run the scripts (notebook or job): `bronze_ingest.py` → `silver_conform.py` → `gold_marts.py`.
```sql
SELECT * FROM investsphere.gold.fact_limit_breach;   -- expect GULF_EQ Banking breach
```
Run `reconcile_gold.py` (must print reconciliation OK). **Capture benchmark numbers now:**
- time a **full backfill** vs a **single-day incremental** run (Workflows run history)
- query profile: **files/bytes scanned** for a one-portfolio/date query, before vs after liquid clustering
**Checkpoint:** breach matches the demo; reconciliation passes; you have screenshots.

## Phase 7 — Lakeflow declarative pipeline
Create a **Lakeflow pipeline** pointing at `pipelines/lakeflow_pipeline.py` (serverless,
Photon). Run it. **Checkpoint:** `silver_transactions` + `dim_investment_asset_history`
(SCD2) materialise; expectation metrics show in the pipeline UI.

## Phase 8 — Orchestrate via Asset Bundle + params/condition task
Compute is **serverless** (Databricks Free Edition): the bundle uses per-job
`environments:` + task `environment_key:` (NOT `job_clusters`). The
`investsphere_platform` library is built into a **wheel** (`pyproject.toml` has a
`[build-system]` table) and attached via each job's serverless
`environments.spec.dependencies` — there are no `sys.path` shims.
```bash
# build the platform wheel (the bundle's artifacts: block builds/uploads it on deploy)
python -m pip wheel . --no-deps -w dist
databricks bundle validate -t dev          # real pre-deploy check (serverless env + deps)
databricks bundle deploy -t dev            # builds/uploads the wheel via artifacts:, attaches it
databricks bundle run investsphere_eod -t dev --params run_date=2026-05-29
```
**Checkpoint:** the EOD job runs `readiness_gate → feeds_ready (condition) → silver → gold → export_metrics`; missing-feed runs branch to `notify_not_ready`.

## Phase 9 — Monitoring
- Run `quality_monitoring/custom_dq_checks.sql`; optionally `create_data_quality_monitor.py`.
- Build **AI/BI dashboards** from `dashboards/dashboard_queries.sql` + `platform_ops_dashboard.sql`.
- (Optional) external: run Prometheus + Pushgateway + Grafana in Docker locally; pass
  `--pushgateway_url` to `export_pipeline_metrics.py`; import `dashboards/grafana_pipeline_dashboard.json`.
**Checkpoint:** ops dashboard shows job runs (system.lakeflow) + spend (system.billing).

## Phase 10 — Storage lifecycle + redundancy
```bash
az storage account management-policy create --account-name $SA -g $RG \
  --policy @storage_lifecycle/adls_lifecycle_policy.json
```
(Or `terraform apply` in `storage_lifecycle/`.) **Checkpoint:** policy listed; Delta paths NOT targeted.

## Phase 11 — ML loop (feature store → batch inference → monitoring) — FREE
Runs after the medallion; all serverless, ~AED 0 (no paid endpoint). Either run the chained
job or the steps individually:
```bash
databricks bundle run investsphere_ml_pipeline -t dev      # feature→train→infer→monitor
```
1. `features/build_portfolio_features.py` — UC feature table `features.portfolio_daily_features`.
2. `ml/prepare_training_set.py` — leakage-controlled labelled set (FeatureLookup).
3. `ml/train_breach_model.py` — train + MLflow + register `ml.breach_classifier` in UC.
4. `ml/batch_inference.py` — score → `ml.breach_predictions` (prediction, probability, model version, audit).
5. `ml/model_monitoring.py` — predicted-vs-actual + PSI drift → `governance.dq_results` (`ml_*`).
**Checkpoint:** `ml.breach_predictions` has rows; `dq_results` shows `ml_*` checks. The EOD job
also triggers this automatically after Gold (`run_job_task`).
*(Real-time Model Serving is an optional, paid extension — intentionally not built.)*

## Phase 12 — AI copilot (do last; tear down after)
1. `ai/01_build_ai_search_index.py` — builds the AI Search index over **approved** PDFs.
2. `ai/02_agent_tools.sql` — create UC function tools; register on a managed MCP server.
3. `ai/agent_exposure_policy.py` — run the headline question; **screenshot the grounded answer.**
4. `ai/evaluate_agent.py` — run MLflow GenAI eval; **screenshot the scores.**
5. **DELETE the AI Search index/endpoint** (billing stops ~24h after the last index).
**Checkpoint:** grounded answer + eval captured; endpoint deleted.

## Phase 13 — Perf-test ONCE (scale evidence)
Run `pipelines/generate_synthetic_data.py` with `--mode perftest` under a spend cap;
re-run gold; **capture runtime + DBU** (`system.billing.usage`); then **drop the synth tables**.
**Checkpoint:** you have measured scale numbers; synthetic data removed.

## Phase 14 — Teardown (stop all cost)
```bash
databricks bundle destroy -t dev          # remove jobs/pipelines
# delete AI Search indexes/endpoints (if not already)
az group delete -n $RG --yes --no-wait    # removes workspace + storage when fully done
```

---

## Verification checklist
- [ ] 21 unit tests pass locally
- [ ] `/gold.fact_limit_breach` shows GULF_EQ Banking breach; `reconcile_gold` OK
- [ ] RLS/masking verified with two user personas
- [ ] EOD job: condition task branches correctly
- [ ] Lakeflow pipeline + SCD2 history materialise
- [ ] Ops dashboard reads system tables; benchmark screenshots captured
- [ ] AI copilot answer + MLflow eval captured, **then endpoint deleted**
- [ ] Perf-test numbers captured, synth dropped
- [ ] Resources torn down; final cost read from `system.billing.usage`

## Cost-control reminders
Auto-terminate every cluster · serverless scale-to-zero · **delete AI Search after demo** ·
run perf-test once · keep Prometheus/Grafana local · watch the Azure budget alert.
