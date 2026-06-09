# InvestSphere — Platform Operations Runbook

How to deploy, run, monitor and recover the platform. Pair with the ops dashboard
(`dashboards/platform_ops_dashboard.sql`).

## Deploy
Compute is **serverless** (Databricks Free Edition): jobs use per-job `environments:` +
task `environment_key:` (no `job_clusters`). The `investsphere_platform` library is
built into a **wheel** (`pyproject.toml` `[build-system]` table; `python -m pip wheel .
--no-deps -w dist`), built/uploaded by the bundle's `artifacts:` block and attached via
each job's serverless `environments.spec.dependencies` (no `sys.path` shims).
1. `databricks bundle validate -t dev` (real pre-deploy check) then `databricks bundle deploy -t dev`.
2. One-time governance: run `governance/01..03` (create groups + `pm_portfolio_map` first).
3. One-time volumes: create `bronze.raw`, `ai.documents`, **and** `bronze._checkpoints`
   + `bronze._schemas` (Auto Loader checkpoint/schema dirs must be UC Volumes — see playbook below).
4. One-time AI (optional): `ai/01_build_ai_search_index.py` (approved docs only).

## Normal operation (what runs when)
| Job | When | Owner action |
|-----|------|--------------|
| `investsphere_ingest` | on file arrival | none (automatic) |
| `investsphere_eod` | daily 18:00 GST | check it went green on the ops dashboard |
| `investsphere_private_nav` | monthly | confirm new private/fund valuations landed |
| `investsphere_docs_rag` | on doc upload | confirm AI Search sync completed |
| `investsphere_maintenance_demo` | weekly | (prod: rely on Predictive Optimization) |

## Daily on-call checklist (ops dashboard)
- Tile 1: last `investsphere_eod` run `SUCCEEDED`.
- Tile 3: all DQ checks `passed = true` (latest row per `check_name`).
- Tile 4: per-run quarantine rate within threshold (**< 2%** — the alert fires above it).
- Tile 5: `latest_holding_date` = today's run date (freshness).
- Tile 2: spend not unexpectedly rising (see AI Search below).
- No open DQ alerts (SQL Alerts; see *Data-quality model & alerting* below).

## Data-quality model & alerting
The platform follows **Observable → Alertable → Actionable**. Three layers:

### 1. Bronze — rescued-data handling (schema drift)
Bronze ingests with Auto Loader `schemaEvolutionMode="rescue"` +
`rescuedDataColumn="_rescued_data"` (`pipelines/bronze_ingest.py`,
`pipelines/lakeflow_pipeline.py`). The explicit schema in `pipelines/schemas.py` is the
**contract**. When an upstream feed adds / renames / mis-types a column, that data is
**captured in `_rescued_data`** — it is *not* auto-promoted into Silver/Gold (we
deliberately avoid `addNewColumns`) and it does *not* crash the stream. Flow when drift
appears: `_rescued_data` populated → `bronze_rescued_rows_count` alert fires → engineer
reviews the rescued payload → if approved, **edits `schemas.py` by hand** → redeploys.
Curated layers never change shape without a human in the loop.

### 2. Silver — quarantine design (tiered policy)
`pipelines/silver_conform.py` (mirrored, unit-tested, in
`investsphere_platform.quality.dq_policy` / `quarantine_rate`):
- **FAIL** — a NULL `transaction_id` (missing PK) raises and stops the job. Unauditable
  rows are never silently dropped or quarantined.
- **QUARANTINE** — bad FK (asset/portfolio), non-positive quantity, invalid type, or
  Bronze schema drift → appended to `investsphere.silver.quarantine_transaction` with
  `quarantine_reason`, `source_table`, `source_file`, `ingestion_ts`, `business_date`,
  `pipeline_run_id`, `job_run_id`, `check_timestamp`, and the full `raw_payload` (so a
  row can be audited and replayed). The table is **append-only** (an audit trail).
- **WARN** — optional field missing (e.g. counterparty) → kept and counted only.

### 3. governance.dq_results → SQL Alerts (the pager)
- The EOD `dq_checks` task runs `quality_monitoring/custom_dq_checks.sql` (params
  `:business_date`, `:job_run_id`) which appends one row per metric to
  `investsphere.governance.dq_results`.
- The **per-run quarantine rate** is authoritative from `silver_conform.py`, not the
  SQL recompute: because the quarantine table is append-only, the rate is computed from
  **this run's** exact counts and tagged with `pipeline_run_id`. A cumulative table
  count would inflate the rate forever — so alerts read the per-run row.
- **Databricks SQL Alerts** (queries in `quality_monitoring/dq_alerts.sql`, provisioned
  by `terraform/dq_alerts.tf`) re-run every 30 min and notify on-call on breach:
  schema drift (>0), quarantine rate (>2%), missing feed (>0), freshness (>1 day),
  expectation failures (>0), breach spike (>1.5× trailing-7-day avg).
- **Parallel path:** `export_pipeline_metrics.py` pushes the same signals (incl.
  `bronze_rescued_rows`) to Prometheus/Grafana — see `docs/MONITORING_EXTERNAL.md`. The
  SQL-alerts loop is the pager; Prometheus/Grafana is the time-series view.

> **Lakeflow event-log caveat (check #7).** `expectation_failed_records` reads the
> Lakeflow pipeline event log via `event_log(TABLE(...))`. The exact form depends on
> your workspace/runtime — it may need the pipeline **id** instead of the output table,
> or you may have to **publish** the event log to a table and query that. Confirm the
> reference resolves in your workspace before relying on that one alert; the comment in
> `custom_dq_checks.sql` lists the alternatives.

## Incident playbooks
**EOD readiness gate FAILED** (`eod_readiness_check.py` raised)
- Cause: a price / FX / holdings feed for the run date hasn't arrived.
- Action: confirm files landed in the right `/raw/<source>/` folder with a *new*
  filename; re-drop if needed; re-run `investsphere_eod`. Do NOT bypass the gate —
  it exists to stop Gold computing on incomplete data.

**Quarantine rate alert** (`transaction_quarantine_rate_pct` > 2%)
- Inspect this run's rows: `SELECT quarantine_reason, count(*) FROM
  investsphere.silver.quarantine_transaction WHERE pipeline_run_id = '<run>' GROUP BY 1`.
- Common: unknown `investment_asset_id` (missing asset master row) or bad
  `transaction_type`. Fix the source/reference data; reprocess. Use `raw_payload` to
  replay specific rows.

**Schema-drift alert** (`bronze_rescued_rows_count` > 0)
- An upstream feed changed shape; the data was rescued, not promoted. Inspect:
  `SELECT _rescued_data FROM investsphere.bronze.raw_<source> WHERE _rescued_data IS NOT NULL`.
- If the change is legitimate, update `pipelines/schemas.py` to add/adjust the column,
  redeploy, and reprocess. Do NOT enable `addNewColumns` to "fix" it automatically.

**Gold integrity failure** (`gold_marts.py` raised)
- Cause: a data-integrity violation — holdings without a price, NULL `market_value`, or
  a single-sector `exposure_pct > 100` (broken calculation). These FAIL by design.
- A limit **breach** is NOT this — breaches are valid business output and never fail Gold.
- Action: find the offending assets (missing-price check #1 / #4 in `dq_results`), fix
  the price/holdings feed or reference mapping, then re-run `investsphere_eod`.

**Bronze job "succeeded" but no tables were written**
- Auto Loader writes nothing yet the script-task exits green. Three causes:
  1. **Missing checkpoint/schema volumes.** Auto Loader's checkpoint + schema-tracking
     dirs must be UC Volumes. Create them and re-run:
     ```sql
     CREATE VOLUME IF NOT EXISTS investsphere.bronze.`_checkpoints`;
     CREATE VOLUME IF NOT EXISTS investsphere.bronze.`_schemas`;
     ```
     (error tell: `UC_VOLUME_NOT_FOUND`; the job may still report success).
  2. **Empty source folder** — no files under `/Volumes/investsphere/bronze/raw/<table>/`.
  3. **Stale Auto Loader checkpoint** — clear `_checkpoints` / `_schemas` and re-run.
- Note: `bronze_ingest.py` collects the `availableNow` streaming queries and calls
  `awaitTermination()` on each. Without it the Python script-task exits before the
  streams commit and the job "succeeds" with no table written.

**Job failed**
- Open the run in Workflows → failed task logs. Pipelines are idempotent
  (MERGE / replaceWhere; quarantine append is keyed by `pipeline_run_id`), so re-running
  `investsphere_eod` is safe.

**AI Search cost rising** (Tile 2)
- A Standard endpoint bills while up (~$0.28/hr). If not actively demoing, **delete
  the AI Search indexes**; endpoint charges stop ~24h after the last index is
  deleted. Keep a single endpoint for small workloads.

**Backfill / reprocess a date**
- Re-run `investsphere_eod` for the date; `replaceWhere` overwrites only that date's
  exposure/breach slice and the holding MERGE upserts by key — no duplicates.

## Cost controls
- Develop on **Free Edition** (≈ AED 0). Auto-terminate clusters; prefer serverless
  scale-to-zero. Delete AI Search indexes when idle. Run `perftest` once under a
  spend cap, then drop the synthetic tables. Production maintenance via Predictive
  Optimization.

## Teardown (stop all cost)
1. Delete AI Search indexes (endpoint stops billing ~24h later).
2. Pause/delete the scheduled jobs (`databricks bundle destroy -t dev`).
3. Drop synthetic perftest tables if created.
