# InvestSphere — Optimization Patterns Implemented

What was changed, why it matters, and where it lives. **These are optimisation
*patterns implemented*, not measured *performance improvements*** — quote real
before/after numbers only after running and measuring (template at the bottom).

| # | Optimization | Before | After | Where |
|---|---|---|---|---|
| 1 | **Explicit schemas + rescue** | `inferSchema=true` (extra scan, silent drift) | `StructType` per source; Auto Loader `schemaEvolutionMode=rescue` captures drift in `_rescued_data` (no auto-promote, no crash) | `pipelines/schemas.py`, `pipelines/bronze_ingest.py` |
| 2 | **Auto Loader (incremental)** | `spark.read.csv` full reload | `cloudFiles` streams only new files, `availableNow` trigger (queries `awaitTermination()`-ed so the task doesn't exit before streams commit) | `pipelines/bronze_ingest.py` |
| 3 | **Mixed-format ingestion** | all CSV | reference = CSV, transaction feeds = JSON (realistic) | `schemas.py` `FORMATS`, generator |
| 4 | **MERGE upsert (idempotent)** | full `overwrite` each run | `silver_transaction` + `fact_daily_holding` MERGE on key | `silver_conform.py`, `gold_marts.py` |
| 5 | **replaceWhere (idempotent slice)** | full overwrite | exposure/breach overwrite only the run date | `gold_marts.py` |
| 6 | **Broadcast joins** | `.collect()` keys to driver (anti-pattern) | `F.broadcast()` of small dims; null-check joins | `silver_conform.py`, `gold_marts.py` |
| 7 | **Liquid clustering** | no data layout | `clusterBy(portfolio_id, as_of_date)` on facts | `gold_marts.py` |
| 8 | **OPTIMIZE / VACUUM / Predictive Optimization** | none | maintenance demo + Predictive Optimization narrative | `pipelines/maintenance.py` |
| 9 | **Declarative pipeline + EXPECTATIONS** | imperative only | Lakeflow Declarative Pipelines with native DQ expectations | `pipelines/lakeflow_pipeline.py` |
| 10 | **Native SCD2 (AUTO CDC FROM SNAPSHOT)** | custom logic | Lakeflow AUTO CDC FROM SNAPSHOT builds Type-2 history | `pipelines/lakeflow_pipeline.py` |
| 11 | **Serverless compute + Photon (pipeline)** | fixed single worker | serverless `environments` (per-job `spec` + `environment_key`, no job clusters); Photon on the Lakeflow pipeline (`photon: true`) | `databricks.yml` |
| 12 | **CI** | none | GitHub Actions runs pytest on every push | `.github/workflows/ci.yml` |
| 13 | **Cost/governance tags + secrets** | none | job tags; `dbutils.secrets` guidance | `databricks.yml` |

## Notes
- **Liquid clustering keys** are chosen for the common access pattern (`portfolio_id`,
  `as_of_date`). In production I would evaluate **`CLUSTER BY AUTO`** or revise keys
  from measured query history rather than guessing.
- **AQE** is on by default in the runtime. Don't claim a specific AQE plan from a SQL
  `EXPLAIN`; verify after a run with `DataFrame.explain()` or the Spark UI.
- **Idempotency** (MERGE / replaceWhere) makes re-running a job safe — no duplicate
  rows, minimal rewrite.
- **Code organisation, not two production pipelines:** the repo has reusable
  pure-Python/PySpark modules + local tests; the **deployed execution path is the
  Lakeflow Spark Declarative Pipeline** (incremental ingestion, expectations, SCD2).
  The imperative scripts illustrate the mechanics and are not a second production
  pipeline producing the same tables.

## Measurement template (fill in after a timed run — then you may say "improved")
| Measurement | Before | After |
|---|---|---|
| Full refresh runtime | _measure_ | _measure_ |
| Incremental EOD run runtime | _measure_ | _measure_ |
| Files scanned for one valuation date | _measure_ | _measure_ |
| Exposure query runtime | _measure_ | _measure_ |
| Quarantine validation (record counts) | _measure_ | _measure_ |
