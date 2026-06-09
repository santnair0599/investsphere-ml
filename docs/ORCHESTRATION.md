# InvestSphere — Parameterization & Conditional Logic

How runs are parameterized and how the pipeline branches at runtime. Maps to the
Databricks "parameterization + conditional task flow" expectations.

## 1. Parameterization

### Bundle level (`databricks.yml`)
- **`variables.catalog`** (default `investsphere`) and **`targets` `dev`/`prod`** —
  environment parameterization; `${bundle.target}` is stamped into job tags.
- **`variables.oncall_email`** (default alert recipient) and **`variables.warehouse_id`**
  (SQL warehouse for the DQ checks) — overridable per target.

### Compute & alerting (`databricks.yml`)
- **Serverless compute** (this workspace is Databricks Free Edition): each job declares
  an **`environments:`** block (`spec.client` + `dependencies`) and every task references
  it via **`environment_key: default`** — no classic `job_clusters`. The reusable library
  reaches the tasks as a **wheel**: the bundle's **`artifacts:`** block builds it
  (`python -m pip wheel . --no-deps -w dist`) and each environment lists it under
  `dependencies` (plus `prometheus_client` on the EOD job, `pypdf` on the docs→RAG job).
- **Alerting on both layers** → `${var.oncall_email}`: jobs via `email_notifications`
  (`on_failure` + duration warning); the Lakeflow pipeline via its own `notifications`
  block (`on-update-failure` / `on-update-fatal-failure` / `on-flow-failure`).

### Job parameters (per-run, overridable)
Defined on the EOD job and passed to tasks as `{{job.parameters.<name>}}`:
| Parameter | Default | Used by |
|---|---|---|
| `run_date` | `2026-05-29` | readiness gate, `gold_marts`, `reconcile_gold`, `silver_reference_scd2` |
| `catalog` | `investsphere` | all bronze/silver/gold scripts (`CATALOG + ".bronze"` etc.) |
| `pushgateway_url` | `http://pushgateway:9091` | `export_pipeline_metrics` |
| `mode` | `realistic` | `generate_synthetic_data` (dev/demo/realistic/perftest) |
| `run_ts` | `2026-05-29T08:00` | `bronze_ingest` (batch id) |

### How scripts read them (`pipelines/job_params.py`)
One helper, `get_param(name, default)`, resolves a value in this order:
1. **Databricks widgets / job parameters** — `dbutils.widgets.get(name)`
2. **`spark_python_task` argv** — `--name value`
3. **default** — so the same scripts also run locally / in tests

```python
from job_params import get_param
RUN_DATE = get_param("run_date", "2026-05-29")
CATALOG  = get_param("catalog", "investsphere")
```

### Running with overrides
```bash
databricks bundle run investsphere_eod -t prod \
  --params run_date=2026-06-04,catalog=investsphere,pushgateway_url=http://pg:9091
```

## 2. Conditional logic

### (a) Databricks Workflows CONDITION TASK (orchestration branching)
The EOD job is **not** a straight line — it branches on data readiness:
```
readiness_gate  →  feeds_ready (condition_task)  ─true→  silver → gold → export_metrics
                                                  └false→ notify_not_ready  (Gold skipped)
```
- `eod_readiness_check.py` checks the price/FX/holdings feeds for `run_date` and
  **publishes a task value**: `dbutils.jobs.taskValues.set(key="feeds_ready", value="true"|"false")`.
- The **`condition_task`** evaluates it:
  ```yaml
  condition_task:
    op: EQUAL_TO
    left: "{{tasks.readiness_gate.values.feeds_ready}}"
    right: "true"
  ```
- Downstream tasks branch via `depends_on … outcome`:
  `silver` depends on `feeds_ready` **outcome `"true"`**; `notify_not_ready` on
  **outcome `"false"`**. So Gold runs only when feeds are present — no analytics on
  incomplete data, and no job *failure* (it branches cleanly).

### (b) Data-level conditional rules (Lakeflow expectations)
`lakeflow_pipeline.py` drops rows conditionally and tracks the rate:
```python
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_drop("valid_transaction_type", "transaction_type IN ('BUY','SELL','ACQUISITION','DISPOSAL')")
```

### (d) Cross-job chaining — the ML pipeline after Gold (`run_job_task`)
The ML loop is a **separate job** (`investsphere_ml_pipeline`) rather than more tasks on the
EOD job, so its ML dependencies (`scikit-learn`, `mlflow`, `databricks-feature-engineering`)
don't bloat the EOD environment. The EOD job triggers it once Gold is ready:
```
gold  →  ml_pipeline (run_job_task → investsphere_ml_pipeline)
            feature_build → prepare_training_set → train_model → batch_inference → model_monitoring
```
```yaml
- task_key: ml_pipeline
  depends_on: [{ task_key: gold }]
  run_job_task:
    job_id: ${resources.jobs.investsphere_ml_pipeline.id}
    job_parameters: { catalog: "{{job.parameters.catalog}}",
                      run_date: "{{job.parameters.run_date}}", job_run_id: "{{job.run_id}}" }
```
The ML job exposes `catalog` / `run_date` / `job_run_id` as **job parameters** (surfaced to the
`spark_python_task` scripts as widgets). Run it standalone with
`databricks bundle run investsphere_ml_pipeline -t dev`. Scoring is **offline batch** only —
no paid Model Serving endpoint.

### (c) Application-level conditionals (idempotency)
- **Create-else-MERGE**: `if not spark.catalog.tableExists(t): create  else: MERGE`
  (`gold_marts.py`, `silver_conform.py`) → idempotent first-run vs incremental.
- **`replaceWhere`** overwrites only the run-date slice (exposure/breach).
- **SCD2** opens/closes versions only when tracked attributes change
  (`scd2.apply_scd2` / AUTO CDC FROM SNAPSHOT).
- **`mode` preset** selection in `generate_synthetic_data.py`.

## Summary
| Mechanism | Where | Type |
|---|---|---|
| Bundle variables + dev/prod targets | `databricks.yml` | parameterization |
| Job parameters (`run_date`/`catalog`/…) + `get_param` (widgets→argv→default) | `job_params.py`, all scripts | parameterization |
| `condition_task` on a `feeds_ready` task value, `depends_on … outcome` branches | EOD job | orchestration conditional |
| `run_job_task` chaining EOD → `investsphere_ml_pipeline` after Gold | EOD job | cross-job orchestration |
| `expect_or_drop` expectations | Lakeflow pipeline | data conditional |
| create-else-MERGE / replaceWhere / SCD2 / mode preset | pipelines | application conditional |
