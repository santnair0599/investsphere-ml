# Feature Store & ML Training Slice

This variant of InvestSphere adds the **AI/ML pipeline** pillar on top of the governed
medallion, so the project covers all three requirements an ML-platform role asks for:

| Requirement | Covered by |
|---|---|
| **Feature store design** | `features/build_portfolio_features.py` → UC feature table `investsphere.features.portfolio_daily_features` (Databricks Feature Engineering; PK `portfolio_id, as_of_date`; `as_of_date` as the time-series key for point-in-time lookups) |
| **Data preparation for model training** | `ml/prepare_training_set.py` → derives a 0/1 breach label and assembles a labelled training set via a **FeatureLookup** (lineage back to the feature table) → `investsphere.features.breach_training_set` |
| **Model training + registry** | `ml/train_breach_model.py` → train/test split → LogisticRegression → **MLflow** tracking → registered UC model `investsphere.ml.breach_classifier` |
| **Batch inference (offline scoring)** | `ml/batch_inference.py` → loads the latest registered UC model, scores the feature table, upserts `investsphere.ml.breach_predictions` (with prediction, probability, model name/version, audit columns) |
| **Model monitoring** | `ml/model_monitoring.py` → compares predictions vs actual breaches and records `ml_*` metrics into `governance.dq_results` (reusing the existing DQ + SQL-alert pattern); maths in `investsphere_platform.ml.monitoring` (pure, unit-tested) |
| **Orchestration** | `databricks.yml` job `investsphere_ml_pipeline` chains feature build → training-set prep → train → batch inference → monitoring, triggered after Gold by the EOD job's `run_job_task` |
| **Vector database integration** | `ai/01_build_ai_search_index.py` + `agent_exposure_policy.py` → Databricks **AI Search (Vector Search)** delta-sync index, managed embeddings, RAG retrieval, MLflow GenAI eval |

## The pipeline (end-to-end loop)

```
Gold marts (fact_daily_holding / fact_portfolio_exposure / fact_limit_breach)
      │
      ▼  features/build_portfolio_features.py
UC FEATURE TABLE  investsphere.features.portfolio_daily_features
   (total_market_value, num_holdings, num_sectors,
    top_sector_exposure_pct, concentration_hhi)        ← no breach_count (leakage)
      │
      ▼  ml/prepare_training_set.py   (label = "did this portfolio breach?")
TRAINING SET  investsphere.features.breach_training_set   (FeatureLookup → features + label)
      │
      ▼  ml/train_breach_model.py     (split → LogisticRegression → MLflow)
REGISTERED UC MODEL  investsphere.ml.breach_classifier
      │
      ▼  ml/batch_inference.py        (load latest UC model → score feature table)
PREDICTIONS TABLE  investsphere.ml.breach_predictions
   (portfolio_id, as_of_date, prediction, breach_probability,
    model_name, model_version, inference_run_date, scored_at, job_run_id)
      │
      ▼  ml/model_monitoring.py       (predicted vs actual + drift)
governance.dq_results  (rows: ml_prediction_volume, ml_predicted_breach_rate,
   ml_actual_breach_rate, ml_pred_vs_actual_accuracy, ml_missing_feature_rows,
   ml_probability_psi)  →  existing Databricks SQL Alerts watch these
```

All five steps are orchestrated by the `investsphere_ml_pipeline` job in `databricks.yml`,
**chained after Gold** by the EOD job (`ml_pipeline` `run_job_task`). It is also runnable
standalone: `databricks bundle run investsphere_ml_pipeline -t dev`.

Separately, the **vector-DB / RAG** pillar (`ai/`) powers the policy copilot.

## Run order (Databricks)

1. Run the medallion so the Gold facts exist (`bronze_ingest` → `silver_conform` → `gold_marts`).
2. `features/build_portfolio_features.py` — build/refresh the feature table.
3. `ml/prepare_training_set.py` — build the labelled training set.
4. `ml/train_breach_model.py` — train, evaluate, register the model in UC.
5. `ml/batch_inference.py` — load the latest registered model and score → `ml.breach_predictions`.
6. `ml/model_monitoring.py` — compare predictions vs actuals + drift → `governance.dq_results`.
7. (Optional, paid) `ai/01_build_ai_search_index.py` for the Vector Search index.

Steps 2–6 run automatically as the `investsphere_ml_pipeline` job; run them individually
only for debugging.

## Monitoring metrics (`governance.dq_results`, `check_name LIKE 'ml_%'`)

| Metric | Meaning | Threshold |
|---|---|---|
| `ml_prediction_volume` | rows scored this run | pass if > 0 |
| `ml_predicted_breach_rate` | mean predicted breach flag | informational |
| `ml_actual_breach_rate` | mean actual breach flag (from `fact_limit_breach`) | informational |
| `ml_pred_vs_actual_accuracy` | agreement of prediction vs actual | pass if ≥ 0.5 |
| `ml_missing_feature_rows` | feature rows with no prediction (inference completeness) | pass if = 0 |
| `ml_probability_psi` | Population Stability Index across the two latest business dates (drift) | pass if < 0.25 |

## Design choices (and honest caveats)

- **No leakage:** `breach_count` is excluded from features — the model predicts breaches,
  so using the answer as an input would be cheating. Features are concentration/size only.
- **Same-day classification, not forecasting:** the demo data is essentially one snapshot
  date, so we classify the *current* day. With multi-date history you'd predict *next-day*
  breach from today's features — same code, richer label. (Stated in the script comments.)
- **Governed by design:** the feature table and the model both live in **Unity Catalog**
  (`features.*`, `ml.*`), so they inherit UC lineage, permissions, and versioning.
- **Runs on Free Edition:** feature table (Delta), training set (Delta), the sklearn +
  MLflow model, **batch inference, and monitoring** all run on serverless at ~AED 0. Only the
  Vector Search **endpoint** (RAG pillar) is paid.
- **Offline batch inference, not online serving:** `ml/batch_inference.py` loads the registered
  model and writes a Delta predictions table — **no paid real-time Model Serving endpoint is
  created.** Real-time Model Serving (a REST endpoint for synchronous scoring) is a documented
  **optional, paid future enhancement**, deliberately not implemented so the loop stays free.
- **Monitoring reuses the DQ pattern:** model metrics land in the same `governance.dq_results`
  table as the data-quality checks, so the existing Databricks SQL Alerts and ops dashboard
  cover ML drift/quality with no new alerting plumbing. The metric maths is pure Python in
  `investsphere_platform.ml.monitoring` and unit-tested (`tests/test_ml_monitoring.py`).

## Optional future enhancement (paid) — real-time Model Serving

Not implemented. To serve the same registered UC model synchronously you would create a
Databricks **Model Serving** endpoint over `investsphere.ml.breach_classifier` (billed while
provisioned) and call it via REST. The current design intentionally stops at governed offline
batch scoring, which satisfies the daily breach-risk use case at zero serving cost.

## What to say in an interview

> "Gold marts feed a Unity Catalog **feature store** (Databricks Feature Engineering),
> keyed by portfolio and date for point-in-time lookups. A **training-set prep** step
> derives the breach label and pulls features via FeatureLookup — leakage-controlled —
> then a tracked **MLflow** model is registered in UC. From there the loop closes:
> **batch inference** scores portfolios into a governed predictions table, and **model
> monitoring** writes predicted-vs-actual and PSI drift metrics into the same
> `dq_results` table my SQL alerts already watch — all orchestrated as a Databricks job
> chained after Gold. Alongside that, a **Vector Search** index over policy documents
> powers a governed RAG copilot. So the platform spans feature store → training data prep
> → model registry → **batch inference → monitoring → orchestration**, plus vector-DB
> integration — offline scoring only, with real-time Model Serving noted as an optional
> paid extension."
