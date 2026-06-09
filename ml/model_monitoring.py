# Databricks script -- MODEL MONITORING.
#
# Runs after batch inference. Compares the model's PREDICTIONS against the ACTUAL
# breach outcomes (where available) and records ML health metrics into the SAME
# governed table the data-quality checks use -- governance.dq_results -- so the existing
# Databricks SQL Alerts (terraform/dq_alerts.tf) and dashboards pick up ML drift/quality
# for free, with no new alerting plumbing.
#
#   Input:  investsphere.ml.breach_predictions     (from ml/batch_inference.py)
#           investsphere.gold.fact_limit_breach     (ground-truth breach outcomes)
#   Output: rows in investsphere.governance.dq_results  (check_name LIKE 'ml_%')
#
# Metrics:
#   ml_prediction_volume        -- rows scored this run            (pass if > 0)
#   ml_predicted_breach_rate    -- mean predicted breach flag      (informational)
#   ml_actual_breach_rate       -- mean actual breach flag         (informational)
#   ml_pred_vs_actual_accuracy  -- agreement vs actuals            (pass if >= 0.5)
#   ml_missing_feature_rows     -- feature rows with no prediction (pass if == 0)
#   ml_probability_psi          -- PSI across the two latest dates  (pass if < 0.25)
#
# The maths lives in investsphere_platform.ml.monitoring (pure, unit-tested); this script
# only does the Spark reads/aggregation and the dq_results write.

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, DateType, TimestampType,
)

from investsphere_platform.ml import monitoring as mon

spark = SparkSession.builder.getOrCreate()


def _widget(name, default):
    try:
        value = dbutils.widgets.get(name)        # noqa: F821 (dbutils provided by Databricks)
        return value if value else default
    except Exception:
        return default


CATALOG = _widget("catalog", "investsphere")
RUN_DATE = _widget("run_date", None)
JOB_RUN_ID = _widget("job_run_id", None)

ML_SCHEMA = CATALOG + ".ml"
GOLD = CATALOG + ".gold"
GOV = CATALOG + ".governance"
FEATURES_SCHEMA = CATALOG + ".features"
PREDICTIONS_TABLE = ML_SCHEMA + ".breach_predictions"
FEATURE_TABLE = FEATURES_SCHEMA + ".portfolio_daily_features"
DQ_TABLE = GOV + ".dq_results"

# governance schema + dq_results must exist (same DDL as quality_monitoring/custom_dq_checks.sql).
spark.sql("CREATE SCHEMA IF NOT EXISTS " + GOV)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DQ_TABLE} (
  check_timestamp TIMESTAMP, check_name STRING, source_table STRING, business_date DATE,
  pipeline_run_id STRING, job_run_id STRING, metric_value DOUBLE, threshold DOUBLE, passed BOOLEAN
)""")

if not spark.catalog.tableExists(PREDICTIONS_TABLE):
    raise RuntimeError(
        f"{PREDICTIONS_TABLE} not found -- run ml/batch_inference.py before monitoring")

# ---- read predictions + actuals on the driver (demo scale is small) ----
preds = spark.table(PREDICTIONS_TABLE).select(
    "portfolio_id", "as_of_date", "prediction", "breach_probability")

# actual breach outcomes: a (portfolio_id, as_of_date) in fact_limit_breach => breached
actuals = (spark.table(GOLD + ".fact_limit_breach")
           .select("portfolio_id", "as_of_date").distinct()
           .withColumn("actual", F.lit(1)))

joined = (preds.join(actuals, ["portfolio_id", "as_of_date"], "left")
          .withColumn("actual", F.coalesce(F.col("actual"), F.lit(0))))

rows = joined.collect()
predicted = [int(r["prediction"]) for r in rows]
actual = [int(r["actual"]) for r in rows]
probs = [float(r["breach_probability"]) for r in rows
         if r["breach_probability"] is not None]

# ---- metrics via the pure helpers ----
volume = float(len(rows))
pred_rate = mon.breach_rate(predicted)
act_rate = mon.breach_rate(actual)
accuracy = mon.agreement_accuracy(predicted, actual)

# coverage: feature rows that did NOT get a prediction (inference completeness)
feature_keys = spark.table(FEATURE_TABLE).select("portfolio_id", "as_of_date").distinct()
pred_keys = preds.select("portfolio_id", "as_of_date").distinct()
missing = float(feature_keys.join(pred_keys, ["portfolio_id", "as_of_date"], "left_anti").count())

# drift: PSI between the probability distributions of the two most recent business dates.
distinct_dates = sorted([r["as_of_date"] for r in
                         preds.select("as_of_date").distinct().collect()])
if len(distinct_dates) >= 2:
    prev_d, curr_d = distinct_dates[-2], distinct_dates[-1]
    base = [float(r["breach_probability"]) for r in rows
            if r["as_of_date"] == prev_d and r["breach_probability"] is not None]
    curr = [float(r["breach_probability"]) for r in rows
            if r["as_of_date"] == curr_d and r["breach_probability"] is not None]
    psi = mon.population_stability_index(base, curr)
    psi_passed = mon.passes(psi, 0.25, "le")
else:
    psi, psi_passed = None, None   # not enough history to assess drift

dist = mon.distribution_summary(probs)
print("probability distribution:", dist)

# ---- assemble dq_results rows: (check_name, source_table, metric_value, threshold, passed) ----
metrics = [
    ("ml_prediction_volume",       PREDICTIONS_TABLE, volume,    1.0,  volume >= 1.0),
    ("ml_predicted_breach_rate",   PREDICTIONS_TABLE, pred_rate, None, None),
    ("ml_actual_breach_rate",      GOLD + ".fact_limit_breach", act_rate, None, None),
    ("ml_pred_vs_actual_accuracy", PREDICTIONS_TABLE, accuracy,  0.5,  mon.passes(accuracy, 0.5, "ge")),
    ("ml_missing_feature_rows",    FEATURE_TABLE,     missing,   0.0,  mon.passes(missing, 0.0, "le")),
    ("ml_probability_psi",         PREDICTIONS_TABLE, psi,       0.25, psi_passed),
]

schema = StructType([
    StructField("check_name", StringType()), StructField("source_table", StringType()),
    StructField("metric_value", DoubleType()), StructField("threshold", DoubleType()),
    StructField("passed", BooleanType()),
])
results = (spark.createDataFrame(
        [(n, s, (float(v) if v is not None else None), (float(t) if t is not None else None), p)
         for (n, s, v, t, p) in metrics], schema)
    .withColumn("check_timestamp", F.current_timestamp())
    .withColumn("business_date",
                F.lit(RUN_DATE).cast(DateType()) if RUN_DATE else F.current_date())
    .withColumn("pipeline_run_id", F.lit(None).cast(StringType()))
    .withColumn("job_run_id", F.lit(JOB_RUN_ID))
    .select("check_timestamp", "check_name", "source_table", "business_date",
            "pipeline_run_id", "job_run_id", "metric_value", "threshold", "passed"))

results.write.format("delta").mode("append").saveAsTable(DQ_TABLE)

print(f"wrote {len(metrics)} ml_* monitoring rows -> {DQ_TABLE}")
results.show(truncate=False)
