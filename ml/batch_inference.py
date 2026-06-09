# Databricks script -- BATCH INFERENCE (offline scoring).
#
# Closes the ML loop: the model trained + registered by ml/train_breach_model.py is
# loaded from the Unity Catalog model registry and used to SCORE every portfolio/date
# in the feature table. Results land in a governed Delta table the dashboards and the
# monitoring step read.
#
#   Input:  registered UC model  investsphere.ml.breach_classifier  (latest version)
#           feature table        investsphere.features.portfolio_daily_features
#   Output: predictions table    investsphere.ml.breach_predictions
#
# Prerequisites: features/build_portfolio_features.py and ml/train_breach_model.py have run.
#
# No paid Model Serving endpoint is created -- this is OFFLINE batch scoring on serverless
# (the registered sklearn model is loaded into the driver, exactly like training). Real-time
# Model Serving is a documented OPTIONAL paid enhancement, not implemented here.

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()


def _widget(name, default):
    """Read a job parameter exposed as a widget, falling back to a default for ad-hoc runs."""
    try:
        value = dbutils.widgets.get(name)        # noqa: F821 (dbutils provided by Databricks)
        return value if value else default
    except Exception:
        return default


CATALOG = _widget("catalog", "investsphere")
RUN_DATE = _widget("run_date", None)             # business date of this scoring run (audit)
JOB_RUN_ID = _widget("job_run_id", None)         # set when triggered by the EOD job

FEATURES_SCHEMA = CATALOG + ".features"
ML_SCHEMA = CATALOG + ".ml"
FEATURE_TABLE = FEATURES_SCHEMA + ".portfolio_daily_features"
MODEL_NAME = ML_SCHEMA + ".breach_classifier"        # UC 3-level name
PREDICTIONS_TABLE = ML_SCHEMA + ".breach_predictions"

# Feature order MUST match training (ml/prepare_training_set.py FeatureLookup order).
FEATURE_COLS = [
    "total_market_value", "num_holdings", "num_sectors",
    "top_sector_exposure_pct", "concentration_hhi",
]

spark.sql("CREATE SCHEMA IF NOT EXISTS " + ML_SCHEMA + " COMMENT 'Registered ML models + scores'")

# ---- 1. resolve + load the latest registered model version from Unity Catalog ----
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
if not versions:
    raise RuntimeError(
        f"no registered versions for {MODEL_NAME}; run ml/train_breach_model.py first")
latest = max(versions, key=lambda v: int(v.version))
model_version = str(latest.version)
model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{model_version}")
print("loaded model:", MODEL_NAME, "version:", model_version)

# ---- 2. score the feature table ----
# Demo scale is small, so we score on the driver via pandas (same style as training).
# At larger scale you'd use mlflow.pyfunc.spark_udf for distributed scoring -- noted in docs.
feature_sdf = spark.table(FEATURE_TABLE).select("portfolio_id", "as_of_date", *FEATURE_COLS)
pdf = feature_sdf.toPandas()
if pdf.empty:
    print(f"feature table {FEATURE_TABLE} is empty -- nothing to score; exiting cleanly")
else:
    X = pdf[FEATURE_COLS]
    pdf["prediction"] = model.predict(X).astype(int)
    # predict_proba may be unavailable for some estimators; guard it.
    try:
        pdf["breach_probability"] = model.predict_proba(X)[:, 1].astype(float)
    except Exception:
        pdf["breach_probability"] = float("nan")

    scored = spark.createDataFrame(pdf[[
        "portfolio_id", "as_of_date", "prediction", "breach_probability",
    ]])

    # ---- 3. add audit columns ----
    scored = (scored
        .withColumn("model_name", F.lit(MODEL_NAME))
        .withColumn("model_version", F.lit(model_version))
        .withColumn("inference_run_date",
                    F.lit(RUN_DATE).cast("date") if RUN_DATE else F.current_date())
        .withColumn("scored_at", F.current_timestamp())
        .withColumn("job_run_id", F.lit(JOB_RUN_ID)))

    # ---- 4. idempotent upsert: re-running a date refreshes its predictions ----
    if not spark.catalog.tableExists(PREDICTIONS_TABLE):
        (scored.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .saveAsTable(PREDICTIONS_TABLE))
    else:
        (DeltaTable.forName(spark, PREDICTIONS_TABLE).alias("t")
            .merge(scored.alias("s"),
                   "t.portfolio_id = s.portfolio_id AND t.as_of_date = s.as_of_date")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())

    n = scored.count()
    print(f"scored {n} portfolio/date rows -> {PREDICTIONS_TABLE} "
          f"(model {MODEL_NAME} v{model_version})")
    scored.show(truncate=False)
