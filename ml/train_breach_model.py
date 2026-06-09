# Databricks script -- MODEL TRAINING + REGISTRATION.
#
# Trains a simple classifier that predicts whether a portfolio is in concentration
# breach, from the prepared training set. The model is tracked with MLflow and
# registered in the Unity Catalog model registry (governed, versioned).
#
#   Input:  investsphere.features.breach_training_set  (from ml/prepare_training_set.py)
#   Output: registered UC model  investsphere.ml.breach_classifier
#
# Dependencies: scikit-learn + mlflow (present on Databricks ML / serverless; otherwise
#   `pip install scikit-learn mlflow`). Runs on Free Edition (no paid endpoint needed).
#
# Beginner note: we keep the model deliberately simple (logistic regression). The point
# is the PIPELINE -- prepared features -> train/test split -> fit -> evaluate -> log to
# MLflow -> register in UC -- not the algorithm.

import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:
    CATALOG = dbutils.widgets.get("catalog")          # noqa: F821
except Exception:
    CATALOG = "investsphere"

FEATURES_SCHEMA = CATALOG + ".features"
TRAINING_TABLE = FEATURES_SCHEMA + ".breach_training_set"
ML_SCHEMA = CATALOG + ".ml"
MODEL_NAME = ML_SCHEMA + ".breach_classifier"          # UC 3-level name

spark.sql("CREATE SCHEMA IF NOT EXISTS " + ML_SCHEMA + " COMMENT 'Registered ML models'")

# register models in Unity Catalog (governed), not the legacy workspace registry
mlflow.set_registry_uri("databricks-uc")

# ---- 1. load the training set into pandas ----
pdf = spark.table(TRAINING_TABLE).toPandas()
y = pdf["label"]
X = pdf.drop(columns=["label"])
FEATURE_COLS = list(X.columns)

# ---- 2. train / test split ----
# stratify keeps the breach/non-breach ratio in both splits (important when breaches
# are the minority class). A tiny demo set may have very few positives -- that's fine.
stratify = y if y.nunique() > 1 and y.value_counts().min() >= 2 else None
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=42, stratify=stratify)

# ---- 3. fit + evaluate + log everything to MLflow ----
with mlflow.start_run(run_name="breach_classifier") as run:
    model = LogisticRegression(max_iter=1000, class_weight="balanced")
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    try:
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    except ValueError:
        auc = float("nan")   # only one class present in a tiny test split

    mlflow.log_params({"model": "LogisticRegression", "class_weight": "balanced"})
    mlflow.log_param("features", ",".join(FEATURE_COLS))
    mlflow.log_metric("accuracy", float(acc))
    if auc == auc:                       # not NaN
        mlflow.log_metric("roc_auc", float(auc))

    # log + register the model in Unity Catalog in one step
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path="model",
        input_example=X_test.head(3),
        registered_model_name=MODEL_NAME,
    )

    print("accuracy:", round(acc, 3), "| roc_auc:", auc)
    print(classification_report(y_test, preds, zero_division=0))
    print("registered UC model:", MODEL_NAME, "| run_id:", run.info.run_id)
