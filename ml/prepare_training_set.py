# Databricks script -- DATA PREPARATION FOR MODEL TRAINING.
#
# Turns the feature table into a labelled training set using a Feature Store
# FeatureLookup (so the model's features have lineage back to the feature table).
#
#   Label question: "Is this portfolio in breach of a concentration limit on this date?"
#   Label:   1 if the (portfolio_id, as_of_date) appears in gold.fact_limit_breach, else 0
#   Features: looked up from features.portfolio_daily_features (NOT the breach count --
#             that would leak the answer).
#
#   Output: investsphere.features.breach_training_set  (ready for ml/train_breach_model.py)
#
# Prerequisite: run features/build_portfolio_features.py first.
#
# Beginner note: in a real platform you'd predict NEXT-day breach from TODAY's features
# (to be genuinely predictive and avoid leakage). The demo data is mostly a single
# snapshot date, so we classify the current day from concentration features instead --
# the PATTERN (label + feature lookup + split) is what matters here.

from pyspark.sql import SparkSession, functions as F
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

spark = SparkSession.builder.getOrCreate()

try:
    CATALOG = dbutils.widgets.get("catalog")          # noqa: F821
except Exception:
    CATALOG = "investsphere"

GOLD = CATALOG + ".gold"
FEATURES_SCHEMA = CATALOG + ".features"
FEATURE_TABLE = FEATURES_SCHEMA + ".portfolio_daily_features"
TRAINING_TABLE = FEATURES_SCHEMA + ".breach_training_set"

# ---- 1. build the label dataframe: every portfolio/date + a 0/1 breach flag ----
# universe = all keys we have features for
universe = spark.table(FEATURE_TABLE).select("portfolio_id", "as_of_date").distinct()

# the keys that DID breach (mark them 1)
breached = (spark.table(GOLD + ".fact_limit_breach")
            .select("portfolio_id", "as_of_date").distinct()
            .withColumn("label", F.lit(1)))

labels = (universe.join(breached, ["portfolio_id", "as_of_date"], "left")
          .withColumn("label", F.coalesce(F.col("label"), F.lit(0))))

# ---- 2. assemble the training set via a Feature Store lookup ----
fe = FeatureEngineeringClient()
feature_lookups = [
    FeatureLookup(
        table_name=FEATURE_TABLE,
        lookup_key=["portfolio_id", "as_of_date"],
        timeseries_lookup_key="as_of_date",
        feature_names=[
            "total_market_value", "num_holdings", "num_sectors",
            "top_sector_exposure_pct", "concentration_hhi",
        ],
    )
]

training_set = fe.create_training_set(
    df=labels,
    feature_lookups=feature_lookups,
    label="label",
    exclude_columns=["portfolio_id", "as_of_date"],   # keys are not model inputs
)

training_df = training_set.load_df()

# ---- 3. persist it so the trainer (and reviewers) can read it ----
(training_df.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(TRAINING_TABLE))

n = training_df.count()
pos = training_df.filter(F.col("label") == 1).count()
print("training set ready:", TRAINING_TABLE, "| rows:", n, "| breaches (label=1):", pos)
training_df.show(truncate=False)
