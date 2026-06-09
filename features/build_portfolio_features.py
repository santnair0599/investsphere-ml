# Databricks script -- FEATURE STORE.
#
# Builds a Unity Catalog feature table from the Gold marts using Databricks Feature
# Engineering (the UC-native feature store). This is the "feature store design" piece:
# governed, reusable, point-in-time features keyed by (portfolio_id, as_of_date).
#
#   Output: investsphere.features.portfolio_daily_features
#
# Prerequisites (run the medallion first):
#   gold.fact_daily_holding, gold.fact_portfolio_exposure, gold.fact_limit_breach
#
# Notes for beginners:
#   - A "feature" is just a model-ready number describing something (here, a portfolio
#     on a date). A "feature table" stores them so many models can reuse the SAME
#     definitions -- no copy/paste feature logic, and you get lineage + governance.
#   - We DELIBERATELY do not include "breach_count" as a feature here, because the model
#     we train later predicts breaches -- using the answer as an input would be leakage.

from pyspark.sql import SparkSession, functions as F
from databricks.feature_engineering import FeatureEngineeringClient

spark = SparkSession.builder.getOrCreate()

# Catalog is read from a job/notebook widget if present, else defaults to investsphere.
try:
    CATALOG = dbutils.widgets.get("catalog")          # noqa: F821 (dbutils is provided by Databricks)
except Exception:
    CATALOG = "investsphere"

GOLD = CATALOG + ".gold"
FEATURES_SCHEMA = CATALOG + ".features"
FEATURE_TABLE = FEATURES_SCHEMA + ".portfolio_daily_features"

# The feature table lives in its own schema.
spark.sql("CREATE SCHEMA IF NOT EXISTS " + FEATURES_SCHEMA +
          " COMMENT 'Reusable ML features (UC Feature Engineering)'")

# ---- read the Gold facts ----
holdings = spark.table(GOLD + ".fact_daily_holding")        # portfolio_id, as_of_date, market_value, sector...
exposure = spark.table(GOLD + ".fact_portfolio_exposure")   # portfolio_id, as_of_date, sector, exposure_pct
breaches = spark.table(GOLD + ".fact_limit_breach")         # portfolio_id, as_of_date, sector ...

# ---- portfolio-level features (one row per portfolio per date) ----
# 1) size + diversification from the holdings fact
holding_feats = (holdings.groupBy("portfolio_id", "as_of_date")
    .agg(F.sum("market_value").alias("total_market_value"),
         F.count("*").alias("num_holdings"),
         F.countDistinct("sector").alias("num_sectors")))

# 2) concentration from the exposure fact:
#    - top_sector_exposure_pct = the single biggest sector weight
#    - concentration_hhi = Herfindahl index (sum of squared sector weights);
#      higher = more concentrated = riskier.
exposure_feats = (exposure.groupBy("portfolio_id", "as_of_date")
    .agg(F.max("exposure_pct").alias("top_sector_exposure_pct"),
         F.sum(F.col("exposure_pct") * F.col("exposure_pct")).alias("concentration_hhi")))

features = (holding_feats
    .join(exposure_feats, ["portfolio_id", "as_of_date"], "left")
    .na.fill(0))

# ---- write to the UC feature store ----
# create_table is idempotent-friendly: create once, then write/merge. The primary keys
# + timeseries column enable correct point-in-time feature lookups during training.
fe = FeatureEngineeringClient()

if not spark.catalog.tableExists(FEATURE_TABLE):
    fe.create_table(
        name=FEATURE_TABLE,
        primary_keys=["portfolio_id", "as_of_date"],
        timeseries_columns="as_of_date",
        df=features,
        description="Per-portfolio, per-date features for breach-risk modelling.",
    )
else:
    # merge=True upserts on the primary keys, so daily re-runs are safe.
    fe.write_table(name=FEATURE_TABLE, df=features, mode="merge")

print("feature table ready:", FEATURE_TABLE)
features.show(truncate=False)
