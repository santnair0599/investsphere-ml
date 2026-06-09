# Lakeflow Spark Declarative Pipelines -- using the current pyspark.pipelines API
# (`from pyspark import pipelines as dp`). Declarative tables, native data-quality
# EXPECTATIONS, and native SCD Type 2 via AUTO CDC FROM SNAPSHOT. Create a pipeline
# pointing at this file; the engine builds the dependency graph and runs it
# incrementally. This is the DEPLOYED Databricks ETL path.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from schemas import SCHEMAS

RAW_PATH = "/Volumes/investsphere/bronze/raw"
VALID_TYPES = "('BUY', 'SELL', 'ACQUISITION', 'DISPOSAL')"

# ---- BRONZE: stream raw transactions (JSON) with Auto Loader + RESCUE mode ----
# schemaEvolutionMode=rescue => drift is captured in _rescued_data, never auto-promoted
# into curated layers and never fails the stream. No business filtering in Bronze.

@dp.table(name="bronze_transactions", comment="Raw transactions from JSON feed (Autoloader, rescue mode)")
def bronze_transactions():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/Volumes/investsphere/bronze/_schemas/lakeflow_transactions")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            .schema(SCHEMAS["investment_transactions"])
            .load(RAW_PATH + "/investment_transactions")
            .withColumn("_ingest_ts", F.current_timestamp()))
    

# Reference snapshots used for FK validation (full CSV snapshot)
@dp.table(name="bronze_portfolio_master_snapshot")
def bronze_portfolio_master_snapshot():
    return (spark.read.format("csv").option("header", "true")
            .schema(SCHEMAS["portfolio_master"])
            .load(RAW_PATH + "/portfolio_master"))
    

# ----STAGING: stamp a _dq_reason on every row (no rows dropped here) -----
# FK checks via broadcast LEFT joins (null after join = key not found).

@dp.table(name="stg_transactions_flagged", temporary=True)
def stg_transactions_flagged():
    txns = dp.read_stream("bronze_transactions")
    assets = spark.read.table("bronze_asset_master_snapshot").select("investment_asset_id").distinct()
    portfolios = spark.read.table("bronze_portfolio_master_snapshot").select("portfolio_id").distinct()
    checked = (txns
               .join(F.broadcast(assets.withColumn("_a_ok", F.lit(True))), "investment_asset_id", "left")
               .join(F.broadcast(portfolios.withColumn("_p_ok", F.lit(True))), "portfolio_id", "left"))
    reason = F.concat_ws("; ",
            F.when(F.col("_a_ok").isNull(), F.lit("bad_asset")),
            F.when(F.col("_p_ok").isNull(), F.lit("bad_portfolio")),
            F.when(F.col("quantity") <= 0, F.lit("bad_quantity")),
            F.when(~F.expr("transaction_type IN " + VALID_TYPES), F.lit("bad_type")),
            F.when(F.col("_rescued_data").isNotNull(), F.lit("schema_drift")))
    # Compute _dq_reason (it references _a_ok/_p_ok) BEFORE dropping those helper
    # columns -- dropping first leaves `reason` referencing columns that no longer
    # exist (UNRESOLVED_COLUMN).
    return (checked
            .withColumn("_dq_reason", F.when(reason == "", None).otherwise(reason))
            .drop("_a_ok", "_p_ok"))
    
# ---- SILVER (valid): tiered expectation policy ----
#   FAIL  : missing primary key (transaction_id) -> stop the pipeline (expect_or_fail)
#   WARN  : missing optional counterparty -> kept + tracked in the pipeline UI (expect)
#   QUARANTINE: FK / quantity / type / schema-drift issues are routed out below.
@dp.table(name="silver_transactions", comment="clean, validated transactions")
@dp.expect_or_fail("has_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("has_counterparty", "counterparty_id IS NOT NULL")
def silver_transactions():
    return dp.read_stream("stg_transactions_flagged").filter("_dq_reason IS NULL").drop("_dq_reason")

# ---- QUARANTINE: invalid rows are PRESERVED (not silently dropped) for audit ----
@dp.table(name="quarantine_transactions", comment="Invalid transactions held for review")
def quarantine_transactions():
    return (dp.read_stream("stg_transactions_flagged")
            .filter("_dq_reason IS NOT NULL")
            .withColumnRenamed("_dq_reason", "quarantine_reason")
            .withColumn("quarantined_ts", F.current_timestamp()))

# ---- Reference dimension history via NATIVE SCD2 (AUTO CDC FROM SNAPSHOT) ----
# The asset master arrives as full CSV SNAPSHOTS, so AUTO CDC FROM SNAPSHOT is the
# correct native API (vs a CDC feed). This is the DEPLOYED SCD2 pattern. The
# pure-Python MERGE (silver_reference_scd2.py / transformations/scd2.py) is kept
# only as the unit-tested concept + local fallback.
@dp.table(name="bronze_asset_master_snapshot")
def bronze_asset_master_snapshot():
    return (spark.read.format("csv").option("header", "true")
            .schema(SCHEMAS["investment_asset_master"])
            .load(RAW_PATH + "/investment_asset_master"))


dp.create_streaming_table("dim_investment_asset_history")
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_investment_asset_history",
    source="bronze_asset_master_snapshot",
    keys=["investment_asset_id"],
    stored_as_scd_type=2,   # full effective_from / effective_to history
)
