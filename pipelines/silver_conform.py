"""
Databricks script -- SILVER layer transaction validation.

Purpose:
    Read raw transactions from Bronze.
    Validate them using reference/master data.
    Send good rows to Silver.
    Send bad rows to Quarantine.
    Publish quarantine-rate metric to governance.dq_results.

DQ policy:
    1. FAIL:
       transaction_id is missing.
       This is serious because transaction_id is the primary key.

    2. QUARANTINE:
       Bad asset ID
       Bad portfolio ID
       Quantity <= 0
       Invalid transaction type
       Schema drift from Bronze _rescued_data

    3. WARN:
       Missing counterparty is optional, so the row is kept.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from job_params import get_param
from investsphere_platform.quality.quarantine_rate import quarantine_rate_pct


# -------------------------------------------------------
# 1. Create Spark session
# -------------------------------------------------------

spark = SparkSession.builder.getOrCreate()


# -------------------------------------------------------
# 2. Read job parameters
# -------------------------------------------------------

CATALOG = get_param("catalog", "investsphere")

RUN_TS = get_param("run_ts", "2026-05-29T08:00")
BUSINESS_DATE = get_param("run_date", "2026-05-29")
JOB_RUN_ID = get_param("job_run_id", "local")


# -------------------------------------------------------
# 3. Build schema names
# -------------------------------------------------------

BRONZE_SCHEMA = CATALOG + ".bronze"
SILVER_SCHEMA = CATALOG + ".silver"
GOVERNANCE_SCHEMA = CATALOG + ".governance"


# -------------------------------------------------------
# 4. Define table names
# -------------------------------------------------------

bronze_transactions_table = BRONZE_SCHEMA + ".raw_investment_transactions"
bronze_assets_table = BRONZE_SCHEMA + ".raw_investment_asset_master"
bronze_portfolios_table = BRONZE_SCHEMA + ".raw_portfolio_master"

silver_transactions_table = SILVER_SCHEMA + ".silver_transaction"
quarantine_transactions_table = SILVER_SCHEMA + ".quarantine_transaction"

dq_results_table = GOVERNANCE_SCHEMA + ".dq_results"


# -------------------------------------------------------
# 5. Valid transaction types
# -------------------------------------------------------

VALID_TRANSACTION_TYPES = [
    "BUY",
    "SELL",
    "ACQUISITION",
    "DISPOSAL"
]


# -------------------------------------------------------
# 6. Read Bronze tables
# -------------------------------------------------------

transactions_df = spark.table(bronze_transactions_table)

assets_df = (
    spark.table(bronze_assets_table)
    .select("investment_asset_id")
    .distinct()
)

portfolios_df = (
    spark.table(bronze_portfolios_table)
    .select("portfolio_id")
    .distinct()
)


# -------------------------------------------------------
# 7. FAIL FAST if primary key is missing
# -------------------------------------------------------
# transaction_id is mandatory.
# If it is missing, the row cannot be audited or merged safely.

missing_transaction_id_count = (
    transactions_df
    .filter(F.col("transaction_id").isNull())
    .count()
)

if missing_transaction_id_count > 0:
    raise ValueError(
        f"Silver job stopped: "
        f"{missing_transaction_id_count} transaction row(s) have NULL transaction_id."
    )


# -------------------------------------------------------
# 8. Check whether _rescued_data column exists
# -------------------------------------------------------
# _rescued_data appears when Bronze Auto Loader uses rescue mode.
# It stores unexpected/schema-drift data.

has_rescued_data_column = "_rescued_data" in transactions_df.columns


# -------------------------------------------------------
# 9. Deduplicate transactions
# -------------------------------------------------------
# If the same transaction_id appears more than once,
# keep the latest row based on _ingest_ts.

latest_transaction_window = (
    Window
    .partitionBy("transaction_id")
    .orderBy(F.col("_ingest_ts").desc())
)

deduped_transactions_df = (
    transactions_df
    .withColumn("row_number", F.row_number().over(latest_transaction_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)


# -------------------------------------------------------
# 10. Validate foreign keys using broadcast joins
# -------------------------------------------------------
# We check:
#   Does investment_asset_id exist in asset master?
#   Does portfolio_id exist in portfolio master?
#
# If a left join does not find a match, the helper column becomes null.

assets_with_flag_df = assets_df.withColumn("asset_exists", F.lit(True))

portfolios_with_flag_df = portfolios_df.withColumn("portfolio_exists", F.lit(True))

checked_df = (
    deduped_transactions_df
    .join(
        F.broadcast(assets_with_flag_df),
        on="investment_asset_id",
        how="left"
    )
    .join(
        F.broadcast(portfolios_with_flag_df),
        on="portfolio_id",
        how="left"
    )
)


# -------------------------------------------------------
# 11. Create validation flags
# -------------------------------------------------------

checked_df = (
    checked_df
    .withColumn(
        "bad_asset",
        F.col("asset_exists").isNull()
    )
    .withColumn(
        "bad_portfolio",
        F.col("portfolio_exists").isNull()
    )
    .withColumn(
        "bad_quantity",
        F.col("quantity") <= 0
    )
    .withColumn(
        "bad_type",
        ~F.col("transaction_type").isin(VALID_TRANSACTION_TYPES)
    )
)

if has_rescued_data_column:
    checked_df = checked_df.withColumn(
        "bad_schema",
        F.col("_rescued_data").isNotNull()
    )
else:
    checked_df = checked_df.withColumn(
        "bad_schema",
        F.lit(False)
    )

checked_df = checked_df.drop("asset_exists", "portfolio_exists")


# -------------------------------------------------------
# 12. Build human-readable quarantine reason
# -------------------------------------------------------

quarantine_reason = F.concat_ws(
    "; ",
    F.when(F.col("bad_asset"), F.lit("bad_asset")),
    F.when(F.col("bad_portfolio"), F.lit("bad_portfolio")),
    F.when(F.col("bad_quantity"), F.lit("bad_quantity")),
    F.when(F.col("bad_type"), F.lit("bad_type")),
    F.when(F.col("bad_schema"), F.lit("schema_drift"))
)


# -------------------------------------------------------
# 13. Define bad-row condition
# -------------------------------------------------------

is_bad_row = (
    F.col("bad_asset")
    | F.col("bad_portfolio")
    | F.col("bad_quantity")
    | F.col("bad_type")
    | F.col("bad_schema")
)

validation_flag_columns = [
    "bad_asset",
    "bad_portfolio",
    "bad_quantity",
    "bad_type",
    "bad_schema"
]


# -------------------------------------------------------
# 14. Split valid and quarantine rows
# -------------------------------------------------------

valid_transactions_df = (
    checked_df
    .filter(~is_bad_row)
    .drop(*validation_flag_columns)
)

source_file_column = (
    F.col("_source_file")
    if "_source_file" in checked_df.columns
    else F.lit(None).cast("string")
)

ingestion_timestamp_column = (
    F.col("_ingest_ts")
    if "_ingest_ts" in checked_df.columns
    else F.current_timestamp()
)

quarantine_transactions_df = (
    checked_df
    .filter(is_bad_row)
    .withColumn("quarantine_reason", quarantine_reason)
    .withColumn("source_table", F.lit(bronze_transactions_table))
    .withColumn("source_file", source_file_column)
    .withColumn("ingestion_ts", ingestion_timestamp_column)
    .withColumn("business_date", F.lit(BUSINESS_DATE).cast("date"))
    .withColumn("pipeline_run_id", F.lit(str(RUN_TS)))
    .withColumn("job_run_id", F.lit(str(JOB_RUN_ID)))
    .withColumn("check_timestamp", F.current_timestamp())
    .withColumn("raw_payload", F.to_json(F.struct(*deduped_transactions_df.columns)))
    .drop(*validation_flag_columns)
)


# -------------------------------------------------------
# 15. Helper function: upsert valid records
# -------------------------------------------------------
# Upsert means:
#   If transaction_id already exists, update it.
#   If transaction_id does not exist, insert it.
#
# This makes the job idempotent.
# Running the same job twice will not create duplicate transactions.

def upsert_to_delta_table(source_df, target_table_name, key_column):
    """
    Upsert source DataFrame into target Delta table.
    """

    table_exists = spark.catalog.tableExists(target_table_name)

    if not table_exists:
        (
            source_df
            .write
            .format("delta")
            .saveAsTable(target_table_name)
        )
        return

    target_table = DeltaTable.forName(spark, target_table_name)

    merge_condition = f"target.{key_column} = source.{key_column}"

    (
        target_table.alias("target")
        .merge(
            source_df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# -------------------------------------------------------
# 16. Write valid rows to Silver
# -------------------------------------------------------

upsert_to_delta_table(
    valid_transactions_df,
    silver_transactions_table,
    "transaction_id"
)


# -------------------------------------------------------
# 17. Write bad rows to quarantine
# -------------------------------------------------------
# Quarantine is append-only.
# We keep history of bad rows for audit and investigation.

(
    quarantine_transactions_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(quarantine_transactions_table)
)


# -------------------------------------------------------
# 18. Calculate quarantine rate for this run
# -------------------------------------------------------

valid_count = valid_transactions_df.count()
quarantined_count = quarantine_transactions_df.count()

quarantine_rate = quarantine_rate_pct(
    quarantined_count,
    valid_count
)


# -------------------------------------------------------
# 19. Create governance DQ results table if missing
# -------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {dq_results_table}
    (
        check_timestamp TIMESTAMP,
        check_name STRING,
        source_table STRING,
        business_date DATE,
        pipeline_run_id STRING,
        job_run_id STRING,
        metric_value DOUBLE,
        threshold DOUBLE,
        passed BOOLEAN
    )
    """
)


# -------------------------------------------------------
# 20. Build one DQ metric row
# -------------------------------------------------------

dq_metric_df = spark.createDataFrame(
    [
        (
            BUSINESS_DATE,
            "transaction_quarantine_rate_pct",
            silver_transactions_table,
            str(RUN_TS),
            str(JOB_RUN_ID),
            float(quarantine_rate),
            2.0,
            bool(quarantine_rate <= 2.0)
        )
    ],
    """
    business_date string,
    check_name string,
    source_table string,
    pipeline_run_id string,
    job_run_id string,
    metric_value double,
    threshold double,
    passed boolean
    """
)

dq_metric_df = (
    dq_metric_df
    .withColumn("business_date", F.col("business_date").cast("date"))
    .withColumn("check_timestamp", F.current_timestamp())
    .select(
        "check_timestamp",
        "check_name",
        "source_table",
        "business_date",
        "pipeline_run_id",
        "job_run_id",
        "metric_value",
        "threshold",
        "passed"
    )
)


# -------------------------------------------------------
# 21. Write DQ metric to governance table
# -------------------------------------------------------

(
    dq_metric_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(dq_results_table)
)


# -------------------------------------------------------
# 22. Print final result
# -------------------------------------------------------

print("Silver transaction validation completed.")
print("Valid rows:", valid_count)
print("Quarantined rows:", quarantined_count)
print("Quarantine rate %:", quarantine_rate)
