
"""
Databricks script -- Simple SCD Type 2 demo.

Purpose:
    Track history of investment asset master changes.

This is a fallback / learning version.
In production, this project uses Lakeflow AUTO CDC FROM SNAPSHOT.

SCD Type 2 logic:
    1. If dimension table does not exist:
       create it with all incoming records as current.

    2. If table already exists:
       compare incoming asset data with current dimension data.

    3. If tracked columns changed:
       close old current record.

    4. Insert new current record.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from delta.tables import DeltaTable
from job_params import get_param

spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------
# 2. Read job parameters
# -------------------------------------------------------

CATALOG = get_param("catalog", "investsphere")
RUN_DATE = get_param("run_date", "2026-05-29")

# -------------------------------------------------------
# 3. Define schemas / table names
# -------------------------------------------------------

BRONZE_SCHEMA = CATALOG + ".bronze"
SILVER_SCHEMA = CATALOG + ".silver"

SOURCE_TABLE = BRONZE_SCHEMA + ".raw_investment_asset_master"
TARGET_TABLE = SILVER_SCHEMA + ".dim_investment_asset_scd2_demo"

# -------------------------------------------------------
# 4. SCD2 settings
# -------------------------------------------------------

HIGH_DATE = "9999-12-31"

TRACKED_COLUMNS = [
    "sector",
    "issuer_id",
    "asset_type",
    "name"
]


# -------------------------------------------------------
# 5. Read incoming asset master snapshot
# -------------------------------------------------------

incoming_assets_df = (
    spark.table(SOURCE_TABLE)
    .select(
        "investment_asset_id",
        "name",
        "asset_type",
        "isin",
        "ticker",
        "issuer_id",
        "sector",
        "country"
    )
)


# -------------------------------------------------------
# 6. First load: create SCD2 table if it does not exist
# -------------------------------------------------------
if not spark.catalog.tableExists(TARGET_TABLE):
    first_version_df = (
        incoming_assets_df
        .withColumn("effective_from", F.lit(RUN_DATE).cast("date"))
        .withColumn("effective_to", F.lit(HIGH_DATE).cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    
    (first_version_df
     .write
     .format("delta")
     .saveAsTable(TARGET_TABLE))
    
    print("Created SCD2 Table:", TARGET_TABLE)
    
# -------------------------------------------------------
# 7. Incremental load: table already exists
# -------------------------------------------------------

else:
    target_delta_table = DeltaTable.forName(spark, TARGET_TABLE)
    
    # ---------------------------------------------------
    # 7A. Build change condition
    # ---------------------------------------------------
    # Example:
    # old sector is different from new sector
    # OR old issuer_id is different from new issuer_id
    # OR old asset_type is different from new asset_type
    # OR old name is different from new name
    
    change_conditions = []
    
    for column_name in TRACKED_COLUMNS:
        condition = f"target.{column_name} <> source.{column_name}"
        change_conditions.append(condition)
        
    final_change_condition = " OR ".join(change_conditions)
    
     # ---------------------------------------------------
    # 7B. Close old current records if data changed
    # ---------------------------------------------------
    # This updates old current row:
    #   is_current = false
    #   effective_to = RUN_DATE

    (
        target_delta_table.alias("target")
        .merge(
            incoming_assets_df.alias("source"),
            """
            target.investment_asset_id = source.investment_asset_id
            AND target.is_current = true
            """
        )
        .whenMatchedUpdate(
            condition = final_change_condition,
            set={
                "is_current": F.lit(False),
                "effective_to": F.lit(RUN_DATE).cast("date")
            }
        )
        .execute()
    )
    
    # ---------------------------------------------------
    # 7C. Find incoming rows that do not have current version
    # ---------------------------------------------------
    # This includes:
    #   1. Brand new assets
    #   2. Changed assets whose old version was just closed
    
    current_asset_keys_df = (
        spark.table(TARGET_TABLE)
        .filter(F.col("is_current") == True)
        .select(
            F.col("investment_asset_id").alias("current_asset_id")
        )
    )
    
    new_current_versions_df = (
        incoming_assets_df
        .join(
            current_asset_keys_df,
            incoming_assets_df["investment_asset_id"] == current_asset_keys_df['current_asset_id'],
            "left"
        )
        .filter(F.col("current_asset_id").isNull())
        .drop("current_asset_id")
        .withColumn("effective_from", F.lit(RUN_DATE).cast("date"))
        .withColumn("effective_to", F.lit(HIGH_DATE).cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    
    # ---------------------------------------------------
    # 7D. Insert new current versions
    # ---------------------------------------------------
    
    (
        new_current_versions_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(TARGET_TABLE)
    )
    
    print("SCD2 chagne applied to:", TARGET_TABLE)


