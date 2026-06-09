"""
Databricks GOLD script -- Monthly / quarterly valuation marts.

Purpose:
    Private assets and funds are not priced daily.

    Private valuations may come monthly or quarterly.
    Fund NAVs may also come monthly or quarterly.

This script keeps only the latest available value per asset.

It creates:
    1. gold.fact_private_valuation
    2. gold.fact_fund_nav
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.window import Window

from job_params import get_param

spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------
# 2. Read catalog parameter
# -------------------------------------------------------

CATALOG = get_param("catalog", "investsphere")

BRONZE_SCHEMA = CATALOG + ".bronze"
GOLD_SCHEMA = CATALOG + ".gold"


# -------------------------------------------------------
# 3. Define source and target tables
# -------------------------------------------------------
private_valuation_source_table = BRONZE_SCHEMA + ".raw_private_valuation_snapshot"
fund_nav_source_table = BRONZE_SCHEMA + ".raw_fund_nav_snapshot"

private_valuation_gold_table = GOLD_SCHEMA + ".fact_private_valuation"
fund_nav_gold_table = GOLD_SCHEMA + ".fact_fund_nav"

# -------------------------------------------------------
# 4. Helper function: keep latest row per asset
# -------------------------------------------------------

def get_latest_record_per_asset(source_table_name, date_column_name):
    """
    Keep only the latest row for each investment_asset_id.

    Example:
        Asset PE_1 has valuations on:
            2026-01-31
            2026-02-28
            2026-03-31

        This function keeps only:
            2026-03-31
    """
    source_df = spark.table(source_table_name)
    
    window_by_asset_latest_date = (
        Window
        .partitionBy("investment_asset_id")
        .orderBy(F.col(date_column_name).desc())
    )
    
    latest_df = (
        source_df
        .withColumn(
            "row_number", 
            F.row_number().over(window_by_asset_latest_date)
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )
    
    return latest_df

# -------------------------------------------------------
# 5. Create latest private valuation table
# -------------------------------------------------------

latest_private_valuation_df = get_latest_record_per_asset(
    private_valuation_source_table,
    "valuation_date"
)

(
    latest_private_valuation_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(private_valuation_gold_table)
)

# -------------------------------------------------------
# 6. Create latest fund NAV table
# -------------------------------------------------------
latest_fund_nav_df = get_latest_record_per_asset(
    fund_nav_source_table,
    "nav_date"
)

(
    latest_fund_nav_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(fund_nav_gold_table)
)

# -------------------------------------------------------
# 7. Print completion message
# -------------------------------------------------------

print("Monthly / quarterly valuation Gold tables refreshed.")
print("Created table:", private_valuation_gold_table)
print("Created table:", fund_nav_gold_table)
