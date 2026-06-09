"""
Databricks GOLD layer script.

Purpose:
    Build business-ready Gold tables for investment reporting.

This script creates:

1. fact_daily_holding
   - One row per portfolio holding
   - Adds close price, sector, issuer_id, and market_value

2. fact_portfolio_exposure
   - Calculates sector exposure percentage per portfolio

3. fact_limit_breach
   - Finds sector limit breaches

Example:
    If portfolio PF_1 has 60% Technology exposure
    and the allowed Technology limit is 40%,
    then the breach is 20%.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from delta.tables import DeltaTable
from job_params import get_param

# -------------------------------------------------------
# 1. Create Spark session
# -------------------------------------------------------
spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------
# 2. Read job parameters
# -------------------------------------------------------

# Catalog name.
# Default is "investsphere".

CATALOG = get_param("catalog", "investsphere")

# Business run date.
# The Gold calculations are done for this date.
RUN_DATE = get_param("run_date", "2026-05-29")

# -------------------------------------------------------
# 3. Build schema names
# -------------------------------------------------------
BRONZE_SCHEMA = CATALOG + ".bronze"
GOLD_SCHEMA = CATALOG + ".gold"

# -------------------------------------------------------
# 4. Define source table names
# -------------------------------------------------------

positions_table = BRONZE_SCHEMA + ".raw_investment_holdings_snapshot"
prices_table = BRONZE_SCHEMA + ".raw_listed_market_prices"
assets_table = BRONZE_SCHEMA + ".raw_investment_asset_master"
limits_table = BRONZE_SCHEMA + ".raw_investment_limits"

# -------------------------------------------------------
# 5. Define target Gold table names
# -------------------------------------------------------

daily_holding_table = GOLD_SCHEMA + ".fact_daily_holding"
portfolio_exposure_table = GOLD_SCHEMA + ".fact_portfolio_exposure"
limit_breach_table = GOLD_SCHEMA + ".fact_limit_breach"

# -------------------------------------------------------
# 6. Read required source data
# -------------------------------------------------------
# Holdings for the run date
positions_df = (
    spark.table(positions_table)
    .filter(F.col("as_of_date") == RUN_DATE)
)

# prices for the run date
prices_df = (
    spark.table(prices_table)
    .filter(F.col("price_date") == RUN_DATE)
)

# Asset reference data
assets_df = (
    spark.table(assets_table)
    .select(
        "investment_asset_id",
        "sector",
        "issuer_id"
    )
)

# investment limits
limits_df = spark.table(limits_table)

# -------------------------------------------------------
# 7. Helper function: fail job if serious issue exists
# -------------------------------------------------------

def fail_job_if(condition, message):
    """
    Stop the Gold job if a serious data-integrity issue is found.

    Example:
        Missing price is a serious issue.
        Without price, market value cannot be calculated correctly.

    Note:
        A business limit breach is NOT a job failure.
        A breach is valid business output.
    """
    
    if condition:
        raise ValueError("Gold job stopped: " + message)
    
# -------------------------------------------------------
# 8. Helper function: create Gold table if missing
# -------------------------------------------------------

def create_empty_gold_table_if_missing(sample_dataframe, table_name, cluster_columns):
    """
    Create an empty Delta table if the Gold table does not exist.

    The table is clustered by commonly filtered columns like:
        portfolio_id
        as_of_date

    Clustering helps query performance.
    """
    table_exists = spark.catalog.tableExists(table_name)
    
    if not table_exists:
        (
            sample_dataframe
            .limit(0)
            .writeTo(table_name)
            .using("delta")
            .clusterBy(*cluster_columns)
            .create()
        )
        
# -------------------------------------------------------
# 9. Integrity check: every holding must have price
# -------------------------------------------------------
missing_price_df = (
    positions_df
    .join(
        F.broadcast(prices_df.select("investment_asset_id")),
        on = "investment_asset_id",
        how="left_anti"
    )
)

missing_price_count = missing_price_df.count()

fail_job_if(
    missing_price_count > 0,
    f"{missing_price_count} holding(s) on {RUN_DATE} have no price."
    
)

# -------------------------------------------------------
# 10. Build fact_daily_holding
# -------------------------------------------------------

fact_daily_holding_df = (
    positions_df
    .join(
        F.broadcast(prices_df.select("investment_asset_id", "close_price")),
        on="investment_asset_id",
        how="inner"
    )
    .join(
        F.broadcast(assets_df),
        on="investment_asset_id",
        how="inner"
    )
    .withColumn(
        "market_value",
        F.col("quantity") * F.col("close_price")
    )
)

# -------------------------------------------------------
# 11. Integrity check: market value should not be null
# -------------------------------------------------------

null_market_value_count = (
    fact_daily_holding_df
    .filter(F.col("market_value").isNull())
    .count()
)

fail_job_if(
    null_market_value_count > 0,
    "market_value is null. Check quantity or close_price."
)

# -------------------------------------------------------
# 12. Write fact_daily_holding using MERGE
# -------------------------------------------------------

create_empty_gold_table_if_missing(
    fact_daily_holding_df,
    daily_holding_table,
    ["portfolio_id", "as_of_date"]
)

(
    DeltaTable.forName(spark, daily_holding_table)
    .alias("target")
    .merge(
        fact_daily_holding_df.alias("source"),
          """
        target.portfolio_id = source.portfolio_id
        AND target.as_of_date = source.as_of_date
        AND target.investment_asset_id = source.investment_asset_id
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# -------------------------------------------------------
# 13. Calculate total portfolio market value
# -------------------------------------------------------

portfolio_total_df = (
    fact_daily_holding_df
    .groupBy("portfolio_id")
    .agg(
        F.sum("market_value").alias("portfolio_total_market_value")
    )
)    

# -------------------------------------------------------
# 14. Calculate sector market value
# -------------------------------------------------------
sector_market_value_df = (
    fact_daily_holding_df
    .groupBy("portfolio_id", "sector")
    .agg(
        F.sum("market_value").alias("sector_market_value")
    )
)

# -------------------------------------------------------
# 15. Calculate sector exposure percentage
# -------------------------------------------------------

portfolio_exposure_df = (
    sector_market_value_df
    .join(
        portfolio_total_df,
        on="portfolio_id",
        how="inner"
    )
    .withColumn(
        "exposure_pct",
        F.round(
            F.col("sector_market_value") / 
            F.col("portfolio_total_market_value") * 100,
            4
        )
    )
    .withColumn(
        "as_of_date",
        F.lit(RUN_DATE)
    )
)

# -------------------------------------------------------
# 16. Integrity check: exposure should not exceed 100%
# -------------------------------------------------------
bad_exposure_count = (
    portfolio_exposure_df
    .filter(F.col("exposure_pct") > 100.01)
    .count()
)

fail_job_if(
    bad_exposure_count > 0,
    "A sector exposure is greater than 100%. Calculation may be wrong."
)

# -------------------------------------------------------
# 17. Write fact_portfolio_exposure
# -------------------------------------------------------

create_empty_gold_table_if_missing(
    portfolio_exposure_df,
    portfolio_exposure_table,
    ["portfolio_id", "as_of_date"]
)

(
    portfolio_exposure_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"as_of_date = '{RUN_DATE}'")
    .saveAsTable(portfolio_exposure_table)
)

# -------------------------------------------------------
# 18. Filter only sector limits
# -------------------------------------------------------

sector_limits_df = (
    limits_df
    .filter(F.upper(F.col("scope_type")) == "SECTOR")
)

# -------------------------------------------------------
# 19. Detect sector limit breaches
# -------------------------------------------------------

limit_breach_df = (
    portfolio_exposure_df.alias("exposure")
    .join(
        F.broadcast(sector_limits_df.alias("limit")),
        (
            F.col("exposure.sector") == F.col("limit.scope_value")
        )
        &
        (
            (F.col("limit.portfolio_id") == F.col("exposure.portfolio_id"))
            |
            (F.col("limit.portfolio_id") == "ALL")
        ),
        how="inner"
    )
    .filter(
        F.col("exposure.exposure_pct") > F.col("limit.max_pct")
    )
    .withColumn(
        "breach_by_pct",
        F.round(
            F.col("exposure.exposure_pct") - F.col("limit.max_pct"),
            4
        )
    )
    .select(
        F.col("exposure.portfolio_id"),
        F.col("exposure.sector"),
        F.col("exposure.exposure_pct"),
        F.col("limit.max_pct"),
        F.col("breach_by_pct"),
        F.col("limit.limit_id"),
        F.col("exposure.as_of_date")
    )
    
)

# -------------------------------------------------------
# 20. Write fact_limit_breach
# -------------------------------------------------------

create_empty_gold_table_if_missing(
    limit_breach_df,
    limit_breach_table,
    ["portfolio_id", "as_of_date"]
)

(
    limit_breach_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"as_of_date = '{RUN_DATE}'")
    .saveAsTable(limit_breach_table)
)

# -------------------------------------------------------
# 21. Print result
# -------------------------------------------------------

print("Gold tables created successfully for run date:", RUN_DATE)

print("limit breaches:")
limit_breach_df.show(truncate=False)
