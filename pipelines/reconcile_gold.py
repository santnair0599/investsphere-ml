"""
Databricks script -- Reconciliation check.

Purpose:
    Compare Spark Gold output with pure-Python reference logic.

Why:
    Your pure-Python functions are unit-tested.
    Your Spark Gold job is the production implementation.

This script checks:
    Did Spark calculate the same limit breaches as the pure-Python logic?

Run this after:
    gold_marts.py

Important:
    This script is for small controlled sample data only.
    It collects data to the driver, so do not run this on huge production data.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from job_params import get_param
from investsphere_platform.transformations import exposure_calculator


# -------------------------------------------------------
# 1. Create Spark session
# -------------------------------------------------------

spark = SparkSession.builder.getOrCreate()


# -------------------------------------------------------
# 2. Read job parameters
# -------------------------------------------------------

CATALOG = get_param("catalog", "investsphere")
RUN_DATE = get_param("run_date", "2026-05-29")

BRONZE_SCHEMA = CATALOG + ".bronze"
GOLD_SCHEMA = CATALOG + ".gold"

# -------------------------------------------------------
# 3. Define source and target tables
# -------------------------------------------------------

holdings_table = BRONZE_SCHEMA + ".raw_investment_holdings_snapshot"
prices_table = BRONZE_SCHEMA + ".raw_listed_market_prices"
assets_table = BRONZE_SCHEMA + ".raw_investment_asset_master"
limits_table = BRONZE_SCHEMA + ".raw_investment_limits"

gold_breach_table = GOLD_SCHEMA + ".fact_limit_breach"

# -------------------------------------------------------
# 4. Read Bronze sample data
# -------------------------------------------------------
# We collect the sample data to Python because the reference logic
# is pure Python, not Spark.

holdings_rows = (
    spark.table(holdings_table)
    .filter(F.col("as_of_date") == RUN_DATE)
    .collect()
)

price_rows = (
    spark.table(prices_table)
    .filter(F.col("price_date") == RUN_DATE)
    .collect()
)

asset_rows = spark.table(assets_table).collect()

limit_rows = spark.table(limits_table).collect()

# -------------------------------------------------------
# 5. Convert Spark Rows into normal Python structures
# -------------------------------------------------------
holdings = []
for row in holdings_rows:
    holdings.append(row.asDict())

prices = {}
for row in price_rows:
    asset_id = row["investment_asset_id"]
    close_price = float(row["close_price"])

    prices[asset_id] = close_price
    
assets = {}
for row in asset_rows:
    asset_id = row["investment_asset_id"]
    
    assets[asset_id] = {
        "sector": row["sector"],
        "issuer_id": row["issuer_id"]
    }
    
limits = []
for row in limit_rows:
    limits.append(row.asDict())
    
# -------------------------------------------------------
# 6. Calculate reference breaches using pure Python logic
# -------------------------------------------------------

reference_breaches = set()
portfolio_ids = set()

for holding in holdings:
    portfolio_ids.add(holding["portfolio_id"])

for portfolio_id in portfolio_ids:
    # Get holdings for this portfolio
    portfolio_postions = []
    for holding in holdings:
        if holding["portfolio_id"] == portfolio_id:
            position = {
                "asset_id": holding["investment_asset_id"],
                "quantity": float(holding["quantity"])
            }
            
            portfolio_postions.append(position)
    
    # Enrich positions with market value, sector and issuer
    enriched_positions = exposure_calculator.enrich_positions(
        portfolio_postions,
        prices,
        assets
    )
    
    # Calculate exposure by sector
    sector_exposure = exposure_calculator.exposure_pct(
        enriched_positions,
        "sector"
    )
    
    # calculate exposure by issuer
    issuer_exposure = exposure_calculator.exposure_pct(
        enriched_positions,
        "issuer_id"
    )
    
    # Detect limit breaches
    breaches = exposure_calculator.detect_breaches(
        portfolio_id,
        sector_exposure,
        issuer_exposure,
        limits
    )

    # The Gold fact_limit_breach table only holds SECTOR-scope breaches,
    # so the reference set must keep SECTOR breaches only -- keyed by
    # (portfolio_id, scope_value) to match the Spark Gold key below.
    for breach in breaches:
        if breach["scope_type"] == "SECTOR":
            breach_key = (breach["portfolio_id"], breach["scope_value"])
            reference_breaches.add(breach_key)
    

# -------------------------------------------------------
# 7. Read Spark Gold breaches
# -------------------------------------------------------

gold_rows = spark.table(gold_breach_table).collect()

spark_gold_breaches = set()

for row in gold_rows:
    breach_key = (
        row["portfolio_id"],
        row["sector"]
        
    )
    
    spark_gold_breaches.add(breach_key)
    
# -------------------------------------------------------
# 8. Compare pure-Python result with Spark Gold result
# -------------------------------------------------------

if reference_breaches != spark_gold_breaches:
    error_message = (
        "RECONCILIATION MISMATCH\n"
        + "Pure Python reference result: "
        + str(reference_breaches)
        + "\nSpark Gold result: "
        + str(spark_gold_breaches)
    )
    
    raise AssertionError(error_message)

# -------------------------------------------------------
# 9. Print success message
# -------------------------------------------------------

print("Reconciliation OK.")
print("Spark Gold breaches match the pure-Python reference result:")
print(spark_gold_breaches)
