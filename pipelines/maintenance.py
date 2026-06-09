# Databricks script  ---  table maintenance (DEMO of the mechanics)
# Runs OPTIMIZE (compact/cluster) and VACUUM (remove old files) via spark.sql.
#
# PRODUCTION NOTE: for Unity Catalog managed tables, prefer Predictive Optimization
# (Databricks runs OPTIMIZE/VACUUM/ANALYZE automatically) instead of this scheduled
# job. VACUUM uses the safe 7-day (168h) default below -- never use RETAIN 0 HOURS,
# which breaks time travel and concurrent reads.

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

OPTIMIZE_TABLES = [
    "investsphere.gold.fact_daily_holding",
    "investsphere.gold.fact_portfolio_exposure",
    "investsphere.gold.fact_limit_breach",
    "investsphere.silver.silver_transaction",
]
VACUUM_TABLES = [
    "investsphere.gold.fact_daily_holding",
    "investsphere.silver.silver_transaction",
]

for table in OPTIMIZE_TABLES:
    spark.sql("OPTIMIZE " + table)
    print("optimized", table)
    
for table in VACUUM_TABLES:
    spark.sql("VACUUM " + table + " RETAIN 168 HOURS")
    print("vacuumed", table)