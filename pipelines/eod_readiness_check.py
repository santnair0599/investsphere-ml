"""
Databricks script -- EOD data-readiness gate.

Purpose:
    Before running Silver and Gold jobs, this script checks whether
    the required Bronze data has arrived for the run date.

It checks these feeds:
    1. Listed market prices
    2. Investment holdings snapshot
    3. Currency rates

If all feeds have data for the run date:
    feeds_ready = true

If any feed is missing:
    feeds_ready = false

A downstream Databricks Workflow condition task can use this value
to decide whether to continue or send a "data not ready" alert.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from job_params import get_param

#-------------------------------------------
# 1. Create Spark Session
#-----------------------------------------------

spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------
# 2. Read job parameters
# -------------------------------------------------------

# Catalog name
# if no catalog parameter is passed, use "investsphere"
CATALOG = get_param("catalog", "investsphere")

# Run date.
# if not run date parameter is passed, use this default date
RUN_DATE = get_param("run_date", "2026-05-29")

# Bronze database/schema name
# Example: investsphere.bronze
BRONZE_SCHEMA = CATALOG + ".bronze"

# -------------------------------------------------------
# 3. Function to check whether table has data for run date
# -------------------------------------------------------

def table_has_data_for_date(table_name, date_column_name):
    """
    Check whether a table has at least one row for the run date.

    Example:
        table_name = "investsphere.bronze.raw_listed_market_prices"
        date_column_name = "price_date"

    This checks:
        SELECT *
        FROM table_name
        WHERE price_date = RUN_DATE
        LIMIT 1

    If at least one row exists, return True.
    Otherwise, return False.
    """
    matching_rows_count = (
        spark.table(table_name)
        .filter(F.col(date_column_name) == RUN_DATE)
        .limit(1)
        .count()
    )
    
    if matching_rows_count > 0:
        return True
    else:
        return False
    
    # -------------------------------------------------------
# 4. Check required feeds
# -------------------------------------------------------

prices_table = BRONZE_SCHEMA + ".raw_listed_market_prices"
holdings_table = BRONZE_SCHEMA + ".raw_investment_holdings_snapshot"
currency_rates_table = BRONZE_SCHEMA + ".raw_currency_rates"

prices_received = table_has_data_for_date(
    prices_table, "price_date"
)

holdings_received = table_has_data_for_date(
    holdings_table, "as_of_date"
)

fx_received = table_has_data_for_date(
    currency_rates_table, "rate_date"
)

# -------------------------------------------------------
# 5. Store check results in one dictionary
# -------------------------------------------------------

readiness_checks = {
    "prices_received": prices_received,
    "holdings_received": holdings_received,
    "fx_received": fx_received
}


# -------------------------------------------------------
# 6. Decide final readiness status
# -------------------------------------------------------

all_feeds_ready = all(readiness_checks.values())

# -------------------------------------------------------
# 7. Print results for logging/debugging
# -------------------------------------------------------

print()
print("EOD readiness check")
print("-------------------")
print("Run date:", RUN_DATE)
print("Catalog :", CATALOG)
print()

for check_name, check_result in readiness_checks.items():
    print(check_name, "=", check_result)
    
print()
print("Final feeds_ready =", all_feeds_ready)


# -------------------------------------------------------
# 8. Publish result to Databricks Workflow task value
# -------------------------------------------------------

try:
    from pyspark.dbutils import DBUtils
    
    dbutils = DBUtils(spark)
    if all_feeds_ready:
        task_value = "true"
    else:
        task_value = "false"
    
    dbutils.jobs.taskValues.set(
        key="feeds_ready",
        value=task_value
    )
    
    print("Published task value: feeds_ready =", task_value)

except Exception:
    # This happens when running locally or outside Databricks Workflows.
    # In that case, there is no task value to publish.
    print("Could not publish task value. Probably running locally.")
