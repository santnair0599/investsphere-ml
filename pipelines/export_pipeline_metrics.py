"""
Databricks custom monitoring script.

Purpose:
    This script runs as the last task of the EOD pipeline.

It calculates important pipeline metrics from Delta tables:
    1. Number of valid Silver transactions
    2. Number of quarantined transactions
    3. Number of Gold limit breaches
    4. Number of Bronze rescued rows caused by schema drift

Then it pushes these metrics to Prometheus Pushgateway.

Grafana can read these metrics from Prometheus and show dashboards/alerts.
"""
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from job_params import get_param
from investsphere_platform.monitoring.pipeline_metrics import pipeline_metrics
from investsphere_platform.monitoring.prometheus_exporter import push_metrics

# -------------------------------------------------------
# 1. Create Spark session
# -------------------------------------------------------
spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------
# 2. Read job parameters
# -------------------------------------------------------

# Catalog name.
# If no catalog parameter is passed, use "investsphere".

CATALOG = get_param("catalog", "investsphere")

# Prometheus Pushgateway URL.
# In production, this should usually come from a Databricks secret.
PUSHGATEWAY_URL = get_param(
    "pushgateway_url",
    "http://pushgateway:9091"
)

# Databricks job start time.
# Passed from databricks.yml using {{job.start_time.iso_datetime}}.
# Empty value is allowed for local runs.
JOB_START_ISO = get_param("job_start_iso", "")

# -------------------------------------------------------
# 3. Build database/schema names
# -------------------------------------------------------

BRONZE_SCHEMA = CATALOG + ".bronze"
SILVER_SCHEMA = CATALOG + ".silver"
GOLD_SCHEMA = CATALOG + ".gold"

# -------------------------------------------------------
# 4. Helper function to count rows in a table
# -------------------------------------------------------
def count_table_rows(table_name):
    """
    Count total rows in a Delta table.

    Example:
        count_table_rows("investsphere.silver.silver_transaction")
    """
    row_count = spark.table(table_name).count()
    return row_count
#-----------------------------------------------------------------------
#--------------------------------------------------------------------------

def elapsed_seconds(start_iso):
    """
    Calculate elapsed seconds from Databricks job start time to now.

    Databricks may pass the timestamp in forms like:
        2026-06-06T10:30:00Z
        2026-06-06T10:30:00+00:00

    If the value is empty or invalid, return 0.
    This keeps local runs and tests safe.
    """
    if not start_iso:
        return 0

    try:
        cleaned_start_iso = start_iso.strip()

        # Python datetime.fromisoformat does not handle trailing Z in all versions.
        # Convert Z to +00:00.
        if cleaned_start_iso.endswith("Z"):
            cleaned_start_iso = cleaned_start_iso[:-1] + "+00:00"

        start_datetime = datetime.fromisoformat(cleaned_start_iso)

        # If timezone is missing, assume UTC.
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone.utc)

        start_datetime = start_datetime.astimezone(timezone.utc)
        current_datetime = datetime.now(timezone.utc)

        seconds = int((current_datetime - start_datetime).total_seconds())

        # Avoid negative values if clocks/timestamps behave strangely.
        return max(seconds, 0)

    except Exception:
        return 0

# -------------------------------------------------------
# 5. Helper function to count rescued rows
# -------------------------------------------------------

def count_rescued_rows(table_name):
    """
    Count rows where _rescued_data is populated.

    _rescued_data is created by Auto Loader when unexpected columns
    or schema-drift data are found.

    If the table does not have _rescued_data column, return 0.
    """
    dataframe = spark.table(table_name)
    
    if "_rescued_data" not in dataframe.columns:
        return 0
    
    rescued_count = (
        dataframe
        .filter(F.col("_rescued_data").isNotNull())
        .count()
    )
    return rescued_count

# -------------------------------------------------------
# 6. Count Silver transaction metrics
# -------------------------------------------------------

valid_transaction_table = SILVER_SCHEMA + ".silver_transaction"
quarantine_transaction_table = SILVER_SCHEMA + ".quarantine_transaction"

valid_transaction_count = count_table_rows(valid_transaction_table)
quarantined_transaction_count = count_table_rows(quarantine_transaction_table)

# -------------------------------------------------------
# 7. Count Gold breach metrics
# -------------------------------------------------------

limit_breach_table = GOLD_SCHEMA + ".fact_limit_breach"
limit_breach_count = count_table_rows(limit_breach_table)

# -------------------------------------------------------
# 8. Count Bronze schema-drift / rescued rows
# ------------------------------------------------
bronze_transactions_table = BRONZE_SCHEMA + ".raw_investment_transactions"
bronze_prices_table = BRONZE_SCHEMA + ".raw_listed_market_prices"
bronze_holdings_table = BRONZE_SCHEMA + ".raw_investment_holdings_snapshot"

rescued_transaction_rows = count_rescued_rows(bronze_transactions_table)
rescued_price_rows = count_rescued_rows(bronze_prices_table)
rescued_holding_rows = count_rescued_rows(bronze_holdings_table)

total_rescued_rows = (
    rescued_transaction_rows + rescued_price_rows + rescued_holding_rows
)

# -------------------------------------------------------
# 9. Build pipeline metrics
# -------------------------------------------------------

total_input_rows = (
    valid_transaction_count + quarantined_transaction_count
)

pipeline_duration_seconds = elapsed_seconds(JOB_START_ISO)

metrics = pipeline_metrics(
    rows_in=total_input_rows,
    rows_valid=valid_transaction_count,
    rows_quarantined=quarantined_transaction_count,
    duration_seconds=pipeline_duration_seconds
)

# add extra custom metrics
metrics["breach_count"] = limit_breach_count
metrics["bronze_rescued_rows"] = total_rescued_rows

# -------------------------------------------------------
# 10. Push metrics to Prometheus Pushgateway
# -------------------------------------------------------

number_of_metrics_pushed = push_metrics(
    metrics,
    job_name="investsphere_eod",
    gateway_url=PUSHGATEWAY_URL
)


# -------------------------------------------------------
# 11. Print result for Databricks job logs
# -------------------------------------------------------

print()
print("Pipeline monitoring metrics")
print("---------------------------")
print("Catalog:", CATALOG)
print("Pushgateway URL:", PUSHGATEWAY_URL)
print()

for metric_name, metric_value in metrics.items():
    print(metric_name, "=", metric_value)
    
print()
print(
    "Pushed",
    number_of_metrics_pushed,
    "metrics to",
    PUSHGATEWAY_URL
)
