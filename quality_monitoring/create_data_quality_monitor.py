# Native Data Quality Monitoring on Databricks.
#
# Data Quality Monitoring tracks freshness, completeness, profiling and anomalies
# on a table over time. (Its data-profiling capability was formerly called
# "Lakehouse Monitoring".) Run this once on Databricks to create a monitor on the
# Gold holding fact; Databricks then keeps refreshing profile + drift metrics.
#
# NOTE: the Data Quality Monitoring API/UI is evolving. The simplest reliable way
# is the Catalog Explorer UI: open the table -> "Quality" tab -> Create monitor.
# The SDK snippet below reflects the widely-used pattern; verify the exact import
# in your workspace docs before relying on it.

from databricks import lakehouse_monitoring as lm

lm.create_monitor(
    table_name="investsphere.gold.fact_daily_holding",
    # Snapshot = profile the whole table each refresh (simple to start with).
    profile_type=lm.Snapshot(),
    output_schema_name="investsphere.governance",
)

# After it runs, Databricks creates two tables in investsphere.governance:
#   *_profile_metrics  - column stats, completeness, distributions
#   *_drift_metrics    - how those stats change run-to-run (anomaly signal)
print("Monitor created. Check investsphere.governance for the metrics tables.")
