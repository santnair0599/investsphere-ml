# InvestSphere DATA-QUALITY ALERTS as code.
#
# Turns the queries in quality_monitoring/dq_alerts.sql into scheduled Databricks SQL
# Alerts so DQ regressions are not just dashboarded but actually PAGE someone. Each
# metric becomes: a databricks_query (the saved query) + a databricks_alert (the
# scheduled condition) that notifies the on-call destination on breach.
#
# Requires a SQL warehouse id and the Databricks provider >= 1.61 (databricks_alert v2).
#   terraform apply -var 'warehouse_id=...' -var 'oncall_email=...'

# Notification destination (email) the alerts fire to.
resource "databricks_notification_destination" "dq_oncall" {
  display_name = "investsphere-dq-oncall"
  config {
    email {
      addresses = [var.oncall_email]
    }
  }
}

# The DQ metrics we alert on. `op`/`threshold` mirror the conditions documented in
# quality_monitoring/dq_alerts.sql. `query` selects the latest value AS metric_value.
locals {
  dq_alerts = {
    schema_drift = {
      display   = "InvestSphere DQ - Bronze schema drift (rescued rows > 0)"
      check     = "bronze_rescued_rows_count"
      op        = "GREATER_THAN"
      threshold = 0
    }
    quarantine_rate = {
      display   = "InvestSphere DQ - Quarantine rate > 2%"
      check     = "transaction_quarantine_rate_pct"
      op        = "GREATER_THAN"
      threshold = 2
    }
    missing_feed = {
      display   = "InvestSphere DQ - Missing latest price feed"
      check     = "assets_missing_latest_price"
      op        = "GREATER_THAN"
      threshold = 0
    }
    freshness = {
      display   = "InvestSphere DQ - Transaction feed stale (> 1 day)"
      check     = "transaction_freshness_minutes"
      op        = "GREATER_THAN"
      threshold = 1440
    }
    expectation_failures = {
      display   = "InvestSphere DQ - Lakeflow expectation failures"
      check     = "expectation_failed_records"
      op        = "GREATER_THAN"
      threshold = 0
    }
  }
}

# One saved query per metric: latest value for that check, aliased metric_value.
resource "databricks_query" "dq" {
  for_each     = local.dq_alerts
  warehouse_id = var.warehouse_id
  display_name = "dq_${each.key}"
  query_text   = <<-SQL
    SELECT metric_value
    FROM investsphere.governance.dq_results
    WHERE check_name = '${each.value.check}'
    ORDER BY check_timestamp DESC LIMIT 1
  SQL
}

# One scheduled alert per metric. Fires to the on-call destination on threshold breach.
resource "databricks_alert" "dq" {
  for_each     = local.dq_alerts
  query_id     = databricks_query.dq[each.key].id
  display_name = each.value.display

  condition {
    op = each.value.op
    operand {
      column { name = "metric_value" }
    }
    threshold {
      value { double_value = each.value.threshold }
    }
  }

  # Re-evaluate every 30 minutes.
  schedule {
    quartz_cron_schedule = "0 */30 * * * ?"
    timezone_id          = "Asia/Dubai"
    pause_status         = "UNPAUSED"
  }

  notify_on_ok = true
}
