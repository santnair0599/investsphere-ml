-- InvestSphere DATA-QUALITY ALERTS  --  the "Observable -> Alertable -> Actionable" layer.
--
-- custom_dq_checks.sql already turns raw tables into rows in
-- investsphere.governance.dq_results. THIS file holds the queries a Databricks SQL
-- ALERT runs on a schedule: each returns ONE latest metric value, and the alert
-- fires (emails / notifies a destination) when the value crosses its threshold.
--
-- How alerts are created (pick one):
--   * UI:        SQL > Alerts > New alert -> paste a query below -> set condition + recipients.
--   * Terraform: terraform/dq_alerts.tf provisions a query + alert + email destination
--                for each metric here (reproducible, the recommended path).
--
-- Convention: every query aliases the value column AS `metric_value` so the alert
-- condition is simply `metric_value <op> <threshold>`.

-- 1) SCHEMA DRIFT -- fire if ANY Bronze row was rescued.   condition: metric_value > 0
SELECT metric_value
FROM investsphere.governance.dq_results
WHERE check_name = 'bronze_rescued_rows_count'
ORDER BY check_timestamp DESC LIMIT 1;

-- 2) QUARANTINE RATE -- fire if the transaction quarantine rate exceeds 2%.
--    This metric is PER-RUN: pipelines/silver_conform.py writes it from the run's
--    exact valid/quarantined counts (the quarantine table is append-only, so a
--    cumulative count would inflate the rate forever). condition: metric_value > 2
SELECT metric_value
FROM investsphere.governance.dq_results
WHERE check_name = 'transaction_quarantine_rate_pct'
ORDER BY check_timestamp DESC LIMIT 1;

-- 3) MISSING FEED -- fire if any listed asset is missing its latest price.
--    condition: metric_value > 0
SELECT metric_value
FROM investsphere.governance.dq_results
WHERE check_name = 'assets_missing_latest_price'
ORDER BY check_timestamp DESC LIMIT 1;

-- 4) FRESHNESS -- fire if the newest transaction is older than 1 day (1440 min).
--    condition: metric_value > 1440
SELECT metric_value
FROM investsphere.governance.dq_results
WHERE check_name = 'transaction_freshness_minutes'
ORDER BY check_timestamp DESC LIMIT 1;

-- 5) EXPECTATION FAILURES -- fire if the Lakeflow pipeline dropped/failed any records.
--    condition: metric_value > 0
SELECT metric_value
FROM investsphere.governance.dq_results
WHERE check_name = 'expectation_failed_records'
ORDER BY check_timestamp DESC LIMIT 1;

-- 6) BREACH SPIKE -- breaches are valid business output, but a SUDDEN JUMP is worth a
--    look. Fire if today's breach count is >50% above the trailing-7-day average.
--    condition: metric_value > 0   (1 = spike, 0 = normal)
WITH hist AS (
  SELECT metric_value AS v, check_timestamp,
         row_number() OVER (ORDER BY check_timestamp DESC) AS rn
  FROM investsphere.governance.dq_results
  WHERE check_name = 'limit_breach_count'
)
SELECT CASE
         WHEN (SELECT v FROM hist WHERE rn = 1)
              > 1.5 * coalesce((SELECT avg(v) FROM hist WHERE rn BETWEEN 2 AND 8), 0)
         THEN 1 ELSE 0
       END AS metric_value;
