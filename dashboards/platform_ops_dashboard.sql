-- InvestSphere PLATFORM-OPERATIONS dashboard (AI/BI). Paste each query into a tile.
-- This is the "is the platform healthy?" view for data engineers / on-call -- distinct
-- from the business dashboard (exposure/breaches).
--
-- NOTE: system-table names evolve; verify in your workspace (system.lakeflow.*,
-- system.billing.usage). Tiles 3-5 use the project's own governance tables.

-- TILE 1: recent job run outcomes (last 7 days)
SELECT job_id, run_id, result_state, period_start_time, period_end_time
FROM system.lakeflow.job_run_timeline
WHERE period_start_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY period_start_time DESC
LIMIT 100;

-- TILE 2: serverless / job spend in the last 30 days (cost watch)
SELECT usage_date, sku_name, round(sum(usage_quantity), 2) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY usage_date, sku_name
ORDER BY usage_date DESC;

-- TILE 3: latest data-quality check results (pass/fail)
SELECT check_name, metric_value, threshold, passed
FROM investsphere.governance.dq_results
QUALIFY row_number() OVER (PARTITION BY check_name ORDER BY check_timestamp DESC) = 1;

-- TILE 4: transaction quarantine-rate trend
SELECT date(check_timestamp) AS day, max(metric_value) AS quarantine_rate_pct
FROM investsphere.governance.dq_results
WHERE check_name = 'transaction_quarantine_rate_pct'
GROUP BY date(check_timestamp)
ORDER BY day DESC;

-- TILE 5: Gold freshness -- latest as_of_date present in the holding fact
SELECT max(as_of_date) AS latest_holding_date,
       count(DISTINCT portfolio_id) AS portfolios_loaded
FROM investsphere.gold.fact_daily_holding;

-- TILE 6: open breaches on the latest date
SELECT count(*) AS breach_count
FROM investsphere.gold.fact_limit_breach
WHERE as_of_date = (SELECT max(as_of_date) FROM investsphere.gold.fact_limit_breach);
