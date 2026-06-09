-- InvestSphere dashboard queries.
-- Paste each query into a tile of a Databricks AI/BI dashboard (formerly Lakeview). Together they
-- give a portfolio-management view: exposure, breaches, P&L and data health.

-- TILE 1: sector exposure by portfolio (bar chart: portfolio_id, sector, exposure_pct)
SELECT portfolio_id, sector, exposure_pct
FROM investsphere.gold.fact_portfolio_exposure
ORDER BY portfolio_id, exposure_pct DESC;

-- TILE 2: current limit breaches (table) -- powers the headline demo
SELECT portfolio_id, sector, exposure_pct, max_pct, breach_by_pct, limit_id
FROM investsphere.gold.fact_limit_breach
ORDER BY breach_by_pct DESC;

-- TILE 3: portfolio market value (KPI / bar)
SELECT portfolio_id, round(sum(market_value), 2) AS total_market_value
FROM investsphere.gold.fact_daily_holding
GROUP BY portfolio_id
ORDER BY total_market_value DESC;

-- TILE 4: top holdings by market value (table)
SELECT portfolio_id, isin, sector, round(market_value, 2) AS market_value
FROM investsphere.gold.fact_daily_holding
ORDER BY market_value DESC
LIMIT 15;

-- TILE 5: data-health summary (table) -- latest custom DQ check results
SELECT check_name, metric_value, threshold, passed
FROM investsphere.governance.dq_results
QUALIFY row_number() OVER (PARTITION BY check_name ORDER BY check_timestamp DESC) = 1;
