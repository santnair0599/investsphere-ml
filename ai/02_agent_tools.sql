-- Governed business-rule functions the assistant uses as TOOLS.
-- Keeping these as Unity Catalog SQL functions makes the logic governed, auditable
-- and reusable. For production agents these are exposed via a Databricks MANAGED
-- MCP SERVER (the recommended pattern for new agent tools); the demo agent also
-- calls them directly via SQL to keep the code simple.

-- Tool 1: concentration-limit breaches for one portfolio
CREATE OR REPLACE FUNCTION investsphere.ai.get_portfolio_breaches(target_portfolio STRING)
RETURNS TABLE (sector STRING, exposure_pct DOUBLE, max_pct DOUBLE, breach_by_pct DOUBLE, limit_id STRING)
COMMENT 'Return the concentration-limit breaches for a given portfolio_id'
RETURN
  SELECT sector, exposure_pct, max_pct, breach_by_pct, limit_id
  FROM investsphere.gold.fact_limit_breach
  WHERE portfolio_id = target_portfolio;

-- Tool 2: sector exposure for one portfolio
CREATE OR REPLACE FUNCTION investsphere.ai.get_sector_exposure(target_portfolio STRING)
RETURNS TABLE (sector STRING, exposure_pct DOUBLE)
COMMENT 'Return sector exposure percentages for a given portfolio_id'
RETURN
  SELECT sector, exposure_pct
  FROM investsphere.gold.fact_portfolio_exposure
  WHERE portfolio_id = target_portfolio;

-- Once created, register these on a Databricks managed MCP server so an agent can
-- discover them as tools. See ai/agent_exposure_policy.py for how they are used.
