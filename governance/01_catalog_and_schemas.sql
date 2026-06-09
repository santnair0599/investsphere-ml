-- InvestSphere governance, step 1: create the Unity Catalog structure.
-- One catalog, one schema per medallion layer, plus governance + ai schemas.
-- Run on Databricks (SQL editor or a notebook). Unity Catalog gives you lineage,
-- auditing and access control automatically once tables live here.

CREATE CATALOG IF NOT EXISTS investsphere
  COMMENT 'Governed investment-data platform';

CREATE SCHEMA IF NOT EXISTS investsphere.bronze     COMMENT 'Raw ingested data';
CREATE SCHEMA IF NOT EXISTS investsphere.silver     COMMENT 'Cleaned, validated data';
CREATE SCHEMA IF NOT EXISTS investsphere.gold       COMMENT 'Analytics: marts, exposure, breaches';
CREATE SCHEMA IF NOT EXISTS investsphere.governance COMMENT 'Audit, DQ results, access maps';
CREATE SCHEMA IF NOT EXISTS investsphere.ai         COMMENT 'Documents and AI Search indexes';

-- A small mapping table: which portfolio managers can see which portfolios.
-- The row-level security filter (step 3) reads from this.
CREATE TABLE IF NOT EXISTS investsphere.governance.pm_portfolio_map (
  pm_user      STRING,   -- the user's login email
  portfolio_id STRING
);

-- Example rows (replace with real user emails in your workspace):
-- INSERT INTO investsphere.governance.pm_portfolio_map VALUES
--   ('gulf.pm@example.com',   'GULF_EQ'),
--   ('global.pm@example.com', 'GLOBAL_BAL'),
--   ('income.pm@example.com', 'INCOME_FI');
