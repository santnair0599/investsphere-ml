-- InvestSphere governance, step 3: fine-grained security.
--   (a) Row-level security: a portfolio manager sees only their own portfolios.
--   (b) Column masking: non-engineers cannot see the raw counterparty id.
-- Both use simple Unity Catalog functions. Run on Databricks (serverless / UC compute).
--
-- ============================================================================
-- COLUMN CONTRACT -- keep these names in sync or the filters break at QUERY time
-- (UC evaluates row filters/masks lazily, so a wrong column name only errors when
--  someone reads the table, with a confusing UNRESOLVED_COLUMN about the FILTER).
--   investsphere.governance.pm_portfolio_map( pm_user STRING, portfolio_id STRING )
--     - pm_user      = the user's login email (matched against current_user())
--     - portfolio_id = a portfolio that user may see
-- The function below references ONLY these two columns, via the alias `m`.
-- ============================================================================

-- Safety: make sure the mapping table exists with the EXACT contract above.
CREATE TABLE IF NOT EXISTS investsphere.governance.pm_portfolio_map (
  pm_user      STRING,
  portfolio_id STRING
);

-- (a) ROW FILTER -------------------------------------------------------------
-- Returns TRUE for rows the current user is allowed to see. The parameter is named
-- `pid` (NOT portfolio_id) so it can never be confused with the table column.
CREATE OR REPLACE FUNCTION investsphere.governance.portfolio_row_filter(pid STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('investsphere_engineers')   -- engineers see everything
  OR is_account_group_member('investsphere_analysts')  -- analysts see everything
  OR pid IN (                                          -- PMs see only mapped portfolios
       SELECT m.portfolio_id
       FROM investsphere.governance.pm_portfolio_map m
       WHERE m.pm_user = current_user()                -- contract column: pm_user
     );

-- Attach the SAME function to every Gold fact that has a portfolio_id. Because tables
-- reference the filter BY NAME, fixing the function once fixes all of these at once.
ALTER TABLE investsphere.gold.fact_portfolio_exposure
  SET ROW FILTER investsphere.governance.portfolio_row_filter ON (portfolio_id);

ALTER TABLE investsphere.gold.fact_daily_holding
  SET ROW FILTER investsphere.governance.portfolio_row_filter ON (portfolio_id);

ALTER TABLE investsphere.gold.fact_limit_breach
  SET ROW FILTER investsphere.governance.portfolio_row_filter ON (portfolio_id);

-- To detach (undo) the filter from a table:
--   ALTER TABLE investsphere.gold.fact_portfolio_exposure DROP ROW FILTER;
--   ALTER TABLE investsphere.gold.fact_daily_holding      DROP ROW FILTER;
--   ALTER TABLE investsphere.gold.fact_limit_breach       DROP ROW FILTER;

-- IMPORTANT (AI governance): Databricks AI Search does NOT support row/column-level
-- permissions, and you CANNOT build an AI Search index from a table that has a row
-- filter or column mask. So the copilot does NOT index these secured Gold tables.
-- Instead it reads structured results through governed UC functions (which DO
-- enforce these policies) and only indexes APPROVED, non-restricted documents.


-- (b) COLUMN MASK ------------------------------------------------------------
-- Hides the counterparty id from anyone who is not an engineer. Applied to a column
-- that actually exists: silver.silver_transaction.counterparty_id (produced by
-- pipelines/silver_conform.py). (The old version masked silver.silver_counterparty,
-- a table this project does not build -- that ALTER always failed.)
CREATE OR REPLACE FUNCTION investsphere.governance.mask_counterparty_id(cpty STRING)
RETURNS STRING
RETURN CASE
         WHEN is_account_group_member('investsphere_engineers') THEN cpty
         ELSE '***MASKED***'
       END;

ALTER TABLE investsphere.silver.silver_transaction
  ALTER COLUMN counterparty_id SET MASK investsphere.governance.mask_counterparty_id;

-- To detach (undo) the mask:
--   ALTER TABLE investsphere.silver.silver_transaction ALTER COLUMN counterparty_id DROP MASK;

-- Quick verification (should return rows, no error -- proves the filter/mask resolve):
--   SELECT * FROM investsphere.gold.fact_limit_breach LIMIT 5;
--   SELECT transaction_id, counterparty_id FROM investsphere.silver.silver_transaction LIMIT 5;
