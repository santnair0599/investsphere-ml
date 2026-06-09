-- Table maintenance -- schedule as a job (e.g., daily).
--   OPTIMIZE  compacts many small files into fewer large ones (faster reads). On
--             liquid-clustered tables it also re-clusters the data.
--   VACUUM    removes old, unreferenced files to control storage cost.

OPTIMIZE investsphere.gold.fact_daily_holding;

OPTIMIZE investsphere.gold.fact_portfolio_exposure;
OPTIMIZE investsphere.gold.fact_limit_breach;
OPTIMIZE investsphere.silver.silver_transaction;

-- keep 7 days of history (168 hours) so time-travel still works
VACUUM investsphere.gold.fact_daily_holding RETAIN 168 HOURS;
VACUUM investsphere.silver.silver_transaction RETAIN 168 HOURS;

-- BETTER: let Databricks run OPTIMIZE/VACUUM automatically for managed tables, so
-- you don't have to schedule this at all:
ALTER CATALOG investsphere ENABLE PREDICTIVE OPTIMIZATION;