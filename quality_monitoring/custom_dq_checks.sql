-- InvestSphere custom data-quality checks (plain SQL -- runs on any Databricks SQL
-- warehouse). Each check computes one metric and appends it to a results table you
-- can chart or alert on. This complements the native Data Quality Monitoring and is
-- the most reliable way to encode domain business rules.
--
-- Parameters (passed by the EOD job's sql_task; set them when running ad-hoc):
--   :business_date  -- the data date being checked (e.g. 2026-05-29)
--   :job_run_id     -- the Databricks job run id ({{job.run_id}})
--
-- NOTE ON THE QUARANTINE RATE: quarantine_transaction is APPEND-ONLY, so a whole-table
-- count would be cumulative and inflate the rate forever. The authoritative PER-RUN
-- `transaction_quarantine_rate_pct` is written by pipelines/silver_conform.py (it knows
-- the exact valid/quarantined counts for the run). Check #2 below only records a
-- per-run COUNT scoped to the latest pipeline_run_id as a cross-check -- it does NOT
-- recompute the rate cumulatively.

CREATE TABLE IF NOT EXISTS investsphere.governance.dq_results (
  check_timestamp TIMESTAMP,
  check_name      STRING,
  source_table    STRING,
  business_date   DATE,
  pipeline_run_id STRING,
  job_run_id      STRING,
  metric_value    DOUBLE,
  threshold       DOUBLE,
  passed          BOOLEAN
);

-- 1. holdings without a market price (should be 0)
INSERT INTO investsphere.governance.dq_results
  (check_timestamp, check_name, source_table, business_date, pipeline_run_id, job_run_id,
   metric_value, threshold, passed)
SELECT current_timestamp(), 'holdings_without_price',
       'bronze.raw_investment_holdings_snapshot', date(:business_date), NULL, :job_run_id,
       count(*), 0, count(*) = 0
FROM investsphere.bronze.raw_investment_holdings_snapshot h
LEFT JOIN investsphere.bronze.raw_listed_market_prices m
       ON h.investment_asset_id = m.investment_asset_id AND h.as_of_date = m.price_date
WHERE m.investment_asset_id IS NULL;

-- 2. PER-RUN quarantine count for the LATEST run (cross-check; rate owned by Silver).
INSERT INTO investsphere.governance.dq_results
  (check_timestamp, check_name, source_table, business_date, pipeline_run_id, job_run_id,
   metric_value, threshold, passed)
WITH latest AS (
  SELECT max(pipeline_run_id) AS rid FROM investsphere.silver.quarantine_transaction
)
SELECT current_timestamp(), 'transaction_quarantine_count_latest_run',
       'silver.quarantine_transaction', date(:business_date),
       (SELECT rid FROM latest), :job_run_id,
       count(*), NULL, NULL
FROM investsphere.silver.quarantine_transaction
WHERE pipeline_run_id = (SELECT rid FROM latest);

-- 3. number of limit breaches (informational -- expected in the demo data)
INSERT INTO investsphere.governance.dq_results
  (check_timestamp, check_name, source_table, business_date, pipeline_run_id, job_run_id,
   metric_value, threshold, passed)
SELECT current_timestamp(), 'limit_breach_count', 'gold.fact_limit_breach',
       date(:business_date), NULL, :job_run_id, count(*), 0, true
FROM investsphere.gold.fact_limit_breach;

-- 4. listed assets with no price on the run date (stale feed -> should be 0)
INSERT INTO investsphere.governance.dq_results
  (check_timestamp, check_name, source_table, business_date, pipeline_run_id, job_run_id,
   metric_value, threshold, passed)
SELECT current_timestamp(), 'assets_missing_latest_price',
       'bronze.raw_investment_asset_master', date(:business_date), NULL, :job_run_id,
       count(*), 0, count(*) = 0
FROM investsphere.bronze.raw_investment_asset_master a
LEFT JOIN (SELECT DISTINCT investment_asset_id FROM investsphere.bronze.raw_listed_market_prices
           WHERE price_date = :business_date) p ON a.investment_asset_id = p.investment_asset_id
WHERE a.asset_type LIKE 'LISTED%' AND p.investment_asset_id IS NULL;

-- 5. SCHEMA-DRIFT: rows whose Bronze _rescued_data is populated (should be 0).
--    Non-zero => an upstream feed changed shape and was rescued (NOT auto-promoted).
--    A data engineer must review and, if approved, update schemas.py.
INSERT INTO investsphere.governance.dq_results
  (check_timestamp, check_name, source_table, business_date, pipeline_run_id, job_run_id,
   metric_value, threshold, passed)
SELECT current_timestamp(), 'bronze_rescued_rows_count', 'bronze.raw_*',
       date(:business_date), NULL, :job_run_id, sum(c), 0, sum(c) = 0
FROM (
  SELECT count(*) c FROM investsphere.bronze.raw_investment_transactions WHERE _rescued_data IS NOT NULL
  UNION ALL
  SELECT count(*) c FROM investsphere.bronze.raw_listed_market_prices    WHERE _rescued_data IS NOT NULL
  UNION ALL
  SELECT count(*) c FROM investsphere.bronze.raw_investment_holdings_snapshot WHERE _rescued_data IS NOT NULL
);

-- 6. FRESHNESS: minutes since the newest transaction was ingested (warn if > 1440 = 1 day).
INSERT INTO investsphere.governance.dq_results
  (check_timestamp, check_name, source_table, business_date, pipeline_run_id, job_run_id,
   metric_value, threshold, passed)
SELECT current_timestamp(), 'transaction_freshness_minutes',
       'bronze.raw_investment_transactions', date(:business_date), NULL, :job_run_id,
       timestampdiff(MINUTE, max(_ingest_ts), current_timestamp()), 1440.0,
       timestampdiff(MINUTE, max(_ingest_ts), current_timestamp()) <= 1440.0
FROM investsphere.bronze.raw_investment_transactions;

-- 7. EXPECTATION FAILURES from the Lakeflow pipeline event log (warn if > 0).
--    CAVEAT: the event_log(...) table-valued function requires (a) Databricks
--    Runtime / DBSQL that supports it and (b) the argument to resolve to YOUR
--    pipeline. Forms differ by workspace -- use whichever resolves; the argument must
--    be a STREAMING TABLE the pipeline produced (NOT the pipeline name), or the id:
--       event_log(TABLE(investsphere.silver.silver_transactions))   -- by an output table
--       event_log("<pipeline-id>")                                  -- by pipeline id
--    If your runtime does not expose event_log, publish the pipeline event log to a
--    table (pipeline setting) and read that instead. Counts dropped/failed records.
--
-- DISABLED for the imperative path (bronze_ingest -> silver_conform -> gold_marts): this
-- check needs the Lakeflow pipeline (investsphere_lakeflow) to have RUN, so its event log
-- and a streaming table like silver.silver_transactions exist. Re-enable the block below
-- once the Lakeflow pipeline is deployed, and confirm the event_log(...) arg resolves.
/*
INSERT INTO investsphere.governance.dq_results
  (check_timestamp, check_name, source_table, business_date, pipeline_run_id, job_run_id,
   metric_value, threshold, passed)
SELECT current_timestamp(), 'expectation_failed_records', 'lakeflow.event_log',
       date(:business_date), NULL, :job_run_id,
       coalesce(sum(e.failed_records), 0), 0, coalesce(sum(e.failed_records), 0) = 0
FROM (
  SELECT explode(from_json(get_json_object(details, '$.flow_progress.data_quality.expectations'),
                'array<struct<name:string,failed_records:bigint>>')) AS e
  FROM event_log(TABLE(investsphere.silver.silver_transactions))
  WHERE event_type = 'flow_progress'
);
*/

-- See the latest results:
-- SELECT * FROM investsphere.governance.dq_results ORDER BY check_timestamp DESC;
