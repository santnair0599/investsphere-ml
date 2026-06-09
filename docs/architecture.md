# InvestSphere Architecture

```
              SOURCE DATA (reference CSV + transaction JSON + doc PDF)
   portfolio_master  investment_asset_master  issuer_master  counterparty_master
   investment_transactions  investment_holdings_snapshot  listed_market_prices
   investment_limits  benchmark_or_target_allocation  currency_rates  documents
                                  |
                                  v
+---------------------------------------------------------------------------+
|  BRONZE  (raw Delta + audit columns: _ingest_ts, _source_file, _batch_id) |
|  pipelines/bronze_ingest.py                                               |
+---------------------------------------------------------------------------+
                                  |
                                  v
+---------------------------------------------------------------------------+
|  SILVER  (conformed + validated)                                          |
|  - data-quality rules  -> quarantine_transaction (bad rows + reasons)     |
|  - dedupe latest transaction_id                                                 |
|  pipelines/silver_conform.py   (rules = quality/dq_rules.py, unit-tested) |
+---------------------------------------------------------------------------+
                                  |
                                  v
+---------------------------------------------------------------------------+
|  GOLD  (dimensional star + analytics)                                     |
|  dim_portfolio  dim_investment_asset  dim_issuer  dim_counterparty        |
|  fact_daily_holding  fact_portfolio_exposure  fact_limit_breach           |
|  pipelines/gold_marts.py  (logic = transformations/exposure_calculator.py)|
+---------------------------------------------------------------------------+
                                  |
              +-------------------+-------------------+
              v                                       v
   GOVERNANCE (Unity Catalog)              AI COPILOT (extension, build last)
   - RBAC, row-level, masking              - Databricks AI Search over policy docs
   - lineage, classification               - governed breach functions via managed
   MONITORING                                MCP server
   - Data Quality Monitoring               - MLflow for GenAI evaluation
   - pipeline metrics -> Grafana

DELIVERY: Git + pytest + GitHub Actions CI + Terraform + Declarative Automation Bundles
```

## Why business logic is separated from Spark
`transformations/` and `quality/` are **pure Python reference logic** (no Spark
import), unit-tested on a laptop. The `pipelines/` scripts implement *equivalent*
logic with Spark-native expressions/SQL, and `reconcile_gold.py` asserts the two
agree on controlled data — testable reference logic + scalable implementation +
reconciliation for correctness.
