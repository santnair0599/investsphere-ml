# InvestSphere — Scale, Cadence & Cost

## Business-volume model (corrected)
For a diversified investment **holding company** (Dubai-Holding-Investments style),
the LARGE tables are **daily prices** and **daily holdings/valuation snapshots**.
Transactions, cashflows and valuations are business-critical but **low volume** —
we never present a transaction-heavy dataset as the normal business volume.

### Realistic distribution (~11.2M rows, ~2 years)
| Dataset | ~Rows | Driver |
|---|---|---|
| `listed_market_prices` | 7,300,000 | assets × daily |
| `investment_holdings_snapshot` | 3,650,000 | portfolios × holdings × daily |
| `investment_transactions` | 182,500 | low-volume, business-critical |
| `cashflows` | 36,500 | dividends / fees / distributions |
| `currency_rates` | 14,600 | currencies × daily |
| `private_valuation_snapshot` | 12,000 | private assets × monthly |
| `fund_nav_snapshot` | 7,200 | funds × monthly |
| master / reference / limits / benchmarks | < 25,000 | slowly changing |
| **Total** | **≈ 11.2M** | |

> Talking point: *"The ~11M-row historical scale is driven primarily by daily
> prices and holdings valuations; transactions and cashflows are lower-volume but
> business-critical."*

## Scale modes (`pipelines/generate_synthetic_data.py`, set `MODE`)
| Mode | Purpose | Rows | Claim allowed |
|---|---|---|---|
| `sample` | unit tests / logic (committed, local CSV+JSON) | 300–1,000 | "used for tests" |
| `dev` | iterative pipeline development | 50k–100k | "iterative dev" |
| `demo` | GitHub screenshots / end-to-end run | 500k–800k | "working demo" |
| `realistic` | modelled historical scale | ~11.2M | "modelled diversified workload" |
| `perftest` | **capital-markets stress test** only | 50–100M | only claim **measured** results |

`perftest` is transaction-heavy — valid for a brokerage / asset-manager / trading
platform, **not** the normal holding-company volume. Treat it as a stress test.

## Processing cadence (orchestration — `databricks.yml`)
| Job | Trigger / schedule | What it does |
|---|---|---|
| `investsphere_ingest` | **file arrival** (immutable filenames) | Auto Loader lands new files into Bronze |
| `investsphere_eod` | **daily 18:00 GST** | readiness gate → Silver clean → Gold marts |
| `investsphere_private_nav` | **monthly** (configurable) | private valuations + fund NAVs |
| `investsphere_docs_rag` | **on document upload** | parse/chunk → MERGE chunks → trigger AI Search sync |
| `investsphere_maintenance_demo` | **weekly** (demo only) | OPTIMIZE / VACUUM (prod prefers Predictive Optimization) |
| `investsphere_lakeflow` | triggered | declarative pipeline (Auto Loader + expectations) |

Note on file-arrival: triggers fire on **new** files; drop immutable names
(`transactions_2026_06_04_0900.json`), never overwrite the same name. EOD computes
only after the price/FX/holdings feeds pass the readiness gate.

## Cost on a personal Databricks account

### Free Edition (recommended for development)
Free, **serverless-only**, under a fair-usage quota; if exceeded, compute pauses
for the rest of the day (extreme cases, the month). Key limits: one workspace, one
**2X-Small** SQL warehouse, up to **5 concurrent job tasks**, **one AI Search
endpoint (1 search unit)**, one active Lakeflow pipeline per type.
- `sample`, `dev`, `demo` are **designed for Free Edition ≈ AED 0**.
- The **~11.2M `realistic` run will be attempted once after validation and remains
  subject to the Free Edition quota** — it is not guaranteed to complete free.

### Paid (pay-as-you-go) — measure, don't quote
Databricks cost is consumption-based and varies by **cloud (AWS/Azure), region,
compute SKU, serverless vs classic, runtime per job, run frequency, SQL warehouse
time, storage/logging, and whether RAG/model calls are included**. Anchors (2026):
classic Jobs Compute ≈ $0.15/DBU (+ your cloud VM); serverless ≈ $0.35–0.50/DBU.
- **Daily batch cadence:** *to be measured after a timed workload run* — do not
  quote a monthly figure as fact until measured from Databricks billing usage.
- **`perftest` (50–100M):** run **only under an explicit spend limit**; record the
  **actual measured cost** afterwards rather than estimating.

### AI Search cost (the main paid risk)
A Standard AI Search endpoint bills per **search unit** (US East ≈ $0.28/hr ≈
~$200/30-day month if it stays up). Important behaviour:
- Charges begin **only after an index is created**.
- After the **last index is deleted**, the endpoint stops charging **after 24 hours**.
- **One endpoint can serve multiple small indexes** (cheaper than many endpoints).

**Cost control (paid):** *delete all AI Search indexes after the demo unless needed
continuously; endpoint charges stop 24h after the final index is deleted; keep a
single endpoint for small workloads.* In Free Edition AI Search cost is AED 0
(subject to the 1-endpoint / 1-unit limit).

## Cost controls (do these)
1. Develop on **Free Edition** (sample/dev/demo) → ~AED 0.
2. **Auto-terminate** clusters; prefer **serverless scale-to-zero**.
3. **Delete AI Search indexes** when not demoing (endpoint stops 24h after last index).
4. Run **`perftest` once** under a spend cap; then drop the synthetic tables.
5. Production maintenance via **Predictive Optimization**, not a hand-scheduled VACUUM.

Sources: [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) ·
[AI Search (Vector Search) pricing & teardown](https://docs.databricks.com/aws/en/vector-search/vector-search) ·
[Predictive Optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization) ·
[Databricks pricing guide 2026](https://www.cloudzero.com/blog/databricks-pricing/)
