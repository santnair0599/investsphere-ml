# InvestSphere — Business Problem & Why This Solution

## Context
A diversified investment organisation (a Dubai-Holding-style holding company /
asset manager) runs **multiple portfolios** across **listed equities & bonds,
private companies and funds**, in **multiple currencies**. Every day it must know its
**exposure, P&L, and whether any portfolio breaches an investment-policy limit**
(sector / issuer / counterparty concentration) — and answer questions from portfolio
managers, compliance and analysts.

## The business problem
1. **Fragmented, ungoverned data.** Holdings, prices, trades, valuations, FX and
   reference data arrive from many feeds (custodians, market-data vendors, internal)
   in mixed formats (CSV / JSON / PDF). No single trusted source → inconsistent
   numbers and constant manual reconciliation.
2. **Late / manual compliance risk.** Concentration breaches (e.g. over-exposure to
   UAE banking) are caught late or in spreadsheets → mandate / regulatory risk.
3. **Slow, non-reproducible reporting.** Daily P&L / exposure assembled by hand; no
   lineage, hard to audit, error-prone.
4. **Silent data-quality failures.** A bad or late feed corrupts analytics with no
   validation or quarantine.
5. **Access & security.** Sensitive portfolio and counterparty data needs role-based,
   **row-level** control — a PM should see only their own book.
6. **Manual policy interpretation.** *"Does this portfolio breach a limit, by how
   much, and which policy clause applies?"* means digging through both structured
   data **and** policy PDFs → slow and inconsistent.
7. **Cost & scale.** Years of history at millions of rows; ad-hoc processing is slow
   and expensive.

## Why this solution (each choice maps to a pain point)
| Pain point | Solution in InvestSphere |
|---|---|
| Fragmented data, no trusted source | Governed **medallion lakehouse** (Bronze/Silver/Gold on Delta + Unity Catalog) — one ACID, lineage-tracked source |
| Late / manual compliance | **`fact_limit_breach`** Gold mart — automated, timely, auditable breach detection |
| Slow, non-reproducible reporting | **Dimensional Gold model** (exposure, P&L) — fast, consistent, reproducible |
| Silent bad data | **DQ rules + quarantine + reconciliation** — trusted numbers, bad rows isolated with reasons |
| Access & security | **Unity Catalog RBAC, row-level security, masking** — need-to-know consumption |
| Manual policy answers | **AI Research & Policy Copilot** (RAG over policy docs + governed business-rule functions) — grounded, cited answers in one step |
| Cost & scale | **Incremental Auto Loader, liquid clustering, idempotent processing** — scales cost-effectively |

## The headline use case embodies the whole point
> *"Which portfolios breach the UAE banking concentration limit, by how much, and
> which policy clause explains it?"*

That single question fuses governed structured analytics + business rules + policy
retrieval + access control — exactly what is slow and fragmented today.

## Business value (outcomes)
- Faster, reproducible reporting — less manual spreadsheet work.
- Earlier, auditable breach detection → lower compliance / mandate risk.
- A single trusted source → fewer reconciliation disputes.
- Secure self-service for PMs / analysts (they see only what they're entitled to).
- Decision support via the policy-grounded copilot.
- Lower compute/storage cost via incremental processing + tiered storage.

## Why Databricks specifically
A **lakehouse unifies BI *and* AI on one governed platform**: it handles
**structured + unstructured** data (RAG over PDFs), enforces governance centrally via
**Unity Catalog**, scales for large history, and runs the GenAI copilot **next to the
governed data** rather than copying it out. That fits this problem better than
stitching together a warehouse + a separate ML stack + a separate document-search tool.

> Note: InvestSphere is a **portfolio project that models** this business problem; it
> is not a deployed production system at a named employer.
