"""Local end-to-end demo -- NO Spark, NO cloud needed.

Reads the generated files, runs the data-quality check on transactions, then computes
sector exposure and concentration-limit breaches using the same pure functions the
Databricks Gold pipeline uses. Great for seeing real numbers on your laptop.

Run:  python examples/run_local_demo.py
"""
import csv
import json
import os
import sys

# make the library importable when running this script directly
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

from investsphere_platform.quality import dq_rules, dq_runner
from investsphere_platform.transformations import exposure_calculator as ec

DATA = os.path.join(HERE, "..", "data")
LATEST = "2026-05-29"


def read_csv(name):
    with open(os.path.join(DATA, name), newline="") as f:
        return list(csv.DictReader(f))


def read_jsonl(name):
    """Read line-delimited JSON (one object per line)."""
    rows = []
    with open(os.path.join(DATA, name)) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main():
    securities_rows = read_csv(os.path.join("reference_data", "investment_asset_master.csv"))
    portfolios_rows = read_csv(os.path.join("reference_data", "portfolio_master.csv"))
    prices_rows = read_csv(os.path.join("valuation_data", "listed_market_prices.csv"))
    positions_rows = read_csv(os.path.join("valuation_data", "investment_holdings_snapshot.csv"))
    trades_rows = read_jsonl(os.path.join("transaction_data", "investment_transactions.json"))
    limits_rows = read_csv(os.path.join("reference_data", "investment_limits.csv"))

    # ---- 1. data quality on transactions ----
    ref = {
        "portfolios": {r["portfolio_id"] for r in portfolios_rows},
        "assets": {r["investment_asset_id"] for r in securities_rows},
    }
    valid, quarantined = dq_runner.run_rules(trades_rows, dq_rules.TRANSACTION_RULES, ref)
    print(f"Transactions: {len(valid)} valid, {len(quarantined)} quarantined")
    for bad in quarantined:
        print(f"   quarantined {bad['transaction_id']}: {bad['_dq_reasons']}")

    # ---- 2. build lookups ----
    assets = {r["investment_asset_id"]: {"sector": r["sector"], "issuer_id": r["issuer_id"]}
              for r in securities_rows}
    prices = {r["investment_asset_id"]: float(r["close_price"])
              for r in prices_rows if r["price_date"] == LATEST}
    limits = [{"limit_id": r["limit_id"], "scope_type": r["scope_type"],
               "scope_value": r["scope_value"], "portfolio_id": r["portfolio_id"],
               "max_pct": float(r["max_pct"])} for r in limits_rows]

    # ---- 3. exposure + breaches per portfolio ----
    print(f"\nSector exposure & breaches as of {LATEST}:")
    portfolio_ids = sorted({r["portfolio_id"] for r in positions_rows})
    for pf in portfolio_ids:
        pos = [{"portfolio_id": pf, "asset_id": r["investment_asset_id"], "quantity": float(r["quantity"])}
               for r in positions_rows
               if r["portfolio_id"] == pf and r["as_of_date"] == LATEST]
        enriched = ec.enrich_positions(pos, prices, assets)
        sector = ec.exposure_pct(enriched, "sector")
        issuer = ec.exposure_pct(enriched, "issuer_id")
        breaches = ec.detect_breaches(pf, sector, issuer, limits)

        print(f"\n  {pf}")
        for s, pct in sorted(sector.items(), key=lambda x: -x[1]):
            print(f"     {s:12s} {pct:6.2f}%")
        if breaches:
            for b in breaches:
                print(f"     >> BREACH {b['scope_value']} = {b['exposure_pct']}% "
                      f"> limit {b['max_pct']}% (over by {b['breach_by_pct']}%, {b['limit_id']})")
        else:
            print("     (no breaches)")


if __name__ == "__main__":
    main()
