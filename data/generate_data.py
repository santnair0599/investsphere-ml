"""Generate the InvestSphere sample source catalog, organised into four folders:

  reference_data/    master / slowly-changing data (CSV)
  transaction_data/  flows of activity (JSON)
  valuation_data/    prices / holdings / valuations (CSV)
  documents/         unstructured policy & research (PDF -- see generate_documents.py)

Deterministic so numbers are stable for tests/demos. GULF_EQ is engineered to
breach the 20% Banking limit; a few dirty transaction rows exercise quarantine.

Asset modelling: every asset has an internal `investment_asset_id` (the business
key). `isin` is populated only for listed securities; private/fund assets use
`internal_investment_code` instead.

Run:  python data/generate_data.py
"""
import csv
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
DATES = [f"2026-05-{d:02d}" for d in range(18, 30)]
LATEST, PREV = DATES[-1], DATES[-2]

# investment_asset_id, name, asset_type, isin, ticker, internal_code, issuer_id, sector, country, currency, base_price
ASSETS = [
    ("AE_ENBD",     "Emirates NBD",             "LISTED_EQUITY",  "AEE001", "ENBD",  "", "ISS_ENBD",    "Banking",     "AE", "AED",  22.0),
    ("AE_FAB",      "First Abu Dhabi Bank",     "LISTED_EQUITY",  "AEF002", "FAB",   "", "ISS_FAB",     "Banking",     "AE", "AED",  13.5),
    ("AE_ADCB",     "Abu Dhabi Commercial Bank","LISTED_EQUITY",  "AEA003", "ADCB",  "", "ISS_ADCB",    "Banking",     "AE", "AED",   9.0),
    ("AE_DIB_SUK",  "Dubai Islamic Bank Sukuk", "LISTED_BOND",    "AED004", "DIB",   "", "ISS_DIB",     "Banking",     "AE", "AED", 100.0),
    ("AE_ADNOCGAS", "ADNOC Gas",                "LISTED_EQUITY",  "AEA005", "ADNOC", "", "ISS_ADNOC",   "Energy",      "AE", "AED",   3.4),
    ("AE_EAND",     "e& (Etisalat)",            "LISTED_EQUITY",  "AEE006", "EAND",  "", "ISS_EAND",    "Telecom",     "AE", "AED",  17.0),
    ("AE_EMAAR",    "Emaar Properties",         "LISTED_EQUITY",  "AEE007", "EMAAR", "", "ISS_EMAAR",   "RealEstate",  "AE", "AED",   8.2),
    ("AE_ALDAR",    "Aldar Properties",         "LISTED_EQUITY",  "AEA008", "ALDAR", "", "ISS_ALDAR",   "RealEstate",  "AE", "AED",   6.5),
    ("US_AAPL",     "Apple Inc",                "LISTED_EQUITY",  "US0378", "AAPL",  "", "ISS_AAPL",    "Technology",  "US", "USD", 190.0),
    ("US_MSFT",     "Microsoft Corp",           "LISTED_EQUITY",  "US5949", "MSFT",  "", "ISS_MSFT",    "Technology",  "US", "USD", 420.0),
    ("PE_GULFLOG",  "Gulf Logistics Holding",   "PRIVATE_COMPANY", "",      "",  "PVT-GLOG", "ISS_GULFLOG", "Industrials", "AE", "USD",   0.0),
    ("FUND_GBOND",  "Global Bond Fund",         "FUND",            "",      "",  "FND-GBND", "ISS_GBF",     "FixedIncome", "US", "USD",   0.0),
]
LISTED = [a for a in ASSETS if a[2].startswith("LISTED")]
BASE = {a[0]: a[10] for a in ASSETS}

ISSUERS = [
    ("ISS_ENBD", "Emirates NBD Group", "AE", "A2"), ("ISS_FAB", "First Abu Dhabi Bank PJSC", "AE", "Aa3"),
    ("ISS_ADCB", "Abu Dhabi Commercial Bank", "AE", "A1"), ("ISS_DIB", "Dubai Islamic Bank", "AE", "A3"),
    ("ISS_ADNOC", "ADNOC Group", "AE", "Aa2"), ("ISS_EAND", "Emirates Telecom Group", "AE", "Aa3"),
    ("ISS_EMAAR", "Emaar Properties PJSC", "AE", "Baa3"), ("ISS_ALDAR", "Aldar Properties PJSC", "AE", "Baa2"),
    ("ISS_AAPL", "Apple Inc", "US", "Aaa"), ("ISS_MSFT", "Microsoft Corp", "US", "Aaa"),
    ("ISS_GULFLOG", "Gulf Logistics Holding", "AE", "NR"), ("ISS_GBF", "Global Bond Fund Mgr", "US", "NR"),
]
COUNTERPARTIES = [
    ("CP_001", "EFG Hermes", "BROKER", "LOW", "AE"), ("CP_002", "Arqaam Capital", "BROKER", "MEDIUM", "AE"),
    ("CP_003", "Goldman Sachs Intl", "BROKER", "LOW", "US"), ("CP_004", "Daman Securities", "BROKER", "MEDIUM", "AE"),
    ("CP_005", "Shuaa Capital", "BROKER", "HIGH", "AE"),
]
PORTFOLIOS = [
    ("GULF_EQ", "Gulf Equity Fund", "S. Nair", "Regional Equity", "AED"),
    ("GLOBAL_BAL", "Global Balanced Fund", "A. Khan", "Balanced", "USD"),
    ("INCOME_FI", "Income Fixed Income", "R. Patel", "Fixed Income", "AED"),
]
HOLDINGS = {
    "GULF_EQ": {"AE_ENBD": 100000, "AE_FAB": 80000, "AE_ADCB": 60000, "AE_DIB_SUK": 5000,
                "AE_ADNOCGAS": 40000, "AE_EMAAR": 30000, "AE_ALDAR": 20000},
    "GLOBAL_BAL": {"US_AAPL": 5000, "US_MSFT": 4000, "AE_ENBD": 10000, "AE_ADNOCGAS": 8000, "AE_EAND": 15000},
    "INCOME_FI": {"AE_DIB_SUK": 50000, "AE_ENBD": 5000},
}
LIMITS = [
    ("LIM_001", "SECTOR", "Banking", "GULF_EQ", 20.0), ("LIM_002", "SECTOR", "Technology", "GLOBAL_BAL", 40.0),
    ("LIM_003", "ISSUER", "ISS_ENBD", "ALL", 25.0), ("LIM_004", "COUNTERPARTY", "CP_005", "ALL", 15.0),
    ("LIM_005", "SECTOR", "Banking", "ALL", 35.0),
]
BENCHMARK = [("BMK_GULF", "Banking", 30.0), ("BMK_GULF", "Energy", 20.0), ("BMK_GULF", "Telecom", 15.0),
             ("BMK_GULF", "RealEstate", 20.0), ("BMK_GULF", "Technology", 10.0), ("BMK_GULF", "Other", 5.0)]
CURRENCIES = [("USD", 1.0), ("AED", 0.2723), ("EUR", 1.08)]


def price_on(base, day_idx):
    return round(base * (1 + 0.002 * (day_idx - 5)), 4)


def write_csv(category, name, header, rows):
    folder = os.path.join(HERE, category)
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  {category}/{name:38s} ({len(rows)} rows)")


def write_jsonl(category, name, columns, rows):
    folder = os.path.join(HERE, category)
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, name), "w", newline="") as f:
        for row in rows:
            f.write(json.dumps(dict(zip(columns, row))) + "\n")
    print(f"  {category}/{name:38s} ({len(rows)} rows)")


def main():
    # ---- reference_data (CSV) ----
    write_csv("reference_data", "portfolio_master.csv",
              ["portfolio_id", "portfolio_name", "manager", "mandate", "base_currency"], PORTFOLIOS)
    write_csv("reference_data", "investment_asset_master.csv",
              ["investment_asset_id", "name", "asset_type", "isin", "ticker",
               "internal_investment_code", "issuer_id", "sector", "country", "currency"],
              [a[:10] for a in ASSETS])
    write_csv("reference_data", "issuer_master.csv",
              ["issuer_id", "issuer_name", "country", "credit_rating"], ISSUERS)
    write_csv("reference_data", "counterparty_master.csv",
              ["counterparty_id", "name", "type", "risk_category", "country"], COUNTERPARTIES)
    write_csv("reference_data", "investment_limits.csv",
              ["limit_id", "scope_type", "scope_value", "portfolio_id", "max_pct"], LIMITS)
    write_csv("reference_data", "benchmark_or_target_allocation.csv",
              ["benchmark_id", "sector", "target_weight_pct"], BENCHMARK)
    write_csv("reference_data", "currency_rates.csv",
              ["rate_date", "currency", "rate_to_base"], [(LATEST, c, r) for c, r in CURRENCIES])

    # ---- valuation_data (CSV) ----
    prices = [(d, aid, price_on(BASE[aid], i)) for i, d in enumerate(DATES) for aid in BASE if BASE[aid] > 0]
    write_csv("valuation_data", "listed_market_prices.csv",
              ["price_date", "investment_asset_id", "close_price"], prices)

    holdings = [(pf, asof, aid, qty) for asof in (PREV, LATEST)
                for pf, hold in HOLDINGS.items() for aid, qty in hold.items()]
    write_csv("valuation_data", "investment_holdings_snapshot.csv",
              ["portfolio_id", "as_of_date", "investment_asset_id", "quantity"], holdings)

    write_csv("valuation_data", "private_valuation_snapshot.csv",
              ["investment_asset_id", "valuation_date", "fair_value", "currency"],
              [("PE_GULFLOG", PREV, 5000000.0, "USD"), ("PE_GULFLOG", LATEST, 5200000.0, "USD")])
    write_csv("valuation_data", "fund_nav_snapshot.csv",
              ["investment_asset_id", "nav_date", "nav_per_unit", "currency"],
              [("FUND_GBOND", PREV, 101.8, "USD"), ("FUND_GBOND", LATEST, 102.5, "USD")])

    # ---- transaction_data (JSON) ----
    txns, tid = [], 1
    for pf, hold in HOLDINGS.items():
        for aid, qty in hold.items():
            cp = COUNTERPARTIES[tid % len(COUNTERPARTIES)][0]
            txns.append((f"T{tid:04d}", DATES[1], pf, aid, "BUY", qty, price_on(BASE[aid], 1), cp))
            tid += 1
    # dirty rows for the DQ / quarantine demo
    txns.append(("T9001", LATEST, "GULF_EQ", "XX_UNKNOWN", "BUY", 1000, 5.0, "CP_001"))      # unknown asset
    txns.append(("T9002", LATEST, "NO_SUCH_PF", "AE_ENBD", "BUY", 1000, 22.0, "CP_001"))     # unknown portfolio
    txns.append(("T0001", DATES[1], "GULF_EQ", "AE_ENBD", "BUY", 100000, 22.0, "CP_001"))    # duplicate id
    txns.append(("T9003", LATEST, "INCOME_FI", "AE_DIB_SUK", "SELL", -500, 100.0, "CP_002"))  # negative qty
    write_jsonl("transaction_data", "investment_transactions.json",
                ["transaction_id", "transaction_date", "portfolio_id", "investment_asset_id",
                 "transaction_type", "quantity", "price", "counterparty_id"], txns)

    write_jsonl("transaction_data", "cashflows.json",
                ["cashflow_id", "portfolio_id", "cashflow_date", "type", "amount", "currency"],
                [("CF001", "GULF_EQ", LATEST, "DIVIDEND_RECEIPT", 120000.0, "AED"),
                 ("CF002", "INCOME_FI", LATEST, "FEE", -5000.0, "AED"),
                 ("CF003", "GLOBAL_BAL", LATEST, "CAPITAL_CALL", -250000.0, "USD")])

    write_jsonl("transaction_data", "corporate_actions.json",
                ["action_id", "investment_asset_id", "action_type", "ex_date", "value"],
                [("CA001", "AE_ENBD", "RIGHTS_ISSUE", LATEST, 0.40),
                 ("CA002", "US_AAPL", "SPLIT", LATEST, 4.0)])

    print("\nDone. Source catalog written under data/{reference_data,transaction_data,valuation_data}.")
    print("Run 'python data/generate_documents.py' for the documents/*.pdf files.")
    print("GULF_EQ is engineered to breach the 20% Banking limit (LIM_001).")


if __name__ == "__main__":
    main()
