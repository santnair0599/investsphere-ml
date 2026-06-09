"""Pure-Python investment exposure & concentration-breach logic.

No PySpark import here on purpose: these are the **reference** business rules, kept
free of the execution engine so they unit-test on a laptop without a cluster. The
production PySpark gold pipeline implements **equivalent** logic with Spark-native
column expressions/SQL (not row-by-row Python UDFs); reconciliation tests confirm
the two agree on controlled datasets.

Positions/assets are keyed by `asset_id` (the platform's internal investment_asset_id),
which works for listed securities, private investments and funds alike.
"""
from __future__ import annotations


def market_value(quantity: float, price: float) -> float:
    """Market value of a single position."""
    return float(quantity) * float(price)


def enrich_positions(positions, prices, assets):
    """Attach market_value, sector and issuer_id to each position.

    positions: iterable of {"portfolio_id","asset_id","quantity"}
    prices:    {asset_id: close_price}
    assets:    {asset_id: {"sector":..., "issuer_id":...}}

    Positions with no matching price/asset are skipped (the DQ layer catches those).
    """
    out = []
    for p in positions:
        aid = p["asset_id"]
        if aid not in prices or aid not in assets:
            continue
        a = assets[aid]
        out.append({**p,
                    "market_value": market_value(p["quantity"], prices[aid]),
                    "sector": a["sector"],
                    "issuer_id": a["issuer_id"]})
    return out


def exposure_pct(enriched, key):
    """Percentage exposure grouped by `key` ('sector' or 'issuer_id') for one portfolio."""
    total = sum(p["market_value"] for p in enriched)
    if total == 0:
        return {}
    agg = {}
    for p in enriched:
        agg[p[key]] = agg.get(p[key], 0.0) + p["market_value"]
    return {k: round(v / total * 100, 4) for k, v in agg.items()}


def _limit_applies(limit, portfolio_id):
    return limit["portfolio_id"] in ("ALL", portfolio_id)


def detect_breaches(portfolio_id, sector_pct, issuer_pct, limits):
    """Return the list of concentration breaches for one portfolio.

    Each breach: portfolio_id, scope_type, scope_value, exposure_pct, max_pct,
    breach_by_pct, limit_id.
    """
    breaches = []
    for lim in limits:
        if not _limit_applies(lim, portfolio_id):
            continue
        scope = lim["scope_type"]
        if scope == "SECTOR":
            actual = sector_pct.get(lim["scope_value"], 0.0)
        elif scope == "ISSUER":
            actual = issuer_pct.get(lim["scope_value"], 0.0)
        else:
            continue
        if actual > lim["max_pct"]:
            breaches.append({
                "portfolio_id": portfolio_id,
                "scope_type": scope,
                "scope_value": lim["scope_value"],
                "exposure_pct": round(actual, 4),
                "max_pct": lim["max_pct"],
                "breach_by_pct": round(actual - lim["max_pct"], 4),
                "limit_id": lim.get("limit_id"),
            })
    return breaches
