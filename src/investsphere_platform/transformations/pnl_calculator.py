"""Daily mark-to-market P&L from position snapshots (pure)."""
from __future__ import annotations

from .exposure_calculator import market_value


def position_pnl(quantity, curr_price, prev_price):
    """Mark-to-market P&L for a held position between two valuation dates."""
    return market_value(quantity, curr_price) - market_value(quantity, prev_price)


def portfolio_daily_pnl(positions, curr_prices, prev_prices):
    """Sum position-level MTM P&L across a portfolio.

    positions: iterable of {"asset_id","quantity"} held on the current date.
    Snapshot-to-snapshot MTM; intraday transaction cashflows are out of scope for the
    MVP (documented simplification, easy to extend with fact_investment_transaction).
    """
    total = 0.0
    for p in positions:
        isin = p["isin"]
        if isin in curr_prices and isin in prev_prices:
            total += position_pnl(p["quantity"], curr_prices[isin], prev_prices[isin])
    return round(total, 4)
