"""Tests for the exposure and breach logic. Plain functions with asserts."""
from investsphere_platform.transformations import exposure_calculator as ec

ASSETS = {
    "AE_ENBD": {"sector": "Banking", "issuer_id": "ISS_ENBD"},
    "AE_FAB": {"sector": "Banking", "issuer_id": "ISS_FAB"},
    "AE_ADNOCGAS": {"sector": "Energy", "issuer_id": "ISS_ADNOC"},
}
PRICES = {"AE_ENBD": 20.0, "AE_FAB": 10.0, "AE_ADNOCGAS": 5.0}


def test_market_value():
    assert ec.market_value(100, 20.0) == 2000.0


def test_sector_exposure_and_breach():
    positions = [
        {"portfolio_id": "GULF_EQ", "asset_id": "AE_ENBD", "quantity": 1000},      # 20000
        {"portfolio_id": "GULF_EQ", "asset_id": "AE_FAB", "quantity": 1000},       # 10000
        {"portfolio_id": "GULF_EQ", "asset_id": "AE_ADNOCGAS", "quantity": 1000},  # 5000
    ]  # total = 35000 ; Banking = 30000 -> 85.71%
    enriched = ec.enrich_positions(positions, PRICES, ASSETS)
    sector = ec.exposure_pct(enriched, "sector")
    assert round(sector["Banking"], 2) == 85.71

    issuer = ec.exposure_pct(enriched, "issuer_id")
    limits = [{"limit_id": "LIM_001", "scope_type": "SECTOR",
               "scope_value": "Banking", "portfolio_id": "GULF_EQ", "max_pct": 20.0}]
    breaches = ec.detect_breaches("GULF_EQ", sector, issuer, limits)
    assert len(breaches) == 1
    assert breaches[0]["scope_value"] == "Banking"
    assert breaches[0]["breach_by_pct"] > 0


def test_no_breach_when_under_limit():
    sector = {"Banking": 15.0}
    limits = [{"limit_id": "L", "scope_type": "SECTOR", "scope_value": "Banking",
               "portfolio_id": "ALL", "max_pct": 20.0}]
    assert ec.detect_breaches("ANY", sector, {}, limits) == []
