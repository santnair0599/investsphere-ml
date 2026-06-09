"""Tests for the data-quality rules and runner."""
from investsphere_platform.quality import dq_rules, dq_runner

REF = {
    "portfolios": {"GULF_EQ", "INCOME_FI"},
    "assets": {"AE_ENBD", "AE_DIB_SUK"},
}


def make_row(**changes):
    row = {"transaction_id": "T1", "portfolio_id": "GULF_EQ", "investment_asset_id": "AE_ENBD",
           "transaction_type": "BUY", "quantity": 100}
    row.update(changes)
    return row


def test_clean_row_passes():
    valid, quarantined = dq_runner.run_rules([make_row()], dq_rules.TRANSACTION_RULES, REF)
    assert len(valid) == 1
    assert quarantined == []


def test_unknown_asset_is_quarantined():
    valid, quarantined = dq_runner.run_rules([make_row(investment_asset_id="XX")],
                                             dq_rules.TRANSACTION_RULES, REF)
    assert valid == []
    assert "unknown investment_asset_id=XX" in quarantined[0]["_dq_reasons"]


def test_bad_type_and_negative_quantity_quarantined():
    rows = [make_row(transaction_type="WUT"), make_row(quantity=-5)]
    valid, quarantined = dq_runner.run_rules(rows, dq_rules.TRANSACTION_RULES, REF)
    assert valid == []
    assert len(quarantined) == 2
