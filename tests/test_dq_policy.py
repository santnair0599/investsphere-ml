"""Tests for the Silver DQ policy tiers and the per-run quarantine rate.

These exercise the pure-Python policy that pipelines/silver_conform.py mirrors:
  1. Silver FAILS hard when a mandatory primary key (transaction_id) is missing.
  2. Silver QUARANTINES invalid-FK / invalid-quantity rows with the right reason.
  3. The quarantine RATE uses only the current pipeline_run_id, not cumulative history.
"""
import pytest

from investsphere_platform.quality import dq_rules, dq_policy
from investsphere_platform.quality.quarantine_rate import (
    quarantine_rate_pct, quarantine_rate_for_run,
)

REF = {
    "portfolios": {"GULF_EQ", "INCOME_FI"},
    "assets": {"AE_ENBD", "AE_DIB_SUK"},
}


def make_row(**changes):
    row = {"transaction_id": "T1", "portfolio_id": "GULF_EQ", "investment_asset_id": "AE_ENBD",
           "transaction_type": "BUY", "quantity": 100}
    row.update(changes)
    return row


# 1) FAIL tier --------------------------------------------------------------
def test_missing_primary_key_fails_the_pipeline():
    rows = [make_row(), make_row(transaction_id=None)]   # one row has no PK
    with pytest.raises(dq_policy.MandatoryFieldError):
        dq_policy.run_with_policy(rows, dq_rules.TRANSACTION_RULES, REF)


def test_empty_string_primary_key_also_fails():
    rows = [make_row(transaction_id="")]
    with pytest.raises(dq_policy.MandatoryFieldError):
        dq_policy.enforce_mandatory_keys(rows)


def test_all_keys_present_does_not_fail():
    rows = [make_row(transaction_id="T1"), make_row(transaction_id="T2")]
    assert dq_policy.enforce_mandatory_keys(rows) == rows


# 2) QUARANTINE tier --------------------------------------------------------
def test_invalid_fk_is_quarantined_with_reason():
    rows = [make_row(), make_row(investment_asset_id="NOPE")]
    valid, quarantined = dq_policy.run_with_policy(rows, dq_rules.TRANSACTION_RULES, REF)
    assert len(valid) == 1
    assert len(quarantined) == 1
    assert "unknown investment_asset_id=NOPE" in quarantined[0]["quarantine_reason"]


def test_invalid_quantity_is_quarantined_with_reason():
    rows = [make_row(quantity=-5)]
    valid, quarantined = dq_policy.run_with_policy(rows, dq_rules.TRANSACTION_RULES, REF)
    assert valid == []
    assert "non-positive quantity=-5" in quarantined[0]["quarantine_reason"]


def test_multiple_failures_are_joined_in_reason():
    rows = [make_row(portfolio_id="GHOST", quantity=0)]
    _, quarantined = dq_policy.run_with_policy(rows, dq_rules.TRANSACTION_RULES, REF)
    reason = quarantined[0]["quarantine_reason"]
    assert "unknown portfolio_id=GHOST" in reason
    assert "non-positive quantity=0" in reason
    assert "; " in reason   # reasons are joined


# 3) PER-RUN quarantine rate ------------------------------------------------
def test_quarantine_rate_pct_basic():
    assert quarantine_rate_pct(quarantined_count=2, valid_count=98) == 2.0
    assert quarantine_rate_pct(quarantined_count=0, valid_count=0) == 0.0


def test_rate_uses_only_current_run_not_cumulative_history():
    # Append-only quarantine table holding rows from THREE runs.
    quarantine_rows = (
        [{"pipeline_run_id": "run_old"}] * 50    # noise from earlier runs
        + [{"pipeline_run_id": "run_older"}] * 30
        + [{"pipeline_run_id": "run_today"}] * 2  # only 2 belong to today's run
    )
    # Today's run had 98 valid rows -> rate must be 2/(2+98) = 2.0%, NOT inflated by
    # the 80 historical quarantine rows (which would give 82/180 = 45.6%).
    rate = quarantine_rate_for_run(quarantine_rows, valid_count=98,
                                   pipeline_run_id="run_today")
    assert rate == 2.0


def test_rate_zero_when_run_has_no_quarantined_rows():
    quarantine_rows = [{"pipeline_run_id": "run_old"}] * 10
    assert quarantine_rate_for_run(quarantine_rows, valid_count=100,
                                   pipeline_run_id="run_today") == 0.0
