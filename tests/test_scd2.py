"""Tests for the SCD Type-2 change detection."""
from investsphere_platform.transformations import scd2


def test_new_key_is_inserted():
    out = scd2.apply_scd2([], [{"id": 1, "rating": "A"}], ["id"], ["rating"], "2026-01-01")
    assert len(out) == 1
    assert out[0]["is_current"] is True
    assert out[0]["effective_from"] == "2026-01-01"


def test_changed_attribute_creates_new_version():
    existing = [{"id": 1, "rating": "A", "effective_from": "2025-01-01",
                 "effective_to": scd2.HIGH_DATE, "is_current": True}]
    out = scd2.apply_scd2(existing, [{"id": 1, "rating": "B"}], ["id"], ["rating"], "2026-01-01")
    current = [r for r in out if r["is_current"]]
    closed = [r for r in out if not r["is_current"]]
    assert len(current) == 1 and current[0]["rating"] == "B"
    assert len(closed) == 1 and closed[0]["effective_to"] == "2026-01-01"


def test_unchanged_attribute_keeps_one_row():
    existing = [{"id": 1, "rating": "A", "effective_from": "2025-01-01",
                 "effective_to": scd2.HIGH_DATE, "is_current": True}]
    out = scd2.apply_scd2(existing, [{"id": 1, "rating": "A"}], ["id"], ["rating"], "2026-01-01")
    assert len(out) == 1
    assert out[0]["is_current"] is True
