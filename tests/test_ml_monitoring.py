"""Tests for the pure-Python model-monitoring metrics. Plain functions with asserts,
no Spark/Databricks -- same convention as the transformation/quality tests."""
import math

import pytest

from investsphere_platform.ml import monitoring as mon


def test_breach_rate():
    assert mon.breach_rate([0, 0, 1, 1]) == 0.5
    assert mon.breach_rate([1, 1, 1]) == 1.0
    assert mon.breach_rate([0, 0]) == 0.0
    assert mon.breach_rate([]) == 0.0          # empty -> 0, not an error


def test_agreement_accuracy():
    assert mon.agreement_accuracy([1, 0, 1], [1, 0, 1]) == 1.0
    assert mon.agreement_accuracy([1, 0, 1], [1, 1, 1]) == pytest.approx(2 / 3)
    assert mon.agreement_accuracy([], []) == 1.0    # vacuously perfect


def test_agreement_accuracy_length_mismatch_raises():
    with pytest.raises(ValueError):
        mon.agreement_accuracy([1, 0], [1])


def test_distribution_summary():
    s = mon.distribution_summary([0.1, 0.2, 0.6])
    assert s["count"] == 3
    assert s["mean"] == pytest.approx(0.3)
    assert s["min"] == 0.1
    assert s["max"] == 0.6
    empty = mon.distribution_summary([])
    assert empty == {"count": 0, "mean": 0.0, "min": 0.0, "max": 0.0}


def test_psi_identical_distributions_is_near_zero():
    data = [0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95]
    assert mon.population_stability_index(data, data) == pytest.approx(0.0, abs=1e-9)


def test_psi_detects_a_shift():
    # baseline clustered low, current clustered high -> large PSI
    base = [0.05, 0.1, 0.08, 0.12, 0.06]
    curr = [0.9, 0.95, 0.85, 0.92, 0.88]
    psi = mon.population_stability_index(base, curr)
    assert psi > 0.25                      # well past the "significant shift" threshold


def test_psi_empty_side_is_zero():
    assert mon.population_stability_index([], [0.5]) == 0.0
    assert mon.population_stability_index([0.5], []) == 0.0


def test_passes_comparisons():
    assert mon.passes(0.0, 0.0, "le") is True
    assert mon.passes(0.3, 0.25, "le") is False
    assert mon.passes(0.6, 0.5, "ge") is True
    assert mon.passes(0.4, 0.5, "ge") is False
    assert mon.passes(1.0, 1.0, "eq") is True
    with pytest.raises(ValueError):
        mon.passes(1.0, 1.0, "xx")
