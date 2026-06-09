"""Pure-Python model-monitoring metrics for the breach classifier.

These functions take plain Python numbers/lists (already aggregated by Spark on the
driver) and return metrics. No Spark, no Databricks, no numpy -- so they run in
milliseconds under pytest, exactly like the transformation/quality reference logic.

Used by ``ml/model_monitoring.py`` to compute the metrics it writes to
``governance.dq_results``.
"""
from __future__ import annotations

import math
from typing import Sequence


def breach_rate(labels: Sequence[int]) -> float:
    """Fraction of 1s in a 0/1 sequence (predicted or actual breach flags).

    Empty input -> 0.0 (no rows means no breaches, not an error).
    """
    if not labels:
        return 0.0
    return sum(1 for x in labels if x) / len(labels)


def agreement_accuracy(predicted: Sequence[int], actual: Sequence[int]) -> float:
    """Share of rows where predicted == actual. Inputs must be equal length.

    Empty input -> 1.0 (vacuously, nothing disagrees). Raises if lengths differ,
    because a length mismatch means the join upstream was wrong and should fail loudly.
    """
    if len(predicted) != len(actual):
        raise ValueError(
            f"predicted/actual length mismatch: {len(predicted)} vs {len(actual)}"
        )
    if not predicted:
        return 1.0
    correct = sum(1 for p, a in zip(predicted, actual) if p == a)
    return correct / len(predicted)


def distribution_summary(values: Sequence[float]) -> dict:
    """Min/mean/max/count of a numeric sequence (e.g. predicted probabilities).

    Empty input -> all-zero summary so callers can record a row rather than crash.
    """
    if not values:
        return {"count": 0, "mean": 0.0, "min": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "mean": sum(values) / len(values),
        "min": min(values),
        "max": max(values),
    }


def population_stability_index(
    expected: Sequence[float],
    actual: Sequence[float],
    bins: int = 10,
    lo: float = 0.0,
    hi: float = 1.0,
) -> float:
    """Population Stability Index between two probability distributions (drift metric).

    PSI = sum over bins of (a% - e%) * ln(a% / e%), where e%/a% are the share of the
    expected/actual populations falling in each fixed-width bin over ``[lo, hi]``.
    Probabilities live in [0, 1], so fixed equal-width bins are deterministic and make
    this trivially testable (identical distributions -> ~0).

    Rule-of-thumb reading: < 0.1 no shift, 0.1-0.25 moderate, > 0.25 significant shift.

    Either side empty -> 0.0 (nothing to compare). A small epsilon avoids log(0)/div-0
    when a bin is empty on one side.
    """
    if not expected or not actual:
        return 0.0

    eps = 1e-6
    width = (hi - lo) / bins

    def _bin_index(v: float) -> int:
        if v <= lo:
            return 0
        if v >= hi:
            return bins - 1
        return min(int((v - lo) / width), bins - 1)

    exp_counts = [0] * bins
    act_counts = [0] * bins
    for v in expected:
        exp_counts[_bin_index(v)] += 1
    for v in actual:
        act_counts[_bin_index(v)] += 1

    n_exp, n_act = len(expected), len(actual)
    psi = 0.0
    for e, a in zip(exp_counts, act_counts):
        e_pct = max(e / n_exp, eps)
        a_pct = max(a / n_act, eps)
        psi += (a_pct - e_pct) * math.log(a_pct / e_pct)
    return psi


def passes(metric_value: float, threshold: float, comparison: str = "le") -> bool:
    """Generic pass/fail used to fill the ``passed`` column of dq_results.

    comparison: 'le' (value <= threshold), 'ge' (value >= threshold),
    'eq' (value == threshold). Defaults to 'le' (most checks are "stay under a ceiling").
    """
    if comparison == "le":
        return metric_value <= threshold
    if comparison == "ge":
        return metric_value >= threshold
    if comparison == "eq":
        return metric_value == threshold
    raise ValueError(f"unknown comparison: {comparison}")
