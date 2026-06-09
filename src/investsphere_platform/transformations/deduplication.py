"""Deterministic dedupe keeping the latest record per key (pure)."""
from __future__ import annotations


def dedupe_latest(rows, key_cols, order_col):
    """Keep one row per key -- the one with the max `order_col` value.

    On ties the last-seen row wins, matching 'latest ingestion timestamp' semantics.
    """
    best = {}
    for r in rows:
        k = tuple(r[c] for c in key_cols)
        if k not in best or r[order_col] >= best[k][order_col]:
            best[k] = r
    return list(best.values())
