"""Pure SCD Type-2 change detection over list-of-dicts.

Used for slowly-changing reference data (investment_asset_master, issuer ratings,
investment_limits). Kept pure so the versioning logic is unit-tested without Spark;
the silver pipeline applies the same logic via a merge.
"""
from __future__ import annotations

HIGH_DATE = "9999-12-31"


def apply_scd2(existing, incoming, key_cols, tracked_cols, effective_date):
    """Compute the new SCD2 state for the keys present in `incoming`.

    existing: current dimension rows (may carry is_current/effective_from/_to)
    incoming: latest source rows
    Returns the full new current+history row set:
      - new key            -> inserted as current
      - tracked attr change -> old row closed, new current row opened
      - no change          -> current row kept as-is
      - key absent in batch -> existing current row preserved
    """
    def key_of(r):
        return tuple(r[k] for k in key_cols)

    def tracked_of(r):
        return tuple(r.get(c) for c in tracked_cols)

    current = {key_of(r): r for r in existing if r.get("is_current", True)}
    result = [r for r in existing if not r.get("is_current", True)]  # carry history

    seen = set()
    for row in incoming:
        k = key_of(row)
        seen.add(k)
        cur = current.get(k)
        if cur is None:
            result.append({**row, "effective_from": effective_date,
                           "effective_to": HIGH_DATE, "is_current": True})
        elif tracked_of(cur) != tracked_of(row):
            result.append({**cur, "effective_to": effective_date, "is_current": False})
            result.append({**row, "effective_from": effective_date,
                           "effective_to": HIGH_DATE, "is_current": True})
        else:
            result.append(cur)

    for k, cur in current.items():
        if k not in seen:
            result.append(cur)
    return result
