"""Per-run quarantine rate.

The Delta `quarantine_transaction` table is APPEND-ONLY (it is an audit trail), so it
accumulates rows from MANY pipeline runs. The alerting rate must therefore be scoped
to the CURRENT run via `pipeline_run_id` -- counting the whole table would inflate the
rate with historical rows forever and the alert would never reflect today's run.

The Silver job writes the authoritative per-run rate into governance.dq_results using
`quarantine_rate_pct` (it knows the exact valid/quarantined counts for the batch it
just processed). `quarantine_rate_for_run` is the same calculation expressed over rows
of the append-only table, used to verify the per-run scoping.
"""


def quarantine_rate_pct(quarantined_count, valid_count):
    """Quarantine % = quarantined / (quarantined + valid). 0.0 when there are no rows."""
    total = quarantined_count + valid_count
    if total == 0:
        return 0.0
    return round(quarantined_count / total * 100, 4)


def quarantine_rate_for_run(quarantine_rows, valid_count, pipeline_run_id):
    """Quarantine % for ONE run, ignoring rows from other runs.

    quarantine_rows : rows from the append-only quarantine table; each a dict that
                      carries a `pipeline_run_id`.
    valid_count     : number of VALID rows for this same run.
    pipeline_run_id : the run to scope to.
    """
    quarantined = sum(1 for row in quarantine_rows
                      if row.get("pipeline_run_id") == pipeline_run_id)
    return quarantine_rate_pct(quarantined, valid_count)
