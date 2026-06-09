"""Helpers for the audit/lineage values we stamp on every ingested row."""

# the audit columns the bronze layer adds to every table
AUDIT_COLUMNS = ["_ingest_ts", "_source_file", "_batch_id"]


def make_batch_id(source, run_timestamp):
    """Build a simple batch id like 'trades__2026-05-29T08:00'.

    run_timestamp is passed in by the caller (not generated here) so the same
    inputs always produce the same batch id, which keeps tests reproducible.
    """
    return source + "__" + str(run_timestamp)
