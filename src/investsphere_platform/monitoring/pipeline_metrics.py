"""Build a small dictionary of run metrics for dashboards / Prometheus."""


def pipeline_metrics(rows_in, rows_valid, rows_quarantined, duration_seconds):
    """Return one dict of metrics describing a pipeline run."""
    if rows_in:
        quarantine_rate = round(rows_quarantined / rows_in * 100, 4)
    else:
        quarantine_rate = 0.0

    return {
        "rows_in": rows_in,
        "rows_valid": rows_valid,
        "rows_quarantined": rows_quarantined,
        "quarantine_rate_pct": quarantine_rate,
        "duration_seconds": duration_seconds,
    }
