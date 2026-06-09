"""Tests for the Prometheus metric-name sanitiser (pure, no prometheus_client needed)."""
from investsphere_platform.monitoring.prometheus_exporter import sanitize_metric_name


def test_replaces_spaces_and_symbols():
    assert sanitize_metric_name("Quarantine Rate %") == "quarantine_rate__"


def test_keeps_valid_chars():
    assert sanitize_metric_name("rows_in") == "rows_in"
    assert sanitize_metric_name("svc:latency_ms") == "svc:latency_ms"


def test_lowercases():
    assert sanitize_metric_name("RowsValid") == "rowsvalid"
