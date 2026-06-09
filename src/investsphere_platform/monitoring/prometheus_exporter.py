"""Push pipeline metrics to a Prometheus Pushgateway.

Databricks jobs are short-lived, so we PUSH metrics to a Pushgateway (rather than
let Prometheus scrape a long-running endpoint). Grafana then visualises the
Prometheus data. `prometheus_client` is imported lazily so the rest of the library
imports without it (and unit tests don't need it).
"""


def sanitize_metric_name(name):
    """Make a Prometheus-legal metric name: [a-zA-Z_:][a-zA-Z0-9_:]* (lower-cased).

    Pure function -> unit-testable without prometheus_client.
    """
    cleaned = "".join(ch if (ch.isalnum() or ch in "_:") else "_" for ch in name)
    return cleaned.lower()


def push_metrics(metrics, job_name, gateway_url, namespace="investsphere"):
    """Push a dict of {name: number} to the Pushgateway as labelled Gauges.

    metrics:     e.g. {"rows_valid": 15, "quarantine_rate_pct": 16.7, ...}
    job_name:    grouping key in Prometheus (e.g. "investsphere_eod")
    gateway_url: e.g. "http://pushgateway:9091"
    Returns the number of metrics pushed.
    """
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    registry = CollectorRegistry()
    pushed = 0
    for key, value in metrics.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            metric_name = namespace + "_" + sanitize_metric_name(key)
            gauge = Gauge(metric_name, "InvestSphere metric: " + key, ["job"], registry=registry)
            gauge.labels(job=job_name).set(value)
            pushed += 1

    push_to_gateway(gateway_url, job=job_name, registry=registry)
    return pushed
