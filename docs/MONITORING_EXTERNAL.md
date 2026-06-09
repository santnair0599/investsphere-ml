# InvestSphere — External Monitoring (Prometheus + Grafana)

Tracks **pipeline performance** outside Databricks, as the JD calls for (Prometheus /
Grafana / custom monitoring scripts).

## Architecture (push model)
Databricks jobs are short-lived, so we **push** metrics rather than expose a scrape
endpoint:

```
EOD job → export_pipeline_metrics.py
            → pipeline_metrics() builds {rows_valid, rows_quarantined,
              quarantine_rate_pct, breach_count, bronze_rescued_rows, ...}
            → push_metrics() → Prometheus PUSHGATEWAY
                                  → Prometheus (scrapes the gateway)
                                      → Grafana dashboard (grafana_pipeline_dashboard.json)
```

This is the **parallel** monitoring path. It runs alongside (not instead of) the
in-platform `governance.dq_results` + Databricks SQL Alerts loop (see
`docs/RUNBOOK.md`): the SQL-alerts loop is the authoritative **pager** for DQ
regressions, while Prometheus/Grafana give engineers a time-series **view** of the
same signals (including `bronze_rescued_rows` for schema drift). You can alert from
either side — Prometheus alert rules can fire on the pushed metrics too.

## Components (in the repo)
- **`investsphere_platform.monitoring.pipeline_metrics`** — builds the metric dict (pure, tested).
- **`investsphere_platform.monitoring.prometheus_exporter`** — `push_metrics(...)` pushes
  each numeric metric to a Pushgateway as a labelled Gauge (`sanitize_metric_name` is unit-tested).
- **`pipelines/export_pipeline_metrics.py`** — custom monitoring script run as the last EOD task.
- **`dashboards/grafana_pipeline_dashboard.json`** — importable Grafana dashboard
  (valid txns, quarantine rate %, open breaches, valid-vs-quarantined trend).

## Setup
1. Run a **Pushgateway** and **Prometheus** (scrape the gateway); stand up **Grafana**
   with Prometheus as a datasource (uid `prometheus`).
2. Pass the gateway URL to the job: parameter `--pushgateway_url http://<host>:9091`
   (or a Databricks secret).
3. Import `grafana_pipeline_dashboard.json` into Grafana.
4. The exporter pushes under job `investsphere_eod`; metric names are prefixed
   `investsphere_` (e.g. `investsphere_quarantine_rate_pct`).

## Alternative (no Prometheus)
Grafana can also read **directly from Databricks SQL** via the Databricks Grafana
datasource plugin — query `investsphere.governance.dq_results` and the Gold tables.
Use this if you don't want to run Prometheus/Pushgateway. The in-platform
`dashboards/platform_ops_dashboard.sql` covers the same signals natively.

## Honest status
- The exporter + Grafana dashboard + script are **real code/artifacts**; the
  `sanitize_metric_name` logic is unit-tested.
- It requires **running Pushgateway/Prometheus/Grafana + a Databricks job** — so it's
  executed in that environment, not locally. `prometheus_client` is an optional dep
  (imported lazily), needed only when the exporter actually pushes.
