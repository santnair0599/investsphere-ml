# Databricks script  ---  runs ONLY on the "false" branch of the EOD condition task
# (a required price/FX/holdings feed for the run date has not arrived). It surfaces
# the gap; in production it would also send an alert (email / Slack / PagerDuty via a
# webhook). Gold is intentionally NOT run, so no analytics are produced on
# incomplete data.

from job_params import get_param
run_date = get_param("run_date", "2026-05-29")
print("EOD SKIPPED for " + run_date + " -- a required price/FX/holdings feed is not "
      "read. Check the landing zone; the job re-runs cleanly once feeds arrive "
      "(pipelines are idempotent).")