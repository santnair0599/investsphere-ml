# InvestSphere build-log posts

LinkedIn-ready write-ups from building InvestSphere — a governed investment-data
platform on Databricks (Free Edition, serverless). Each post turns a real debugging
moment into a teaching point. Drafts here; copy into LinkedIn when ready.

## Published / drafted
- [2026-06-08 — The bug that taught me how Unity Catalog row-level security really works](2026-06-08-unity-catalog-row-level-security.md)
  — row filters evaluate lazily at query time; a column-name drift broke an unrelated query; fix the function once, fix every table.

## Backlog (strong material from this build — each is its own post)
- **"My Databricks job succeeded but created no tables."** Auto Loader + `availableNow`
  is asynchronous — without `awaitTermination()` a Python script task exits before the
  streams commit, so the job goes green with zero output. Plus the silent cousins:
  empty source folder, and a stale checkpoint marking files already-seen.
- **"Unity Catalog made me create my checkpoints as a Volume."** Everything under
  `/Volumes/<cat>/<schema>/` must be a UC Volume — including Auto Loader's `_checkpoints`
  and `_schemas` dirs. Miss it and ingestion writes nothing (`UC_VOLUME_NOT_FOUND`).
- **"Why my fixes did nothing: local vs. Workspace."** Editing files locally doesn't
  update the copy the job runs in `/Workspace`. Git folders / `databricks sync` end the
  "but I fixed it!" loop.
- **"Packaging a reusable library for Databricks serverless."** From `sys.path` hacks to
  a real wheel: `pyproject.toml` `[build-system]`, a bundle `artifacts` block, and
  serverless `environments` dependencies — `import my_lib` just works, everywhere.
- **"Serverless bundles aren't classic bundles."** On Free Edition there are no job
  clusters — jobs declare an `environments:` block and tasks reference `environment_key`.
- **"Alerting on both layers."** Jobs alert via `email_notifications`; Lakeflow pipelines
  need their own `notifications` block (`on-update-failure` / `on-flow-failure`).
- **"From Gold marts to a feature store and a model — without leaking the answer."**
  Turning medallion Gold into a Unity Catalog feature table (Databricks Feature
  Engineering), a leakage-controlled training set via FeatureLookup, and an MLflow model
  registered in UC. The trap: `breach_count` is a feature *and* the label — using it is
  cheating. Why excluding it (and predicting next-day, not same-day) is what makes it real.

## Style notes
- First person, one real error, one clear lesson, 400–600 words.
- Show the error message — people search for those.
- End with takeaways a peer can act on. Light hashtags.
