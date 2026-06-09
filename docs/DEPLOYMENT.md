# InvestSphere — Enterprise Deployment Mapping (AWS / Azure)

The role calls for AWS and Azure expertise. InvestSphere runs on Databricks (cloud-
agnostic), and each logical component maps cleanly to either cloud. You don't deploy
both personally — document one path and relate it to your AWS experience.

| Logical component | AWS | Azure |
|---|---|---|
| Raw landing storage | Amazon S3 | ADLS Gen2 |
| UC storage access | UC external locations + service credentials (IAM role) | UC external locations + managed identity |
| Secrets | AWS Secrets Manager (or Databricks secret scope) | Azure Key Vault (or Databricks secret scope) |
| File/event ingestion | S3 landing + Auto Loader (file-arrival trigger) | ADLS landing + Auto Loader (file-arrival trigger) |
| Compute | Databricks serverless (jobs); Photon on the Lakeflow pipeline | Databricks serverless (jobs); Photon on the Lakeflow pipeline |
| Monitoring export | CloudWatch / Grafana | Azure Monitor / Grafana |
| CI/CD + IaC | GitHub Actions + Terraform + Declarative Automation Bundles | same |
| Identity | IAM + SCIM (paid) | Entra ID + SCIM (paid) |

## Notes
- The pipelines, governance SQL, bundles and Terraform are **identical across clouds**
  — only storage paths, secret backend and identity differ. That portability is a
  selling point.
- Personal-account reality: Free Edition is serverless and cloud-managed; you don't
  configure S3/ADLS directly there. The mapping above is the **enterprise** pattern.
- Tie-in: prior AWS data-engineering experience (S3 + Auto Loader + IAM) maps directly
  to the left column; the Azure column is the same shape with ADLS + managed identity.

## Disaster Recovery (DR)
**Targets (illustrative):** RPO ≤ 24h (last successful daily run), RTO ≤ 4h (restore +
re-point). Tune to the business mandate.

**Strategy — Delta deep clone to a secondary region:**
- Gold (and curated Silver) tables are replicated to a secondary-region catalog with
  scheduled **`CREATE OR REPLACE TABLE <dr>.gold.<t> DEEP CLONE <primary>.gold.<t>`**
  (incremental: a deep clone only copies new/changed files). Source files in cloud
  storage are also covered by storage-level cross-region replication.
- Reference/raw data is reproducible from the immutable landing zone, so Bronze can be
  rebuilt by re-pointing Auto Loader at the replicated landing path.
- Unity Catalog metastore is regional; DR uses a secondary metastore + bundle
  re-deploy (`databricks bundle deploy -t dr`) to recreate jobs/pipelines/grants.

**Recovery procedure (restore drill — run periodically):**
1. Deploy the bundle to the DR target (jobs, pipelines, governance recreated as code).
2. Promote the deep-cloned Gold tables (or rebuild Bronze→Gold from the replicated
   landing zone — pipelines are idempotent, so a full rebuild is safe).
3. Re-point dashboards / AI Search / consumers at the DR workspace.
4. Validate with `reconcile_gold.py` and the ops dashboard freshness tile.
5. **Drill cadence:** quarterly restore test; record actual RTO achieved.

**Why this is credible:** everything is code (bundles + Terraform + governance SQL),
data is reproducible from an immutable landing zone, and writes are idempotent — so
recovery is a redeploy + reprocess, not a fragile manual restore.
