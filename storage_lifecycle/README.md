# InvestSphere — Storage Cost Optimisation & Redundancy (ADLS Gen2)

Backs the "optimise **storage** for large-scale processing" claim with concrete
lifecycle and redundancy configuration. Apply at the **storage-account** level —
this is separate from in-table Delta maintenance.

## Lifecycle tiering rules
| Storage area | Prefix | Rule |
|---|---|---|
| Temporary / staging landing files | `investsphere/staging/` | **Delete after 30 days** |
| Raw immutable source archive | `investsphere/archive/raw/` | **Cool after 30 days; Archive after 180 days** |
| Superseded policy/research PDFs | `investsphere/archive/documents/` | **Cool after 90 days** |
| **Active Delta tables** | `investsphere/delta/...` | **No lifecycle rule** — managed by Delta `OPTIMIZE` / `VACUUM` / Predictive Optimization |

⚠️ **Critical:** never point a generic lifecycle policy at Delta table directories.
Tiering or deleting individual Parquet/Delta files breaks the transaction log and
corrupts the table. Lifecycle rules here target only **staging** and **archive**
prefixes; live landing (Auto Loader reads it) and Delta tables are excluded.

## Storage redundancy (availability vs cost)
Set on the storage account (`account_replication_type`):
| Option | Protects against | Use |
|---|---|---|
| **LRS** | disk failure (single zone) | dev / cheapest |
| **ZRS** | **zone** failure (in-region HA) | **prod default** |
| **GZRS** | **region** failure (DR) | prod + DR |
| **RA-GZRS** | region failure + **read from secondary** | prod + active-read DR |

This links to the reliability posture: **ZRS** gives in-region storage HA; **GZRS/RA-GZRS**
underpins the cross-region DR design (`../docs/DEPLOYMENT.md`).

## Apply
**Azure CLI:**
```bash
az storage account management-policy create \
  --account-name investspherelake \
  --resource-group <rg> \
  --policy @adls_lifecycle_policy.json
```
**Terraform:** `terraform apply` in this folder (`main.tf`).

## AWS equivalent (portability)
- Lifecycle → **S3 Lifecycle rules** (Standard → Standard-IA → Glacier/Deep Archive) or **S3 Intelligent-Tiering**.
- Redundancy → S3 is multi-AZ by default; **Cross-Region Replication (CRR)** for DR.

## Honest status
Real, valid config (Azure management-policy JSON + Terraform), **authored not applied**
— it runs against an Azure storage account. Adjust the container/prefixes to your
landing-zone layout before applying.
