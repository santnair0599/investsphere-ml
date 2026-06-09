# Terraform -- ADLS Gen2 storage account redundancy + lifecycle management policy.
# Same rules as adls_lifecycle_policy.json, expressed as IaC.
#   terraform init && terraform apply
# Provider auth via ARM_* env vars or `az login`.

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.100" }
  }
}

provider "azurerm" {
  features {}
}

variable "resource_group" {
  type = string
}

variable "location" {
  type    = string
  default = "uaenorth"
}

# Storage account. REDUNDANCY is set here:
#   dev  -> LRS  (cheapest, in-zone only)
#   prod -> ZRS  (zone-redundant = in-region HA) ; GZRS / RA-GZRS for cross-region DR
resource "azurerm_storage_account" "lake" {
  name                     = "investspherelake"
  resource_group_name      = var.resource_group
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "ZRS"          # zone-redundant for in-region availability
  is_hns_enabled           = true            # ADLS Gen2 (hierarchical namespace)
  min_tls_version          = "TLS1_2"
}

resource "azurerm_storage_management_policy" "lifecycle" {
  storage_account_id = azurerm_storage_account.lake.id

  # 1) temporary/staging landing files -> delete after 30 days
  rule {
    name    = "delete-staging-after-30d"
    enabled = true
    filters {
      prefix_match = ["investsphere/staging/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob { delete_after_days_since_modification_greater_than = 30 }
    }
  }

  # 2) raw immutable source archive -> Cool after 30d, Archive after 180d
  rule {
    name    = "tier-raw-archive"
    enabled = true
    filters {
      prefix_match = ["investsphere/archive/raw/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 30
        tier_to_archive_after_days_since_modification_greater_than = 180
      }
    }
  }

  # 3) superseded policy/research PDFs -> Cool after 90d
  rule {
    name    = "tier-superseded-documents"
    enabled = true
    filters {
      prefix_match = ["investsphere/archive/documents/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob { tier_to_cool_after_days_since_modification_greater_than = 90 }
    }
  }

  # NOTE: NO rule targets active Delta table paths (investsphere/delta/...).
  # Tiering/deleting Parquet/Delta files via a generic lifecycle policy corrupts
  # the table. Delta storage is managed by OPTIMIZE / VACUUM / Predictive Optimization.
}
