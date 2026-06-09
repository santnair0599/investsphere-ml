# Terraform provider setup for Databricks.
# Authenticate with environment variables before running:
#   DATABRICKS_HOST   = https://YOUR-WORKSPACE.cloud.databricks.com
#   DATABRICKS_TOKEN  = <a personal access token>

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.61"   # databricks_alert / databricks_query (v2 SQL alerting)
    }
  }
}

provider "databricks" {
  host = var.databricks_host
}
