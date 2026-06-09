# InvestSphere infrastructure as code (selected Unity Catalog resources).
# This shows the platform structure being created reproducibly with Terraform.
#   terraform init
#   terraform plan
#   terraform apply

# The catalog
resource "databricks_catalog" "investsphere" {
  name    = "investsphere"
  comment = "Governed investment-data platform (managed by Terraform)"
}

# One schema per layer (bronze/silver/gold/governance/ai)
resource "databricks_schema" "layers" {
  for_each     = toset(var.layers)
  catalog_name = databricks_catalog.investsphere.name
  name         = each.value
  comment      = "InvestSphere ${each.value} layer"
}

# Analysts can read the Gold analytics schema
resource "databricks_grant" "analysts_gold" {
  schema     = "${databricks_catalog.investsphere.name}.${databricks_schema.layers["gold"].name}"
  principal  = "investsphere_analysts"
  privileges = ["USE_SCHEMA", "SELECT"]
}

# Engineers get full control of the catalog
resource "databricks_grant" "engineers_catalog" {
  catalog    = databricks_catalog.investsphere.name
  principal  = "investsphere_engineers"
  privileges = ["ALL_PRIVILEGES"]
}
