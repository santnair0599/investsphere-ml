variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL, e.g. https://adb-1234.5.azuredatabricks.net"
}

variable "layers" {
  type        = list(string)
  description = "Medallion + support schemas to create under the catalog"
  default     = ["bronze", "silver", "gold", "governance", "ai"]
}

variable "warehouse_id" {
  type        = string
  description = "SQL warehouse id the data-quality alert queries run on"
  default     = ""
}

variable "oncall_email" {
  type        = string
  description = "Email address the data-quality alerts notify"
  default     = "data-platform-oncall@example.com"
}
