variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "EU"
}

variable "labels" {
  description = "Labels to apply to datasets"
  type        = map(string)
  default     = {}
}

variable "delete_contents_on_destroy" {
  description = "Delete dataset contents on destroy"
  type        = bool
  default     = false
}

variable "api_services_dependency" {
  description = "Dependency on API services being enabled"
  type        = any
  default     = null
}

variable "datasets" {
  description = "List of BigQuery datasets to create"
  type = list(object({
    dataset_id    = string
    friendly_name = string
    description   = string
  }))
  default = []
}

variable "tables" {
  description = "List of BigQuery tables to create"
  type = list(object({
    dataset_id      = string
    table_id        = string
    schema          = string
    partition_field = optional(string, null)
    partition_type  = optional(string, "DAY")
    clustering      = optional(list(string), null)
  }))
  default = []
}