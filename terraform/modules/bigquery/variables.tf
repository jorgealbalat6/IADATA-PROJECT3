variable "project_id" {
  type = string
}

variable "location" {
  type    = string
  default = "europe-west1"
}

variable "datasets" {
  type = list(object({
    dataset_id    = string
    friendly_name = optional(string, "")
    description   = optional(string, "")
  }))
  default = []
}

variable "tables" {
  type = list(object({
    dataset_id = string
    table_id   = string
    schema     = string
  }))
  default = []
}

variable "delete_contents_on_destroy" {
  type    = bool
  default = false
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "api_services_dependency" {
  type    = any
  default = null
}