variable "project_id" {
  type = string
}

variable "database_name" {
  type    = string
  default = "(default)"
}

variable "location" {
  type    = string
  default = "europe-west1"
}

variable "delete_protection" {
  type    = bool
  default = true
}

variable "api_services_dependency" {
  type    = any
  default = null
}