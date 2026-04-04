variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "service_name" {
  type = string
}

variable "image" {
  type = string
}

variable "service_account_email" {
  type = string
}

variable "container_port" {
  type    = number
  default = 8080
}

variable "cpu" {
  type    = string
  default = "1"
}

variable "memory" {
  type    = string
  default = "512Mi"
}

variable "min_instances" {
  type    = number
  default = 0
}

variable "max_instances" {
  type    = number
  default = 10
}

variable "env_vars" {
  type    = map(string)
  default = {}
}

# Quién puede invocar este Cloud Run
variable "invokers" {
  type    = list(string)
  default = []
}

# Roles extra para el service account de este servicio
variable "extra_roles" {
  type    = list(string)
  default = []
}

variable "api_services_dependency" {
  type    = any
  default = null
}