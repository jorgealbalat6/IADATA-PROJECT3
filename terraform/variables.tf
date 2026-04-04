variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "prefix" {
  type    = string
  default = "app"
}

variable "repository_name" {
  type    = string
  default = "app-repo"
}