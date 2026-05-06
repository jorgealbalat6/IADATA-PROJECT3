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

variable "predicthq_token" {
  type        = string
  sensitive   = true
}

variable "github_owner" {
  description = "Usuario u organizacion de GitHub"
  type = string
  default = "jorgealbalat6"
}
 
variable "github_repo" {
  description = "Nombre del repositorio en GitHub"
  type = string
  default = "IADATA-PROJECT3"
}
 
variable "github_branch" {
  description = "Rama que dispara el deploy"
  type = string
  default = "main"
}
 
variable "github_app_installation_id" {
  description = "Installation ID de la app Cloud Build en GitHub (lo sacas de la consola de GCP)"
  type = number
  default = 0
}