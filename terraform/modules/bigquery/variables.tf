# modules/bigquery/variables.tf

variable "project_id" {
  type        = string
  description = "El ID del proyecto de GCP."
}

variable "dataset_id" {
  type        = string
  description = "El ID único para el Dataset de BigQuery (sin espacios)."
}

variable "friendly_name" {
  type        = string
  description = "Nombre amigable para el Dataset."
  default     = null
}

variable "description" {
  type        = string
  description = "Descripción del propósito del Dataset."
  default     = "Dataset gestionado por Terraform"
}

variable "location" {
  type        = string
  description = "Ubicación geográfica de los datos (ej. 'EU', 'US')."
  default     = "europe-west1"
}

variable "labels" {
  type        = map(string)
  description = "Etiquetas clave-valor."
  default     = {}
}

variable "tables" {
  type = map(object({
    description = string
    schema      = string
  }))
  description = "Mapa de tablas a crear. La clave es el nombre de la tabla."
  default     = {}
}