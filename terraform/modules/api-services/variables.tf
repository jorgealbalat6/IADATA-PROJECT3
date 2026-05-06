variable "apis" {
  type = list(string)
  default = [
    # Cloud Run
    "run.googleapis.com",
    # Firestore
    "firestore.googleapis.com",
    "firebase.googleapis.com",
    # Vertex AI
    "aiplatform.googleapis.com",
    # BigQuery (para métricas de agentes)
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    # Artifact Registry (para guardar las imágenes Docker)
    "artifactregistry.googleapis.com",
    # IAM y gestión del proyecto
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    # Storage (bucket de Terraform state)
    "storage.googleapis.com",
    # Logs y métricas
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "cloudscheduler.googleapis.com"
  ]
}
variable "project_id" {
  type = string
}