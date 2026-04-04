# --- 1. APIs ---
module "api_services" {
  source     = "./modules/api-services"
  project_id = var.project_id
}

# --- 2. Artifact Registry ---
resource "google_artifact_registry_repository" "this" {
  project       = var.project_id
  location      = var.region
  repository_id = var.repository_name
  format        = "DOCKER"

  depends_on = [module.api_services]
}

# --- 3. IAM ---
module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id
  prefix     = var.prefix

  depends_on = [module.api_services]
}

# --- 4. Firestore ---
module "firestore" {
  source     = "./modules/firestore"
  project_id = var.project_id
  location   = "europe-west1"

  delete_protection       = false
  api_services_dependency = module.api_services.enabled_apis
}

# --- 5. BigQuery ---
module "bigquery" {
  source     = "./modules/bigquery"
  project_id = var.project_id
  location   = "EU"
  datasets = [
    {
      dataset_id    = "ingesta"
      friendly_name = "Ingesta"
      description   = "Dataset para datos de ingesta"
    }
  ]

  api_services_dependency = module.api_services.enabled_apis
}

# --- 6. Cloud Run: ingesta ---
module "cloud_run_ingesta" {
  source     = "./modules/cloud-run"
  project_id = var.project_id
  region     = var.region

  service_name          = "${var.prefix}-ingesta"
  image                 = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/ingesta:latest"
  service_account_email = module.iam.cloud_run_service_account_email

  extra_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
  ]

  env_vars = {
    PROJECT_ID       = var.project_id
    BIGQUERY_DATASET = "ingesta"
  }

  api_services_dependency = module.api_services.enabled_apis
}

# --- 7. Cloud Run: api ---
module "cloud_run_api" {
  source     = "./modules/cloud-run"
  project_id = var.project_id
  region     = var.region

  service_name          = "${var.prefix}-api"
  image                 = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/api:latest"
  service_account_email = module.iam.cloud_run_service_account_email

  extra_roles = [
    "roles/datastore.user",
  ]

  env_vars = {
    PROJECT_ID         = var.project_id
    FIRESTORE_DATABASE = module.firestore.database_name
  }

  api_services_dependency = module.api_services.enabled_apis
}

# --- 8. Cloud Run: app ---
module "cloud_run_app" {
  source     = "./modules/cloud-run"
  project_id = var.project_id
  region     = var.region

  service_name          = "${var.prefix}-app"
  image                 = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/app:latest"
  service_account_email = module.iam.cloud_run_service_account_email

  extra_roles = [
    "roles/datastore.user",
  ]

  env_vars = {
    PROJECT_ID         = var.project_id
    FIRESTORE_DATABASE = module.firestore.database_name
    API_URL            = module.cloud_run_api.service_url
  }

  api_services_dependency = module.api_services.enabled_apis
}