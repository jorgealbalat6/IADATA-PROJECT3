module "api_services" {
  source     = "./modules/api-services"
  project_id = var.project_id
}

resource "google_artifact_registry_repository" "this" {
  project       = var.project_id
  location      = var.region
  repository_id = var.repository_name
  format        = "DOCKER"

  depends_on = [module.api_services]
}

module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id
  prefix     = var.prefix

  depends_on = [module.api_services]
}

module "firestore" {
  source     = "./modules/firestore"
  project_id = var.project_id
  location   = "europe-west1"

  delete_protection       = false
  api_services_dependency = module.api_services.enabled_apis
}

module "bigquery" {
  source     = "./modules/bigquery"
  project_id = var.project_id
  location   = "EU"

  datasets = [
    {
      dataset_id    = "airbnb_raw"
      friendly_name = "Airbnb Raw"
      description   = "Datos crudos de Inside Airbnb (snapshots periodicos)"
    },
    {
      dataset_id    = "airbnb_features"
      friendly_name = "Airbnb Features"
      description   = "Tablas transformadas y features para el modelo de pricing"
    }
  ]

  tables = [
    {
      dataset_id = "airbnb_raw"
      table_id = "listings"
      partition_field = "snapshot_date"
      clustering = ["neighbourhood_cleansed", "room_type"]
      schema = jsonencode([
        { name = "id", type = "INT64", mode = "REQUIRED" },
        { name = "name", type = "STRING", mode = "NULLABLE" },
        { name = "host_id", type = "INT64", mode = "NULLABLE" },
        { name = "host_name", type = "STRING", mode = "NULLABLE" },
        { name = "neighbourhood_cleansed", type = "STRING", mode = "NULLABLE" },
        { name = "latitude", type = "FLOAT64", mode = "NULLABLE" },
        { name = "longitude", type = "FLOAT64", mode = "NULLABLE" },
        { name = "room_type", type = "STRING", mode = "NULLABLE" },
        { name = "accommodates", type = "INT64", mode = "NULLABLE" },
        { name = "bedrooms", type = "FLOAT64", mode = "NULLABLE" },
        { name = "bathrooms_text", type = "STRING", mode = "NULLABLE" },
        { name = "beds", type = "FLOAT64", mode = "NULLABLE" },
        { name = "amenities", type = "STRING", mode = "NULLABLE" },
        { name = "price", type = "FLOAT64", mode = "NULLABLE" },
        { name = "minimum_nights", type = "INT64", mode = "NULLABLE" },
        { name = "maximum_nights", type = "INT64", mode = "NULLABLE" },
        { name = "number_of_reviews", type = "INT64", mode = "NULLABLE" },
        { name = "review_scores_rating", type = "FLOAT64", mode = "NULLABLE" },
        { name = "review_scores_cleanliness",type = "FLOAT64", mode = "NULLABLE" },
        { name = "review_scores_location", type = "FLOAT64", mode = "NULLABLE" },
        { name = "review_scores_value", type = "FLOAT64", mode = "NULLABLE" },
        { name = "instant_bookable", type = "BOOLEAN", mode = "NULLABLE" },
        { name = "snapshot_date", type = "DATE", mode = "REQUIRED" }
      ])
    },
    {
      dataset_id = "airbnb_raw"
      table_id = "calendar"
      partition_field = "date"
      clustering = ["listing_id"]
      schema = jsonencode([
        { name = "listing_id", type = "INT64", mode = "REQUIRED" },
        { name = "date", type = "DATE", mode = "REQUIRED" },
        { name = "available", type = "BOOLEAN", mode = "NULLABLE" },
        { name = "price", type = "FLOAT64", mode = "NULLABLE" },
        { name = "adjusted_price", type = "FLOAT64", mode = "NULLABLE" },
        { name = "minimum_nights", type = "INT64", mode = "NULLABLE" },
        { name = "maximum_nights", type = "INT64", mode = "NULLABLE" },
        { name = "snapshot_date",  type = "DATE", mode = "REQUIRED" }
      ])
    },
    {
      dataset_id = "airbnb_raw"
      table_id = "reviews"
      partition_field = "date"
      clustering = ["listing_id"]
      schema = jsonencode([
        { name = "listing_id", type = "INT64",  mode = "REQUIRED" },
        { name = "id", type = "INT64",  mode = "REQUIRED" },
        { name = "date", type = "DATE",   mode = "REQUIRED" },
        { name = "reviewer_id", type = "INT64",  mode = "NULLABLE" },
        { name = "reviewer_name", type = "STRING", mode = "NULLABLE" },
        { name = "comments", type = "STRING", mode = "NULLABLE" },
        { name = "snapshot_date", type = "DATE",   mode = "REQUIRED" }
      ])
    },
    {
      dataset_id = "airbnb_features"
      table_id = "daily_listing_features"
      partition_field = "date"
      clustering = ["neighbourhood_cleansed", "room_type"]
      schema = jsonencode([
        { name = "listing_id", type = "INT64", mode = "REQUIRED" },
        { name = "neighbourhood_cleansed", type = "STRING", mode = "NULLABLE" },
        { name = "room_type", type = "STRING", mode = "NULLABLE" },
        { name = "accommodates", type = "INT64", mode = "NULLABLE" },
        { name = "bedrooms", type = "FLOAT64", mode = "NULLABLE" },
        { name = "review_scores_rating", type = "FLOAT64", mode = "NULLABLE" },
        { name = "date", type = "DATE", mode = "REQUIRED" },
        { name = "available", type = "BOOLEAN", mode = "NULLABLE" },
        { name = "price", type = "FLOAT64", mode = "NULLABLE" },
        { name = "day_of_week", type = "INT64", mode = "NULLABLE" },
        { name = "month", type = "INT64",   mode = "NULLABLE" },
        { name = "reviews_last_30d", type = "INT64", mode = "NULLABLE" },
        { name = "avg_price_nearby", type = "FLOAT64", mode = "NULLABLE" },
        { name = "count_nearby", type = "INT64", mode = "NULLABLE" }
      ])
    },
    {
      dataset_id = "airbnb_features"
      table_id = "holidays"
      schema = jsonencode([
        { name = "date", type = "DATE", mode = "REQUIRED" },
        { name = "local_name", type = "STRING", mode = "NULLABLE" },
        { name = "name", type = "STRING", mode = "NULLABLE" },
        { name = "country_code", type = "STRING", mode = "NULLABLE" },
        { name = "is_national", type = "BOOLEAN", mode = "NULLABLE" },
        { name = "applies_to_valencia", type = "BOOLEAN", mode = "NULLABLE" },
        { name = "counties", type = "STRING", mode = "NULLABLE" },
        { name = "holiday_type", type = "STRING", mode = "NULLABLE" },
        { name = "fixed", type = "BOOLEAN", mode = "NULLABLE" },
        { name = "ingested_at", type = "TIMESTAMP", mode = "NULLABLE" }
      ])
    }
  ]

  api_services_dependency = module.api_services.enabled_apis
}

# module "cloud_run_api" {
#   source     = "./modules/cloud-run"
#   project_id = var.project_id
#   region     = var.region

#   service_name          = "${var.prefix}-api"
#   image                 = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/api:latest"
#   service_account_email = module.iam.cloud_run_service_account_email
#   container_port        = 8080
#   extra_roles = [
#     "roles/datastore.user"
#   ]

#   env_vars = {
#     PROJECT_ID         = var.project_id
#     FIRESTORE_DATABASE = module.firestore.database_name
#   }

#   api_services_dependency = module.api_services.enabled_apis
# }

module "ingesta_airbnb" {
  source = "./modules/cloud-run"

  service_name          = "ingesta-airbnb"
  region                = var.region
  project_id            = var.project_id
  service_account_email = google_service_account.ingesta_airbnb.email

  image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/ingesta-airbnb:latest"

  container_port = 8080
  cpu            = "2"
  memory         = "4Gi"
  min_instances  = 0
  max_instances  = 1

  env_vars = {
    GCP_PROJECT = var.project_id
  }

  invokers = [
    "serviceAccount:sa-ingesta-airbnb@${var.project_id}.iam.gserviceaccount.com"
  ]

  extra_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
  ]

  api_services_dependency = module.api_services.enabled_apis
}

resource "google_service_account" "ingesta_airbnb" {
  project      = var.project_id
  account_id   = "sa-ingesta-airbnb"
  display_name = "Ingesta Airbnb Cloud Run"
  description  = "Service account para el servicio de ingesta de Inside Airbnb"

  depends_on = [module.api_services.enabled_apis]
}

module "ingesta_holidays"{
  source = "./modules/cloud-run"

  service_name = "ingesta-holidays"
  region = var.region
  project_id = var.project_id
  service_account_email = google_service_account.ingesta_holidays.email
  image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_name}/ingesta-holidays:latest"
 
  container_port = 8080
  min_instances  = 0
  max_instances  = 1
 
  env_vars = {
    GCP_PROJECT = var.project_id
  }
 
  invokers = [
    "serviceAccount:sa-ingesta-holidays@${var.project_id}.iam.gserviceaccount.com"
  ]
 
  extra_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
  ]
 
  api_services_dependency = module.api_services.enabled_apis
}
 
resource "google_service_account" "ingesta_holidays" {
  project      = var.project_id
  account_id   = "sa-ingesta-holidays"
  display_name = "Ingesta Holidays Cloud Run"
  description  = "Service account para el servicio de ingesta de festivos"
 
  depends_on = [module.api_services.enabled_apis]
}