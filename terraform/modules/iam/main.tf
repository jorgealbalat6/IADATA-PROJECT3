# Service Account para Cloud Run
resource "google_service_account" "cloud_run" {
  project      = var.project_id
  account_id   = "${var.prefix}-cloud-run-sa"
  display_name = "Cloud Run Service Account"
}

resource "google_project_iam_member" "cloud_run_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}

resource "google_project_iam_member" "cloud_run_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.cloud_run.email}"
}