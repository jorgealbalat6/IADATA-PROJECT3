resource "google_cloud_scheduler_job" "this" {
  for_each = { for job in var.jobs : job.name => job }

  project = var.project_id
  region = var.region
  name = each.value.name
  description = each.value.description
  schedule = each.value.schedule
  time_zone = var.time_zone

  http_target {
    http_method = "GET"
    uri = each.value.uri

    oidc_token {
      service_account_email = each.value.service_account_email
    }
  }

  retry_config {
    retry_count = var.retry_count
  }

  depends_on = [var.api_services_dependency]
}