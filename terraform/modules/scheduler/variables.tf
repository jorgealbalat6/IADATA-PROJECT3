variable "project_id" {
  description = "GCP project ID"
  type = string
}

variable "region" {
  description = "GCP region"
  type = string
}

variable "time_zone" {
  description = "Timezone for cron schedules"
  type = string
  default = "Europe/Madrid"
}

variable "retry_count" {
  description = "Number of retries on failure"
  type = number
  default = 2
}

variable "api_services_dependency" {
  description = "Dependency on API services being enabled"
  type = any
  default = null
}

variable "jobs" {
  description = "List of scheduler jobs to create"
  type = list(object({
    name = string
    description = string
    schedule = string
    uri = string
    service_account_email = string
  }))
}