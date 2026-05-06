resource "google_firestore_database" "this" {
  project     = var.project_id
  name        = var.database_name
  location_id = var.location
  type        = "FIRESTORE_NATIVE"

  delete_protection_state = var.delete_protection ? "DELETE_PROTECTION_ENABLED" : "DELETE_PROTECTION_DISABLED"

  depends_on = [var.api_services_dependency]
}