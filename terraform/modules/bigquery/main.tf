resource "google_bigquery_dataset" "this" {
  for_each = { for ds in var.datasets : ds.dataset_id => ds }

  project       = var.project_id
  dataset_id    = each.value.dataset_id
  friendly_name = each.value.friendly_name
  description   = each.value.description
  location      = var.location
  labels        = var.labels

  delete_contents_on_destroy = var.delete_contents_on_destroy

  depends_on = [var.api_services_dependency]
}

resource "google_bigquery_table" "tables" {
  for_each = { for t in var.tables : "${t.dataset_id}.${t.table_id}" => t }

  project    = var.project_id
  dataset_id = each.value.dataset_id
  table_id   = each.value.table_id
  schema     = each.value.schema

  deletion_protection = false

  dynamic "time_partitioning" {
    for_each = each.value.partition_field != null ? [1] : []
    content {
      type  = each.value.partition_type != null ? each.value.partition_type : "DAY"
      field = each.value.partition_field
    }
  }

  clustering = each.value.clustering

  depends_on = [google_bigquery_dataset.this]
}