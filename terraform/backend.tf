terraform {
  backend "gcs" {
    bucket = "tfstate-grupo1"
    prefix = "terraform/state"
  }
}