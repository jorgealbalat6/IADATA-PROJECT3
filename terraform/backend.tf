terraform {
  backend "gcs" {
    bucket = "bucket-jorge-albalat-dp3"
    prefix = "terraform/state"
  }
}