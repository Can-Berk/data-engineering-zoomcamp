# Provider: https://registry.terraform.io/providers/hashicorp/google/latest/docs
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.17.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


# GCP Bucket: https://registry.terraform.io/providers/hashicorp/google/4.72.1/docs/resources/storage_bucket
resource "google_storage_bucket" "test-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "test_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}