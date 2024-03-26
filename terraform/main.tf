terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.21.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "book_recommendation_datalake" {
  name     = "${local.DE_2004_PROJECT_DATALAKE}_${var.project}"
  location = var.region

  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 10 //days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "book_recommendataion_ext_dataset" {
  project                    = var.project
  location                   = var.region
  dataset_id                 = var.book_recommendataion_ext_datasets
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "book_recommendation_analytics_dataset" {
  project                    = var.project
  location                   = var.region
  dataset_id                 = var.book_recommendation_analytics_datasets
  delete_contents_on_destroy = true
}
