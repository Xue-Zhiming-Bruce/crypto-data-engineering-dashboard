terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "raw_data_lake" {
  name          = var.raw_bucket_name
  location      = var.location
  force_destroy = false

  uniform_bucket_level_access = true

  labels = {
    project = "crypto-final-project"
    layer   = "raw"
  }
}

resource "google_bigquery_dataset" "crypto" {
  dataset_id                 = var.bigquery_dataset_id
  location                   = var.location
  delete_contents_on_destroy = false

  labels = {
    project = "crypto-final-project"
  }
}

resource "google_bigquery_table" "kraken_ticker" {
  dataset_id          = google_bigquery_dataset.crypto.dataset_id
  table_id            = var.kraken_ticker_table_id
  deletion_protection = true

  schema = jsonencode([
    {
      name = "symbol"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "event_type"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "last_price"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "volume"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "vwap"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "low_price"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "high_price"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "change"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "change_pct"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "event_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    }
  ])
}
