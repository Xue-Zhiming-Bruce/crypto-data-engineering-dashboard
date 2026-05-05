variable "project_id" {
  description = "GCP project ID where the resources will be created."
  type        = string
}

variable "region" {
  description = "Default GCP region for the provider."
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Location for GCS and BigQuery resources. Use a BigQuery-compatible location such as US, EU, or asia-east1."
  type        = string
  default     = "US"
}

variable "raw_bucket_name" {
  description = "Globally unique GCS bucket name for raw Kraken JSONL files."
  type        = string
}

variable "bigquery_dataset_id" {
  description = "BigQuery dataset for crypto analytics tables."
  type        = string
  default     = "crypto_analytics"
}

variable "kraken_ticker_table_id" {
  description = "BigQuery table for clean Kraken ticker records."
  type        = string
  default     = "kraken_ticker"
}
