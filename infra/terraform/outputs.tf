output "raw_bucket_name" {
  description = "GCS bucket for raw Kraken JSONL files."
  value       = google_storage_bucket.raw_data_lake.name
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset for crypto analytics."
  value       = google_bigquery_dataset.crypto.dataset_id
}

output "kraken_ticker_table_id" {
  description = "BigQuery table for clean Kraken ticker records."
  value       = google_bigquery_table.kraken_ticker.table_id
}
