output "bronze_bucket_name" {
  description = "GCS bucket name for Bronze raw layer."
  value       = google_storage_bucket.bronze_layer.name
}

output "silver_bucket_name" {
  description = "GCS bucket name for Silver sanitized layer."
  value       = google_storage_bucket.silver_layer.name
}

output "gold_bucket_name" {
  description = "GCS bucket name for Gold analytics layer."
  value       = google_storage_bucket.gold_layer.name
}

output "pubsub_topic_id" {
  description = "Google Cloud Pub/Sub topic ID for messaging event ingestion."
  value       = google_pubsub_topic.messaging_events.id
}

output "pubsub_subscription_id" {
  description = "Subscription ID for real-time PySpark streaming consumers."
  value       = google_pubsub_subscription.streaming_subscription.id
}

output "bigquery_analytics_dataset" {
  description = "BigQuery dataset ID for curated reporting."
  value       = google_bigquery_dataset.messaging_analytics.dataset_id
}

output "service_account_email" {
  description = "Email of the provisioned Data Engineering pipeline service account."
  value       = google_service_account.lakehouse_sa.email
}
