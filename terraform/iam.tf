# Dedicated Service Account for Lakehouse Ingestion & Processing
resource "google_service_account" "lakehouse_sa" {
  account_id   = "sa-lakehouse-pipeline-${var.environment}"
  display_name = "Lakehouse Data Engineering Service Account"
  description  = "Identity used by PySpark, Pub/Sub, and Dataproc workloads to access GCS and BigQuery."
}

# Grant GCS Object Admin on Lakehouse Buckets
resource "google_storage_bucket_iam_member" "bronze_writer" {
  bucket = google_storage_bucket.bronze_layer.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}

resource "google_storage_bucket_iam_member" "silver_writer" {
  bucket = google_storage_bucket.silver_layer.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}

resource "google_storage_bucket_iam_member" "gold_writer" {
  bucket = google_storage_bucket.gold_layer.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}

# Grant Pub/Sub Subscriber & Publisher
resource "google_project_iam_member" "pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}

resource "google_project_iam_member" "pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}

# Grant BigQuery Data Editor
resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}
