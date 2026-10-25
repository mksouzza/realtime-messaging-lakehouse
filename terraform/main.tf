terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.15.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ==============================================================================
# Google Cloud Storage (GCS) - Lakehouse Storage Layers (Medallion Pattern)
# ==============================================================================

# Bronze Bucket: Raw, unpartitioned JSON event dumps
resource "google_storage_bucket" "bronze_layer" {
  name                        = "${var.project_id}-lakehouse-bronze-${var.environment}"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = var.environment == "dev" ? true : false

  lifecycle_rule {
    condition {
      age = 90 # Transition raw dumps to Nearline for cost optimization
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    environment = var.environment
    layer       = "bronze"
    managed_by  = "terraform"
  }
}

# Silver Bucket: Sanitized, schema-enforced Parquet / Delta records
resource "google_storage_bucket" "silver_layer" {
  name                        = "${var.project_id}-lakehouse-silver-${var.environment}"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  labels = {
    environment = var.environment
    layer       = "silver"
    managed_by  = "terraform"
  }
}

# Gold Bucket: Curated dimensional tables and aggregated business marts
resource "google_storage_bucket" "gold_layer" {
  name                        = "${var.project_id}-lakehouse-gold-${var.environment}"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  labels = {
    environment = var.environment
    layer       = "gold"
    managed_by  = "terraform"
  }
}

# ==============================================================================
# Google Cloud Pub/Sub - Real-Time Ingestion Messaging Backbone
# ==============================================================================

# Dead-Letter Topic for unprocessable messages
resource "google_pubsub_topic" "dead_letter_topic" {
  name = "omnichannel-messaging-dlq-${var.environment}"

  labels = {
    environment = var.environment
    role        = "dlq"
  }
}

# Primary Messaging Events Topic
resource "google_pubsub_topic" "messaging_events" {
  name = "omnichannel-messaging-events-${var.environment}"

  labels = {
    environment = var.environment
    role        = "ingestion"
  }
}

# Subscription for PySpark / Dataproc Streaming Consumer
resource "google_pubsub_subscription" "streaming_subscription" {
  name  = "sub-omnichannel-pyspark-stream-${var.environment}"
  topic = google_pubsub_topic.messaging_events.name

  ack_deadline_seconds       = 60
  retain_acked_messages      = false
  message_retention_duration = "604800s" # 7 days retention

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter_topic.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# ==============================================================================
# Google Cloud BigQuery - Analytics & Serving Layer
# ==============================================================================

resource "google_bigquery_dataset" "messaging_analytics" {
  dataset_id                  = "messaging_analytics_${var.environment}"
  friendly_name               = "Omnichannel Messaging Analytics"
  description                 = "Curated Gold layer tables and views for conversational analytics and bot KPIs."
  location                    = var.region
  default_table_expiration_ms = null

  labels = {
    environment = var.environment
    domain      = "conversational_ai"
  }
}

resource "google_bigquery_dataset" "messaging_governance" {
  dataset_id    = "messaging_governance_${var.environment}"
  friendly_name = "Messaging Data Governance & Auditing"
  description   = "Audit trails, data quality test metrics, and pipeline execution logs."
  location      = var.region

  labels = {
    environment = var.environment
    domain      = "governance"
  }
}
