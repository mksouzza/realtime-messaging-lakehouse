variable "project_id" {
  description = "The Google Cloud Platform (GCP) Project ID."
  type        = string
  default     = "blip-messaging-analytics"
}

variable "region" {
  description = "Default GCP deployment region."
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment lifecycle environment (dev, staging, prod)."
  type        = string
  default     = "prod"
}
