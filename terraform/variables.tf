locals {
  data_lake_bucket = "sotck_fundamentals_us_data_lake"
}

variable "project" {
  description = "Project Name"
  type        = string
}

variable "region" {
  description = "Region for GCP resources."
  default     = "southamerica-east1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  default     = "sotck_fundamentals_us"
  type        = string
}
