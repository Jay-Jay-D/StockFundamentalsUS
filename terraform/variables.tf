locals {
  data_lake_bucket = "data_lake_sfu"
}

variable "project" {
  description = "Project Name"
  type        = string
}

variable "region" {
  description = "Region for GCP resources."
  default     = "us"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  default     = "data_warehouse"
  type        = string
}
