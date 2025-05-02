variable "credentials" {
  description = "credentials"
  default     = "/home/obaliuta/.google/credentials/google_credentials.json"
}

variable "region" {
  description = "project region"
  default     = "europe-central2"
}

variable "project" {
  description = "project"
  default     = "zoomcamp-455010"
}

variable "location" {
  description = "project location"
  default     = "EU"
}

variable "gsc_bucket_name" {
  description = "My storage bucket name"
  default     = "trips_raw_data"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "trips_data_all"
}

variable "gsc_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"

}