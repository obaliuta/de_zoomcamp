variable "credentials" {
  description = "credentials"
  default     = "/home/obaliuta/working_directory/terrademo/keys/my-keys.json"
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
  default     = "zoomcamp-455010-terra-bucket"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}

variable "gsc_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"

}