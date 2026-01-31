variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my_creds.json"
}

variable "project" {
  description = "Project"
  default     = "de-terraform-485812"
}

variable "region" {
  description = "Region"
  default  = "europe-west3"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "nytaxi_data"
}

# GCP Bucket: https://registry.terraform.io/providers/hashicorp/google/4.72.1/docs/resources/storage_bucket
variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "de-terraform-485812-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
