variable "postgres_user" {
  type      = string
  sensitive = true
}
variable "postgres_password" {
  type      = string
  sensitive = true
}
variable "minio_root_user" {
  type      = string
  sensitive = true
}
variable "minio_root_password" {
  type      = string
  sensitive = true
}