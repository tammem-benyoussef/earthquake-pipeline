provider "postgresql" {
  host     = "localhost"
  port     = 5432
  username = var.postgres_user
  password = var.postgres_password
  sslmode  = "disable"
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = var.minio_root_user
  secret_key                  = var.minio_root_password
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
  s3_use_path_style           = true

  endpoints {
    s3 = "http://localhost:9000"
  }
}