resource "postgresql_schema" "staging" {
  name = "staging"
  database = "earthquake"
}

resource "postgresql_schema" "intermediate" {
  name     = "intermediate"
  database = "earthquake"
}

resource "postgresql_schema" "marts" {
  name = "marts"
  database = "earthquake"
}

resource "aws_s3_bucket" "raw" {
  bucket = "raw"
}

resource "aws_s3_bucket" "processed" {
  bucket = "processed"
}