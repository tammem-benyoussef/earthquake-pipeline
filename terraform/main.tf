resource "postgresql_schema" "staging" {
  name = "staging"
}

resource "postgresql_schema" "marts" {
  name = "marts"
}

resource "aws_s3_bucket" "raw" {
  bucket = "raw"
}

resource "aws_s3_bucket" "processed" {
  bucket = "processed"
}