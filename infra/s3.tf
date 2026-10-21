# Single bucket, prefixes matching the four physical variants in PROJECT_BRIEF.md §3:
#   raw/csv-flat/, raw/parquet-flat/, raw/parquet-smallfiles/, optimized/parquet-partitioned/
# One bucket keeps IAM and lifecycle rules simple — same pattern as retail-lakehouse-aws.

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data" {
  bucket = "${var.project_name}-${random_id.bucket_suffix.hex}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# No 30-day Glacier transition here (unlike retail-lakehouse-aws) — this
# project's total dataset is ~2-3GB source + a handful of derived variants,
# small enough that Glacier's retrieval friction isn't worth it for an
# actively-iterated benchmark repo. Just abort stuck multipart uploads.
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {
      prefix = ""
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

output "bucket_name" {
  value = aws_s3_bucket.data.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.data.arn
}
