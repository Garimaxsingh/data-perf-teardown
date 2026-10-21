# Single Glue database — this project doesn't need the raw/staging/curated
# zone split from retail-lakehouse-aws, since each experiment's tables are
# just the four physical variants (A-D) registered directly via DDL/crawler.

resource "aws_glue_catalog_database" "main" {
  name = replace(var.project_name, "-", "_")
}

# --- Crawler role: crawls each variant prefix to catalog schema for Athena.

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "${var.project_name}-glue-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_crawler_s3_access" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.data.arn}/raw/*", "${aws_s3_bucket.data.arn}/optimized/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "optimized/*"]
    }
  }
}

resource "aws_iam_role_policy" "glue_crawler_s3" {
  name   = "${var.project_name}-glue-crawler-s3"
  role   = aws_iam_role.glue_crawler.id
  policy = data.aws_iam_policy_document.glue_crawler_s3_access.json
}

resource "aws_glue_crawler" "variants" {
  name          = "${var.project_name}-variants-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.main.name

  s3_target {
    path = "s3://${aws_s3_bucket.data.bucket}/raw/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.data.bucket}/optimized/parquet-partitioned/"
  }

  # Manual runs only (`aws glue start-crawler`) — no schedule, same rationale
  # as retail-lakehouse-aws: a scheduled crawler is recurring cost for no benefit
  # on a static, one-time-loaded benchmark dataset.
}

output "glue_database" {
  value = aws_glue_catalog_database.main.name
}
