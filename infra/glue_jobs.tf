# AWS Glue ETL Jobs (Spark) — needed for E1's genuine task-count measurement
# (Athena/Trino doesn't expose per-split task counts the way Spark does)
# and later for E5 (join-strategy DAG/shuffle inspection) and E6 (Iceberg
# table writes). Same proven pattern as retail-lakehouse-aws/infra/glue_jobs.tf.

data "aws_iam_policy_document" "glue_job_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${var.project_name}-glue-job"
  assume_role_policy = data.aws_iam_policy_document.glue_job_assume_role.json
}

resource "aws_iam_role_policy_attachment" "glue_job_service" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_job_s3_access" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
  }
}

resource "aws_iam_role_policy" "glue_job_s3" {
  name   = "${var.project_name}-glue-job-s3"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job_s3_access.json
}

resource "aws_s3_object" "job_scripts" {
  for_each = fileset("${path.module}/../jobs", "*.py")

  bucket = aws_s3_bucket.data.id
  key    = "scripts/${each.value}"
  source = "${path.module}/../jobs/${each.value}"
  etag   = filemd5("${path.module}/../jobs/${each.value}")
}

resource "aws_glue_job" "e1_small_files" {
  name              = "${var.project_name}-e1-small-files"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data.bucket}/${aws_s3_object.job_scripts["e1_small_files.py"].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--bucket"                           = aws_s3_bucket.data.bucket
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
  }

  tags = {
    Project = var.project_name
  }
}

# E5: same worker config as E1 (G.1X x2) for both join variants. Spark event
# logs go to S3 so shuffle bytes/stage counts can be attributed per run —
# the same data the Spark UI's DAG view is built from.
resource "aws_glue_job" "e5_join_strategy" {
  name              = "${var.project_name}-e5-join-strategy"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data.bucket}/${aws_s3_object.job_scripts["e5_join_strategy.py"].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--bucket"                           = aws_s3_bucket.data.bucket
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data.bucket}/spark-event-logs/e5/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
  }

  tags = {
    Project = var.project_name
  }
}

# E6 build: writes the Iceberg (unsorted + sorted) and sorted-plain-Parquet
# variants. Iceberg catalog config is the same proven pattern as
# retail-lakehouse-aws/infra/glue_jobs.tf. 4 workers since the sorted
# variant needs a full range-partition shuffle of all 167.8M rows.
resource "aws_glue_job" "e6_build_iceberg" {
  name              = "${var.project_name}-e6-build-iceberg"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 45

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data.bucket}/${aws_s3_object.job_scripts["e6_build_iceberg.py"].key}"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.data.bucket}/iceberg --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    "--bucket"                           = aws_s3_bucket.data.bucket
    "--database"                         = aws_glue_catalog_database.main.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-language"                     = "python"
  }

  tags = {
    Project = var.project_name
  }
}

output "glue_job_names" {
  value = {
    e1_small_files   = aws_glue_job.e1_small_files.name
    e5_join_strategy = aws_glue_job.e5_join_strategy.name
    e6_build_iceberg = aws_glue_job.e6_build_iceberg.name
  }
}
