# E4 fallback: Postgres on a single EC2 t3.micro, since Redshift Serverless
# is unavailable on this account (docs/RUNBOOK.md incident 1).
#
# OFF by default. The instance is the project's only continuously-billing
# resource, so it exists only for the duration of one E4 session:
#   terraform apply -var e4_enabled=true    # bring up
#   terraform apply                         # tear down (e4_enabled=false)
#
# No SSH key and no inbound ports: commands run through SSM Run Command,
# which only needs outbound HTTPS from a public subnet of the default VPC.

variable "e4_enabled" {
  description = "Create the E4 Postgres EC2 instance (same-day session only)"
  type        = bool
  default     = false
}

data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "aws_security_group" "e4" {
  count       = var.e4_enabled ? 1 : 0
  name        = "${var.project_name}-e4"
  description = "E4 Postgres box: no inbound, outbound only (SSM + S3)"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "e4" {
  count              = var.e4_enabled ? 1 : 0
  name               = "${var.project_name}-e4-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

resource "aws_iam_role_policy_attachment" "e4_ssm" {
  count      = var.e4_enabled ? 1 : 0
  role       = aws_iam_role.e4[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "e4_s3" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data.arn}/optimized/parquet-partitioned/*", "${aws_s3_bucket.data.arn}/dim/*", "${aws_s3_bucket.data.arn}/scripts/*"]
  }
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.data.arn}/e4-results/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }
}

resource "aws_iam_role_policy" "e4_s3" {
  count  = var.e4_enabled ? 1 : 0
  name   = "${var.project_name}-e4-s3"
  role   = aws_iam_role.e4[0].id
  policy = data.aws_iam_policy_document.e4_s3.json
}

resource "aws_iam_instance_profile" "e4" {
  count = var.e4_enabled ? 1 : 0
  name  = "${var.project_name}-e4"
  role  = aws_iam_role.e4[0].name
}

resource "aws_s3_object" "e4_script" {
  bucket = aws_s3_bucket.data.id
  key    = "scripts/e4_run.sh"
  source = "${path.module}/../experiments/e4_redshift_keys/e4_run.sh"
  etag   = filemd5("${path.module}/../experiments/e4_redshift_keys/e4_run.sh")
}

resource "aws_instance" "e4" {
  count                       = var.e4_enabled ? 1 : 0
  ami                         = data.aws_ssm_parameter.al2023_ami.value
  instance_type               = "t3.micro"
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.e4[0].id]
  iam_instance_profile        = aws_iam_instance_profile.e4[0].name
  associate_public_ip_address = true

  # Unlimited: a t3.micro in standard mode starts with zero CPU credits and
  # is held to a 10% baseline, which would stretch a 40M-row load into many
  # hours. Surplus CPU is billed at $0.05/vCPU-hr — bounded to well under
  # $1 for a same-day session (see docs/COST.md).
  credit_specification {
    cpu_credits = "unlimited"
  }

  root_block_device {
    volume_size           = 30
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data = <<-EOF
    #!/bin/bash
    set -eux
    dnf install -y postgresql16-server postgresql16 python3-pip
    postgresql-setup --initdb
    cat >> /var/lib/pgsql/data/postgresql.conf <<'CONF'
    shared_buffers = 128MB
    work_mem = 32MB
    maintenance_work_mem = 128MB
    effective_cache_size = 512MB
    max_parallel_workers_per_gather = 2
    track_io_timing = on
    CONF
    systemctl enable --now postgresql
    pip3 install duckdb==1.1.3
    touch /var/tmp/e4-bootstrap-done
  EOF

  tags = {
    Name    = "${var.project_name}-e4-postgres"
    Project = var.project_name
  }
}

output "e4_instance_id" {
  value = var.e4_enabled ? aws_instance.e4[0].id : null
}
