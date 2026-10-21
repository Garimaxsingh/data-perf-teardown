variable "aws_region" {
  description = "AWS region — locked to ap-south-1 (Mumbai), same as the sibling retail-lakehouse-aws project"
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Short name used as a prefix for all resources"
  type        = string
  default     = "data-perf-teardown"
}

variable "environment" {
  description = "Deployment environment tag"
  type        = string
  default     = "portfolio"
}
