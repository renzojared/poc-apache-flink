variable "floci_endpoint" {
  type        = string
  description = "Endpoint URL for Floci AWS emulator"
  default     = "http://localhost:4566"
}

variable "aws_region" {
  type        = string
  description = "Target AWS region"
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
  default     = "dev"
}

variable "project_name" {
  type        = string
  description = "Project identifier"
  default     = "poc-apache-flink"
}
