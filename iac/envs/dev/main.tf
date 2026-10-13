# S3 Bucket for Flink Checkpoints & Savepoints (Critical for Exactly-Once & Recovery)
resource "aws_s3_bucket" "flink_storage" {
  bucket        = "${var.project_name}-storage-${var.environment}"
  force_destroy = true
}

# Base DynamoDB Table for Ledger / Idempotency / State Storage
resource "aws_dynamodb_table" "ledger_store" {
  name         = "${var.project_name}-ledger-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }
}
