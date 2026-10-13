output "flink_storage_bucket" {
  description = "S3 bucket for Flink checkpoints and savepoints"
  value       = aws_s3_bucket.flink_storage.bucket
}

output "dynamodb_ledger_table" {
  description = "DynamoDB ledger table name"
  value       = aws_dynamodb_table.ledger_store.name
}
