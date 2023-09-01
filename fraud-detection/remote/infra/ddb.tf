resource "aws_dynamodb_table" "transactions_table" {
  name           = "${local.name}-flagged-transactions"
  billing_mode   = "PROVISIONED"
  read_capacity  = 2
  write_capacity = 2
  hash_key       = "transaction_id"
  range_key      = "transaction_date"

  attribute {
    name = "transaction_id"
    type = "S"
  }

  attribute {
    name = "account_id"
    type = "N"
  }

  attribute {
    name = "transaction_date"
    type = "S"
  }

  global_secondary_index {
    name            = "account"
    hash_key        = "account_id"
    range_key       = "transaction_date"
    write_capacity  = 2
    read_capacity   = 2
    projection_type = "ALL"
  }

  tags = local.tags
}
