resource "aws_s3_bucket" "default_bucket" {
  bucket = local.default_bucket.name

  force_destroy = true

  tags = local.tags
}

resource "aws_s3_bucket_acl" "default_bucket" {
  count = local.default_bucket.to_set_acl ? 1 : 0

  bucket = aws_s3_bucket.default_bucket.id
  acl    = "private"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "default_bucket" {
  bucket = aws_s3_bucket.default_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
