# dynamodb table
resource "aws_dynamodb_table" "taxi_rides" {
  count = local.connect.to_create ? 1 : 0

  name           = "${local.name}-taxi-rides"
  billing_mode   = "PROVISIONED"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "id"

  attribute {
    name = "id"
    type = "S"
  }

  tags = local.tags
}

# camel dynamodb sink
resource "aws_mskconnect_connector" "taxi_rides_sink" {
  count = local.connect.to_create ? 1 : 0

  name = "${local.name}-taxi-rides-sink"

  kafkaconnect_version = "2.7.1"

  capacity {
    provisioned_capacity {
      mcu_count    = 1
      worker_count = 1
    }
  }

  connector_configuration = {
    # connector configuration
    "connector.class"                = "org.apache.camel.kafkaconnector.awsddbsink.CamelAwsddbsinkSinkConnector",
    "tasks.max"                      = "1",
    "key.converter"                  = "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable"   = false,
    "value.converter"                = "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable" = false,
    # camel ddb sink configuration
    "topics"                                                   = "taxi-rides",
    "camel.kamelet.aws-ddb-sink.table"                         = aws_dynamodb_table.taxi_rides[0].id,
    "camel.kamelet.aws-ddb-sink.region"                        = local.region,
    "camel.kamelet.aws-ddb-sink.operation"                     = "PutItem",
    "camel.kamelet.aws-ddb-sink.writeCapacity"                 = 1,
    "camel.kamelet.aws-ddb-sink.useDefaultCredentialsProvider" = true,
    "camel.sink.unmarshal"                                     = "jackson"
  }

  kafka_cluster {
    apache_kafka_cluster {
      bootstrap_servers = aws_msk_cluster.msk_data_cluster.bootstrap_brokers_sasl_iam

      vpc {
        security_groups = [aws_security_group.msk.id]
        subnets         = module.vpc.private_subnets
      }
    }
  }

  kafka_cluster_client_authentication {
    authentication_type = "IAM"
  }

  kafka_cluster_encryption_in_transit {
    encryption_type = "TLS"
  }

  plugin {
    custom_plugin {
      arn      = aws_mskconnect_custom_plugin.camel_ddb_sink[0].arn
      revision = aws_mskconnect_custom_plugin.camel_ddb_sink[0].latest_revision
    }
  }

  log_delivery {
    worker_log_delivery {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.camel_ddb_sink[0].name
      }
      s3 {
        enabled = true
        bucket  = aws_s3_bucket.default_bucket.id
        prefix  = "logs/msk/connect/taxi-rides-sink"
      }
    }
  }

  service_execution_role_arn = aws_iam_role.kafka_connector_role[0].arn
}

resource "aws_mskconnect_custom_plugin" "camel_ddb_sink" {
  count = local.connect.to_create ? 1 : 0

  name         = "${local.name}-camel-ddb-sink"
  content_type = "ZIP"

  location {
    s3 {
      bucket_arn = aws_s3_bucket.default_bucket.arn
      file_key   = aws_s3_object.camel_ddb_sink[0].key
    }
  }
}

resource "aws_s3_object" "camel_ddb_sink" {
  count = local.connect.to_create ? 1 : 0

  bucket = aws_s3_bucket.default_bucket.id
  key    = "plugins/camel-aws-ddb-sink-kafka-connector.zip"
  source = "connectors/camel-aws-ddb-sink-kafka-connector.zip"

  etag = filemd5("connectors/camel-aws-ddb-sink-kafka-connector.zip")
}

resource "aws_cloudwatch_log_group" "camel_ddb_sink" {
  count = local.connect.to_create ? 1 : 0

  name = "/msk/connect/camel-ddb-sink"

  retention_in_days = 1

  tags = local.tags
}

## IAM permission
resource "aws_iam_role" "kafka_connector_role" {
  count = local.connect.to_create ? 1 : 0

  name = "${local.name}-connector-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "kafkaconnect.amazonaws.com"
        }
      },
    ]
  })
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess",
    aws_iam_policy.kafka_connector_policy[0].arn
  ]
}

resource "aws_iam_policy" "kafka_connector_policy" {
  count = local.connect.to_create ? 1 : 0

  name = "${local.name}-connector-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid = "PermissionOnCluster"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:AlterCluster",
          "kafka-cluster:DescribeCluster"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:kafka:${local.region}:${data.aws_caller_identity.current.account_id}:cluster/${local.name}-msk-cluster/*"
      },
      {
        Sid = "PermissionOnTopics"
        Action = [
          "kafka-cluster:*Topic*",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:kafka:${local.region}:${data.aws_caller_identity.current.account_id}:topic/${local.name}-msk-cluster/*"
      },
      {
        Sid = "PermissionOnGroups"
        Action = [
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:kafka:${local.region}:${data.aws_caller_identity.current.account_id}:group/${local.name}-msk-cluster/*"
      },
      {
        Sid = "PermissionOnDataBucket"
        Action = [
          "s3:ListBucket",
          "s3:*Object"
        ]
        Effect = "Allow"
        Resource = [
          "${aws_s3_bucket.default_bucket.arn}",
          "${aws_s3_bucket.default_bucket.arn}/*"
        ]
      },
      {
        Sid = "LoggingPermission"
        Action = [
          "logs:CreateLogStream",
          "logs:CreateLogGroup",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}
