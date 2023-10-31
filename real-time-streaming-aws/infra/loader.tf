resource "aws_kinesisanalyticsv2_application" "loader_app" {
  count = local.loader.to_create ? 1 : 0

  name                   = "${local.name}-loader-app"
  runtime_environment    = local.loader.runtime_env
  service_execution_role = aws_iam_role.loader_app_role[0].arn

  application_configuration {
    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = aws_s3_bucket.default_bucket.arn
          file_key   = aws_s3_object.loader_package[0].key
        }
      }

      code_content_type = "ZIPFILE"
    }

    vpc_configuration {
      security_group_ids = [aws_security_group.loader_sg[0].id]
      subnet_ids         = module.vpc.private_subnets
    }

    environment_properties {
      property_group {
        property_group_id = "kinesis.analytics.flink.run.options"

        property_map = {
          python  = "processor.py"
          jarfile = "package/lib/s3-data-loader-1.0.0.jar"
        }
      }

      property_group {
        property_group_id = "source.config.0"

        property_map = {
          "table.name" = "taxi_trip_source"
          "file.path"  = "s3://${aws_s3_bucket.default_bucket.id}/taxi-csv/"
        }
      }

      property_group {
        property_group_id = "sink.config.0"

        property_map = {
          "table.name"        = "taxi_trip_sink"
          "topic.name"        = "taxi-trip"
          "bootstrap.servers" = aws_msk_cluster.msk_data_cluster.bootstrap_brokers_sasl_iam
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type = "DEFAULT"
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        auto_scaling_enabled = true
        parallelism          = 1
        parallelism_per_kpu  = 1
      }
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.loader_ls[0].arn
  }

  # tags = local.tags
}

resource "aws_s3_object" "loader_package" {
  count = local.loader.to_create ? 1 : 0

  bucket = aws_s3_bucket.default_bucket.id
  key    = "package/${local.loader.package_name}"
  source = "${dirname(path.cwd)}/${local.loader.package_name}"

  etag = filemd5("${dirname(path.cwd)}/${local.loader.package_name}")
}

resource "aws_iam_role" "loader_app_role" {
  count = local.loader.to_create ? 1 : 0

  name = "${local.name}-loader-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "kinesisanalytics.amazonaws.com"
        }
      },
    ]
  })

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/CloudWatchFullAccess",
    "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
    "arn:aws:iam::aws:policy/AmazonKinesisAnalyticsFullAccess"
  ]

  inline_policy {
    name = "loader-msk-access"

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
        }
      ]
    })
  }

  inline_policy {
    name = "loader-vpc-access"
    # https://docs.aws.amazon.com/kinesisanalytics/latest/java/vpc-permissions.html

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Sid = "VPCReadOnlyPermissions"
          Action = [
            "ec2:DescribeVpcs",
            "ec2:DescribeSubnets",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeDhcpOptions"
          ]
          Effect   = "Allow"
          Resource = "*"
        },
        {
          Sid = "ENIReadWritePermissions"
          Action = [
            "ec2:CreateNetworkInterface",
            "ec2:CreateNetworkInterfacePermission",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DeleteNetworkInterface"
          ]
          Effect   = "Allow"
          Resource = "*"
        }

      ]
    })
  }

  inline_policy {
    name = "loader-s3-access"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Sid      = "ListObjectsInBucket"
          Action   = ["s3:ListBucket"]
          Effect   = "Allow"
          Resource = "arn:aws:s3:::${aws_s3_bucket.default_bucket.id}"
        },
        {
          Sid      = "AllObjectActions"
          Action   = ["s3:*Object"]
          Effect   = "Allow"
          Resource = "arn:aws:s3:::${aws_s3_bucket.default_bucket.id}/*"
        },
      ]
    })
  }

  tags = local.tags
}

resource "aws_security_group" "loader_sg" {
  count = local.loader.to_create ? 1 : 0

  name   = "${local.name}-loader-sg"
  vpc_id = module.vpc.vpc_id

  egress {
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }

  tags = local.tags
}

resource "aws_cloudwatch_log_group" "loader_lg" {
  count = local.loader.to_create ? 1 : 0

  name = "/${local.name}-loader-log-group"
}

resource "aws_cloudwatch_log_stream" "loader_ls" {
  count = local.loader.to_create ? 1 : 0

  name = "/${local.name}-loader-log-stream"

  log_group_name = aws_cloudwatch_log_group.loader_lg[0].name
}

resource "aws_s3_object" "loader_data" {
  count = local.loader.to_create ? 1 : 0

  bucket = aws_s3_bucket.default_bucket.id
  key    = "taxi-csv/${local.loader.data_file_name}"
  source = "${path.cwd}/data/${local.loader.data_file_name}"

  etag = filemd5("${path.cwd}/data/${local.loader.data_file_name}")
}
