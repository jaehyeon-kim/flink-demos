module "kafka_consumer" {
  source  = "terraform-aws-modules/lambda/aws"
  version = ">=5.1.0, <6.0.0"

  create = local.consumer.to_create

  function_name          = local.consumer.function_name
  handler                = local.consumer.handler
  runtime                = local.consumer.runtime
  timeout                = local.consumer.timeout
  memory_size            = local.consumer.memory_size
  source_path            = local.consumer.src_path
  vpc_subnet_ids         = module.vpc.private_subnets
  vpc_security_group_ids = local.consumer.to_create ? [aws_security_group.kafka_consumer[0].id] : null
  attach_network_policy  = true
  attach_policies        = true
  policies               = local.consumer.to_create ? [aws_iam_policy.kafka_consumer[0].arn] : null
  number_of_policies     = 1

  depends_on = [
    aws_msk_cluster.msk_data_cluster
  ]

  tags = local.tags
}

resource "aws_lambda_event_source_mapping" "kafka_consumer" {
  count             = local.consumer.to_create ? 1 : 0
  event_source_arn  = aws_msk_cluster.msk_data_cluster.arn
  function_name     = module.kafka_consumer.lambda_function_name
  topics            = [local.consumer.topic_name]
  starting_position = local.consumer.starting_position
  amazon_managed_kafka_event_source_config {
    consumer_group_id = "${local.consumer.topic_name}-group-01"
  }
}

resource "aws_lambda_permission" "kafka_consumer" {
  count         = local.consumer.to_create ? 1 : 0
  statement_id  = "InvokeLambdaFunction"
  action        = "lambda:InvokeFunction"
  function_name = local.consumer.function_name
  principal     = "kafka.amazonaws.com"
  source_arn    = aws_msk_cluster.msk_data_cluster.arn
}

resource "aws_iam_policy" "kafka_consumer" {
  count = local.consumer.to_create ? 1 : 0
  name  = "${local.consumer.function_name}-msk-lambda-permission"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid = "PermissionOnKafkaCluster"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeGroup",
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:ReadData",
          "kafka-cluster:DescribeClusterDynamicConfiguration"
        ]
        Effect = "Allow"
        Resource = [
          "arn:aws:kafka:${local.region}:${data.aws_caller_identity.current.account_id}:cluster/${local.name}-msk-cluster/*",
          "arn:aws:kafka:${local.region}:${data.aws_caller_identity.current.account_id}:topic/${local.name}-msk-cluster/*",
          "arn:aws:kafka:${local.region}:${data.aws_caller_identity.current.account_id}:group/${local.name}-msk-cluster/*"
        ]
      },
      {
        Sid = "PermissionOnKafka"
        Action = [
          "kafka:DescribeCluster",
          "kafka:GetBootstrapBrokers"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Sid = "PermissionOnNetwork"
        Action = [
          # The first three actions also exist in netwrok policy attachment in lambda module
          # "ec2:CreateNetworkInterface",
          # "ec2:DescribeNetworkInterfaces",
          # "ec2:DeleteNetworkInterface",
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_security_group" "kafka_consumer" {
  count = local.consumer.to_create ? 1 : 0

  name   = "${local.name}-lambda-consumer-sg"
  vpc_id = module.vpc.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }

  tags = local.tags
}

