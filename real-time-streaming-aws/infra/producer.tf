module "eventbridge" {
  source  = "terraform-aws-modules/eventbridge/aws"
  version = ">=2.3.0, <3.0.0"

  create     = local.producer.to_create
  create_bus = false

  rules = {
    crons = {
      description         = "Kafka producer lambda schedule"
      schedule_expression = local.producer.schedule_rate
    }
  }

  targets = {
    crons = [for i in range(local.producer.concurrency) : {
      name = "lambda-target-${i}"
      arn  = module.kafka_producer[0].lambda_function_arn
    }]
  }

  depends_on = [
    module.kafka_producer
  ]

  tags = local.tags
}

module "kafka_producer" {
  source  = "terraform-aws-modules/lambda/aws"
  version = ">=5.1.0, <6.0.0"

  create = local.producer.to_create

  function_name          = local.producer.function_name
  handler                = local.producer.handler
  runtime                = local.producer.runtime
  timeout                = local.producer.timeout
  memory_size            = local.producer.memory_size
  source_path            = local.producer.src_path
  vpc_subnet_ids         = module.vpc.private_subnets
  vpc_security_group_ids = [aws_security_group.kafka_producer[0].id]
  attach_network_policy  = true
  attach_policies        = true
  policies               = [aws_iam_policy.producer_msk_permission[0].arn]
  number_of_policies     = 1
  environment_variables = {
    BOOTSTRAP_SERVERS = aws_msk_cluster.msk_data_cluster.bootstrap_brokers_sasl_iam
    TOPIC_NAME        = local.producer.environment.topic_name
    MAX_RUN_SEC       = local.producer.environment.max_run_sec
  }

  tags = local.tags
}

resource "aws_lambda_function_event_invoke_config" "kafka_producer" {
  count = local.producer.to_create ? 1 : 0

  function_name          = module.kafka_producer[0].lambda_function_name
  maximum_retry_attempts = 0
}

resource "aws_lambda_permission" "allow_eventbridge" {
  count         = local.producer.to_create && local.producer.to_enable_trigger ? 1 : 0
  statement_id  = "InvokeLambdaFunction"
  action        = "lambda:InvokeFunction"
  function_name = local.producer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = module.eventbridge.eventbridge_rule_arns["crons"]

  depends_on = [
    module.eventbridge
  ]
}

resource "aws_iam_policy" "kafka_producer" {
  count = local.producer.to_create ? 1 : 0
  name  = "${local.producer.function_name}-msk-lambda-permission"

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

resource "aws_security_group" "kafka_producer" {
  count = local.producer.to_create ? 1 : 0

  name   = "${local.name}-lambda-sg"
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

