resource "aws_opensearch_domain" "opensearch" {
  count = local.opensearch.to_create ? 1 : 0

  domain_name    = local.name
  engine_version = "OpenSearch_${local.opensearch.engine_version}"

  cluster_config {
    dedicated_master_enabled = false
    instance_type            = local.opensearch.instance_type  # m5.large.search
    instance_count           = local.opensearch.instance_count # 2
    zone_awareness_enabled   = true
  }

  advanced_security_options {
    enabled                        = false
    anonymous_auth_enabled         = true
    internal_user_database_enabled = true
  }

  domain_endpoint_options {
    enforce_https           = true
    tls_security_policy     = "Policy-Min-TLS-1-2-2019-07"
    custom_endpoint_enabled = false
  }

  ebs_options {
    ebs_enabled = true
    volume_size = 10
  }

  log_publishing_options {
    cloudwatch_log_group_arn = aws_cloudwatch_log_group.opensearch_log_group_index_slow_logs[0].arn
    log_type                 = "INDEX_SLOW_LOGS"
  }

  vpc_options {
    subnet_ids         = slice(module.vpc.private_subnets, 0, local.opensearch.instance_count)
    security_group_ids = [aws_security_group.opensearch[0].id]
  }

  access_policies = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "es:*",
        Principal = "*",
        Effect    = "Allow",
        Resource  = "arn:aws:es:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:domain/${local.name}/*"
      }
    ]
  })
}

resource "aws_security_group" "opensearch" {
  count = local.opensearch.to_create ? 1 : 0

  name   = "${local.name}-opensearch-sg"
  vpc_id = module.vpc.vpc_id

  lifecycle {
    create_before_destroy = true
  }

  tags = local.tags
}

resource "aws_security_group_rule" "opensearch_msk_inbound_https" {
  count                    = local.opensearch.to_create ? 1 : 0
  type                     = "ingress"
  description              = "Allow inbound traffic for OpenSearch Dashboard from MSK"
  security_group_id        = aws_security_group.opensearch[0].id
  protocol                 = "tcp"
  from_port                = 443
  to_port                  = 443
  source_security_group_id = aws_security_group.msk.id
}

resource "aws_security_group_rule" "opensearch_msk_inbound_rest" {
  count                    = local.opensearch.to_create ? 1 : 0
  type                     = "ingress"
  description              = "Allow inbound traffic for OpenSearch REST API from MSK"
  security_group_id        = aws_security_group.opensearch[0].id
  protocol                 = "tcp"
  from_port                = 9200
  to_port                  = 9200
  source_security_group_id = aws_security_group.msk.id
}

resource "aws_security_group_rule" "opensearch_vpn_inbound_https" {
  count                    = local.vpn.to_create && local.opensearch.to_create ? 1 : 0
  type                     = "ingress"
  description              = "Allow inbound traffic for OpenSearch Dashboard from VPN"
  security_group_id        = aws_security_group.opensearch[0].id
  protocol                 = "tcp"
  from_port                = 443
  to_port                  = 443
  source_security_group_id = aws_security_group.vpn[0].id
}

resource "aws_security_group_rule" "opensearch_vpn_inbound_rest" {
  count                    = local.vpn.to_create && local.opensearch.to_create ? 1 : 0
  type                     = "ingress"
  description              = "Allow inbound traffic for OpenSearch REST API from VPN"
  security_group_id        = aws_security_group.opensearch[0].id
  protocol                 = "tcp"
  from_port                = 9200
  to_port                  = 9200
  source_security_group_id = aws_security_group.vpn[0].id
}

resource "aws_cloudwatch_log_resource_policy" "opensearch_log_resource_policy" {
  count = local.opensearch.to_create ? 1 : 0

  policy_name = "${local.name}-domain-log-resource-policy"

  policy_document = jsonencode(({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "es.amazonaws.com"
        },
        Action = [
          "logs:PutLogEvents",
          "logs:PutLogEventsBatch",
          "logs:CreateLogStream"
        ],
        Resource = [
          "${aws_cloudwatch_log_group.opensearch_log_group_index_slow_logs[0].arn}:*"
        ]
      }
    ]
  }))
}

resource "aws_cloudwatch_log_group" "opensearch_log_group_index_slow_logs" {
  count = local.opensearch.to_create ? 1 : 0

  name = "/aws/opensearch/${local.name}/index-slow"

  retention_in_days = 1

  tags = local.tags
}

resource "aws_iam_service_linked_role" "opensearch" {
  count = data.aws_iam_role.opensearch_service_linked_role.id != "" ? 0 : 1

  aws_service_name = "opensearchservice.amazonaws.com"
  description      = "Allows Amazon OpenSearch to manage AWS resources for a domain on your behalf."
}
