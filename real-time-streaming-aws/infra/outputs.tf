# VPC
output "vpc_id" {
  description = "The ID of the VPC"
  value       = module.vpc.vpc_id
}

output "vpc_cidr_block" {
  description = "The CIDR block of the VPC"
  value       = module.vpc.vpc_cidr_block
}

output "private_subnets" {
  description = "List of IDs of private subnets"
  value       = module.vpc.private_subnets
}

output "public_subnets" {
  description = "List of IDs of public subnets"
  value       = module.vpc.public_subnets
}

output "nat_public_ips" {
  description = "List of public Elastic IPs created for AWS NAT Gateway"
  value       = module.vpc.nat_public_ips
}

output "azs" {
  description = "A list of availability zones specified as argument to this module"
  value       = module.vpc.azs
}

# Default bucket
output "default_bucket_name" {
  description = "Default bucket name"
  value       = aws_s3_bucket.default_bucket.id
}

# VPN
output "vpn_launch_template_arn" {
  description = "The ARN of the VPN launch template"
  value = {
    for k, v in module.vpn : k => v.launch_template_arn
  }
}

output "vpn_autoscaling_group_id" {
  description = "VPN autoscaling group id"
  value = {
    for k, v in module.vpn : k => v.autoscaling_group_id
  }
}

output "vpn_autoscaling_group_name" {
  description = "VPN autoscaling group name"
  value = {
    for k, v in module.vpn : k => v.autoscaling_group_name
  }
}

output "vpn_secret_id" {
  description = "VPN secret ID"
  value       = local.vpn.to_create ? aws_secretsmanager_secret.vpn_secrets[0].id : null
}

output "vpn_secret_version" {
  description = "VPN secret version ID"
  value       = local.vpn.to_create ? aws_secretsmanager_secret_version.vpn_secrets[0].version_id : null
}

# MSK
output "msk_arn" {
  description = "Amazon Resource Name (ARN) of the MSK cluster"
  value       = aws_msk_cluster.msk_data_cluster.arn
}

output "msk_bootstrap_brokers_sasl_iam" {
  description = "One or more DNS names (or IP addresses) and SASL IAM port pairs"
  value       = aws_msk_cluster.msk_data_cluster.bootstrap_brokers_sasl_iam
}

# Lambda Kafka producer
output "kafka_lambda_arn" {
  description = "Lambda Kafka producer ARN"
  value       = local.producer.to_create ? module.kafka_producer.lambda_function_arn : null
}

