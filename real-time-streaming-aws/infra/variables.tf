variable "vpn_to_create" {
  description = "Flag to indicate whether to create VPN"
  type        = bool
  default     = true
}

variable "vpn_to_use_spot" {
  description = "Flag to indicate whether to use a spot instance for VPN"
  type        = bool
  default     = false
}

variable "vpn_to_limit_vpn_ingress" {
  description = "Flag to indicate whether to limit ingress from the current machine's IP address"
  type        = bool
  default     = true
}

locals {
  name        = "real-time-streaming"
  region      = data.aws_region.current.name
  environment = "dev"

  vpc = {
    cidr = "10.0.0.0/16"
    azs  = slice(data.aws_availability_zones.available.names, 0, 3)
  }

  default_bucket = {
    name       = "${local.name}-${data.aws_caller_identity.current.account_id}-${local.region}"
    to_set_acl = false
  }

  vpn = {
    to_create    = var.vpn_to_create
    to_use_spot  = var.vpn_to_use_spot
    ingress_cidr = var.vpn_to_limit_vpn_ingress ? "${data.http.local_ip_address.response_body}/32" : "0.0.0.0/0"
    spot_override = [
      { instance_type : "t3.small" },
      { instance_type : "t3a.small" },
    ]
  }

  msk = {
    version                    = "2.8.1"
    instance_size              = "kafka.m5.large"
    ebs_volume_size            = 20
    log_retention_ms           = 604800000 # 7 days
    number_of_broker_nodes     = 2
    num_partitions             = 2
    default_replication_factor = 2
  }

  producer = {
    to_create         = false
    src_path          = "src"
    function_name     = "kafka_producer"
    handler           = "app.lambda_function"
    concurrency       = 5
    timeout           = 90
    memory_size       = 128
    runtime           = "python3.8"
    schedule_rate     = "rate(1 minute)"
    to_enable_trigger = false
    environment = {
      topic_name  = "orders"
      max_run_sec = 60
    }
  }

  kda = {
    to_create    = false
    runtime_env  = "FLINK-1_15"
    package_name = "kda-package.zip"
  }

  tags = {
    Name        = local.name
    Environment = local.environment
  }
}
