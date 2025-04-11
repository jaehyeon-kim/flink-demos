#!/bin/bash

set -e

function show_usage() {
    cat <<EOF
    usage: $0 <command>
    commands:
      fetch                             fetch docker images
      <cluster-name> start              start a Datorios flink cluster named <cluster-name>
      <cluster-name> stop               stop a Datorios flink cluster named <cluster-name>
      list                              list all Datorios flink clusters
      <cluster-name> flink <params>     run flink CLI command on cluster <cluster-name>
EOF
}

function cmd_v() {
  echo "$@"
  eval "$@"
}

function get_param_from_env() {
  KEY=$1
  VALUE=$(grep "${KEY}="  ./.env | cut -d'=' -f 2 | xargs)
  echo "${VALUE}"
}

function do_fetch() {
  # Read new ACCESS and SECRET
  DATORIOS_ACCESS_KEY_ID=$(get_param_from_env AWS_ACCESS_KEY_ID)
  DATORIOS_SECRET_ACCESS_KEY=$(get_param_from_env AWS_SECRET_ACCESS_KEY)

  # Set ECR according to latency check
  check_latency
  DATORIOS_REGION=${BEST_REGION}
  ECR_REGISTRY=${ECR_ACCOUNT}.dkr.ecr.${BEST_REGION}.amazonaws.com

  # Get Token from AWS and login to ECR registry
  AWS_ACCESS_KEY_ID=${DATORIOS_ACCESS_KEY_ID}
  AWS_SECRET_ACCESS_KEY=${DATORIOS_SECRET_ACCESS_KEY}
  AWS_REGION=${BEST_REGION}
  token=$(aws ecr get-authorization-token --registry-ids ${ECR_ACCOUNT} | jq ".authorizationData[0].authorizationToken" | tr -d "\"" | base64 --decode | cut -d ":" -f 2)
  token=$(curl -s --url "https://api.ecr.${DATORIOS_REGION}.amazonaws.com" --aws-sigv4 "aws:amz:${DATORIOS_REGION}:ecr" --user "${DATORIOS_ACCESS_KEY_ID}:${DATORIOS_SECRET_ACCESS_KEY}" -H "X-Amz-Target: AmazonEC2ContainerRegistry_V20150921.GetAuthorizationToken" -H "Content-Type: application/x-amz-json-1.1" -H "User-Agent: Boto3/1.26.32 Python/3.10.12 Linux/6.5.0-17-generic Botocore/1.29.32" -d "{}"| jq ".authorizationData[0].authorizationToken" | tr -d "\"" | base64 --decode | cut -d ":" -f 2)
  echo "${token}" | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

  # Fetch images to localhost
  cmd_v docker pull "${ECR_REGISTRY}"/metro-flink:"${FLINK_VERSION}"
  cmd_v docker pull "${ECR_REGISTRY}"/metro-flink-runner:"${FLINK_VERSION}"
  cmd_v docker pull "${ECR_REGISTRY}"/metro-fluent-bit:"${FLUENTBIT_VERSION}"

  cmd_v docker tag "${ECR_REGISTRY}"/metro-flink:"${FLINK_VERSION}" localhost/metro-flink:"${FLINK_BASE_VERSION}"
  cmd_v docker tag "${ECR_REGISTRY}"/metro-flink-runner:"${FLINK_VERSION}" localhost/metro-flink-runner:"${FLINK_BASE_VERSION}"
  cmd_v docker tag "${ECR_REGISTRY}"/metro-fluent-bit:"${FLUENTBIT_VERSION}" localhost/metro-fluent-bit:"${FLUENTBIT_BASE_VERSION}"

  # Logout from ECR registry
  cmd_v docker logout "${ECR_REGISTRY}"
}


function do_start() {
  CLUSTER_NAME=$1

  if [ -z "${CLUSTER_NAME}" ]; then
    echo "Cluster name missing"
    show_usage
    exit 1
  fi

  export CLUSTER_NAME
  cmd_v docker compose -p datorios-"${CLUSTER_NAME}" up --scale taskmanager="${CLUSTER_TASK_MANAGER_SCALE}" -d
}

function do_stop() {
  CLUSTER_NAME=$1

  if [ -z "${CLUSTER_NAME}" ]; then
    echo "Cluster name missing"
    show_usage
    exit 1
  fi

  export CLUSTER_NAME
  cmd_v docker compose -p datorios-"${CLUSTER_NAME}" down
}

function do_list() {
  cmd_v docker compose ls -a --filter name=datorios-
}

function do_flink() {
  CLUSTER_NAME=$1

  shift

  if [ -z "${CLUSTER_NAME}" ]; then
    echo "Cluster ID missing"
    show_usage
    exit 1
  fi

  export CLUSTER_NAME
  cmd_v docker exec "datorios-${CLUSTER_NAME}-runner" /flink.sh "$@"
}

function check_latency() {
  REGIONS=("eu-west-1" "us-east-2")
  BEST_REGION="eu-west-1"
  MIN_PING=10000
  for region in ${REGIONS[@]}
  do
    PING_RESULT=$(ping  -c1 ec2.${region}.amazonaws.com | grep -o "time=.*" | cut -d= -f2 | awk '{print$1}' | cut -d. -f1)
    if [ "$PING_RESULT" -lt "$MIN_PING" ]
    then
      MIN_PING=$PING_RESULT
      BEST_REGION=$region
    fi
  done
}

function dispatch_cmd() {
  ECR_ACCOUNT=$(get_param_from_env ECR_ACCOUNT)
  FLINK_BASE_VERSION="1.17.2"
  FLUENTBIT_BASE_VERSION="2.2.2"
  FLINK_VERSION=${FLINK_BASE_VERSION}-0.2.6-c9137899
  FLUENTBIT_VERSION=${FLUENTBIT_BASE_VERSION}-0.2.6-eeaa494

  ROOT=$(dirname $(readlink -f "$0"))

  if [ ! -f ".env" ]; then
    echo ".env file does no exist, exiting"
    exit 1
  fi

  set -a && source .env && set +a

  local cmd=$1
  if [ -z "$cmd" ]; then
      show_usage
      exit 0
  fi

  shift

  case $cmd in
  fetch)
    do_fetch "$@"
    ;;
  list)
    do_list "$@"
    ;;
  *)
    local subcmd=$1
    if [ -z "subcmd" ]; then
        show_usage
        exit 0
    fi

    shift

    case $subcmd in
    start)
      do_start "$cmd" "$@"
      ;;
    stop)
      do_stop "$cmd" "$@"
      ;;
    flink)
      do_flink "$cmd" "$@"
      ;;
    *)
      show_usage
      ;;
    esac
  esac
}

dispatch_cmd "$@"