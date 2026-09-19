# Kafka, Flink and DynamoDB for Real Time Fraud Detection

A PyFlink application that reads transaction and flagged account records from Kafka, filters the transactions that belong to a flagged account, and writes the flagged transactions to a DynamoDB table through the Camel DynamoDB sink connector. The folder holds the same application twice: `local/` runs it against a Kafka cluster on Docker, and `remote/` deploys it to Amazon Managed Service for Apache Flink against an MSK cluster.

## Stack

- Apache Flink 1.15.2 through PyFlink (`apache-flink==1.15.2` in `requirements-dev.txt`), with `flink-sql-connector-kafka-1.15.2` as the pipeline jar locally.
- Kafka 2.8.1 on Docker (`bitnami/kafka:2.8.1`, `bitnami/zookeeper:3.5`) and Kpow Community Edition (`factorhouse/kpow-ce:91.2.1`) as the Kafka user interface.
- Kafka Connect in distributed mode running the Camel DynamoDB sink connector 3.20.3.
- `kafka-python==2.0.2` for the record producer.
- Amazon Managed Service for Apache Flink on runtime `FLINK-1_15`, created with Terraform under `remote/infra`.
- Amazon MSK 2.8.1, two `kafka.m5.large` brokers with 20 GB EBS each, MSK Connect, DynamoDB, S3 and a SoftEther VPN server on EC2, also under `remote/infra`.
- Maven builds the uber jar `pyflink-getting-started-1.0.0.jar` from `remote/package/uber-jar-for-pyflink` for the MSK IAM authentication path.

## How to run

### Local, on Docker

`build.sh` recreates the two ignored artefacts: `package/` holds the Kafka connector jar and the pip packages the Flink application needs, and `connectors/` holds the unpacked Camel DynamoDB sink connector that Kafka Connect mounts.

```bash
cd local

./build.sh

docker-compose up -d

python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt

# create the sink table, then the connector
aws dynamodb create-table --cli-input-json file://configs/ddb.json
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @configs/sink.json

# send records, one command per terminal
DATE_TYPE=account python producer.py
DATE_TYPE=transaction python producer.py

# run the Flink application
RUNTIME_ENV=LOCAL python processor.py
```

Kpow is on `localhost:3000` and the Kafka Connect REST API on `localhost:8083`. The sink is a real DynamoDB table in your AWS account, so the local path is not free either. Stop the containers with `docker-compose down -v`, and delete the table with `aws dynamodb delete-table --table-name flagged-transactions`.

### Remote, on AWS

`build.sh` here builds the uber jar with Maven and packages `kda-package.zip`, which Terraform uploads to S3 and Managed Flink runs.

```bash
cd remote

./build.sh

cd infra
terraform init
terraform plan
terraform apply -auto-approve=true
```

The MSK cluster sits in private subnets, so the producer on your machine reaches it through the SoftEther VPN server that `vpn.tf` creates on EC2. Connect to the VPN first, then run the producer and the Kpow container. The bootstrap server addresses come from the `msk_bootstrap_brokers_sasl_iam` Terraform output.

```bash
export BOOTSTRAP_SERVERS=<msk_bootstrap_brokers_sasl_iam>
python producer.py
docker-compose up -d
```

These Terraform files create paid AWS resources: an MSK cluster of two `kafka.m5.large` brokers with 20 GB EBS each, an MSK Connect connector, a Managed Service for Apache Flink application, a DynamoDB table, an S3 bucket, NAT gateways and an EC2 instance for the VPN server. The MSK cluster, the NAT gateways and the Managed Flink application charge by the hour whether or not any data flows. Destroy everything as soon as you are finished:

```bash
cd remote/infra
terraform destroy -auto-approve=true
```

## Posts

- [Kafka, Flink and DynamoDB for Real Time Fraud Detection - Part 1 Local Development](https://jaehyeon.me/blog/2023-08-10-fraud-detection-part-1/) covers `local/`.
- [Kafka, Flink and DynamoDB for Real Time Fraud Detection - Part 2 Deployment via AWS Managed Flink](https://jaehyeon.me/blog/2023-09-14-fraud-detection-part-2/) covers `remote/`.

## Back to the repository

[flink-demos](../README.md)

## Bitnami images

Bitnami's public Docker images have been moved to the [Bitnami Legacy](https://hub.docker.com/u/bitnamilegacy) repository. Update the image references in `local/docker-compose.yml` and `local/compose-connect.yml` accordingly:

- `bitnami/kafka:2.8.1` becomes `bitnamilegacy/kafka:2.8.1`
- `bitnami/zookeeper:3.5` becomes `bitnamilegacy/zookeeper:3.5`
