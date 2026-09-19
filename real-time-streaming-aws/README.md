# Real Time Streaming with Kafka and Flink

A six-lab project that builds a taxi ride streaming pipeline on AWS. A Lambda function produces ride records into Kafka, three PyFlink applications move and transform them between S3, Kafka, Athena and OpenSearch, Kafka Connect writes them to DynamoDB, and a second Lambda function consumes them.

## Labs and what runs each one

| Lab | Post | Source in this folder |
|---|---|---|
| Lab 1 | Produce data to Kafka using Lambda | `producer/app.py`, `infra/producer.tf` |
| Lab 2 | Write data to Kafka from S3 using Flink | `loader/processor.py`, jar `package/lab2-pipeline` |
| Lab 3 | Transform and write data to S3 from Kafka using Flink | `exporter/processor.py` and `exporter/athena.sql`, jar `package/lab3-pipeline` |
| Lab 4 | Clean, aggregate and enrich events with Flink | `forwarder/processor.py`, jar `package/lab4-pipeline`, OpenSearch from `infra/opensearch.tf` |
| Lab 5 | Write data to DynamoDB using Kafka Connect | `configs/sink.json`, `configs/ddb.json`, `infra/msk-connect.tf` |
| Lab 6 | Consume data from Kafka using Lambda | `consumer/app.py`, `infra/consumer.tf` |

The folder names do not follow the lab order. `loader` loads S3 data into Kafka in lab 2, `exporter` exports Kafka data to S3 in lab 3, and `forwarder` forwards aggregated Kafka data to OpenSearch in lab 4. Each application names its own jar, so `loader/application_properties.json` points at `lab2-pipeline-1.0.0.jar`, `exporter` at `lab3-pipeline-1.0.0.jar` and `forwarder` at `lab4-pipeline-1.0.0.jar`.

## Stack

- Apache Flink 1.17.1 through PyFlink (`apache-flink==1.17.1` in `requirements-dev.txt`). The `Dockerfile` builds the image `real-time-streaming-aws:1.17.1` from `flink:1.17.1` with Python 3.8.10, the `s3-fs-hadoop` plugin and `kafka-clients-3.2.3` added.
- Maven builds three pipeline jars from `package/lab2-pipeline`, `package/lab3-pipeline` and `package/lab4-pipeline`. They carry the Kafka, S3 and OpenSearch connectors together with the MSK IAM authentication library.
- Kafka 2.8.1 on Docker (`bitnami/kafka:2.8.1`, `bitnami/zookeeper:3.5`) and Kpow Community Edition (`factorhouse/kpow-ce:91.5.1`) as the Kafka user interface.
- OpenSearch 2.7.0 and OpenSearch Dashboards 2.7.0 on Docker for the local variant of lab 4.
- Camel DynamoDB sink connector 3.20.3 for lab 5, downloaded by `download.sh`.
- A fork of `kafka-python` with IAM authentication support, pinned by commit in `producer/requirements.txt`.
- AWS resources created with Terraform under `infra/`: a VPC with three public and three private subnets, a SoftEther VPN server on EC2, an MSK cluster on Kafka 2.8.1 with two `kafka.m5.large` brokers and 20 GB EBS each, MSK Connect, an OpenSearch domain of two `m5.large.search` nodes on engine 2.7, an S3 bucket, and Python 3.8 Lambda functions for the producer and the consumer.

## How to run

### Build the artefacts

Both scripts recreate folders that are not in git.

```bash
# three pipeline jars into package/lib
./build.sh

# the Camel DynamoDB sink connector into infra/connectors
./download.sh

docker build -t=real-time-streaming-aws:1.17.1 .
```

### Create the AWS resources

Everything except the VPC, the VPN server, the MSK cluster and the S3 bucket is behind a flag that defaults to `false`, so turn on only what the lab you are following needs.

```bash
cd infra
terraform init
terraform plan

# lab 1, lab 2 and lab 3
terraform apply -auto-approve=true -var 'producer_to_create=true'

# lab 4 adds the OpenSearch domain
terraform apply -auto-approve=true -var 'producer_to_create=true' -var 'opensearch_to_create=true'

# lab 5 adds the MSK Connect connector
terraform apply -auto-approve=true -var 'producer_to_create=true' -var 'connect_to_create=true'

# lab 6 adds the consumer Lambda function
terraform apply -auto-approve=true -var 'producer_to_create=true' -var 'consumer_to_create=true'
```

Terraform also uploads `data/taxi-trips.csv` to `s3://<bucket>/taxi-csv/`, which is the source that the lab 2 application reads.

### Run the Flink applications

`compose-msk.yml` starts a Flink cluster that talks to the MSK cluster, and `compose-local-kafka.yml` starts a Flink cluster with its own Kafka broker so that no AWS Kafka is needed. `compose-extra.yml` adds a local OpenSearch cluster and a Kafka Connect worker. `compose-ui.yml` starts Kpow on its own. Connect to the VPN before using `compose-msk.yml`, because the MSK cluster sits in private subnets.

```bash
export AWS_ACCESS_KEY_ID=<aws-access-key-id>
export AWS_SECRET_ACCESS_KEY=<aws-secret-access-key>
export BOOTSTRAP_SERVERS=<msk_bootstrap_brokers_sasl_iam>
export OPENSEARCH_HOSTS=<opensearch-hosts>

# against MSK
docker-compose -f compose-msk.yml up -d

# or entirely locally, with the producer started in another terminal
docker-compose -f compose-local-kafka.yml up -d
docker-compose -f compose-extra.yml up -d
python producer/app.py

# lab 2
docker exec jobmanager /opt/flink/bin/flink run \
    --python /etc/flink/loader/processor.py \
    --jarfile /etc/flink/package/lib/lab2-pipeline-1.0.0.jar \
    -d

# lab 3
docker exec jobmanager /opt/flink/bin/flink run \
    --python /etc/flink/exporter/processor.py \
    --jarfile /etc/flink/package/lib/lab3-pipeline-1.0.0.jar \
    -d

# lab 4
docker exec jobmanager /opt/flink/bin/flink run \
    --python /etc/flink/forwarder/processor.py \
    --jarfile /etc/flink/package/lib/lab4-pipeline-1.0.0.jar \
    -d
```

Each application also runs outside the cluster, for example `RUNTIME_ENV=LOCAL BOOTSTRAP_SERVERS=localhost:29092 python loader/processor.py`. The Flink web interface is on `localhost:8081`, Kpow on `localhost:3000` and OpenSearch Dashboards on `localhost:5601`.

### Lab 5, the DynamoDB sink

On AWS, `connect_to_create=true` creates the MSK Connect connector, and `infra/msk-connect.tf` creates the `real-time-streaming-taxi-rides` DynamoDB table whether or not that flag is set. Locally, the same connector runs on the Kafka Connect worker in `compose-extra.yml` and writes to a table you create yourself with the same name.

```bash
aws dynamodb create-table --cli-input-json file://configs/ddb.json

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @configs/sink.json

curl http://localhost:8083/connectors/real-time-streaming-taxi-rides-sink/status
```

### Tear down

The Terraform files create paid AWS resources: an MSK cluster of two `kafka.m5.large` brokers with 20 GB EBS each, an OpenSearch domain of two `m5.large.search` nodes, an MSK Connect connector, two Lambda functions, an S3 bucket, NAT gateways and an EC2 instance for the VPN server. The MSK cluster, the OpenSearch domain and the NAT gateways charge by the hour whether or not any data flows, and the producer Lambda function is on a one-minute schedule. Destroy everything as soon as you are finished, repeating the same variables you applied with.

```bash
cd infra
terraform destroy -auto-approve=true -var 'producer_to_create=true' -var 'opensearch_to_create=true' -var 'connect_to_create=true' -var 'consumer_to_create=true'
```

If you ran lab 5 locally rather than through Terraform, delete the connector and the table yourself:

```bash
curl -X DELETE http://localhost:8083/connectors/real-time-streaming-taxi-rides-sink
aws dynamodb delete-table --table-name real-time-streaming-taxi-rides
```

Stop the containers with `docker-compose -f <file> down -v` for each compose file you started.

## Posts

- [Real Time Streaming with Kafka and Flink - Introduction](https://jaehyeon.me/blog/2023-10-05-real-time-streaming-with-kafka-and-flink-1/)
- [Real Time Streaming with Kafka and Flink - Lab 1 Produce data to Kafka using Lambda](https://jaehyeon.me/blog/2023-10-26-real-time-streaming-with-kafka-and-flink-2/)
- [Real Time Streaming with Kafka and Flink - Lab 2 Write data to Kafka from S3 using Flink](https://jaehyeon.me/blog/2023-11-09-real-time-streaming-with-kafka-and-flink-3/)
- [Real Time Streaming with Kafka and Flink - Lab 3 Transform and write data to S3 from Kafka using Flink](https://jaehyeon.me/blog/2023-11-16-real-time-streaming-with-kafka-and-flink-4/)
- [Real Time Streaming with Kafka and Flink - Lab 4 Clean, Aggregate, and Enrich Events with Flink](https://jaehyeon.me/blog/2023-11-23-real-time-streaming-with-kafka-and-flink-5/)
- [Real Time Streaming with Kafka and Flink - Lab 5 Write data to DynamoDB using Kafka Connect](https://jaehyeon.me/blog/2023-11-30-real-time-streaming-with-kafka-and-flink-6/)
- [Real Time Streaming with Kafka and Flink - Lab 6 Consume data from Kafka using Lambda](https://jaehyeon.me/blog/2023-12-14-real-time-streaming-with-kafka-and-flink-7/)

## Back to the repository

[flink-demos](../README.md)

## Bitnami images

Bitnami's public Docker images have been moved to the [Bitnami Legacy](https://hub.docker.com/u/bitnamilegacy) repository. Update the image references in `compose-local-kafka.yml` and `compose-extra.yml` accordingly:

- `bitnami/kafka:2.8.1` becomes `bitnamilegacy/kafka:2.8.1`
- `bitnami/zookeeper:3.5` becomes `bitnamilegacy/zookeeper:3.5`
