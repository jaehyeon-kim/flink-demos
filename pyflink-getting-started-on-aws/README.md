# Getting Started with Pyflink on AWS

A PyFlink Table API application that reads records from one Kafka topic, aggregates them in a tumbling window and writes the result to another topic. The folder holds the same application twice: `local/` runs it against a Kafka cluster and a Flink cluster on Docker, and `remote/` runs it against Amazon MSK, first from a Flink cluster on Docker and then from Amazon Managed Service for Apache Flink.

## Stack

- Apache Flink 1.15.2 through PyFlink (`apache-flink==1.15.2` in `requirements-dev.txt`).
- `local/` builds the image `pyflink:1.15.2-scala_2.12` from `flink:1.15.2-scala_2.12` with Python 3.8.10 and PyFlink added, and takes `flink-sql-connector-kafka-1.15.2` as its pipeline jar.
- `remote/` builds the same image name from `flink:1.15.4-scala_2.12`, and Maven builds the uber jar `pyflink-getting-started-1.0.0.jar` from `remote/package/uber-jar-for-pyflink` so that the application can use MSK IAM authentication.
- Kafka 2.8.1 on Docker (`bitnami/kafka:2.8.1`, `bitnami/zookeeper:3.5`) and Kpow Community Edition (`factorhouse/kpow-ce:91.2.1`) as the Kafka user interface.
- `kafka-python==2.0.2` for the record producer.
- Amazon MSK 2.8.1, two `kafka.m5.large` brokers with 20 GB EBS each, and a SoftEther VPN server on EC2, created with Terraform under `remote/infra`.
- Amazon Managed Service for Apache Flink on runtime `FLINK-1_15`, also under `remote/infra`.

## How to run

### Local, on Docker

`build.sh` recreates the ignored `package/` folder: it downloads the Kafka connector jar into `package/lib`, installs the pip packages into `package/site_packages`, and zips `kda-package.zip`.

```bash
cd local

./build.sh
docker build -t pyflink:1.15.2-scala_2.12 .

docker-compose -f compose-kafka.yml up -d
docker-compose -f compose-flink.yml up -d

python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt

# send records in one terminal
python producer.py

# run the application in another terminal, either directly
RUNTIME_ENV=LOCAL python processor.py

# or on the Flink cluster
docker exec jobmanager /opt/flink/bin/flink run \
  --python /etc/flink/processor.py \
  --jarfile /etc/flink/package/lib/flink-sql-connector-kafka-1.15.2.jar \
  --pyFiles /etc/flink/package/site_packages/ \
  -d
```

The Flink web interface is on `localhost:8081` and Kpow on `localhost:3000`. Stop everything with `docker-compose -f compose-flink.yml down -v` and `docker-compose -f compose-kafka.yml down -v`.

### Remote, on AWS

`build.sh` here runs Maven to build the uber jar, installs the pip packages and zips `kda-package.zip`, which Terraform uploads to the S3 bucket.

```bash
cd remote

./build.sh
docker build -t pyflink:1.15.2-scala_2.12 .

cd infra
terraform init
terraform plan
terraform apply -auto-approve=true
```

`kda.to_create` in `remote/infra/variables.tf` is `false` as checked in, so the first apply creates the VPC, the VPN server, the MSK cluster and the S3 bucket only. That is the part 2 setup: connect to the VPN, then run the producer and a local Flink cluster against MSK.

```bash
export BOOTSTRAP_SERVERS=<msk_bootstrap_brokers_sasl_iam>
python producer.py
docker-compose -f compose-flink.yml up -d
docker-compose -f compose-ui.yml up -d

docker exec jobmanager /opt/flink/bin/flink run \
  --python /etc/flink/processor.py \
  --jarfile /etc/flink/package/lib/pyflink-getting-started-1.0.0.jar \
  --pyFiles /etc/flink/package/site_packages/ \
  -d
```

For part 3, set `kda.to_create` to `true` and apply again. Terraform then uploads `kda-package.zip` and creates the Managed Service for Apache Flink application that runs the same `processor.py`.

These Terraform files create paid AWS resources: an MSK cluster of two `kafka.m5.large` brokers with 20 GB EBS each, a Managed Service for Apache Flink application, an S3 bucket, NAT gateways and an EC2 instance for the VPN server. The MSK cluster, the NAT gateways and the Managed Flink application charge by the hour whether or not any data flows. Destroy everything as soon as you are finished:

```bash
cd remote/infra
terraform destroy -auto-approve=true
```

## Posts

- [Getting Started with Pyflink on AWS - Part 1 Local Flink and Local Kafka](https://jaehyeon.me/blog/2023-08-17-getting-started-with-pyflink-on-aws-part-1/) covers `local/`.
- [Getting Started with Pyflink on AWS - Part 2 Local Flink and MSK](https://jaehyeon.me/blog/2023-08-28-getting-started-with-pyflink-on-aws-part-2/) covers `remote/` with the Flink cluster on Docker.
- [Getting Started with Pyflink on AWS - Part 3 AWS Managed Flink and MSK](https://jaehyeon.me/blog/2023-09-04-getting-started-with-pyflink-on-aws-part-3/) covers `remote/` with the application on Managed Service for Apache Flink.

## Back to the repository

[flink-demos](../README.md)

## Bitnami images

Bitnami's public Docker images have been moved to the [Bitnami Legacy](https://hub.docker.com/u/bitnamilegacy) repository. Update the image references in `local/compose-kafka.yml` accordingly:

- `bitnami/kafka:2.8.1` becomes `bitnamilegacy/kafka:2.8.1`
- `bitnami/zookeeper:3.5` becomes `bitnamilegacy/zookeeper:3.5`
