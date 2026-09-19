# Flink Demos

Apache Flink examples in PyFlink and Kotlin: local Docker clusters, AWS Managed Flink deployments with Kafka, a Flink SQL cookbook setup, and worked exercises from Confluent courses and the Stream Processing with Apache Flink book.

## Projects

| Folder | What it demonstrates | Posts |
|---|---|---|
| [building-pyflink-apps](./building-pyflink-apps) | Confluent's Building Apache Flink Applications in Java course reworked in PyFlink, on a Kafka and Flink cluster running on Docker. | [Building Apache Flink Applications in Python](https://jaehyeon.me/blog/2023-10-19-build-pyflink-apps/) |
| [flink-sql-cookbook](./flink-sql-cookbook) | A Flink 1.20.1 cluster with the Flink SQL Faker connector added, so that the queries in Ververica's Flink SQL Cookbook run from the SQL client. | [Run Flink SQL Cookbook in Docker](https://jaehyeon.me/blog/2025-04-15-sql-cookbook/) |
| [fraud-detection](./fraud-detection) | A PyFlink application that filters transactions from flagged accounts and writes them to DynamoDB, with `local/` on Docker and `remote/` on MSK and Managed Flink. | [Part 1](https://jaehyeon.me/blog/2023-08-10-fraud-detection-part-1/), [Part 2](https://jaehyeon.me/blog/2023-09-14-fraud-detection-part-2/) |
| [pyflink-getting-started-on-aws](./pyflink-getting-started-on-aws) | A PyFlink Table API windowed aggregation between two Kafka topics, with `local/` on Docker and `remote/` on MSK and Managed Flink. | [Part 1](https://jaehyeon.me/blog/2023-08-17-getting-started-with-pyflink-on-aws-part-1/), [Part 2](https://jaehyeon.me/blog/2023-08-28-getting-started-with-pyflink-on-aws-part-2/), [Part 3](https://jaehyeon.me/blog/2023-09-04-getting-started-with-pyflink-on-aws-part-3/) |
| [real-time-streaming-aws](./real-time-streaming-aws) | A six-lab taxi ride pipeline on AWS: Lambda producer and consumer, three PyFlink applications moving data between S3, Kafka, Athena and OpenSearch, and Kafka Connect writing to DynamoDB. | [Introduction](https://jaehyeon.me/blog/2023-10-05-real-time-streaming-with-kafka-and-flink-1/), [Lab 1](https://jaehyeon.me/blog/2023-10-26-real-time-streaming-with-kafka-and-flink-2/), [Lab 2](https://jaehyeon.me/blog/2023-11-09-real-time-streaming-with-kafka-and-flink-3/), [Lab 3](https://jaehyeon.me/blog/2023-11-16-real-time-streaming-with-kafka-and-flink-4/), [Lab 4](https://jaehyeon.me/blog/2023-11-23-real-time-streaming-with-kafka-and-flink-5/), [Lab 5](https://jaehyeon.me/blog/2023-11-30-real-time-streaming-with-kafka-and-flink-6/), [Lab 6](https://jaehyeon.me/blog/2023-12-14-real-time-streaming-with-kafka-and-flink-7/) |
| [stream-processing-with-flink](./stream-processing-with-flink) | A Kotlin Gradle project with the chapter 1, 5, 6, 7 and 8 examples from the Stream Processing with Apache Flink book on Flink 1.20.1. This is the current version of the book examples. | [Stream Processing with Flink in Kotlin](https://jaehyeon.me/blog/2025-12-10-streaming-processing-with-flink-in-kotlin/) |
| [stream-processing-with-pyflink](./stream-processing-with-pyflink) | An earlier PyFlink port of the same book examples on Flink 1.17.1, kept because it shows where the Python API falls short. | none |

## Learning notes and scratch

These folders are course exercises, study notes or one-off trials. No post covers them and they have no README of their own.

- `confluent-flink-101`: notes and SQL exercises from Confluent's Apache Flink 101 course.
- `datorios`: a single trial of the Datorios tooling, with the vendor's own README and scripts.
- `learning-materials`: twelve markdown documents on the DataStream API, written alongside the Kotlin project.
- `pyflink-doc`: copies of the two tutorials from the official PyFlink documentation.
- `pyflink-udemy`: nineteen short Table API scripts from a Udemy course.
- `sql-cookbook`: a 2023 attempt at the Ververica SQL cookbook on Flink 1.17.1, superseded by `flink-sql-cookbook`. It stays because `sql-training` builds on the Docker image it creates.
- `sql-training`: an unfinished setup for Ververica's sql-training, which depends on the `sql-cookbook` image.

## Posts

- [Getting Started with Pyflink on AWS](https://jaehyeon.me/blog/2023-08-17-getting-started-with-pyflink-on-aws-part-1/)
  - Apache Flink is widely used for building real-time stream processing applications. On AWS, Amazon Managed Service for Apache Flink is the easiest option to develop a Flink app as it provides the underlying infrastructure. Updating a guide from AWS, this series of posts discuss how to develop and deploy a Flink (Pyflink) application on AWS where the data source and sink are Kafka topics.
- [Kafka, Flink and DynamoDB for Real Time Fraud Detection](https://jaehyeon.me/blog/2023-08-10-fraud-detection-part-1/)
  - Re-implementing a solution from an AWS workshop, this series of posts discuss how to develop and deploy a fraud detection app using Kafka, Flink and DynamoDB. Part 1 covers local development using Docker while deployment on AWS will be discussed in part 2.
- [Real Time Streaming with Kafka and Flink](https://jaehyeon.me/blog/2023-10-05-real-time-streaming-with-kafka-and-flink-1/)
  - This series updates a real time analytics app based on Amazon Kinesis from an AWS workshop. Data is ingested from multiple sources into a Kafka cluster instead and Flink (Pyflink) apps are used extensively for data ingesting and processing. As an introduction, this post compares the original architecture with the new architecture, and the app will be implemented in subsequent posts.
- [Building Apache Flink Applications in Python](https://jaehyeon.me/blog/2023-10-19-build-pyflink-apps/)
  - Building Apache Flink Applications in Java by Confluent is a course to introduce Apache Flink through a series of hands-on exercises. Utilising the Flink DataStream API, the course develops three Flink applications from ingesting source data into calculating usage statistics. As part of learning the Flink DataStream API in Pyflink, I converted the Java apps into Python equivalent while performing the course exercises in Pyflink. This post summarises the progress of the conversion and shows the final output.
- [Run Flink SQL Cookbook in Docker](https://jaehyeon.me/blog/2025-04-15-sql-cookbook/)
  - The [Flink SQL Cookbook](https://github.com/ververica/flink-sql-cookbook) is a practical guide packed with self-contained examples for learning [Apache Flink SQL](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/overview/), covering everything from basic queries to advanced stream processing patterns. Since it's designed for the Ververica Platform and lacks cluster setup instructions, this post shows how to run the examples locally using Docker Compose for a smooth, hands-on experience.
- [Stream Processing with Flink in Kotlin](https://jaehyeon.me/blog/2025-12-10-streaming-processing-with-flink-in-kotlin/)
  - A couple of years ago, I read [Stream Processing with Apache Flink](https://www.oreilly.com/library/view/stream-processing-with/9781491974285/) and worked through the examples using PyFlink. While the book offered a solid introduction to Flink, I frequently hit limitations with the Python API, as many features from the book weren't supported. This time, I decided to revisit the material, but using Kotlin. The experience has been much more rewarding and fun.

## License

This repository is licensed under the MIT License, see [LICENSE](./LICENSE). Three folders hold third-party material that keeps its own terms, which the root licence does not change.

- `sql-training/client-image` carries three Apache-2.0 `LICENSE` files from Ververica.
- `datorios` holds vendor scripts and a README from Datorios.
- `pyflink-doc` holds copies of the official PyFlink tutorials, which are Apache-2.0.
