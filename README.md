# Flink Demos

- [Getting Started With Pyflink on AWS](https://jaehyeon.me/blog/2023-08-17-getting-started-with-pyflink-on-aws-part-1/)
  - Apache Flink is widely used for building real-time stream processing applications. On AWS, Amazon Managed Service for Apache Flink is the easiest option to develop a Flink app as it provides the underlying infrastructure. Updating a guide from AWS, this series of posts discuss how to develop and deploy a Flink (Pyflink) application on AWS where the data source and sink are Kafka topics.
- [Kafka, Flink and DynamoDB for Real Time Fraud Detection](https://jaehyeon.me/blog/2023-08-10-fraud-detection-part-1/)
  - Re-implementing a solution from an AWS workshop, this series of posts discuss how to develop and deploy a fraud detection app using Kafka, Flink and DynamoDB. Part 1 covers local development using Docker while deployment on AWS will be discussed in part 2.
- [Building Apache Flink Applications in Python](https://jaehyeon.me/blog/2023-10-19-build-pyflink-apps/)
  - Building Apache Flink Applications in Java by Confluent is a course to introduce Apache Flink through a series of hands-on exercises. Utilising the Flink DataStream API, the course develops three Flink applications from ingesting source data into calculating usage statistics. As part of learning the Flink DataStream API in Pyflink, I converted the Java apps into Python equivalent while performing the course exercises in Pyflink. This post summarises the progress of the conversion and shows the final output.
