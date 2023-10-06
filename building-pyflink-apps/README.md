# Building Pyflink Apps

Implement exercises of [Building Apache Flink Applications in Java](https://developer.confluent.io/courses/flink-java/overview/) using Pyflink.

- Overview
- Datastream Programming
- [x] Setup your Flink environment (Exercise)
  - [Dockerfile](./Dockerfile): `docker build -t=building-pyflink-apps:1.17.1 .`
  - [docker-compose.yml](./docker-compose.yml): `docker-compose up -d`
- The Flink Job Lifecycle
- [x] Running a Flink Job (Exercise)
  - [s05_data_gen.py](./src/s05_data_gen.py)
  - Pyflink doesn't have DataGen DataStream connector. Use Kafka producers instead.
- Anatomy of a Stream
- Flink Data Sources
- [x] Creating a Flink Data Source (Exercise)
  - [s08_create_source.py](./src/s08_create_source.py)
- Serializers & Deserializers
- [ ] Deserializing Messages in Flink (Exercise)
- Transforming Data in Flink
- [ ] Flink Data Transformations (Exercise)
- Flink Data Sinks
- [ ] Creating a Flink Data Sink (Exercise)
- Closing Remarks
