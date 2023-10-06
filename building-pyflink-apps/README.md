# Building Pyflink Apps

Implement exercises of [Building Apache Flink Applications in Java](https://developer.confluent.io/courses/flink-java/overview/) using Pyflink.

1. Apache Flink with Java - An Introduction
2. Datastream Programming
3. How to Start Flink and Get Setup (Exercise) [x]
   - [Dockerfile](./Dockerfile): `docker build -t=building-pyflink-apps:1.17.1 .`
   - [docker-compose.yml](./docker-compose.yml): `docker-compose up -d`
4. The Flink Job Lifecycle
   - [s04_intro.py](./src/s04_intro.py)
5. Running a Flink Job (Exercise) [x]
   - [s05_data_gen.py](./src/s05_data_gen.py)
   - Pyflink doesn't have DataGen DataStream connector. Use Kafka producers instead.
6. Anatomy of a Stream
7. Flink Data Sources
8. Creating a Flink Data Source (Exercise) [x]
   - [s08_create_source.py](./src/s08_create_source.py)
9. Serializers & Deserializers
10. Deserializing Messages in Flink (Exercise) [x]
    - [s10_deserialization.py](./src/s10_deserialization.py)
    - No change to the previous exercise because of no POJO serialization on Pyflink
11. Transforming Data in Flink
12. Flink Data Transformations (Exercise) [ ]
13. Flink Data Sinks
14. Creating a Flink Data Sink (Exercise) [ ]
15. Creating Branching Data Streams in Flink
16. Merging Flink Data Streams (Exercise) [ ]
17. Windowing and Watermarks in Flink
18. Aggregating Flink Data using Windowing (Exercise) [ ]
19. Working with Keyed State in Flink
20. Managing State in Flink (Exercise) [ ]
21. Closing Remarks

## Reference

- [Intro to the Python DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/python/datastream/intro_to_datastream_api/)
- [Flink DataStream API Programming Guide](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/)
- [All You Need to Know About PyFlink](https://www.alibabacloud.com/blog/all-you-need-to-know-about-pyflink_600306)
