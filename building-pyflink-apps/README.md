# Building Pyflink Apps

Implement the applications of the [Building Apache Flink Applications in Java](https://developer.confluent.io/courses/flink-java/overview/) course by Confluent using Pyflink.

- [Kafka Producer (Python)](./src/s05_data_gen.py)
- [Flight Importer Job (Pyflink)](./src/s16_merge.py)
- [Usage Statistics Calculator (Pyflink)](./src/s20_manage_state.py)

Unlike the course, the source data is sent by a Kafka producer application because the DataGen DataStream connector is not available in Pyflink. The other apps are implemented gradually while performing course exercises. See below for details.

![](./images/featured.png)

## Application Description

1. Apache Flink with Java - An Introduction
2. Datastream Programming
3. ✅ How to Start Flink and Get Setup (Exercise)
   - Built Kafka and Flink clusters using Docker
   - Bitnami images are used for the Kafka cluster - see [this page](https://jaehyeon.me/blog/2023-05-04-kafka-development-with-docker-part-1/) for details.
   - A custom Docker image (_building-pyflink-apps:1.17.1_) is created to install Python and the Pyflink package as well as to save dependent Jar files
     - See the [Dockerfile](./Dockerfile), and it can be built by `docker build -t=building-pyflink-apps:1.17.1 .`
   - See the [docker-compose.yml](./docker-compose.yml) and the clusters can be started by `docker-compose up -d`
4. ☑️ The Flink Job Lifecycle
   - A minimal example of executing a Pyflink app is added.
   - [s04_intro.py](./src/s04_intro.py)
5. ✅ Running a Flink Job (Exercise)
   - Pyflink doesn't have the DataGen DataStream connector. Used a Kafka producer instead to create topics and send messages.
     - 4 topics are created (_skyone_, _sunset_, _flightdata_ and _userstatistics_) and messages are sent to the first two topics.
   - [s05_data_gen.py](./src/s05_data_gen.py)
     - Topics are created by a flag argument so add it if it is the first time running it. i.e. `python src/s05_data_gen.py --create`. Basically it deletes the topics if exits and creates them.
6. Anatomy of a Stream
7. Flink Data Sources
8. ✅ Creating a Flink Data Source (Exercise)
   - It reads from the _skyone_ topic and prints the values. The values are deserialized as string in this exercise.
   - This and all the other Pyflink applications can be executed locally or run in the Flink cluster. See the script for details.
   - [s08_create_source.py](./src/s08_create_source.py)
9. Serializers & Deserializers
10. ✅ Deserializing Messages in Flink (Exercise)
    - The _skyone_ message values are deserialized as Json string and they are returned as the [named Row type](https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.common/api/pyflink.common.typeinfo.Types.ROW_NAMED.html#pyflink.common.typeinfo.Types.ROW_NAMED). As the Flink type is not convenient for processing, it is converted into a Python object, specifically [Data Classes](https://docs.python.org/3/library/dataclasses.html).
    - [s10_deserialization.py](./src/s10_deserialization.py)
11. ☑️ Transforming Data in Flink
    - _Map_, _FlatMap_, _Filter_ and _Reduce_ transformations are illustrated using built-in operators and process functions.
    - [s11_transformation.py](./src/s11_transformation.py)
    - [s11_process_function.py](./src/s11_process_function.py)
12. ✅ Flink Data Transformations (Exercise)
    - The source data is transformed into the flight data. Later data from _skyone_ and _sunset_ will be converted into it for merging them.
    - The transformation is performed in a function called _define_workflow_ and tested. This function will be updated gradually.
    - [s12_transformation.py](./src/s12_transformation.py)
    - [test_s12_transformation.py](./src/test_s12_transformation.py)
      - Expected to run testing scripts individually eg) `pytest src/test_s12_transformation.py -svv`
13. Flink Data Sinks
14. ✅ Creating a Flink Data Sink (Exercise)
    - The converted data from _skyone_ will be pushed into a Kafka topic (_flightdata_).
    - Note that the Python Data Classes cannot be serialized, records are converted into the named Row type before pushing them.
    - [s14_sink.py](./src/s14_sink.py)
15. ☑️ Creating Branching Data Streams in Flink
    - Various branching methods are illustrated, which covers _Union_, _CoProcessFunction_, _CoMapFunction_, _CoFlatMapFunction_, and _Side Outputs_.
    - [s15_branching.py](./src/s15_branching.py)
16. ✅ Merging Flink Data Streams (Exercise)
    - Records from the _skyone_ and _sunset_ topics are merged and pushed into the _flightdata_ topic after being converted into the flight data.
    - [s16_merge.py](./src/s16_merge.py)
    - [test_s16_merge.py](./src/test_s16_merge.py)
17. Windowing and Watermarks in Flink
18. ✅ Aggregating Flink Data using Windowing (Exercise)
    - Usage statistics (total flight duration and number of flights) are calculated by email address, and they are pushed into the _userstatistics_ topic.
    - Note the transformation is _stateless_ in a sense that aggregation is entirely within the tumbling window over 1 minute.
    - [s18_aggregation.py](./src/s18_aggregation.py)
    - [test_s18_aggregation.py](./src/test_s18_aggregation.py)
19. Working with Keyed State in Flink
20. ✅ Managing State in Flink (Exercise)
    - The transformation gets _stateful_ so that usage statistics are continuously updated by accessing the state values.
    - The _reduce_ function includes a window function that allows you to access the global state. The window function takes the responsibility to keep updating the global state and to return updated values.
    - [s20_manage_state.py](./src/s20_manage_state.py)
    - [test_s20_manage_state.py](./src/test_s20_manage_state.py)
21. Closing Remarks

- ✅ - exercise
- ☑️ - course material

## More Resources

- [Intro to the Python DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/python/datastream/intro_to_datastream_api/)
- [Flink DataStream API Programming Guide](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/overview/)
- [All You Need to Know About PyFlink](https://www.alibabacloud.com/blog/all-you-need-to-know-about-pyflink_600306)
