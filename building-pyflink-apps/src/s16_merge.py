import os
import datetime
import logging

from pyflink.common import WatermarkStrategy
from pyflink.datastream import DataStream
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema

from models import SkyoneData, SunsetData, FlightData

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")


def define_workflow(skyone_stream: DataStream, sunset_stream: DataStream):
    flight_stream_from_skyone = skyone_stream.map(SkyoneData.to_flight_data).filter(
        lambda data: datetime.datetime.fromisoformat(data.arrival_time) > datetime.datetime.now()
    )
    flight_stream_from_sunset = sunset_stream.map(SunsetData.to_flight_data).filter(
        lambda data: datetime.datetime.fromisoformat(data.arrival_time) > datetime.datetime.now()
    )
    return flight_stream_from_skyone.union(flight_stream_from_sunset)


if __name__ == "__main__":
    """
    ## local execution
    python src/s16_merge.py

    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/s16_merge.py \
        --pyFiles file:///tmp/src/models.py,file:///tmp/src/utils.py \
        -d
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"RUNTIME_ENV - {RUNTIME_ENV}, BOOTSTRAP_SERVERS - {BOOTSTRAP_SERVERS}")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # env.set_parallelism(5)
    if RUNTIME_ENV != "docker":
        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        jar_files = ["flink-sql-connector-kafka-1.17.1.jar"]
        jar_paths = tuple(
            [f"file://{os.path.join(CURRENT_DIR, 'jars', name)}" for name in jar_files]
        )
        logging.info(f"adding local jars - {', '.join(jar_files)}")
        env.add_jars(*jar_paths)

    skyone_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("skyone")
        .set_group_id("group.skyone")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder()
            .type_info(SkyoneData.get_value_type_info())
            .build()
        )
        .build()
    )

    sunset_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("sunset")
        .set_group_id("group.sunset")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder()
            .type_info(SunsetData.get_value_type_info())
            .build()
        )
        .build()
    )

    skyone_stream = env.from_source(
        skyone_source, WatermarkStrategy.no_watermarks(), "skyone_source"
    )

    sunset_stream = env.from_source(
        sunset_source, WatermarkStrategy.no_watermarks(), "sunset_source"
    )

    flight_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("flightdata")
            .set_key_serialization_schema(
                JsonRowSerializationSchema.builder()
                .with_type_info(FlightData.get_key_type_info())
                .build()
            )
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder()
                .with_type_info(FlightData.get_value_type_info())
                .build()
            )
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    define_workflow(skyone_stream, sunset_stream).map(
        lambda d: d.to_row(), output_type=FlightData.get_value_type_info()
    ).sink_to(flight_sink).name("flightdata_sink")

    env.execute("flight_importer")
