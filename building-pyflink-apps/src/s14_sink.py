import os
import datetime
import logging

from pyflink.common import Types, Row
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import DataStream
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema

from models import SkyoneData, FlightData

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")


def define_workflow(source_stream: DataStream):
    flight_stream = source_stream.map(SkyoneData.to_flight_data).filter(
        lambda data: datetime.datetime.fromisoformat(data.arrival_time) > datetime.datetime.now()
    )
    return flight_stream


def convert_to_row(obj: FlightData):
    return Row(
        email_address=obj.email_address,
        departure_time=obj.departure_time,
        departure_airport_code=obj.departure_airport_code,
        arrival_time=obj.arrival_time,
        arrival_airport_code=obj.arrival_airport_code,
        flight_number=obj.flight_number,
        confirmation=obj.confirmation,
    )


if __name__ == "__main__":
    """
    ## local execution
    python src/s12_transformation.py

    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/s12_transformation.py \
        --pyFiles file:///tmp/src/models.py \
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
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    skyone_stream = env.from_source(
        skyone_source, WatermarkStrategy.no_watermarks(), "skyone_source"
    )

    value_schema = (
        JsonRowSerializationSchema.builder()
        .with_type_info(
            Types.ROW_NAMED(
                field_names=[
                    "email_address",
                    "departure_time",
                    "departure_airport_code",
                    "arrival_time",
                    "arrival_airport_code",
                    "flight_number",
                    "confirmation",
                    "source",
                ],
                field_types=[
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                ],
            )
        )
        .build()
    )

    key_schema = (
        JsonRowSerializationSchema.builder()
        .with_type_info(
            Types.ROW_NAMED(
                field_names=[
                    "confirmation",
                ],
                field_types=[
                    Types.STRING(),
                ],
            )
        )
        .build()
    )

    flight_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("flightdata")
            # .set_key_serialization_schema(key_schema)
            .set_value_serialization_schema(value_schema)
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # define_workflow(skyone_stream).map(convert_to_row).sink_to(flight_sink).name("flightdata_sink")
    define_workflow(skyone_stream).print()

    env.execute("flight_importer")
