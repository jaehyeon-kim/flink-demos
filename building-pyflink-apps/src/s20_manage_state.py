import os
import logging
from typing import Iterable

from pyflink.common import WatermarkStrategy
from pyflink.datastream import DataStream
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema

from models import FlightData, UserStatistics

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")


class ProcessUserStatisticsFunction(ProcessWindowFunction):
    def __init__(self):
        self.state_descriptor = None

    def open(self, context: RuntimeContext):
        self.state_descriptor = ValueStateDescriptor(
            "User Statistics", UserStatistics.get_value_type_info()
        )

    def process(
        self, key: str, context: "ProcessWindowFunction.Context", elements: Iterable[UserStatistics]
    ) -> Iterable:
        state = context.global_state().get_state(self.state_descriptor)
        accumulated_stats = state.value()
        for new_stats in elements:
            # can't use python object, convert to row for state value
            if accumulated_stats is None:
                accumulated_stats = new_stats.to_row()
            else:
                accumulated_stats = UserStatistics.merge(
                    UserStatistics.from_row(accumulated_stats), new_stats
                ).to_row()
        state.update(accumulated_stats)
        # return back to python class
        yield UserStatistics.from_row(accumulated_stats)


def define_workflow(flight_data_stream: DataStream):
    return (
        flight_data_stream.map(FlightData.to_user_statistics_data)
        .key_by(lambda s: s.email_address)
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .reduce(UserStatistics.merge, window_function=ProcessUserStatisticsFunction())
    )


if __name__ == "__main__":
    """
    ## local execution
    ## it takes too long to launch in a local cluster, better to submit it to cluster
    python src/s20_manage_state.py

    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/s20_manage_state.py \
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

    flight_data_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics("flightdata")
        .set_group_id("group.flightdata")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder()
            .type_info(FlightData.get_value_type_info())
            .build()
        )
        .build()
    )

    flight_data_stream = env.from_source(
        flight_data_source, WatermarkStrategy.for_monotonous_timestamps(), "flight_data_source"
    )

    stats_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("userstatistics")
            .set_key_serialization_schema(
                JsonRowSerializationSchema.builder()
                .with_type_info(UserStatistics.get_key_type_info())
                .build()
            )
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder()
                .with_type_info(UserStatistics.get_value_type_info())
                .build()
            )
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    define_workflow(flight_data_stream).map(
        lambda d: d.to_row(), output_type=UserStatistics.get_value_type_info()
    ).sink_to(stats_sink).name("userstatistics_sink").uid("userstatistics_sink")

    env.execute("user_statistics")
