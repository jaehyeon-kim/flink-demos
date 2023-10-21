import os
import time
from typing import Iterable, Tuple

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import DataStream
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema

from models import SensorReading

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")


class AggreteProcessWindowFunction(ProcessWindowFunction):
    def process(
        self, key: str, context: ProcessWindowFunction.Context, elements: Iterable[Tuple[int, int]]
    ) -> Iterable[Row]:
        id, count, temperature = SensorReading.process_elements(elements)
        yield Row(
            id=id,
            timestamp=int(context.window().end),
            temperature=round(temperature / count, 2),
        )


def define_workflow(source_stream: DataStream):
    sensor_stream = (
        source_stream.key_by(lambda e: e[0])
        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        .process(AggreteProcessWindowFunction(), output_type=SensorReading.get_value_type())
    )
    return sensor_stream


if __name__ == "__main__":
    """
    ## local execution
    python src/c01_sensor_reading.py

    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/c01_sensor_reading.py \
        --pyFiles file:///tmp/src/models.py \
        -d
    """

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    if RUNTIME_ENV == "local":
        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        jar_files = ["flink-faker-0.5.3.jar", "flink-sql-connector-kafka-1.17.1.jar"]
        jar_paths = tuple(
            [f"file://{os.path.join(CURRENT_DIR, 'jars', name)}" for name in jar_files]
        )
        print(jar_paths)
        env.add_jars(*jar_paths)

    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.execute_sql(
        """
        CREATE TABLE sensor_source (
            `id`      INT,
            `rand`    INT
        )
        WITH (
            'connector' = 'faker',
            'rows-per-second' = '10',
            'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
            'fields.rand.expression' = '#{number.numberBetween ''0'',''100''}'
        );
        """
    )

    class DefaultTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return int(time.time_ns() / 1000000)

    source_stream = t_env.to_append_stream(
        t_env.from_path("sensor_source"), Types.TUPLE([Types.INT(), Types.INT()])
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
            DefaultTimestampAssigner()
        )
    )

    sensor_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("sensor-reading")
            .set_key_serialization_schema(
                JsonRowSerializationSchema.builder()
                .with_type_info(SensorReading.get_key_type())
                .build()
            )
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder()
                .with_type_info(SensorReading.get_value_type())
                .build()
            )
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # define_workflow(source_stream).print()  # it works!
    define_workflow(source_stream).sink_to(sensor_sink).name("sensor_sink").uid("sensor_sink")

    env.execute()
