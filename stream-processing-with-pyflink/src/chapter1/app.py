import os

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema

from model import SensorReading
from workflow import define_workflow

if __name__ == "__main__":
    """
    ## local execution
    python src/chapter1/app.py

    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/chapter1/app.py \
        --pyFiles file:///tmp/src/chapter1/model.py,file:///tmp/src/chapter1/workflow.py \
        -d
    """

    RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    if RUNTIME_ENV == "local":
        SRC_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        jar_files = ["flink-faker-0.5.3.jar", "flink-sql-connector-kafka-1.17.1.jar"]
        jar_paths = tuple([f"file://{os.path.join(SRC_DIR, 'jars', name)}" for name in jar_files])
        print(jar_paths)
        env.add_jars(*jar_paths)

    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.execute_sql(
        """
        CREATE TABLE sensor_source (
            `id`        INT,
            `log_time`  TIMESTAMP(3)
        )
        WITH (
            'connector' = 'faker',
            'rows-per-second' = '10',
            'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
            'fields.log_time.expression' =  '#{date.past ''10'',''5'',''SECONDS''}'
        );
        """
    )

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return int(value[1].strftime("%s")) * 1000

    source_stream = t_env.to_append_stream(
        t_env.from_path("sensor_source"), Types.TUPLE([Types.INT(), Types.SQL_TIMESTAMP()])
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
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

    # define_workflow(source_stream).print()
    define_workflow(source_stream).sink_to(sensor_sink).name("sensor_sink").uid("sensor_sink")

    env.execute()
