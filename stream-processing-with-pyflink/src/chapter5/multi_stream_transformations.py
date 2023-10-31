import os
import datetime
from typing import Tuple

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import DataStream
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import CoFlatMapFunction
from pyflink.table import StreamTableEnvironment

from utils.model import SensorReading, SmokeLevel, Alert


class RaiseAlertFlatMap(CoFlatMapFunction):
    smoke_level = SmokeLevel("Low")

    def flat_map1(self, value: Row):
        # example value
        # - Row(f0='sensor_5', f1=SensorReading(id='sensor_5', timestamp=xxx, num_records=1, temperature=82.0))
        # - should be SensorReading if not keyed by id
        sensor_reading: SensorReading = value.f1
        if self.smoke_level.value == "High" and sensor_reading.temperature > 80:
            yield Alert(
                f"Risk of fire from {sensor_reading.id}",
                sensor_reading.timestamp,
                sensor_reading.temperature,
            )

    def flat_map2(self, value: SmokeLevel):
        self.smoke_level = value


def define_workflow(temp_source: DataStream, smoke_source: DataStream):
    # convert to data classes
    temp_readings = temp_source.map(SensorReading.from_tuple)
    smoke_readings = smoke_source.map(SmokeLevel.from_tuple).set_parallelism(1)
    # connect two streams and raise an alert
    keyed = temp_readings.key_by(lambda e: e.id)
    return keyed.connect(smoke_readings.broadcast()).flat_map(RaiseAlertFlatMap())


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter5/multi_stream_transformations.py
    """

    RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    if RUNTIME_ENV == "local":
        SRC_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        jar_files = ["flink-faker-0.5.3.jar"]
        jar_paths = tuple([f"file://{os.path.join(SRC_DIR, 'jars', name)}" for name in jar_files])
        print(jar_paths)
        env.add_jars(*jar_paths)

    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().set_local_timezone("Australia/Sydney")
    t_env.execute_sql(
        """
        CREATE TABLE sensor_source (
            `id`        INT,
            `rn`        INT,
            `log_time`  TIMESTAMP_LTZ(3)
        )
        WITH (
            'connector' = 'faker',
            'rows-per-second' = '1',
            'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
            'fields.rn.expression' = '#{number.numberBetween ''0'',''100''}',
            'fields.log_time.expression' =  '#{date.past ''10'',''5'',''SECONDS''}'
        );
        """
    )

    t_env.execute_sql(
        """
        CREATE TABLE smoke_source (
            `rn`        INT
        )
        WITH (
            'connector' = 'faker',
            'rows-per-second' = '1',
            'fields.rn.expression' = '#{number.numberBetween ''0'',''100''}'
        );
        """
    )

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    temp_source = t_env.to_append_stream(
        t_env.from_path("sensor_source"),
        Types.TUPLE([Types.INT(), Types.INT(), Types.SQL_TIMESTAMP()]),
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    smoke_source = t_env.to_append_stream(
        t_env.from_path("smoke_source"),
        Types.TUPLE([Types.INT()]),
    )

    define_workflow(temp_source, smoke_source).print()

    env.execute("Multi-Stream Transformations Example")
