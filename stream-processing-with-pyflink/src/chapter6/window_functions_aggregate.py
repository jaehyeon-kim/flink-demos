import os
import datetime
from typing import Tuple

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import AggregateFunction
from pyflink.table import StreamTableEnvironment

from utils.model import SensorReading


class AvgTempFunction(AggregateFunction):
    def create_accumulator(self):
        return SensorReading(None, 0, 0, 0.0)

    def add(self, value: SensorReading, accumulator: SensorReading):
        return SensorReading(
            value.id,
            value.timestamp,
            value.num_records + accumulator.num_records,
            value.temperature + accumulator.temperature,
        )

    def get_result(self, accumulator: SensorReading):
        return SensorReading(
            accumulator.id,
            accumulator.timestamp,
            accumulator.num_records,
            round(accumulator.temperature / accumulator.num_records, 2),
        )

    def merge(self, acc_a: SensorReading, acc_b: SensorReading):
        return SensorReading(
            acc_a.id,
            acc_b.timestamp,
            acc_a.num_records + acc_b.num_records,
            acc_a.temperature + acc_b.timestamp,
        )


def define_workflow(source_stream: DataStream):
    return (
        source_stream.map(SensorReading.from_tuple)
        .key_by(lambda e: e.id)
        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        .aggregate(AvgTempFunction())
    )


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter5/window_functions_a.py
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

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    source_stream = t_env.to_append_stream(
        t_env.from_path("sensor_source"),
        Types.TUPLE([Types.INT(), Types.INT(), Types.SQL_TIMESTAMP()]),
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    define_workflow(source_stream).print()

    env.execute("Window function example - aggregate")
