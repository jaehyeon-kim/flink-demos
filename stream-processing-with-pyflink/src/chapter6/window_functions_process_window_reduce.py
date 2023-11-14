import os
import datetime
from typing import Iterable, Tuple

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction, ReduceFunction
from pyflink.table import StreamTableEnvironment

from utils.model import MinMaxTemp


class HighAndRowTempProcessFunction(ProcessWindowFunction):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[Tuple[str, float, float, int]],
    ) -> Iterable[Tuple[str, float, float, int, int]]:
        elem = next(iter(elements))
        yield MinMaxTemp(key, elem[1], elem[2], elem[3], int(context.window().end))


class ExpandToTuple(MapFunction):
    def map(self, value: Tuple[int, int, datetime.datetime]):
        return f"sensor_{value[0]}", 65 + (value[1] / 100 * 20), 65 + (value[1] / 100 * 20), 1


class MinMaxReduce(ReduceFunction):
    def reduce(self, value1: Tuple[str, float, float, int], value2: Tuple[str, float, float, int]):
        return (
            value1[0],
            min(value1[1], value2[1]),
            max(value1[2], value2[2]),
            value1[3] + value2[3],
        )


def define_workflow(source_stream: DataStream):
    return (
        source_stream.map(ExpandToTuple())
        .key_by(lambda e: e[0])
        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        .reduce(reduce_function=MinMaxReduce(), window_function=HighAndRowTempProcessFunction())
    )


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter6/window_functions_process_window_reduce.py
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
