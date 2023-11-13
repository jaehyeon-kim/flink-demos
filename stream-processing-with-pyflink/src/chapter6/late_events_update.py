import os
import datetime
from typing import Iterable, Tuple

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
    OutputTag,
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.table import StreamTableEnvironment

from utils.model import SensorReading, MinMaxTemp

late_reading_output = OutputTag("late-reading", SensorReading.set_value_type_info())


class HighAndRowTempProcessFunction(ProcessWindowFunction):
    def process(
        self, key: str, context: ProcessWindowFunction.Context, elements: Iterable[Row]
    ) -> Iterable[Tuple[str, float, float, int, int]]:
        temps = [e.temperature for e in elements]
        yield MinMaxTemp(key, min(temps), max(temps), len(temps), int(context.window().end))


## how to identify if a record is updated? possibly add watermark?
def define_workflow(source_stream: DataStream):
    return (
        source_stream.map(SensorReading.from_tuple)
        .map(SensorReading.to_row)
        .key_by(lambda e: e.id)
        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        .allowed_lateness(time_ms=5 * 1000)
        # .side_output_late_data(late_reading_output) # can be added together
        .process(HighAndRowTempProcessFunction())
    )


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter6/late_events_update.py
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
            'fields.log_time.expression' =  '#{date.past ''30'',''5'',''SECONDS''}'
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

    main_stream = define_workflow(source_stream)

    main_stream.print()
    # main_stream.get_side_output(late_reading_output).print()

    env.execute("Update results when late readings are received in a window operator")
