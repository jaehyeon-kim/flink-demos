import os
import datetime
from typing import Tuple

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
    OutputTag,
)
from pyflink.datastream.functions import ProcessFunction
from pyflink.table import StreamTableEnvironment

from utils.model import SensorReading

main_stream_output = OutputTag("main-output")
late_reading_output = OutputTag(
    "late-reading",
    Types.ROW_NAMED(
        field_names=["id", "timestamp", "watermark"],
        field_types=[Types.STRING(), Types.LONG(), Types.LONG()],
    ),
)


class LateReadingsFilter(ProcessFunction):
    def process_element(self, value: SensorReading, ctx: ProcessFunction.Context):
        ts = value.timestamp
        wm = ctx.timer_service().current_watermark()
        print(f"{ts} {wm}")
        if ts < wm:
            yield late_reading_output, Row(id=value.id, timestamp=ts, watermark=wm)
        yield main_stream_output, value


def define_workflow(source_stream: DataStream):
    return source_stream.map(SensorReading.from_tuple).process(LateReadingsFilter())


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter6/late_events_filter_out.py
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

    main_stream.get_side_output(main_stream_output).print()
    main_stream.get_side_output(late_reading_output).print()

    env.execute("Filter out late readings (to a side output) using a ProcessFunction")
