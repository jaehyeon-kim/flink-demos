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
from pyflink.datastream.state import ValueStateDescriptor, ValueState
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.table import StreamTableEnvironment

from utils.model import SensorReading


class SelfCleaningTemperatureAlertFunction(KeyedProcessFunction):
    def __init__(self, threshold: float) -> None:
        self.last_temp = None
        self.last_timer = None
        self.threshold = threshold

    def open(self, runtime_context: RuntimeContext):
        self.last_temp = runtime_context.get_state(
            ValueStateDescriptor("last_temp", Types.DOUBLE())
        )
        self.last_timer = runtime_context.get_state(
            ValueStateDescriptor("last_timer", Types.LONG())
        )

    def process_element(self, value: SensorReading, ctx: KeyedProcessFunction.Context):
        # compute timestamp of new clearn up timer as record timestamp + one hour
        new_timer = ctx.timestamp() + (3600 * 1000)
        # get timestmp of current timer
        cur_timer = self.last_timer.value()
        # delete previous timer and register new timer
        ctx.timer_service().delete_event_time_timer(cur_timer or 0)
        ctx.timer_service().register_event_time_timer(new_timer)
        # update timer timestamp state
        self.last_timer.update(new_timer)

        # check if we need to emit an alert
        temp_diff = round(abs(value.temperature - (self.last_temp.value() or 0.0)), 2)
        if self.last_temp.value() is not None and temp_diff > self.threshold:
            yield value.id, value.temperature, self.last_temp.value(), temp_diff, self.threshold
        self.last_temp.update(value.temperature)

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        self.last_temp.clear()
        self.last_timer.clear()


def define_workflow(source_stream: DataStream):
    return (
        source_stream.map(SensorReading.from_tuple)
        .key_by(lambda e: e.id)
        .process(SelfCleaningTemperatureAlertFunction(1.7))
    )


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter7/stateful_process_function.py
    """

    RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    if RUNTIME_ENV == "local":
        SRC_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        jar_files = ["flink-faker-0.5.3.jar"]
        jar_paths = tuple(
            [f"file://{os.path.join(SRC_DIR, 'jars', name)}" for name in jar_files]
        )
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

    env.execute("Generate Temperature Alerts + Clear State after 1 Hour")
