import os
import datetime
from typing import Collection, Tuple, Iterable

from pyflink.common import WatermarkStrategy
from pyflink.common.serializer import TypeSerializer, VoidNamespaceSerializer
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.datastream.window import (
    WindowAssigner,
    TimeWindow,
    EventTimeTrigger,
    Trigger,
    TriggerResult,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.table import StreamTableEnvironment

from utils.model import SensorReading

###
### Come back later...
###


class FiveSecondWindows(WindowAssigner):
    def __init__(self) -> None:
        self.window_size = 5 * 1000

    def assign_windows(
        self, element: SensorReading, timestamp: int, context: WindowAssigner.WindowAssignerContext
    ) -> Collection[TimeWindow]:
        start_time = timestamp - timestamp % self.window_size
        end_time = start_time + self.window_size
        return [TimeWindow(start_time, end_time)]

    def get_default_trigger(self, env: StreamExecutionEnvironment) -> Trigger:
        return EventTimeTrigger.create()

    def get_window_serializer(self) -> TypeSerializer:
        return VoidNamespaceSerializer()

    def is_event_time(self) -> bool:
        return True


class OneSecondIntervalTrigger(Trigger):
    def __init__(self) -> None:
        self.first_seen = None

    def on_element(
        self,
        element: SensorReading,
        timestamp: int,
        window: TimeWindow,
        ctx: Trigger.TriggerContext,
    ) -> TriggerResult:
        self.first_seen = ctx.get_partitioned_state(
            ValueStateDescriptor("first_seen", Types.BOOLEAN())
        )
        # first_seen will be None if not set yet
        if self.first_seen.value() is None:
            # compute time for next early firing by rounding watermark to second
            t = ctx.get_current_watermark() + (1000 - (ctx.get_current_watermark() % 1000))
            ctx.register_event_time_timer(t)
            # register timer for the window end
            ctx.register_event_time_timer(window.end)
            self.first_seen.update(True)
        # continue. do not evaluate per element
        TriggerResult.CONTINUE

    def on_event_time(
        self, time: int, window: TimeWindow, ctx: Trigger.TriggerContext
    ) -> TriggerResult:
        if time == window.end:
            # final evaluation and purge window state
            TriggerResult.FIRE_AND_PURGE
        else:
            # register next early firing timer
            t = ctx.get_current_watermark() + (1000 - (ctx.get_current_watermark() % 1000))
            if t < window.end:
                ctx.register_event_time_timer(t)
            # first trigger to evaluate window
            TriggerResult.FIRE

    def on_processing_time(
        self, time: int, window: TimeWindow, ctx: Trigger.TriggerContext
    ) -> TriggerResult:
        # continue. we don't use processing time timers
        TriggerResult.CONTINUE

    def on_merge(self, window: TimeWindow, ctx: Trigger.OnMergeContext) -> None:
        pass

    def clear(self, window: TimeWindow, ctx: Trigger.TriggerContext) -> None:
        self.first_seen = ctx.get_partitioned_state(
            ValueStateDescriptor("first_seen", Types.BOOLEAN())
        )
        self.first_seen.clear()


class CountFunction(ProcessWindowFunction):
    def process(
        self, key: str, context: ProcessWindowFunction.Context, elements: Iterable[SensorReading]
    ) -> Iterable[Tuple[str, float, float, int, int]]:
        temps = [e.temperature for e in elements]
        yield key, context.window().end, context.current_watermark(), len(temps)


def define_workflow(source_stream: DataStream):
    return (
        source_stream.map(SensorReading.from_tuple)
        .key_by(lambda e: e.id)
        .window(FiveSecondWindows())
        .trigger(OneSecondIntervalTrigger())
        .process(CountFunction())
    )


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter6/custom_window.py
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

    env.execute("Window function example - reduce")
