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
    BroadcastStream,
)
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.datastream.functions import (
    KeyedBroadcastProcessFunction,
    FlatMapFunction,
    RuntimeContext,
)
from pyflink.table import StreamTableEnvironment

from utils.model import SensorReading

THRESHOLDS_STATE_DESCRIPTER = MapStateDescriptor("thresholds", Types.STRING(), Types.DOUBLE())


class UpdatableTemperatureAlertFunction(KeyedBroadcastProcessFunction):
    def __init__(
        self, threshold_descriptor: MapStateDescriptor = THRESHOLDS_STATE_DESCRIPTER
    ) -> None:
        self.last_temp = None
        self.threshold_descriptor = threshold_descriptor

    def open(self, runtime_context: RuntimeContext):
        self.last_temp = runtime_context.get_state(
            ValueStateDescriptor("last_temp", Types.DOUBLE())
        )

    def process_broadcast_element(
        self, value: Tuple[str, float], ctx: KeyedBroadcastProcessFunction.Context
    ):
        thresholds = ctx.get_broadcast_state(self.threshold_descriptor)
        if value[1] != 0.0:
            thresholds.put(value[0], value[1])
        else:
            thresholds.remove(value[0])

    def process_element(
        self, value: SensorReading, ctx: KeyedBroadcastProcessFunction.ReadOnlyContext
    ):
        thresholds = ctx.get_broadcast_state(self.threshold_descriptor)
        if thresholds.contains(value.id):
            sensor_threshold = thresholds.get(value.id)
            temp_diff = round(abs(value.temperature - (self.last_temp.value() or 0.0)), 2)
            if self.last_temp.value() is not None and temp_diff > sensor_threshold:
                yield value.id, value.temperature, self.last_temp.value(), temp_diff, sensor_threshold
        self.last_temp.update(value.temperature)

    # def on_timer(self, timestamp: int, ctx: KeyedBroadcastProcessFunction.OnTimerContext):
    #     return super().on_timer(timestamp, ctx)


def define_workflow(source_stream: DataStream, broadcast_stream: BroadcastStream):
    return (
        source_stream.map(SensorReading.from_tuple)
        .key_by(lambda e: e.id)
        .connect(broadcast_stream)
        .process(UpdatableTemperatureAlertFunction())
    )


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter7/broadcast_state_function.py
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

    thresholds = env.from_collection(
        collection=[
            ("sensor_1", 5.0),
            ("sensor_2", 0.9),
            ("sensor_3", 0.5),
            ("sensor_1", 1.2),
            ("sensor_3", 0.0),
        ]
    )
    broadcast_thresholds = thresholds.broadcast(THRESHOLDS_STATE_DESCRIPTER)

    define_workflow(source_stream, broadcast_thresholds).print()

    env.execute("Generate Temperature Alerts")
