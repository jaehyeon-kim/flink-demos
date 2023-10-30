import argparse
import os
import json
import datetime
from typing import Tuple

from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
    TimeCharacteristic,
)
from pyflink.datastream.state import ValueStateDescriptor, ValueState
from pyflink.datastream.functions import CoProcessFunction, RuntimeContext
from pyflink.datastream.time_domain import TimeDomain
from pyflink.table import StreamTableEnvironment

from utils.model import SensorReading

###
### Incomplete as on_timer() method is not yet implemented
###


class ReadingFilter(CoProcessFunction):
    def __init__(self):
        self.forwarding_enabled = None
        self.disable_timer = None

    class OnTimerContext(CoProcessFunction.Context):
        def time_domain(self) -> TimeDomain:
            pass

    def open(self, runtime_context: RuntimeContext):
        self.forwarding_enabled = runtime_context.get_state(
            ValueStateDescriptor("filter_switch", Types.BOOLEAN())
        )
        self.disable_timer = runtime_context.get_state(ValueStateDescriptor("timer", Types.LONG()))

    def process_element1(self, value: SensorReading, ctx: CoProcessFunction.Context):
        if self.forwarding_enabled.value():
            yield value

    def process_element2(self, value: Tuple[str, int], ctx: CoProcessFunction.Context):
        # enable reading forwarding
        self.forwarding_enabled.update(True)
        # set disable forward timer
        cpt = ctx.timer_service().current_processing_time()
        timer_timestamp = cpt + value[1]
        cur_timer_timestamp = self.disable_timer.value() or 0
        print(f"{value} - {cpt} - {timer_timestamp} - {self.disable_timer.value()}")
        if timer_timestamp > cur_timer_timestamp:
            # remove current timer and register new timer
            ctx.timer_service().delete_processing_time_timer(cur_timer_timestamp)
            ctx.timer_service().register_processing_time_timer(timer_timestamp)
            self.disable_timer.update(timer_timestamp)

    # def on_timer(self, timestamp: int, ctx: OnTimerContext):
    #     print("TIMER CALLED")
    #     import ipdb

    #     ipdb.set_trace()
    #     self.forwarding_enabled.clear()
    #     self.disable_timer.clear()


def define_workflow(source_stream: DataStream, filter_switches: DataStream):
    pass


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter5/process_function_timers.py
    """
    RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    parser.set_defaults(verbose=False)
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
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
            'fields.id.expression' = '#{number.numberBetween ''0'',''4''}',
            'fields.rn.expression' = '#{number.numberBetween ''0'',''100''}',
            'fields.log_time.expression' =  '#{date.past ''10'',''5'',''SECONDS''}'
        );
        """
    )

    source_stream = t_env.to_append_stream(
        t_env.from_path("sensor_source"),
        Types.TUPLE([Types.INT(), Types.INT(), Types.SQL_TIMESTAMP()]),
    )

    filter_switches = env.from_collection(collection=[("sensor_0", 5 * 1000)])

    forward_readings = (
        source_stream.map(SensorReading.from_tuple)
        .connect(filter_switches)
        .key_by(lambda s: s.id, lambda f: f[0])
        .process(ReadingFilter())
    )

    forward_readings.print()

    # source_stream.map(SensorReading.from_tuple).print()

    env.execute("Monitor sensor temperatures.")
