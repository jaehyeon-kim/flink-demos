import argparse
import os
import json
import datetime

from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
    TimeCharacteristic,
)
from pyflink.datastream.state import ValueStateDescriptor, ValueState
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.table import StreamTableEnvironment

from utils.model import SensorReading


class TempIncAlertFunc(KeyedProcessFunction):
    def __init__(
        self, last_temp: ValueState = None, current_timer: ValueState = None, verbose: bool = False
    ):
        self.last_temp = last_temp
        self.current_timer = current_timer
        self.verbose = verbose

    def open(self, runtime_context: RuntimeContext):
        if self.last_temp is None and self.current_timer is None:
            self.last_temp = runtime_context.get_state(
                ValueStateDescriptor("last_temp", Types.DOUBLE())
            )
            self.current_timer = runtime_context.get_state(
                ValueStateDescriptor("timer", Types.LONG())
            )

    def process_element(self, value: SensorReading, ctx: "KeyedProcessFunction.Context"):
        prev_temp = self.last_temp.value() or 0.0
        self.last_temp.update(value.temperature)
        curr_timer_timestamp = self.current_timer.value() or 0
        if prev_temp == 0.0:
            # first sensor reading for this key
            # we cannot compare it with a previous value
            pass
        elif value.temperature < prev_temp:
            # temperature decreased. delete current timer
            ctx.timer_service().delete_processing_time_timer(curr_timer_timestamp)
            self.current_timer.clear()
        elif value.temperature > prev_temp and curr_timer_timestamp == 0:
            # temperature increased and we have not set a timer yet
            # set timer for now + 1 second
            timer_ts = ctx.timer_service().current_processing_time() + 1000
            ctx.timer_service().register_processing_time_timer(timer_ts)
            # remember current timer
            self.current_timer.update(timer_ts)
        if self.verbose and value.id == "sensor_1":
            print(
                f"{value.id} - {prev_temp} - {value.temperature} - {curr_timer_timestamp} - {self.current_timer.value()} - {datetime.datetime.now().isoformat(timespec='milliseconds')}"
            )

    def on_timer(self, timestamp: int, ctx: "KeyedProcessFunction.OnTimerContext"):
        sensor_reading = json.dumps({"id": ctx.get_current_key(), "timestamp": timestamp})
        if self.verbose:
            if ctx.get_current_key() == "sensor_1":
                yield sensor_reading
        else:
            yield sensor_reading
        self.current_timer.clear()


def define_workflow(source_stream: DataStream, verbose: bool = False):
    return (
        source_stream.map(SensorReading.from_tuple)
        .key_by(lambda e: e.id)
        .process(TempIncAlertFunc(verbose=verbose), output_type=Types.STRING())
    )


if __name__ == "__main__":
    """
    ## local execution
    python src/chapter6/process_function_timers.py
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
            'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
            'fields.rn.expression' = '#{number.numberBetween ''0'',''100''}',
            'fields.log_time.expression' =  '#{date.past ''10'',''5'',''SECONDS''}'
        );
        """
    )

    source_stream = t_env.to_append_stream(
        t_env.from_path("sensor_source"),
        Types.TUPLE([Types.INT(), Types.INT(), Types.SQL_TIMESTAMP()]),
    )

    define_workflow(source_stream, verbose=args.verbose).print()

    env.execute("Monitor sensor temperatures.")
