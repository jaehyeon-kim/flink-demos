from typing import Iterable

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    RuntimeContext, ProcessWindowFunction, WindowFunction, ReduceFunction, AggregateFunction,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import CountWindow


class Process(ProcessWindowFunction[dict, dict, str, CountWindow]):
    def __init__(self):
        super().__init__()
        self._state = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("process_count", Types.INT())
        self._state = runtime_context.get_state(state_descriptor)

    def process(self, key: str, ctx: ProcessWindowFunction.Context[CountWindow], elements: Iterable[dict]):
        # retrieve the current value
        cur_val = self._state.value()
        if cur_val is None:
            cur_val = 0

        # process elements
        result = 0
        for element in elements:
            result += int(element["Value"])
            cur_val += 1

        # update the value state
        self._state.update(cur_val)

        yield {key: result}


class Apply(WindowFunction[dict, dict, str, CountWindow]):
    def __init__(self):
        super().__init__()
        self._state = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("apply_count", Types.INT())
        self._state = runtime_context.get_state(state_descriptor)

    def apply(self, key: str, window: CountWindow, inputs: Iterable[dict]):
        # retrieve the current value
        cur_val = self._state.value()
        if cur_val is None:
            cur_val = 0

        # process inputs
        result = 0
        for input in inputs:
            result += int(input["Value"])
            cur_val += 1

        # update the value state
        self._state.update(cur_val)

        yield {key: result}


class Reduce(ReduceFunction):
    def reduce(self, value1, value2):
        return {
            "Name": value1["Name"],
            "Value": str(int(value1["Value"]) + int(value2["Value"]))
        }


class Aggregate(AggregateFunction):
    def create_accumulator(self):
        return {
            "name": "",
            "sum": 0,
            "count": 0,
        }

    def add(self, element, accumulator):
        accumulator["name"] = element["Name"]
        accumulator["sum"] += int(element["Value"])
        accumulator["count"] += 1
        return accumulator

    def get_result(self, accumulator):
        return {accumulator["name"]: accumulator["sum"]}

    def merge(self, accumulator1, accumulator2):
        return {
            "name": accumulator1["name"],
            "sum": accumulator1["sum"] + accumulator2["sum"],
            "count": accumulator1["count"] + accumulator2["count"],
        }


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    ds = env.from_collection(
        collection=[
            {"Name": "hi", "Value": "1"},
            {"Name": "hello", "Value": "2"},
            {"Name": "hi", "Value": "3"},
            {"Name": "hello", "Value": "4"},
            {"Name": "hi", "Value": "5"},
            {"Name": "hello", "Value": "6"},
            {"Name": "hello", "Value": "6"},
        ],
        type_info=Types.MAP(Types.STRING(), Types.STRING())
    )

    # tumbling count window
    ds = ds.key_by(lambda entry: entry["Name"], key_type=Types.STRING()) \
        .count_window(2)

    # process
    ds_process = ds.process(Process(), Types.MAP(Types.STRING(), Types.INT()))
    ds_process.print("ds_process")

    # apply
    ds_apply = ds.apply(Apply(), Types.MAP(Types.STRING(), Types.INT()))
    ds_apply.print("ds_apply")

    # reduce
    ds_reduce = ds.reduce(Reduce(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
    ds_reduce.print("ds_reduce")

    # aggregate
    ds_aggregate = ds.aggregate(Aggregate(), output_type=Types.MAP(Types.STRING(), Types.INT()))
    ds_aggregate.print("ds_aggregate")

    env.execute()


if __name__ == "__main__":
    main()