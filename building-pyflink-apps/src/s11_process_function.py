from pyflink.common import Row
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

if __name__ == "__main__":
    # fmt: off
    env = StreamExecutionEnvironment.get_execution_environment()

    #### Process Function
    ## Map
    print(">>>> Map <<<<")
    class MapProcessFunction(ProcessFunction):
        def process_element(self, value: int, ctx: "ProcessFunction.Context"):
            return Row(value * 2)

    data_stream = env.from_collection(collection=[1, 2, 3, 4, 5])
    data_stream.process(MapProcessFunction()).print()
    env.execute()

    ## FlatMap
    print(">>>> FlatMap <<<<")
    class FlatMapProcessFunction(ProcessFunction):
        def process_element(self, value: str, ctx: "ProcessFunction.Context"):
            splits = value.split(" ")
            for sp in splits:
                yield sp, len(sp)

    data_stream = env.from_collection(collection=["hello apache flink", "streaming compute"])
    data_stream.process(FlatMapProcessFunction()).print()
    env.execute()

    ## Filter
    print(">>>> Filter <<<<")
    class FilterProcessFunction(ProcessFunction):
        def process_element(self, value: int, ctx: "ProcessFunction.Context"):
            if value % 2 == 0:
                return Row(value)

    data_stream = env.from_collection(collection=[1, 2, 3, 4, 5])
    data_stream.process(FilterProcessFunction()).print()
    env.execute()

    ## Reduce
    print(">>>> Reduce <<<<")
    class ReduceProcessFunction(KeyedProcessFunction):
        def __init__(self):
            self.state = None

        def open(self, runtime_context: RuntimeContext):
            self.state = runtime_context.get_state(
                ValueStateDescriptor("my_state", Types.PICKLED_BYTE_ARRAY())
            )

        def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
            current = self.state.value()
            if current is None:
                current = Row(value[0], value[1])
            else:
                current[0] = current[0] + value[0]
            self.state.update(current)
            yield current[0], current[1]

    data_stream = env.from_collection(
        collection=[(1, "a"), (2, "a"), (3, "a"), (4, "b")],
        type_info=Types.TUPLE([Types.INT(), Types.STRING()]),
    )
    data_stream.key_by(lambda x: x[1]).process(ReduceProcessFunction()).print()
    env.execute()
