from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()

    #### Operator
    ## Map
    print(">>>> Map <<<<")
    data_stream = env.from_collection(collection=[1, 2, 3, 4, 5])
    data_stream.map(lambda x: 2 * x, output_type=Types.INT()).print()
    env.execute()

    ## FlatMap
    print(">>>> FlatMap <<<<")

    def split(s: str):
        splits = s.split(" ")
        for sp in splits:
            yield sp, len(sp)

    data_stream = env.from_collection(collection=["hello apache flink", "streaming compute"])
    data_stream.flat_map(split, output_type=Types.TUPLE([Types.STRING(), Types.INT()])).print()
    env.execute()

    ## Filter
    print(">>>> Filter <<<<")
    data_stream = env.from_collection(collection=[1, 2, 3, 4, 5])
    data_stream.filter(lambda x: x % 2 == 0).print()
    env.execute()

    ## KeyBy
    print(">>>> KeyBy <<<<")
    data_stream = env.from_collection(collection=[(1, "a"), (2, "a"), (3, "b")])
    data_stream.key_by(lambda x: x[1], key_type=Types.STRING()).print()
    env.execute()

    ## Reduce
    print(">>>> Reduce <<<<")
    data_stream = env.from_collection(
        collection=[(1, "a"), (2, "a"), (3, "a"), (4, "b")],
        type_info=Types.TUPLE([Types.INT(), Types.STRING()]),
    )
    data_stream.key_by(lambda x: x[1]).reduce(lambda a, b: (a[0] + b[0], b[1])).print()
    env.execute()
