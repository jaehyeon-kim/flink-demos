import os

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

if __name__ == "__main__":
    """
    ## local execution
    python src/chapter5/rolling_sum.py
    """

    RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    input_stream = env.from_collection(collection=[(1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3)])

    result_stream = input_stream.key_by(lambda e: e[0]).sum(1)

    result_stream.print()

    env.execute("Rolling Sum Example")
