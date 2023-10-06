from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    env.from_collection([1, 2, 3, 4, 5]).print()

    env.execute()

"""
> local execution

python src/s04_basic.py

> cluster execution

docker exec jobmanager /opt/flink/bin/flink run \
    --python /tmp/src/s04_basic.py -d
"""
