from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

if __name__ == "__main__":
    """
    ## local execution
    python src/s04_intro.py

    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/s04_intro.py -d
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    env.from_collection([1, 2, 3, 4, 5]).print()

    env.execute()
