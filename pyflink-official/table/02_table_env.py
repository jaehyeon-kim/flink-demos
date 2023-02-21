from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment, StreamTableEnvironment
from pyflink.common import Configuration

## simple
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

## with configuration
config = Configuration()
config.set_string("execution.buffer-timeout", "1 min")
env_settings = (
    EnvironmentSettings.new_instance().in_streaming_mode().with_configuration(config).build()
)

## from stream env
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

## Statebackend, Checkpoint and Restart Strategy
# set the restart strategy to "fixed-delay"
table_env.get_config().set("restart-strategy", "fixed-delay")
table_env.get_config().set("restart-strategy.fixed-delay.attempts", "3")
table_env.get_config().set("restart-strategy.fixed-delay.delay", "30s")

# set the checkpoint mode to EXACTLY_ONCE
table_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
table_env.get_config().set("execution.checkpointing.interval", "3min")

# set the statebackend type to "rocksdb", other available options are "filesystem" and "jobmanager"
# you can also set the full qualified Java class name of the StateBackendFactory to this option
# e.g. org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory
table_env.get_config().set("state.backend", "rocksdb")

# set the checkpoint directory, which is required by the RocksDB statebackend
table_env.get_config().set("state.checkpoints.dir", "file:///tmp/checkpoints/")
