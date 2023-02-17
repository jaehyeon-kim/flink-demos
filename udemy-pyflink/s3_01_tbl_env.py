# batch/stream table env
from pyflink.table import EnvironmentSettings, TableEnvironment

batch_tbl_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
stream_tbl_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

# from DataStram environment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

ds_env = StreamExecutionEnvironment.get_execution_environment()
tbl_env = StreamTableEnvironment.create(ds_env)
