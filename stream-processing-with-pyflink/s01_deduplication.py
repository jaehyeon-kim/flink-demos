import os

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "redpanda:9092")
RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")

config = Configuration()
config.set_string("parallelism.default", "1")
if RUNTIME_ENV != "docker":
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    PIPELINE_JAR = "flink-sql-connector-kafka-1.17.0.jar"
    config.set_string(
        "pipeline.jars", f"file://{os.path.join(CURRENT_DIR, '.external', 'jars', PIPELINE_JAR)}"
    )
env_settings = (
    EnvironmentSettings.new_instance().in_streaming_mode().with_configuration(config).build()
)

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

# from pyflink.common import Configuration
# from pyflink.table import EnvironmentSettings, TableEnvironment

# # t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
# # t_env.get_config().set("parallelism.default", "5")

# config = Configuration()
# config.set_string("parallelism.default", "1")
# if RUNTIME_ENV != "docker":
#     CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
#     PIPELINE_JAR = "flink-sql-connector-kafka-1.17.0.jar"
#     config.set_string(
#         "pipeline.jars", f"file://{os.path.join(CURRENT_DIR, '.external', 'jars', PIPELINE_JAR)}"
#     )

# env_settings = (
#     EnvironmentSettings.new_instance().in_streaming_mode().with_configuration(config).build()
# )
# t_env = TableEnvironment.create(env_settings)

t_env.execute_sql(
    f"""
CREATE TABLE transactions (
  transactionId       STRING,
  accountId           STRING,
  customerId          STRING,
  eventTime           BIGINT,
  eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
  eventTimeFormatted  STRING,
  type                STRING,
  operation           STRING,
  amount              DOUBLE,
  balance             DOUBLE,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'group.transactions',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);
    """
)

t_env.execute_sql(
    """
SELECT transactionId, rowNum
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY transactionId ORDER BY eventTime_ltz) AS rowNum
    FROM transactions
)
WHERE rowNum = 1;
    """
).print()

# BOOTSTRAP_SERVERS=localhost:19092 python 01_deduplication.py
