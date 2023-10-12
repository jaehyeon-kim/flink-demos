import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
TOPIC_NAME = os.getenv("TOPIC_NAME", "transactions")

# 1. create execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
if RUNTIME_ENV != "docker":
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    PIPELINE_JAR = "flink-sql-connector-kafka-1.17.0.jar"
    env.add_jars(f"file://{os.path.join(CURRENT_DIR, '.external', 'jars', PIPELINE_JAR)}")

# 2. create source
txn_source = (
    KafkaSource.builder()
    .set_bootstrap_servers(BOOTSTRAP_SERVERS)
    .set_topics(TOPIC_NAME)
    .set_group_id(f"group.finance.{TOPIC_NAME}")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)
print(txn_source)

# 3. create stream
txn_stream = (
    env.from_source(
        txn_source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(1)),
        "Transaction Source",
    )
    .set_parallelism(5)
    .name("TransactionSource")
)
print(txn_stream)

# 4. print to console
txn_stream.print().set_parallelism(1).uid("print").name("print")
