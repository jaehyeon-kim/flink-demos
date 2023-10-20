import os
import logging

from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

from models import Transaction

RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.info(f"RUNTIME_ENV - {RUNTIME_ENV}, BOOTSTRAP_SERVERS - {BOOTSTRAP_SERVERS}")

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

if RUNTIME_ENV != "docker":
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    jar_files = ["flink-sql-connector-kafka-1.17.1.jar"]
    jar_paths = tuple([f"file://{os.path.join(CURRENT_DIR, 'jars', name)}" for name in jar_files])
    logging.info(f"adding local jars - {', '.join(jar_files)}")
    env.add_jars(*jar_paths)

transaction_source = (
    KafkaSource.builder()
    .set_bootstrap_servers(BOOTSTRAP_SERVERS)
    .set_topics("transactions")
    .set_group_id("group.transactions")
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    # .set_value_only_deserializer(
    #     JsonRowDeserializationSchema.builder().type_info(Transaction.get_value_type()).build()
    # )
    .build()
)

transaction_stream = (
    env.from_source(
        transaction_source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
        "transaction_source",
    )
    .set_parallelism(5)
    # .map(Transaction.from_str)
    # .map(Transaction.from_row)
    .name("transaction_source")
    .uid("transaction_source")
)

transaction_stream.print().set_parallelism(1)

env.execute("transaction_importer")
