import os

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")
# https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/kafka/
version_map = {"15": "1.15.4", "16": "1.16.0"}
FLINK_VERSION = version_map[os.getenv("MINOR_VERSION", "15")]
FLINK_SQL_CONNECTOR_KAFKA = f"flink-sql-connector-kafka-{FLINK_VERSION}.jar"

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/python/dependency_management/
kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), FLINK_SQL_CONNECTOR_KAFKA)
table_env.get_config().set("pipeline.jars", f"file://{kafka_jar}")
table_env.get_config().set("table.exec.source.idle-timeout", "1000")

## create kafka source table
table_env.execute_sql(
    f"""
    CREATE TABLE sales_items (
        `seller_id` VARCHAR,
        `product` VARCHAR,
        `quantity` INT,
        `product_price` DOUBLE,
        `sale_ts` BIGINT,
        `proctime` AS PROCTIME(),
        `evttime` AS TO_TIMESTAMP_LTZ(`sale_ts`, 3),
        WATERMARK FOR `evttime` AS `evttime` - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'sales_items',
        'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
        'properties.group.id' = 'source-demo',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
)
tbl = table_env.from_path("sales_items")
print("\nsource schema")
tbl.print_schema()

## tumbling window aggregate calculation of revenue per seller
windowed_rev = (
    tbl.window(Tumble.over(lit(10).seconds).on(col("evttime")).alias("w"))
    .group_by(col("w"), col("seller_id"))
    .select(
        col("seller_id"),
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        (col("quantity") * col("product_price")).sum.alias("window_sales"),
    )
)

print("\nprocess sink schema")
windowed_rev.print_schema()

table_env.execute_sql(
    f"""
    CREATE TABLE print (
        `seller_id` VARCHAR,
        `window_start` TIMESTAMP(3),
        `window_end` TIMESTAMP(3),
        `window_sales` DOUBLE
    ) WITH (
        'connector' = 'print'
    )
    """
)

table_env.execute_sql(
    f"""
    CREATE TABLE processed_sales (
        `seller_id` VARCHAR,
        `window_start` TIMESTAMP(3),
        `window_end` TIMESTAMP(3),
        `window_sales` DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'processed_sales',
        'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
)

# windowed_rev.execute_insert("processed_sales").wait()
statement_set = table_env.create_statement_set()
statement_set.add_insert("print", windowed_rev)
statement_set.add_insert("processed_sales", windowed_rev)
statement_set.execute().wait()
