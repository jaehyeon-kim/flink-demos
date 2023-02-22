import os

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9093")
# https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/kafka/
FLINK_SQL_CONNECTOR_KAFKA = "flink-sql-connector-kafka-1.16.0.jar"

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
# https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/python/dependency_management/
kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), FLINK_SQL_CONNECTOR_KAFKA)
table_env.get_config().set("pipeline.jars", f"file://{kafka_jar}")

## create kafka source table
table_env.execute_sql(
    f"""
    CREATE TABLE sales_items (
        `seller_id` VARCHAR,
        `product` VARCHAR,
        `quantity` INT,
        `product_price` DOUBLE,
        `sale_ts` BIGINT,
        `proctime` AS PROCTIME()
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

# sliding window aggregate calculation of revenue per seller
windowed_rev = table_env.sql_query(
    """
    SELECT
        seller_id,
        HOP_START(proctime, INTERVAL '10' SECONDS, INTERVAL '30' SECONDS) AS window_start,
        HOP_END(proctime, INTERVAL '10' SECONDS, INTERVAL '30' SECONDS) AS window_end,
        SUM(quantity * product_price) AS window_sales
    FROM sales_items
    GROUP BY
        HOP(proctime, INTERVAL '10' SECONDS, INTERVAL '30' SECONDS), seller_id
    """
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
        'topic' = 'processed_sales2',
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
