import os
import json
import logging

import kafka  # check if --pyFiles works
from pyflink.table import EnvironmentSettings, TableEnvironment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "KDA")  # KDA, LOCAL
logging.info(f"runtime environment - {RUNTIME_ENV}...")

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

APPLICATION_PROPERTIES_FILE_PATH = (
    "/etc/flink/application_properties.json"  # on kda or docker-compose
    if RUNTIME_ENV != "LOCAL"
    else "application_properties.json"
)

if RUNTIME_ENV != "KDA":
    # on non-KDA, multiple jar files can be passed after being delimited by a semicolon
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    PIPELINE_JAR = "flink-sql-connector-kafka-1.15.2.jar"
    table_env.get_config().set(
        "pipeline.jars", f"file://{os.path.join(CURRENT_DIR, 'package', 'lib', PIPELINE_JAR)}"
    )
logging.info(f"app properties file path - {APPLICATION_PROPERTIES_FILE_PATH}")


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        raise RuntimeError(f"A file at '{APPLICATION_PROPERTIES_FILE_PATH}' was not found")


def property_map(props: dict, property_group_id: str):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


def create_flagged_account_source_table(
    table_name: str, topic_name: str, bootstrap_servers: str, startup_mode: str
):
    stmt = f"""
    CREATE TABLE {table_name} (
        account_id BIGINT,
        flag_date TIMESTAMP(3)
    )
    WITH (
        'connector' = 'kafka',
        'topic' = '{topic_name}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id' = 'flagged-account-source-group',
        'format' = 'json',
        'scan.startup.mode' = '{startup_mode}'
    )
    """
    logging.info("flagged account source table statement...")
    logging.info(stmt)
    return stmt


def create_transaction_source_table(
    table_name: str, topic_name: str, bootstrap_servers: str, startup_mode: str
):
    stmt = f"""
    CREATE TABLE {table_name} (
        account_id BIGINT,
        customer_id VARCHAR(15),
        merchant_type VARCHAR(8),
        transaction_id VARCHAR(16),
        transaction_type VARCHAR(20),
        transaction_amount DECIMAL(10,2),
        transaction_date TIMESTAMP(3)
    )
    WITH (
        'connector' = 'kafka',
        'topic' = '{topic_name}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id' = 'transaction-source-group',
        'format' = 'json',
        'scan.startup.mode' = '{startup_mode}'
    )
    """
    logging.info("transaction source table statement...")
    logging.info(stmt)
    return stmt


def create_flagged_transaction_sink_table(table_name: str, topic_name: str, bootstrap_servers: str):
    stmt = f"""
    CREATE TABLE {table_name} (
        account_id BIGINT,
        customer_id VARCHAR(15),
        merchant_type VARCHAR(8),
        transaction_id VARCHAR(16),
        transaction_type VARCHAR(20),
        transaction_amount DECIMAL(10,2),
        transaction_date TIMESTAMP(3)
    )
    WITH (
        'connector' = 'kafka',
        'topic' = '{topic_name}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',        
        'format' = 'json',
        'key.format' = 'json',
        'key.fields' = 'account_id;transaction_id',
        'properties.allow.auto.create.topics' = 'true'
    )
    """
    logging.info("transaction sink table statement...")
    logging.info(stmt)
    return stmt


def create_print_table(table_name: str):
    return f"""
    CREATE TABLE {table_name} (
        account_id BIGINT,
        customer_id VARCHAR(15),
        merchant_type VARCHAR(8),
        transaction_id VARCHAR(16),
        transaction_type VARCHAR(20),
        transaction_amount DECIMAL(10,2),
        transaction_date TIMESTAMP(3)
    )
    WITH (
        'connector' = 'print'
    )
    """


def insert_into_stmt(insert_from_tbl: str, compare_with_tbl: str, insert_into_tbl: str):
    return f"""
    INSERT INTO {insert_into_tbl}
        SELECT l.*
        FROM {insert_from_tbl} AS l
        JOIN {compare_with_tbl} AS r
            ON l.account_id = r.account_id 
            AND l.transaction_date > r.flag_date
    """


def main():
    ## map consumer/producer properties
    props = get_application_properties()
    # consumer for flagged account
    consumer_0_property_group_key = "consumer.config.0"
    consumer_0_properties = property_map(props, consumer_0_property_group_key)
    consumer_0_table_name = consumer_0_properties["table.name"]
    consumer_0_topic_name = consumer_0_properties["topic.name"]
    consumer_0_bootstrap_servers = consumer_0_properties["bootstrap.servers"]
    consumer_0_startup_mode = consumer_0_properties["startup.mode"]
    # consumer for transactions
    consumer_1_property_group_key = "consumer.config.1"
    consumer_1_properties = property_map(props, consumer_1_property_group_key)
    consumer_1_table_name = consumer_1_properties["table.name"]
    consumer_1_topic_name = consumer_1_properties["topic.name"]
    consumer_1_bootstrap_servers = consumer_1_properties["bootstrap.servers"]
    consumer_1_startup_mode = consumer_1_properties["startup.mode"]
    # producer
    producer_0_property_group_key = "producer.config.0"
    producer_0_properties = property_map(props, producer_0_property_group_key)
    producer_0_table_name = producer_0_properties["table.name"]
    producer_0_topic_name = producer_0_properties["topic.name"]
    producer_0_bootstrap_servers = producer_0_properties["bootstrap.servers"]
    # print
    print_table_name = "sink_print"
    ## create the source table for flagged accounts
    table_env.execute_sql(
        create_flagged_account_source_table(
            consumer_0_table_name,
            consumer_0_topic_name,
            consumer_0_bootstrap_servers,
            consumer_0_startup_mode,
        )
    )
    table_env.from_path(consumer_0_table_name).print_schema()
    ## create the source table for transactions
    table_env.execute_sql(
        create_transaction_source_table(
            consumer_1_table_name,
            consumer_1_topic_name,
            consumer_1_bootstrap_servers,
            consumer_1_startup_mode,
        )
    )
    table_env.from_path(consumer_1_table_name).print_schema()
    ## create sink table for flagged accounts
    table_env.execute_sql(
        create_flagged_transaction_sink_table(
            producer_0_table_name, producer_0_topic_name, producer_0_bootstrap_servers
        )
    )
    table_env.from_path(producer_0_table_name).print_schema()
    table_env.execute_sql(create_print_table("sink_print"))
    ## insert into sink tables
    if RUNTIME_ENV == "LOCAL":
        statement_set = table_env.create_statement_set()
        statement_set.add_insert_sql(
            insert_into_stmt(consumer_1_table_name, consumer_0_table_name, producer_0_table_name)
        )
        statement_set.add_insert_sql(
            insert_into_stmt(consumer_1_table_name, consumer_0_table_name, print_table_name)
        )
        statement_set.execute().wait()
    else:
        table_result = table_env.execute_sql(
            insert_into_stmt(consumer_1_table_name, consumer_0_table_name, producer_0_table_name)
        )
        logging.info(table_result.get_job_client().get_job_status())


if __name__ == "__main__":
    main()
