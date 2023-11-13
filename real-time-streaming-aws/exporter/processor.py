import os
import re
import json

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment

RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "LOCAL")  # LOCAL or DOCKER
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS")  # overwrite app config

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.enable_checkpointing(60000)

if RUNTIME_ENV == "LOCAL":
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    PARENT_DIR = os.path.dirname(CURRENT_DIR)
    APPLICATION_PROPERTIES_FILE_PATH = os.path.join(
        CURRENT_DIR, "application_properties.json"
    )
    JAR_FILES = ["lab3-pipeline-1.0.0.jar"]
    JAR_PATHS = tuple(
        [f"file://{os.path.join(PARENT_DIR, 'jars', name)}" for name in JAR_FILES]
    )
    print(JAR_PATHS)
    env.add_jars(*JAR_PATHS)
else:
    APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"

table_env = StreamTableEnvironment.create(stream_execution_environment=env)


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        raise RuntimeError(
            f"A file at '{APPLICATION_PROPERTIES_FILE_PATH}' was not found"
        )


def property_map(props: dict, property_group_id: str):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


def inject_security_opts(opts: dict, bootstrap_servers: str):
    if re.search("9098$", bootstrap_servers):
        opts = {
            **opts,
            **{
                "properties.security.protocol": "SASL_SSL",
                "properties.sasl.mechanism": "AWS_MSK_IAM",
                "properties.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "properties.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            },
        }
    return ", ".join({f"'{k}' = '{v}'" for k, v in opts.items()})


def create_source_table(table_name: str, topic_name: str, bootstrap_servers: str):
    opts = {
        "connector": "kafka",
        "topic": topic_name,
        "properties.bootstrap.servers": bootstrap_servers,
        "properties.group.id": "soruce-group",
        "format": "json",
        "scan.startup.mode": "latest-offset",
    }

    stmt = f"""
    CREATE TABLE {table_name} (
        id                  VARCHAR,
        vendor_id           INT,
        pickup_date         VARCHAR,
        pickup_datetime     AS TO_TIMESTAMP(REPLACE(pickup_date, 'T', ' ')),
        dropoff_date        VARCHAR,
        dropoff_datetime    AS TO_TIMESTAMP(REPLACE(dropoff_date, 'T', ' ')),
        passenger_count     INT,
        pickup_longitude    VARCHAR,
        pickup_latitude     VARCHAR,
        dropoff_longitude   VARCHAR,
        dropoff_latitude    VARCHAR,
        store_and_fwd_flag  VARCHAR,
        gc_distance         INT,
        trip_duration       INT,
        google_distance     INT,
        google_duration     INT
    ) WITH (
        {inject_security_opts(opts, bootstrap_servers)}
    )
    """
    print(stmt)
    return stmt


def create_sink_table(table_name: str, file_path: str):
    stmt = f"""
    CREATE TABLE {table_name} (
        id                  VARCHAR,
        vendor_id           INT,
        pickup_datetime     TIMESTAMP,
        dropoff_datetime    TIMESTAMP,
        passenger_count     INT,
        pickup_longitude    VARCHAR,
        pickup_latitude     VARCHAR,
        dropoff_longitude   VARCHAR,
        dropoff_latitude    VARCHAR,
        store_and_fwd_flag  VARCHAR,
        gc_distance         INT,
        trip_duration       INT,
        google_distance     INT,
        google_duration     INT,
        `year`              VARCHAR,
        `month`             VARCHAR,
        `date`              VARCHAR,
        `hour`              VARCHAR
    ) PARTITIONED BY (`year`, `month`, `date`, `hour`) WITH (
        'connector'= 'filesystem',
        'path' = '{file_path}',
        'format' = 'parquet',
        'sink.partition-commit.delay'='1 h',
        'sink.partition-commit.policy.kind'='success-file'
    )
    """
    print(stmt)
    return stmt


def create_print_table(table_name: str):
    stmt = f"""
    CREATE TABLE {table_name} (
        id                  VARCHAR,
        vendor_id           INT,
        pickup_datetime     TIMESTAMP,
        dropoff_datetime    TIMESTAMP,
        passenger_count     INT,
        pickup_longitude    VARCHAR,
        pickup_latitude     VARCHAR,
        dropoff_longitude   VARCHAR,
        dropoff_latitude    VARCHAR,
        store_and_fwd_flag  VARCHAR,
        gc_distance         INT,
        trip_duration       INT,
        google_distance     INT,
        google_duration     INT,
        `year`              VARCHAR,
        `month`             VARCHAR,
        `date`              VARCHAR,
        `hour`              VARCHAR
    ) WITH (
        'connector'= 'print'
    )
    """
    print(stmt)
    return stmt


def set_insert_sql(source_table_name: str, sink_table_name: str):
    stmt = f"""
    INSERT INTO {sink_table_name}
    SELECT
        id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        store_and_fwd_flag,
        gc_distance,
        trip_duration,
        google_distance,
        google_duration,
        DATE_FORMAT(pickup_datetime, 'yyyy') AS `year`,
        DATE_FORMAT(pickup_datetime, 'MM') AS `month`,
        DATE_FORMAT(pickup_datetime, 'dd') AS `date`,
        DATE_FORMAT(pickup_datetime, 'HH') AS `hour`
    FROM {source_table_name}
    """
    print(stmt)
    return stmt


def main():
    #### map source/sink properties
    props = get_application_properties()
    ## source
    source_property_group_key = "source.config.0"
    source_properties = property_map(props, source_property_group_key)
    print(">> source properties")
    print(source_properties)
    source_table_name = source_properties["table.name"]
    source_file_path = source_properties["file.path"]
    ## sink
    sink_property_group_key = "sink.config.0"
    sink_properties = property_map(props, sink_property_group_key)
    print(">> sink properties")
    print(sink_properties)
    sink_table_name = sink_properties["table.name"]
    sink_topic_name = sink_properties["topic.name"]
    sink_bootstrap_servers = BOOTSTRAP_SERVERS or sink_properties["bootstrap.servers"]
    ## print
    print_table_name = "sink_print"
    #### create tables
    table_env.execute_sql(create_source_table(source_table_name, source_file_path))
    table_env.execute_sql(
        create_sink_table(sink_table_name, sink_topic_name, sink_bootstrap_servers)
    )
    table_env.execute_sql(create_print_table(print_table_name))
    #### insert into sink tables
    if RUNTIME_ENV == "LOCAL":
        statement_set = table_env.create_statement_set()
        statement_set.add_insert_sql(set_insert_sql(source_table_name, sink_table_name))
        statement_set.add_insert_sql(set_insert_sql(print_table_name, sink_table_name))
        statement_set.execute().wait()
    else:
        table_result = table_env.execute_sql(
            set_insert_sql(source_table_name, sink_table_name)
        )
        print(table_result.get_job_client().get_job_status())


if __name__ == "__main__":
    main()
