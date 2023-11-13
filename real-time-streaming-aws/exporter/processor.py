import os
import re
import json

from pyflink.table import EnvironmentSettings, TableEnvironment

RUNTIME_ENV = os.environ.get("RUNTIME_ENV", "LOCAL")  # LOCAL or DOCKER
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS")  # overwrite app config

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

if RUNTIME_ENV == "LOCAL":
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    PARENT_DIR = os.path.dirname(CURRENT_DIR)
    PIPELINE_JAR = "lab3-pipeline-1.0.0.jar"
    APPLICATION_PROPERTIES_FILE_PATH = os.path.join(
        CURRENT_DIR, "application_properties.json"
    )
    print(f"file://{os.path.join(PARENT_DIR, 'package', 'lib', PIPELINE_JAR)}")
    table_env.get_config().set(
        "pipeline.jars",
        f"file://{os.path.join(PARENT_DIR, 'package', 'lib', PIPELINE_JAR)}",
    )
else:
    APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"


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
        "format": "json",
        "key.format": "json",
        "key.fields": "id",
        "properties.allow.auto.create.topics": "true",
    }

    stmt = f"""
    CREATE TABLE {table_name} (
        id                  VARCHAR,
        vendor_id           INT,
        pickup_datetime     VARCHAR,
        dropoff_datetime    VARCHAR,
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
        pickup_datetime     VARCHAR,
        dropoff_datetime    VARCHAR,
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
        'connector'= 'filesystem',
        'format' = 'csv',
        'path' = '{file_path}'
    )
    """
    print(stmt)
    return stmt


def create_print_table(table_name: str):
    stmt = f"""
    CREATE TABLE sink_print (
        id                  VARCHAR,
        vendor_id           INT,
        pickup_datetime     VARCHAR,
        dropoff_datetime    VARCHAR,
        passenger_count     INT,
        pickup_longitude    VARCHAR,
        pickup_latitude     VARCHAR,
        dropoff_longitude   VARCHAR,
        dropoff_latitude    VARCHAR,
        store_and_fwd_flag  VARCHAR,
        gc_distance         DOUBLE,
        trip_duration       INT,
        google_distance     VARCHAR,
        google_duration     VARCHAR
    ) WITH (
        'connector'= 'print'
    )
    """
    print(stmt)
    return stmt


def main():
    """
    ## 1. prep
    docker build -t real-time-streaming-aws:1.17.1 .
    ## 2. start flink (and kafka) cluster
    # with local Kafka cluster
    docker-compose -f compose-local-kafka.yml up -d
    # with MSK
    docker-compose -f compose-msk.yml up -d
    ## 3. run pyflink app
    # local
    RUNTIME_ENV=LOCAL python loader/processor.py
    # on flink cluster on docker
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /etc/flink/processor.py \
        --jarfile /etc/package/lib/pyflink-pipeline-1.0.0.jar \
        -d
    """
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
        source_table = table_env.from_path(source_table_name)
        statement_set = table_env.create_statement_set()
        statement_set.add_insert(sink_table_name, source_table)
        statement_set.add_insert(print_table_name, source_table)
        statement_set.execute().wait()
    else:
        table_result = table_env.execute_sql(
            f"INSERT INTO {sink_table_name} SELECT * FROM {source_table_name}"
        )
        print(table_result.get_job_client().get_job_status())


if __name__ == "__main__":
    main()
