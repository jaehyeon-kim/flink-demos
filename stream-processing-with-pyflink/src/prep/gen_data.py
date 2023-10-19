import os
import logging

from pyflink.table import EnvironmentSettings, TableEnvironment

from tbl_src import Transaction, Customer, Account, assign_operation

if __name__ == "__main__":
    """
    ## local execution
    python src/prep/gen_data.py

    ## cluster execution
    docker exec jobmanager /opt/flink/bin/flink run \
        --python /tmp/src/prep/gen_data.py \
        --pyFiles file:///tmp/src/prep/tbl_src.py \
        -d
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
    BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")

    env_settings = EnvironmentSettings.in_streaming_mode()
    tbl_env = TableEnvironment.create(env_settings)
    tbl_env.create_temporary_system_function("assign_operation", assign_operation)
    if RUNTIME_ENV != "docker":
        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        jar_files = ["flink-faker-0.5.3.jar", "flink-sql-connector-kafka-1.17.0.jar"]
        jar_paths = ";".join(
            [
                f"file://{os.path.join(os.path.dirname(CURRENT_DIR), 'jars', name)}"
                for name in jar_files
            ]
        )
        logging.info(f"adding pipeline jars - {jar_paths}")
        tbl_env.get_config().set("pipeline.jars", jar_paths)

    statement_set = tbl_env.create_statement_set()

    Customer.generate(
        tbl_env,
        statement_set,
        BOOTSTRAP_SERVERS,
        sink_parallelism=1,
        print_only=False,
        row_per_sec=1,
    )
    Account.generate(
        tbl_env,
        statement_set,
        BOOTSTRAP_SERVERS,
        sink_parallelism=1,
        print_only=False,
        row_per_sec=1,
    )
    Transaction.generate(
        tbl_env,
        statement_set,
        BOOTSTRAP_SERVERS,
        sink_parallelism=1,
        print_only=False,
        row_per_sec=3,
    )

    if RUNTIME_ENV == "local":
        statement_set.execute().wait()
    else:
        statement_set.execute()
