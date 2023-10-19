import random
import re

from pyflink.table import TableEnvironment, StatementSet, DataTypes
from pyflink.table.udf import udf


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def assign_operation(transaction_type: str):
    if transaction_type == "Credit":
        operations = ["", "Collection from Another Bank", "Credit in Cash"]
    else:
        operations = ["Cash Withdrawal", "Credit Card Withdrawal", "Remittance to Another Bank"]
    return next(iter(random.sample(operations, 1)))


class Transaction:
    @staticmethod
    def generate(
        tbl_env: TableEnvironment,
        statement_set: StatementSet,
        bootstrap_servers: str,
        print_only: bool = False,
        row_per_sec: int = 1,
    ):
        tbl_env.execute_sql(Transaction.source_ddl(rows_per_sec=row_per_sec))
        tbl_env.execute_sql(
            Transaction.sink_ddl(bootrap_servers=bootstrap_servers, print_only=print_only)
        )
        statement_set.add_insert_sql(Transaction.insert_qry())

    @staticmethod
    def source_ddl(rows_per_sec: int = 1):
        stmt = """
        CREATE TABLE transactions_source (
            `transaction_id`    STRING,
            `account_number`    INT,
            `customer_number`   INT,
            `event_timestamp`   TIMESTAMP(3),
            `type`              STRING,
            `amount`            INT
        )
        WITH (
            'connector' = 'faker', 
            'rows-per-second' = '<rows_per_sec>',
            'fields.transaction_id.expression' = '#{Internet.uuid}',
            'fields.account_number.expression' = '#{number.numberBetween ''0'',''10000''}',
            'fields.customer_number.expression' = '#{number.numberBetween ''0'',''5000''}',
            'fields.event_timestamp.expression' = '#{date.past ''15'',''2'',''SECONDS''}',
            'fields.type.expression' = '#{Options.option ''Credit'',''Debit'')}',
            'fields.amount.expression' = '#{number.numberBetween ''0'',''5000''}'
        );
            """
        stmt = re.sub("<rows_per_sec>", str(rows_per_sec), stmt)
        return stmt

    @staticmethod
    def sink_ddl(bootrap_servers: str, print_only: bool = False):
        if print_only:
            opts = {"connector": "print"}
        else:
            opts = {
                "connector": "kafka",
                "topic": "transactions",
                "properties.bootstrap.servers": bootrap_servers,
                "properties.allow.auto.create.topics": "false",
                "key.format": "json",
                "key.fields": "transaction_id",
                "value.format": "json",
            }
        stmt = f"""
        CREATE TABLE transactions_sink (
            `transaction_id`    STRING,
            `account_id`        STRING,
            `customer_id`       STRING,
            `event_time`        BIGINT,
            `event_timestamp`   TIMESTAMP(3),
            `type`              STRING,
            `operation`         STRING,
            `amount`            INT
        )
        WITH (
            {", ".join({f"'{k}' = '{v}'" for k, v in opts.items()})}
        );
            """
        return stmt

    @staticmethod
    def insert_qry():
        return """
        INSERT INTO transactions_sink
        SELECT
            transaction_id,
            'A' || LPAD(CAST(account_number AS STRING), 8, '0') AS account_id,
            'C' || LPAD(CAST(customer_number AS STRING), 8, '0') AS customer_id,
            (1000 * EXTRACT(EPOCH FROM event_timestamp)) + EXTRACT(MILLISECOND FROM event_timestamp) AS event_time,
            event_timestamp,
            type,
            assign_operation(type) AS operation,
            amount
        FROM transactions_source;
               """


class Customer:
    @staticmethod
    def generate(
        tbl_env: TableEnvironment,
        statement_set: StatementSet,
        bootstrap_servers: str,
        print_only: bool = False,
        row_per_sec: int = 1,
    ):
        tbl_env.execute_sql(Customer.source_ddl(rows_per_sec=row_per_sec))
        tbl_env.execute_sql(
            Customer.sink_ddl(bootrap_servers=bootstrap_servers, print_only=print_only)
        )
        statement_set.add_insert_sql(Customer.insert_qry())

    @staticmethod
    def source_ddl(rows_per_sec: int = 1):
        stmt = """
        CREATE TABLE customers_source (
            `customer_number`   INT,
            `sex`               STRING,
            `dob`               DATE,
            `first_name`        STRING,
            `last_name`         STRING,
            `update_timestamp`  TIMESTAMP(3)
        )
        WITH (
            'connector' = 'faker', 
            'rows-per-second' = '<rows_per_sec>',
            'fields.customer_number.expression' = '#{number.numberBetween ''0'',''10''}',
            'fields.sex.expression' = '#{Options.option ''Male'',''Female'')}',
            'fields.dob.expression' = '#{date.birthday}',
            'fields.first_name.expression' = '#{Name.firstName}',
            'fields.last_name.expression' = '#{Name.lastName}',
            'fields.update_timestamp.expression' = '#{date.past ''15'',''0'',''SECONDS''}'
        );
            """
        stmt = re.sub("<rows_per_sec>", str(rows_per_sec), stmt)
        return stmt

    @staticmethod
    def sink_ddl(bootrap_servers: str, print_only: bool = False):
        if print_only:
            opts = {"connector": "print"}
        else:
            opts = {
                "connector": "kafka",
                "topic": "customers",
                "properties.bootstrap.servers": bootrap_servers,
                "properties.allow.auto.create.topics": "false",
                "key.format": "json",
                "key.fields": "customer_id",
                "value.format": "json",
            }
        stmt = f"""
        CREATE TABLE customers (
            `customer_id`         STRING,
            `sex`                 STRING,
            `dob`                 DATE,
            `first_name`          STRING,
            `last_name`           STRING,
            `email_address`       STRING,
            `update_time`         BIGINT,
            `update_timestamp`    TIMESTAMP(3)
        )
        WITH (
            {", ".join({f"'{k}' = '{v}'" for k, v in opts.items()})}
        );
            """
        return stmt

    @staticmethod
    def insert_qry():
        return """
        INSERT INTO customers
        SELECT
            'C' || LPAD(CAST(customer_number AS STRING), 8, '0') AS customer_id,
            sex,
            dob,
            first_name,
            last_name,
            LOWER(first_name) || '.' || LOWER(last_name) || '@email.com' AS email_address,
            (1000 * EXTRACT(EPOCH FROM update_timestamp)) + EXTRACT(MILLISECOND FROM update_timestamp) AS update_time,
            update_timestamp
        FROM customers_source;
               """


class Account:
    @staticmethod
    def generate(
        tbl_env: TableEnvironment,
        statement_set: StatementSet,
        bootstrap_servers: str,
        print_only: bool = False,
        row_per_sec: int = 1,
    ):
        tbl_env.execute_sql(Account.source_ddl(rows_per_sec=row_per_sec))
        tbl_env.execute_sql(
            Account.sink_ddl(bootrap_servers=bootstrap_servers, print_only=print_only)
        )
        statement_set.add_insert_sql(Account.insert_qry())

    @staticmethod
    def source_ddl(rows_per_sec: int = 1):
        stmt = """
        CREATE TABLE accounts_source (
            `account_number`    INT,
            `district_number`   INT,
            `frequency`         STRING,
            `update_timestamp`  TIMESTAMP(3)
        )
        WITH (
            'connector' = 'faker', 
            'rows-per-second' = '<rows_per_sec>',
            'fields.account_number.expression' = '#{number.numberBetween ''0'',''10000''}',
            'fields.district_number.expression' = '#{number.numberBetween ''0'',''100''}',
            'fields.frequency.expression' = '#{Options.option ''Weekly Issuance'',''Monthly Issuance'',''Issuance After Transaction'')}',
            'fields.update_timestamp.expression' = '#{date.past ''15'',''0'',''SECONDS''}'
        );
            """
        stmt = re.sub("<rows_per_sec>", str(rows_per_sec), stmt)
        return stmt

    @staticmethod
    def sink_ddl(bootrap_servers: str, print_only: bool = False):
        if print_only:
            opts = {"connector": "print"}
        else:
            opts = {
                "connector": "kafka",
                "topic": "accounts",
                "properties.bootstrap.servers": bootrap_servers,
                "properties.allow.auto.create.topics": "false",
                "key.format": "json",
                "key.fields": "account_id",
                "value.format": "json",
            }
        stmt = f"""
        CREATE TABLE accounts_sink (
            `account_id`         STRING,
            `district_id`        STRING,
            `frequency`          STRING,
            `update_time`        BIGINT,
            `update_timestamp`   TIMESTAMP(3)
        )
        WITH (
            {", ".join({f"'{k}' = '{v}'" for k, v in opts.items()})}
        );
            """
        return stmt

    @staticmethod
    def insert_qry():
        return """
        INSERT INTO accounts_sink
        SELECT
            'A' || LPAD(CAST(account_number AS STRING), 8, '0') AS account_id,
            CAST(district_number AS STRING) AS district_id,
            frequency,
            (1000 * EXTRACT(EPOCH FROM update_timestamp)) + EXTRACT(MILLISECOND FROM update_timestamp) AS update_time,
            update_timestamp
        FROM accounts_source;
               """
