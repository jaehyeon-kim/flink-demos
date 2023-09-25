import os
import csv
import io
import datetime
import dataclasses
import json
import typing
import logging
import pandas as pd

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, UnknownTopicOrPartitionError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@dataclasses.dataclass
class Transaction:
    transactionId: str
    accountId: str
    customerId: str
    eventTime: int
    eventTimeFormatted: str
    type: str
    operation: str
    amount: float
    balance: float
    dataTime: int
    dataTimeFormatted: str

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def init(cls, row: typing.Dict[str, str]):
        date_part, time_part = tuple(row["fulldatewithtime"].split("T"))
        data_date = (pd.to_datetime(date_part) + pd.to_timedelta(time_part)).to_pydatetime()
        return cls(
            row["trans_id"],
            row["account_id"],
            row["customer_id"],
            int(
                (datetime.datetime.now() - datetime.datetime.fromtimestamp(0)).total_seconds()
                * 1000
            ),
            datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds"),
            row["type"],
            row["operation"],
            float(row["amount"]),
            float(row["balance"]),
            int((data_date - datetime.datetime.fromtimestamp(0)).total_seconds() * 1000),
            data_date.isoformat(sep=" ", timespec="milliseconds"),
        )

    @staticmethod
    def read_line(item_str: str):
        items = []
        logging.info(f"reading transaction records from item string...")
        csv_file = io.StringIO(item_str)
        reader = csv.DictReader(csv_file)
        for row in reader:
            items.append(Transaction.init(row))
        logging.info(f"{len(items)} records are read")
        return items

    @staticmethod
    def read(file_path: str):
        items = []
        logging.info(f"reading transaction records from {file_path}...")
        with open(file_path) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                items.append(Transaction.init(row))
        logging.info(f"{len(items)} records are read")
        return items


@dataclasses.dataclass
class Account:
    accountId: str
    districtId: str
    frequency: str
    creationDate: str
    updateTime: int

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def init(cls, row: typing.Dict[str, str]):
        return cls(
            row["account_id"],
            row["district_id"],
            row["frequency"],
            row["parseddate"],
            int(
                (datetime.datetime.now() - datetime.datetime.fromtimestamp(0)).total_seconds()
                * 1000
            ),
        )

    @staticmethod
    def read(file_path: str):
        items = []
        logging.info(f"reading account records from {file_path}...")
        with open(file_path) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                items.append(Account.init(row))
        logging.info(f"{len(items)} records are read")
        return items


@dataclasses.dataclass
class Customer:
    customerId: str
    sex: str
    social: str
    fullName: str
    phone: str
    email: str
    address1: str
    address2: str
    city: str
    state: str
    zipcode: str
    districtId: str
    birthDate: str
    updateTime: int

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def init(cls, row: typing.Dict[str, str]):
        return cls(
            row["customer_id"],
            row["sex"],
            row["social"],
            f'{row["first"]} {row["middle"]} {row["last"]}',
            row["phone"],
            row["email"],
            row["address_1"],
            row["address_2"],
            row["city"],
            row["state"],
            row["zipcode"],
            row["district_id"],
            row["date"],
            int(
                (datetime.datetime.now() - datetime.datetime.fromtimestamp(0)).total_seconds()
                * 1000
            ),
        )

    @staticmethod
    def read(file_path: str):
        items = []
        logging.info(f"reading customer records from {file_path}...")
        with open(file_path) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                items.append(Customer.init(row))
        logging.info(f"{len(items)} records are read")
        return items


class KafkaClient:
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = self.create_admin()
        self.producer_client = self.create_producer()
        self.topic_map = {
            "Transaction": "transactions",
            "Account": "accounts",
            "Customer": "customers",
            "SmallTransaction": "",
        }

    def create_admin(self):
        return KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

    def create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda v: json.dumps(v, default=self.serialize).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, default=self.serialize).encode("utf-8"),
        )

    def delete_topics(self, topic_names: typing.List[str]):
        for name in topic_names:
            try:
                self.admin_client.delete_topics([name])
            except UnknownTopicOrPartitionError:
                pass
            except Exception as err:
                raise RuntimeError(f"fails to delete topic - {name}") from err

    def create_topics(self, topics: typing.List[NewTopic], to_recreate: bool = True):
        if to_recreate:
            self.delete_topics([t.name for t in topics])
        for topic in topics:
            try:
                resp = self.admin_client.create_topics([topic])
                name, error_code, error_message = resp.topic_errors[0]
                logging.info(
                    f"topic created, name - {name}, error code - {error_code}, error message - {error_message}"
                )
            except KafkaError as err:
                raise RuntimeError(
                    f"fails to create topics - {', '.join(t.name for t in topics)}"
                ) from err
        logging.info(f"topics created successfully - {', '.join([t.name for t in topics])}")

    def send(
        self,
        items: typing.List[typing.Union[Transaction, Account, Customer]],
        topic_info: typing.Tuple,
    ):
        topic_name, key_attr = topic_info
        topics = (
            [topic_name]
            if topic_name != "transactions"
            else [topic_name, f"{topic_name}.credits", f"{topic_name}.debits"]
        )
        logging.info(f"sending records to topic - {', '.join(topics)}...")
        for item in items:
            try:
                value = item.asdict()
                key = {}
                key[key_attr] = value[key_attr]
                if topic_name == "transactions":
                    extra_topic_suffix = "credits" if value["type"] == "Credit" else "debits"
                    self.producer_client.send(
                        f"{topic_name}.{extra_topic_suffix}", key=key, value=value
                    )
                self.producer_client.send(topic_name, key=key, value=value)
            except Exception as err:
                raise RuntimeError("fails to send a message") from err
        self.producer_client.flush()

    def serialize(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, datetime.date):
            return str(obj)
        return obj


if __name__ == "__main__":
    client = KafkaClient("localhost:19092")

    ## create topics
    topics = [
        NewTopic(name="transactions", num_partitions=5, replication_factor=1),
        NewTopic(name="transactions.debits", num_partitions=5, replication_factor=1),
        NewTopic(name="transactions.credits", num_partitions=5, replication_factor=1),
        NewTopic(name="small-transactions", num_partitions=1, replication_factor=1),
        NewTopic(
            name="customers",
            num_partitions=1,
            replication_factor=1,
            topic_configs={"cleanup.policy": "compact", "retention.ms": 600000},
        ),
        NewTopic(
            name="accounts",
            num_partitions=1,
            replication_factor=1,
            topic_configs={"cleanup.policy": "compact", "retention.ms": 600000},
        ),
    ]

    client.create_topics(topics, to_recreate=True)

    ## send messages
    DATA_PATH = os.getenv("DATA_PATH", ".external/data")
    client.send(
        Account.read(file_path=os.path.join(DATA_PATH, "accounts.csv")),
        topic_info=("accounts", "accountId"),
    )
    client.send(
        Customer.read(file_path=os.path.join(DATA_PATH, "customers.csv")),
        topic_info=("customers", "customerId"),
    )
    client.send(
        Transaction.read(file_path=os.path.join(DATA_PATH, "transactions.csv")),
        topic_info=("transactions", "transactionId"),
    )
    client.send(
        Transaction.read(file_path=os.path.join(DATA_PATH, "transactions-small.csv")),
        topic_info=("small-transactions", "transactionId"),
    )
