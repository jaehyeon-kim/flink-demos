import os
import csv
import datetime
import dataclasses
import json
import typing
import logging

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, UnknownTopicOrPartitionError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DATA_PATH = os.getenv("DATA_PATH", ".external/data")


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

    def asdict(self):
        return dataclasses.asdict(self)

    @staticmethod
    def read(file_path: str = os.path.join(DATA_PATH, "transactions.csv")):
        items = []
        logging.info(f"reading transaction records from {file_path}...")
        with open(file_path) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                items.append(
                    Transaction(
                        row["trans_id"],
                        row["account_id"],
                        row["customer_id"],
                        int(
                            (
                                datetime.datetime.now() - datetime.datetime.fromtimestamp(0)
                            ).total_seconds()
                            * 1000
                        ),
                        datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds"),
                        row["type"],
                        row["operation"],
                        float(row["amount"]),
                        float(row["balance"]),
                    )
                )
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

    @staticmethod
    def read(file_path: str = os.path.join(DATA_PATH, "accounts.csv")):
        items = []
        logging.info(f"reading account records from {file_path}...")
        with open(file_path) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                items.append(
                    Account(
                        row["account_id"],
                        row["district_id"],
                        row["frequency"],
                        row["parseddate"],
                        int(
                            (
                                datetime.datetime.now() - datetime.datetime.fromtimestamp(0)
                            ).total_seconds()
                            * 1000
                        ),
                    )
                )
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

    @staticmethod
    def read(file_path: str = os.path.join(DATA_PATH, "customers.csv")):
        items = []
        logging.info(f"reading customer records from {file_path}...")
        with open(file_path) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                items.append(
                    Customer(
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
                            (
                                datetime.datetime.now() - datetime.datetime.fromtimestamp(0)
                            ).total_seconds()
                            * 1000
                        ),
                    )
                )
        logging.info(f"{len(items)} records are read")
        return items


class KafkaClient:
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = self.create_admin()
        self.producer_client = self.create_producer()

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

    def send(self, items: typing.List[typing.Union[Transaction, Account, Customer]]):
        logging.info(f"sending records, topic - {items[0].__class__.__name__.lower()}s ...")
        for item in items:
            try:
                topic, key, value = self.set_topic_key_value(item)
                self.producer_client.send(topic, key=key, value=value)
            except Exception as err:
                raise RuntimeError("fails to send a message") from err
        self.producer_client.flush()

    def set_topic_key_value(self, item: typing.Union[Transaction, Account, Customer]):
        value = item.asdict()
        class_name = item.__class__.__name__.lower()
        attr_key = f"{class_name}Id"
        key = {}
        key[attr_key] = value[attr_key]
        return f"{class_name}s", key, value

    def serialize(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, datetime.date):
            return str(obj)
        return obj


if __name__ == "__main__":
    client = KafkaClient("localhost:19092")

    # create topics
    topics = [
        NewTopic(name="transactions", num_partitions=5, replication_factor=1),
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
        NewTopic(name="transactions.debits", num_partitions=5, replication_factor=1),
        NewTopic(name="transactions.credits", num_partitions=5, replication_factor=1),
    ]

    client.create_topics(topics, to_recreate=True)

    # send messages
    client.send(Account.read())
    client.send(Customer.read())
    client.send(Transaction.read())
