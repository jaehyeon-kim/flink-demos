import os
import datetime
import time
import json
import typing
import random
import logging
import dataclasses

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@dataclasses.dataclass
class FlagAccount:
    account_id: int
    flag_date: str

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def auto(cls, account_id: int):
        flag_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        return cls(account_id, flag_date)

    @staticmethod
    def create():
        return [FlagAccount.auto(account_id) for account_id in range(1000000001, 1000000010, 2)]


@dataclasses.dataclass
class Transaction:
    account_id: int
    customer_id: str
    merchant_type: str
    transaction_id: str
    transaction_type: str
    transaction_amount: float
    transaction_date: str

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def auto(cls):
        account_id = random.randint(1000000001, 1000000010)
        customer_id = f"C{str(account_id)[::-1]}"
        merchant_type = random.choice(["Online", "In Store"])
        transaction_id = "".join(random.choice("0123456789ABCDEF") for i in range(16))
        transaction_type = random.choice(
            [
                "Grocery_Store",
                "Gas_Station",
                "Shopping_Mall",
                "City_Services",
                "HealthCare_Service",
                "Food and Beverage",
                "Others",
            ]
        )
        transaction_amount = round(random.randint(100, 10000) * random.random(), 2)
        transaction_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        return cls(
            account_id,
            customer_id,
            merchant_type,
            transaction_id,
            transaction_type,
            transaction_amount,
            transaction_date,
        )

    @staticmethod
    def create(num: int):
        return [Transaction.auto() for _ in range(num)]


class Producer:
    def __init__(self, bootstrap_servers: list, account_topic: str, transaction_topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.account_topic = account_topic
        self.transaction_topic = transaction_topic
        self.producer = self.create()

    def create(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda v: json.dumps(v, default=self.serialize).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, default=self.serialize).encode("utf-8"),
            api_version=(2, 8, 1),
        )

    def send(self, records: typing.Union[typing.List[FlagAccount], typing.List[Transaction]]):
        for record in records:
            try:
                key = {"account_id": record.account_id}
                topic = self.account_topic
                if hasattr(record, "transaction_id"):
                    key["transaction_id"] = record.transaction_id
                    topic = self.transaction_topic
                self.producer.send(topic=topic, key=key, value=record.asdict())
            except Exception as e:
                raise RuntimeError("fails to send a message") from e
        self.producer.flush()

    def serialize(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, datetime.date):
            return str(obj)
        return obj


if __name__ == "__main__":
    producer = Producer(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS", "localhost:29092").split(","),
        account_topic=os.getenv("CUSTOMER_TOPIC_NAME", "flagged-accounts"),
        transaction_topic=os.getenv("TRANSACTION_TOPIC_NAME", "transactions"),
    )
    if os.getenv("DATE_TYPE", "account") == "account":
        producer.send(FlagAccount.create())
        producer.producer.close()
    else:
        max_run = int(os.getenv("MAX_RUN", "-1"))
        logging.info(f"max run - {max_run}")
        current_run = 0
        while True:
            current_run += 1
            logging.info(f"current run - {current_run}")
            if current_run - max_run == 0:
                logging.info(f"reached max run, finish")
                producer.producer.close()
                break
            producer.send(Transaction.create(5))
            secs = random.randint(2, 5)
            logging.info(f"messages sent... wait {secs} seconds")
            time.sleep(secs)
