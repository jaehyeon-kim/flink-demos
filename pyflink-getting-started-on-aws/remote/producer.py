import os
import datetime
import time
import json
import typing
import random
import logging
import re
import dataclasses

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


@dataclasses.dataclass
class Stock:
    event_time: str
    ticker: str
    price: float

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def auto(cls, ticker: str):
        # event_time = datetime.datetime.now().isoformat(timespec="milliseconds")
        event_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        price = round(random.random() * 100, 2)
        return cls(event_time, ticker, price)

    @staticmethod
    def create():
        tickers = '["AAPL", "ACN", "ADBE", "AMD", "AVGO", "CRM", "CSCO", "IBM", "INTC", "MA", "MSFT", "NVDA", "ORCL", "PYPL", "QCOM", "TXN", "V"]'
        return [Stock.auto(ticker) for ticker in json.loads(tickers)]


class Producer:
    def __init__(self, bootstrap_servers: list, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = self.create()

    def create(self):
        params = {
            "bootstrap_servers": self.bootstrap_servers,
            "key_serializer": lambda v: json.dumps(v, default=self.serialize).encode("utf-8"),
            "value_serializer": lambda v: json.dumps(v, default=self.serialize).encode("utf-8"),
            "api_version": (2, 8, 1),
        }
        if re.search("9098$", self.bootstrap_servers[0]):
            params = {
                **params,
                **{"security_protocol": "SASL_SSL", "sasl_mechanism": "AWS_MSK_IAM"},
            }
        return KafkaProducer(**params)

    def send(self, stocks: typing.List[Stock]):
        for stock in stocks:
            try:
                self.producer.send(self.topic, key={"ticker": stock.ticker}, value=stock.asdict())
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
        topic=os.getenv("TOPIC_NAME", "stocks-in"),
    )
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
        producer.send(Stock.create())
        secs = random.randint(5, 10)
        logging.info(f"messages sent... wait {secs} seconds")
        time.sleep(secs)
