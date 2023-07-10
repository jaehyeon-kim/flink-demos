import os
import datetime
import time
import json
import random

from kafka import KafkaProducer


class Sales:
    def __init__(self):
        self.products = [
            {"name": "Toothpaste", "product_price": 4.99},
            {"name": "Toothbrush", "product_price": 3.99},
            {"name": "Dental Floss", "product_price": 1.99},
        ]
        self.sellers = ["LNK", "OMA", "KC", "DEN"]

    def make_sales_item(self):
        return {
            **{
                "seller_id": random.choice(self.sellers),
                "quantity": random.randint(1, 5),
                "sale_ts": int(time.time() * 1000),
            },
            **random.choice(self.products),
        }

    def create(self, num: int):
        return [self.make_sales_item() for _ in range(num)]


class Producer:
    def __init__(self, bootstrap_servers: list, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = self.create()

    def create(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=self.serialize).encode("utf-8"),
        )

    def send(self, sales_items: list):
        for item in sales_items:
            self.producer.send(self.topic, value=item)
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
        topic=os.getenv("TOPIC_NAME", "sales_items"),
    )

    while True:
        sales_items = Sales().create(10)
        producer.send(sales_items)
        print("messages sent...")
        time.sleep(5)
