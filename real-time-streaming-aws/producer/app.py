import os
import datetime
import random
import json
import re
import time
import typing
import dataclasses

from kafka import KafkaProducer


@dataclasses.dataclass
class TaxiRide:
    id: str
    vendor_id: int
    pickup_date: str
    dropoff_date: str
    passenger_count: int
    pickup_longitude: str
    pickup_latitude: str
    dropoff_longitude: str
    dropoff_latitude: str
    store_and_fwd_flag: str
    gc_distance: int
    trip_duration: int
    google_distance: int
    google_duration: int

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def auto(cls):
        pickup_lon, pickup_lat = tuple(TaxiRide.get_latlon().split(","))
        dropoff_lon, dropoff_lat = tuple(TaxiRide.get_latlon().split(","))
        distance, duration = random.randint(1, 7), random.randint(8, 10000)
        return cls(
            id=f"id{random.randint(1665586, 8888888)}",
            vendor_id=random.randint(1, 5),
            pickup_date=datetime.datetime.now().isoformat(timespec="milliseconds"),
            dropoff_date=(
                datetime.datetime.now()
                + datetime.timedelta(minutes=random.randint(30, 100))
            ).isoformat(timespec="milliseconds"),
            passenger_count=random.randint(1, 9),
            pickup_longitude=pickup_lon,
            pickup_latitude=pickup_lat,
            dropoff_longitude=dropoff_lon,
            dropoff_latitude=dropoff_lat,
            store_and_fwd_flag=["Y", "N"][random.randint(0, 1)],
            gc_distance=distance,
            trip_duration=duration,
            google_distance=distance,
            google_duration=duration,
        )

    @staticmethod
    def create(num: int):
        return [TaxiRide.auto() for _ in range(num)]

    # fmt: off
    @staticmethod
    def get_latlon():
        location_list = [
            "-73.98174286,40.71915817", "-73.98508453,40.74716568", "-73.97333527,40.76407242", "-73.99310303,40.75263214",
            "-73.98229218,40.75133133", "-73.96527863,40.80104065", "-73.97010803,40.75979996", "-73.99373627,40.74176025",
            "-73.98544312,40.73571014", "-73.97686005,40.68337631", "-73.9697876,40.75758362", "-73.99397278,40.74086761",
            "-74.00531769,40.72866058", "-73.99013519,40.74885178", "-73.9595108,40.76280975", "-73.99025726,40.73703384",
            "-73.99495697,40.745121", "-73.93579865,40.70730972", "-73.99046326,40.75100708", "-73.9536438,40.77526093",
            "-73.98226166,40.75159073", "-73.98831177,40.72318649", "-73.97222137,40.67683029", "-73.98626709,40.73276901",
            "-73.97852325,40.78910065", "-73.97612,40.74908066", "-73.98240662,40.73148727", "-73.98776245,40.75037384",
            "-73.97187042,40.75840378", "-73.87303925,40.77410507", "-73.9921875,40.73451996", "-73.98435974,40.74898529",
            "-73.98092651,40.74196243", "-74.00701904,40.72573853", "-74.00798798,40.74022675", "-73.99419403,40.74555969",
            "-73.97737885,40.75883865", "-73.97051239,40.79664993", "-73.97693634,40.7599144", "-73.99306488,40.73812866",
            "-74.00775146,40.74528885", "-73.98532867,40.74198914", "-73.99037933,40.76152802", "-73.98442078,40.74978638",
            "-73.99173737,40.75437927", "-73.96742249,40.78820801", "-73.97813416,40.72935867", "-73.97171021,40.75943375",
            "-74.00737,40.7431221", "-73.99498749,40.75517654", "-73.91600037,40.74634933", "-73.99924469,40.72764587",
            "-73.98488617,40.73621368", "-73.98627472,40.74737167",
        ]
        return location_list[random.randint(0, len(location_list) - 1)]


# fmt: on
class Producer:
    def __init__(self, bootstrap_servers: list, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = self.create()

    def create(self):
        kwargs = {
            "bootstrap_servers": self.bootstrap_servers,
            "value_serializer": lambda v: json.dumps(v, default=self.serialize).encode(
                "utf-8"
            ),
            "key_serializer": lambda v: json.dumps(v, default=self.serialize).encode(
                "utf-8"
            ),
            "api_version": (2, 8, 1),
        }
        if re.search("9098$", next(iter(self.bootstrap_servers))):
            kwargs = {
                **kwargs,
                **{
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "AWS_MSK_IAM",
                },
            }
        return KafkaProducer(**kwargs)

    def send(self, items: typing.List[TaxiRide]):
        for item in items:
            self.producer.send(self.topic, key={"id": item.id}, value=item.asdict())
        self.producer.flush()

    def serialize(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, datetime.date):
            return str(obj)
        return obj


def lambda_function(event, context):
    producer = Producer(
        bootstrap_servers=os.environ["BOOTSTRAP_SERVERS"].split(","),
        topic=os.environ["TOPIC_NAME"],
    )
    s = datetime.datetime.now()
    total_records = 0
    while True:
        items = TaxiRide.create(10)
        producer.send(items)
        total_records += len(items)
        print(f"sent {len(items)} messages")
        elapsed_sec = (datetime.datetime.now() - s).seconds
        max_run_sec = int(os.environ["MAX_RUN_SEC"])
        if max_run_sec > 0 and elapsed_sec > int(os.environ["MAX_RUN_SEC"]):
            print(f"{total_records} records are sent in {elapsed_sec} seconds ...")
            break
        time.sleep(1)


if __name__ == "__main__":
    os.environ["BOOTSTRAP_SERVERS"] = "localhost:29092"
    os.environ["TOPIC_NAME"] = "taxi-rides"
    os.environ["MAX_RUN_SEC"] = "-1"
    lambda_function({}, {})
