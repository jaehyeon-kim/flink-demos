import argparse
import datetime
import random
import string
import json
import dataclasses
import logging
import typing
import time

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, UnknownTopicOrPartitionError, TopicAlreadyExistsError


@dataclasses.dataclass
class SkyoneData:
    email_address: str
    flight_departure_time: str
    iata_departure_code: str
    flight_arrival_time: str
    iata_arrival_code: str
    flight_number: str
    confirmation: str
    ticket_price: int
    aircraft: str
    booking_agency_email: str

    def asdict(self):
        return dataclasses.asdict(self)

    def topic_name(self):
        return "skyone"


@dataclasses.dataclass
class SunsetData:
    customer_email_address: str
    departure_time: str
    departure_airport: str
    arrival_time: str
    arrival_airport: str
    flight_duration: int
    flight_id: str
    reference_number: str
    total_price: int
    aircraft_details: str

    def asdict(self):
        return dataclasses.asdict(self)

    def topic_name(self):
        return "sunset"


class DataGenerator:
    def __init__(self) -> None:
        self.users = [f"{self.generate_string(5)}@email.com" for _ in range(100)]

    # fmt: off
    def generate_airport_code(self):
        airports = [
            "ATL", "DFW", "DEN", "ORD", "LAX", "CLT", "MCO", "LAS", "PHX", "MIA",
            "SEA", "IAH", "JFK", "EWR", "FLL", "MSP", "SFO", "DTW", "BOS", "SLC",
            "PHL", "BWI", "TPA", "SAN", "LGA", "MDW", "BNA", "IAD", "DCA", "AUS"            
        ]
        return random.choice(airports)

    # fmt: on
    def generate_string(self, size: int):
        return "".join(random.choice(string.ascii_uppercase) for i in range(size))

    def generate_email(self):
        return random.choice(self.users)

    def generate_departure_time(self):
        return datetime.datetime.now() + datetime.timedelta(
            days=random.randrange(60),
            hours=random.randrange(24),
            minutes=random.randrange(60),
            seconds=random.randrange(60),
        )

    def generate_arrival_time(self, departure: datetime.datetime):
        return departure + datetime.timedelta(
            hours=random.randrange(15),
            minutes=random.randrange(60),
            seconds=random.randrange(60),
        )

    def generate_sky_one_data(self) -> SkyoneData:
        departure_time = self.generate_departure_time()
        return SkyoneData(
            email_address=self.generate_email(),
            flight_departure_time=departure_time,
            iata_departure_code=self.generate_airport_code(),
            flight_arrival_time=self.generate_arrival_time(departure_time),
            iata_arrival_code=self.generate_airport_code(),
            flight_number=f"SKY1{random.randrange(1000)}",
            confirmation=f"SKY1{self.generate_string(6)}",
            ticket_price=500 + random.randrange(1000),
            aircraft=f"Aircraft{self.generate_string(3)}",
            booking_agency_email=self.generate_email(),
        )

    def generate_sunset_data(self) -> SunsetData:
        departure_time = self.generate_departure_time()
        arrival_time = self.generate_arrival_time(departure_time)
        return SunsetData(
            customer_email_address=self.generate_email(),
            departure_time=departure_time,
            departure_airport=self.generate_airport_code(),
            arrival_time=self.generate_arrival_time(departure_time),
            arrival_airport=self.generate_airport_code(),
            flight_duration=int((arrival_time - departure_time).seconds / 60),
            flight_id=f"SUN{random.randrange(1000)}",
            reference_number=f"SUN{self.generate_string(6)}",
            total_price=300 + random.randrange(1500),
            aircraft_details=f"Aircraft{self.generate_string(4)}",
        )

    def generate_items(self) -> typing.List[typing.Union[SkyoneData, SunsetData]]:
        sky_ones = [self.generate_sky_one_data() for _ in range(random.randint(1, 3))]
        sunsets = [self.generate_sunset_data() for _ in range(random.randint(1, 3))]
        return sky_ones + sunsets


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
        return KafkaAdminClient(bootstrap_servers=self.bootstrap_servers, api_version=(2, 8, 1))

    def create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda v: json.dumps(v, default=self.serialize).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, default=self.serialize).encode("utf-8"),
            api_version=(2, 8, 1),
        )

    def delete_topics(self, topic_names: typing.List[str]):
        for name in topic_names:
            try:
                self.admin_client.delete_topics([name], timeout_ms=1000)
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
            except TopicAlreadyExistsError:
                pass
            except KafkaError as err:
                raise RuntimeError(
                    f"fails to create topics - {', '.join(t.name for t in topics)}"
                ) from err
        logging.info(f"topics created successfully - {', '.join([t.name for t in topics])}")

    def send_items(self, wait_for: typing.Union[int, float]):
        while True:
            data_gen = DataGenerator()
            items = data_gen.generate_items()
            len_sky = len([item for item in items if item.topic_name() == "skyone"])
            logging.info(f"{len_sky} items from sky one and {len(items) - len_sky} from sunset")
            for item in items:
                try:
                    key = {
                        "ref": item.confirmation
                        if item.topic_name() == "skyone"
                        else item.reference_number
                    }
                    self.producer_client.send(
                        item.topic_name(),
                        key=key,
                        value=item.asdict(),
                    )
                    logging.info(f"record sent, topic - {item.topic_name()}, ref - {key['ref']}")
                except Exception as err:
                    raise RuntimeError("fails to send a message") from err
            logging.info(f"wait for {wait_for} seconds...")
            time.sleep(wait_for)

    def serialize(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat(timespec="milliseconds")
        if isinstance(obj, datetime.date):
            return str(obj)
        return obj


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--create", action="store_true")
    parser.set_defaults(create=False)
    args = parser.parse_args()

    client = KafkaClient("localhost:29092")
    if args.create:
        topics = [
            NewTopic(name="skyone", num_partitions=5, replication_factor=1),
            NewTopic(name="sunset", num_partitions=5, replication_factor=1),
        ]
        logging.info(f"create topics - {', '.join(t.name for t in topics)}")
        client.create_topics(topics, to_recreate=True)
    else:
        logging.info("skip to create topics...")

    # send records
    client.send_items(wait_for=1)
