import os
import argparse
import typing
import logging

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, UnknownTopicOrPartitionError


class KafkaClient:
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = self.create_admin()

    def create_admin(self):
        return KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--delete", action="store_true")
    parser.set_defaults(delete=False)
    parser.add_argument("--create", action="store_true")
    parser.set_defaults(create=False)
    args = parser.parse_args()

    client = KafkaClient(os.getenv("BOOTSTRAP_SERVERS", "localhost:29092"))

    topics = [
        NewTopic(name="transactions", num_partitions=5, replication_factor=1),
        # NewTopic(name="transactions.debits", num_partitions=5, replication_factor=1),
        # NewTopic(name="transactions.credits", num_partitions=5, replication_factor=1),
        NewTopic(
            name="customers",
            num_partitions=1,
            replication_factor=1,
            topic_configs={"cleanup.policy": "compact,delete", "retention.ms": 600000},
        ),
        NewTopic(
            name="accounts",
            num_partitions=1,
            replication_factor=1,
            topic_configs={"cleanup.policy": "compact,delete", "retention.ms": 600000},
        ),
    ]

    if args.delete:
        client.delete_topics(topics)
    if args.create:
        client.create_topics(topics, to_recreate=True)
