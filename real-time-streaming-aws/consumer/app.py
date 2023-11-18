import json
import base64
import datetime


class ConsumerRecord:
    def __init__(self, record: dict):
        self.topic = record["topic"]
        self.partition = record["partition"]
        self.offset = record["offset"]
        self.timestamp = record["timestamp"]
        self.timestamp_type = record["timestampType"]
        self.key = record["key"]
        self.value = record["value"]
        self.headers = record["headers"]

    def parse_record(
        self,
        to_str: bool = True,
        to_json: bool = True,
    ):
        rec = {
            **self.__dict__,
            **{
                "key": json.loads(base64.b64decode(self.key).decode()),
                "value": json.loads(base64.b64decode(self.value).decode()),
                "timestamp": ConsumerRecord.format_timestamp(self.timestamp, to_str),
            },
        }
        return json.dumps(rec, default=ConsumerRecord.serialize) if to_json else rec

    @staticmethod
    def format_timestamp(value, to_str: bool = True):
        ts = datetime.datetime.fromtimestamp(value / 1000)
        return ts.isoformat(timespec="milliseconds") if to_str else ts

    @staticmethod
    def serialize(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, datetime.date):
            return str(obj)
        return obj


def lambda_function(event, context):
    for _, records in event["records"].items():
        for record in records:
            cr = ConsumerRecord(record)
            print(cr.parse_record())


if __name__ == "__main__":
    event = {
        "eventSource": "aws:kafka",
        "eventSourceArn": "arn:aws:kafka:sa-east-1:123456789012:cluster/vpc-2priv-2pub/751d2973-a626-431c-9d4e-d7975eb44dd7-2",
        "bootstrapServers": "b-2.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092,b-1.demo-cluster-1.a1bcde.c1.kafka.us-east-1.amazonaws.com:9092",
        "records": {
            "mytopic-0": [
                {
                    "topic": "mytopic",
                    "partition": 0,
                    "offset": 15,
                    "timestamp": 1545084650987,
                    "timestampType": "CREATE_TIME",
                    "key": "eyJmb28iOiAiYmFyIn0=",
                    "value": "eyJmb28iOiAiYmFyIn0=",
                    "headers": [
                        {
                            "headerKey": [
                                104,
                                101,
                                97,
                                100,
                                101,
                                114,
                                86,
                                97,
                                108,
                                117,
                                101,
                            ]
                        }
                    ],
                }
            ]
        },
    }
    lambda_function(event, {})
