import datetime
import dataclasses
import json

from pyflink.common import Row
from pyflink.common.typeinfo import Types


def str_to_ts(s: str):
    return datetime.datetime.fromisoformat(s[:23].ljust(23, "0"))


@dataclasses.dataclass
class Transaction:
    transaction_id: str
    account_id: str
    customer_id: str
    event_time: int
    event_timestamp: datetime.datetime
    type: str
    operation: str
    amount: int

    @classmethod
    def from_str(cls, s: str):
        d = json.loads(s)
        kwargs = {
            "transaction_id": d["transaction_id"],
            "account_id": d["account_id"],
            "customer_id": d["customer_id"],
            "event_time": d["event_time"],
            "event_timestamp": str_to_ts(d["event_timestamp"]),
            "type": d["type"],
            "operation": d["operation"],
            "amount": d["amount"],
        }
        return cls(**kwargs)

    @classmethod
    def from_row(cls, row: Row):
        kwargs = {
            "transaction_id": row.transaction_id,
            "account_id": row.account_id,
            "customer_id": row.customer_id,
            "event_time": int(row.event_time),
            "event_timestamp": str_to_ts(row.event_timestamp),
            "type": row.type,
            "operation": row.operation,
            "amount": row.amount,
        }
        return cls(**kwargs)

    @staticmethod
    def get_value_type():
        return Types.ROW_NAMED(
            field_names=[
                "transaction_id",
                "account_id",
                "customer_id",
                "event_time",
                "event_timestamp",
                "type",
                "operation",
                "amount",
            ],
            field_types=[
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),  # Types.BIG_INT() not working
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.INT(),
            ],
        )


@dataclasses.dataclass
class Customer:
    customer_id: str
    sex: str
    dob: datetime.date
    first_name: str
    last_name: str
    email_address: str
    update_time: int
    update_timestamp: datetime.datetime

    @classmethod
    def from_row(cls, row: Row):
        kwargs = {
            "customer_id": row.customer_id,
            "sex": row.sex,
            "dob": row.dob,
            "first_name": row.first_name,
            "last_name": row.last_name,
            "email_address": row.email_address,
            "update_time": row.update_time,
            "update_timestamp": row.update_timestamp,
        }
        return cls(**kwargs)

    @staticmethod
    def get_value_type():
        return Types.ROW_NAMED(
            field_names=[
                "customer_id",
                "sex",
                "dob",
                "first_name",
                "last_name",
                "email_address",
                "update_time",
                "update_timestamp",
            ],
            field_types=[
                Types.STRING(),
                Types.STRING(),
                Types.SQL_DATE(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.BIG_INT(),
                Types.STRING(),
            ],
        )


@dataclasses.dataclass
class Account:
    account_id: str
    district_id: str
    frequency: str
    update_time: int
    update_timestamp: datetime.datetime

    @classmethod
    def from_row(cls, row: Row):
        kwargs = {
            "account_id": row.customer_id,
            "district_id": row.sex,
            "frequency": row.dob,
            "update_time": row.update_time,
            "update_timestamp": row.update_timestamp,
        }
        return cls(**kwargs)

    @staticmethod
    def get_value_type():
        return Types.ROW_NAMED(
            field_names=[
                "account_id",
                "district_id",
                "frequency",
                "update_time",
                "update_timestamp",
            ],
            field_types=[
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.BIG_INT(),
                Types.STRING(),
            ],
        )
