import datetime
import dataclasses
from typing import Iterable, Tuple

from pyflink.common import Row
from pyflink.common.typeinfo import Types

from .type_helper import TypeMapping, set_type_info


@dataclasses.dataclass
class SensorReading(TypeMapping):
    id: str
    timestamp: int
    num_records: int
    temperature: float

    def to_row(self):
        return Row(**dataclasses.asdict(self))

    @classmethod
    def from_row(cls, row: Row):
        return cls(**row.as_dict())

    @classmethod
    def from_tuple(cls, tup: Tuple[int, int, datetime.datetime]):
        return cls(
            id=f"sensor_{tup[0]}",
            timestamp=int(tup[2].strftime("%s")) * 1000,
            num_records=1,
            temperature=65 + (tup[1] / 100 * 20),
        )

    @staticmethod
    def process_elements(elements: Iterable[Tuple[int, int, datetime.datetime]]):
        id, count, temperature = None, 0, 0
        for e in elements:
            next_id = f"sensor_{e[0]}"
            if id is not None:
                assert id == next_id
            id = next_id
            count += 1
            temperature += 65 + (e[1] / 100 * 20)
        return id, count, temperature

    @staticmethod
    def type_mapping():
        return {
            "id": Types.STRING(),
            "timestamp": Types.LONG(),
            "num_records": Types.INT(),
            "temperature": Types.DOUBLE(),
        }

    @staticmethod
    def set_key_type_info():
        return set_type_info(SensorReading.type_mapping(), selects=["id"])

    @staticmethod
    def set_value_type_info():
        return set_type_info(SensorReading.type_mapping())


@dataclasses.dataclass
class MinMaxTemp:
    id: str
    min_temp: float
    max_temp: float
    num_records: int
    timestamp: int
