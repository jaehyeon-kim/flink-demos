import dataclasses
from typing import Iterable, Tuple

from pyflink.common.typeinfo import Types


@dataclasses.dataclass
class SensorReading:
    id: str
    timestamp: int
    temperature: float

    @staticmethod
    def process_elements(elements: Iterable[Tuple[int, int]]):
        id, count, temperature = None, 0, 0
        for e in elements:
            next_id = f"sensor_{e[0]}"
            if id is not None:
                assert id == next_id
            id = next_id
            count += 1
            temperature += e[1] / 100 * 20
        return id, count, temperature

    @staticmethod
    def get_key_type():
        return Types.ROW_NAMED(
            field_names=["id"],
            field_types=[Types.STRING()],
        )

    @staticmethod
    def get_value_type():
        return Types.ROW_NAMED(
            field_names=["id", "timestamp", "temperature"],
            field_types=[Types.STRING(), Types.LONG(), Types.DOUBLE()],
        )
