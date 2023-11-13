import datetime
from typing import List, Tuple

import pytest
from pyflink.common import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import DataStream, StreamExecutionEnvironment

from utils.model import SensorReading
from app import define_workflow


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


def test_process_elements_return_correct_id_and_count():
    elements = [(1, 0, datetime.datetime.now()), (1, 0, datetime.datetime.now())]
    id, count, temperature = SensorReading.process_elements(elements)

    assert id == "sensor_1"
    assert count == 2
    assert temperature == 65 * 2


def test_define_workflow_should_aggregate_values_by_id(env):
    source_1 = (1, 0, datetime.datetime.now())
    source_2 = (1, 0, datetime.datetime.now() + datetime.timedelta(milliseconds=200))
    source_3 = (2, 0, datetime.datetime.now())

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    source_stream: DataStream = env.from_collection(
        collection=[source_1, source_2, source_3]
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    elements: List[SensorReading] = list(define_workflow(source_stream).execute_and_collect())
    for e in elements:
        if e.id == "sensor_1":
            assert e.num_records == 2
            assert e.temperature == 65
        elif e.id == "sensor_2":
            assert e.num_records == 1
            assert e.temperature == 65
        else:
            RuntimeError("incorrect sensor id")


def test_define_workflow_should_aggregate_values_by_minute(env):
    source_1 = (1, 0, datetime.datetime.now())
    source_2 = (1, 0, datetime.datetime.now() + datetime.timedelta(milliseconds=100))
    source_3 = (1, 100, datetime.datetime.now() + datetime.timedelta(milliseconds=1000))

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    source_stream: DataStream = env.from_collection(
        collection=[source_1, source_2, source_3]
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    elements: List[SensorReading] = list(define_workflow(source_stream).execute_and_collect())
    for e in elements:
        if e.num_records == 1:
            assert e.temperature == 85
        elif e.num_records == 2:
            assert e.temperature == 65
        else:
            raise RuntimeError("records grouped incorrectly")
