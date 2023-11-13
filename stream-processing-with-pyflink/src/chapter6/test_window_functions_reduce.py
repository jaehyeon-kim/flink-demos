import datetime
from typing import Tuple

import pytest
from pyflink.common import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import DataStream, StreamExecutionEnvironment

from window_functions_reduce import define_workflow


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


def test_define_workflow_should_return_records_having_mininum_temperature_by_id(env):
    source_1 = (1, 0, datetime.datetime.now())
    source_2 = (1, 50, datetime.datetime.now())
    source_3 = (2, 20, datetime.datetime.now())
    source_4 = (2, 100, datetime.datetime.now())

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    source_stream: DataStream = env.from_collection(
        collection=[source_1, source_2, source_3, source_4]
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    elements = list(define_workflow(source_stream).execute_and_collect())
    assert len(elements) == 2
    for e in elements:
        if e.id == "sensor_1":
            assert e.temperature == 65
        else:
            assert e.temperature == 69


def test_define_workflow_should_return_records_having_mininum_temperature_within_window(env):
    source_1 = (1, 0, datetime.datetime.now())
    source_2 = (1, 50, datetime.datetime.now() + datetime.timedelta(milliseconds=100))
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

    elements = list(define_workflow(source_stream).execute_and_collect())
    assert len(elements) == 2
    assert elements[0].temperature == 65
    assert elements[1].temperature == 85
