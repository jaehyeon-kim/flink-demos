import datetime
from typing import Tuple, List

import pytest
from pyflink.common import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import DataStream, StreamExecutionEnvironment

from utils.model import MinMaxTemp
from window_functions_process_window import define_workflow


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


def test_define_workflow_should_return_min_max_temp_records_by_id(env):
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

    elements: List[MinMaxTemp] = list(define_workflow(source_stream).execute_and_collect())
    assert len(elements) == 2
    assert elements[0].timestamp == elements[1].timestamp
    for e in elements:
        if e.id == "sensor_1":
            assert e.min_temp == 65
            assert e.max_temp == 75
            assert e.num_records == 2
        else:
            assert e.min_temp == 69
            assert e.max_temp == 85
            assert e.num_records == 2


def test_define_workflow_should_return_min_max_temp_records_within_window(env):
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

    elements: List[MinMaxTemp] = list(define_workflow(source_stream).execute_and_collect())
    assert len(elements) == 2
    assert elements[0].timestamp < elements[1].timestamp
    assert elements[0].min_temp == 65
    assert elements[0].max_temp == 75
    assert elements[0].num_records == 2
    assert elements[1].min_temp == 85
    assert elements[1].max_temp == 85
    assert elements[1].num_records == 1
