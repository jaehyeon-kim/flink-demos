import datetime
from typing import Tuple

import pytest
from pyflink.common import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import DataStream, StreamExecutionEnvironment

from utils.model import Alert
from multi_stream_transformations import define_workflow


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


def test_define_workflow_should_not_alert_if_low_temperature(env):
    temp_source_1 = (1, 0, datetime.datetime.now())
    temp_source_2 = (2, 0, datetime.datetime.now())
    smoke_source_1 = (100,)

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    temp_source: DataStream = env.from_collection(
        collection=[temp_source_1, temp_source_2]
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    smoke_source: DataStream = env.from_collection(collection=[smoke_source_1])

    elements = list(define_workflow(temp_source, smoke_source).execute_and_collect())
    assert len(elements) == 0


def test_define_workflow_should_not_alert_if_low_smoke_level(env):
    temp_source_1 = (1, 100, datetime.datetime.now())
    temp_source_2 = (2, 100, datetime.datetime.now())
    smoke_source_1 = (20,)

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    temp_source: DataStream = env.from_collection(
        collection=[temp_source_1, temp_source_2]
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    smoke_source: DataStream = env.from_collection(collection=[smoke_source_1])

    elements = list(define_workflow(temp_source, smoke_source).execute_and_collect())
    assert len(elements) == 0


def test_define_workflow_should_alert_if_high_temperature_high_smoke_level(env):
    temp_source_1 = (1, 0, datetime.datetime.now())
    temp_source_2 = (2, 100, datetime.datetime.now())
    smoke_source_1 = (100,)

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    temp_source: DataStream = env.from_collection(
        collection=[temp_source_1, temp_source_2]
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    smoke_source: DataStream = env.from_collection(collection=[smoke_source_1])

    elements = list(define_workflow(temp_source, smoke_source).execute_and_collect())
    assert len(elements) == 1

    elem: Alert = next(iter(elements))
    assert elem.message == "Risk of fire from sensor_2"
    assert elem.temperature > 80
