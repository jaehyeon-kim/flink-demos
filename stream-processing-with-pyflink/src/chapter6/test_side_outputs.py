import datetime
from typing import Tuple

import pytest
from pyflink.common import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import DataStream, StreamExecutionEnvironment

from side_outputs import define_workflow, main_stream_output, freezing_alarm_output


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


def test_define_workflow_should_not_produce_freezing_alarm_if_high_temperature(env):
    source_1 = (1, 80, datetime.datetime.now())
    source_2 = (1, 90, datetime.datetime.now())

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    source_stream: DataStream = env.from_collection(
        collection=[source_1, source_2]
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    output_stream = define_workflow(source_stream)
    main_output_elements = list(
        output_stream.get_side_output(main_stream_output).execute_and_collect()
    )
    assert len(main_output_elements) == 2
    for e in main_output_elements:
        assert e.temperature >= 70.0

    freezing_alarm_elements = list(
        output_stream.get_side_output(freezing_alarm_output).execute_and_collect()
    )
    assert len(freezing_alarm_elements) == 0


def test_define_workflow_should_produce_freezing_alarm_if_low_temperature(env):
    source_1 = (1, 0, datetime.datetime.now())
    source_2 = (1, 10, datetime.datetime.now())

    class SourceTimestampAssigner(TimestampAssigner):
        def extract_timestamp(
            self, value: Tuple[int, int, datetime.datetime], record_timestamp: int
        ):
            return int(value[2].strftime("%s")) * 1000

    source_stream: DataStream = env.from_collection(
        collection=[source_1, source_2]
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(SourceTimestampAssigner())
    )

    output_stream = define_workflow(source_stream)
    main_output_elements = list(
        output_stream.get_side_output(main_stream_output).execute_and_collect()
    )
    assert len(main_output_elements) == 2
    for e in main_output_elements:
        assert e.temperature < 70.0

    freezing_alarm_elements = list(
        output_stream.get_side_output(freezing_alarm_output).execute_and_collect()
    )
    assert len(freezing_alarm_elements) == 2
    for e in freezing_alarm_elements:
        assert e.temperature < 70.0
