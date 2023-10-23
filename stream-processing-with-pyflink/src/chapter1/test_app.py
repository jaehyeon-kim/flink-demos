import time
import pytest

from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, Duration
from pyflink.datastream import DataStream, StreamExecutionEnvironment

from model import SensorReading
from app import define_workflow


def create_sensor_source(id: int, rand: int):
    assert id >= 0 and id <= 20
    assert rand >= 0 and rand <= 100
    return id, rand


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


def test_define_workflow_should_group_by_key(env):
    source_1 = create_sensor_source(0, 10)
    source_2 = create_sensor_source(0, 20)

    class DefaultTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return int(time.time_ns() / 1000000)

    source_stream: DataStream = env.from_collection(
        collection=[source_1, source_2]
    ).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(5)
        ).with_timestamp_assigner(DefaultTimestampAssigner())
    )

    elements = list(define_workflow(source_stream).execute_and_collect())
    print(int(time.time_ns() / 1000000))
    print(elements)


# def test_define_workflow_should_aggregate_values_over_window(env):
#     pass
