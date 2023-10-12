import typing
import time
import pytest

from pyflink.common import WatermarkStrategy, Row
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment

from models import UserStatistics
from helpers import build_flight
from s20_manage_state import define_workflow


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


@pytest.fixture(scope="module")
def default_watermark_strategy():
    class DefaultTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return int(time.time_ns() / 1000000)

    return WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
        DefaultTimestampAssigner()
    )


def test_define_workflow_should_convert_flight_data_to_user_statistics(
    env, default_watermark_strategy
):
    flight_data = build_flight()
    flight_stream = env.from_collection(
        collection=[flight_data.to_row()]
    ).assign_timestamps_and_watermarks(default_watermark_strategy)

    elements: typing.List[UserStatistics] = list(
        define_workflow(flight_stream).execute_and_collect()
    )

    expected = UserStatistics.from_flight(flight_data)

    assert expected.email_address == next(iter(elements)).email_address
    assert expected.total_flight_duration == next(iter(elements)).total_flight_duration
    assert expected.number_of_flights == next(iter(elements)).number_of_flights


def test_define_workflow_should_group_statistics_by_email_address(env, default_watermark_strategy):
    flight_data_1 = build_flight()
    flight_data_2 = build_flight()
    flight_data_3 = build_flight()
    flight_data_3.email_address = flight_data_1.email_address

    flight_stream = env.from_collection(
        collection=[flight_data_1.to_row(), flight_data_2.to_row(), flight_data_3.to_row()]
    ).assign_timestamps_and_watermarks(default_watermark_strategy)

    elements: typing.List[UserStatistics] = list(
        define_workflow(flight_stream).execute_and_collect()
    )

    expected_1 = UserStatistics.merge(
        UserStatistics.from_flight(flight_data_1), UserStatistics.from_flight(flight_data_3)
    )
    expected_2 = UserStatistics.from_flight(flight_data_2)

    assert len(elements) == 2
    for e in elements:
        if e.email_address == flight_data_1.email_address:
            assert e.total_flight_duration == expected_1.total_flight_duration
            assert e.number_of_flights == expected_1.number_of_flights
        else:
            assert e.total_flight_duration == expected_2.total_flight_duration
            assert e.number_of_flights == expected_2.number_of_flights


def test_define_workflow_should_window_statistics_by_minute(env):
    flight_data_1 = build_flight()
    flight_data_2 = build_flight()
    flight_data_2.email_address = flight_data_1.email_address
    flight_data_3 = build_flight()
    flight_data_3.email_address = flight_data_1.email_address
    flight_data_3.departure_airport_code = "LATE"

    class CustomTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            if value.departure_airport_code == "LATE":
                return int(time.time_ns() / 1000000) + 60000
            else:
                return int(time.time_ns() / 1000000)

    custom_watermark_strategy = (
        WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
            CustomTimestampAssigner()
        )
    )

    flight_stream = env.from_collection(
        collection=[flight_data_1.to_row(), flight_data_2.to_row(), flight_data_3.to_row()]
    ).assign_timestamps_and_watermarks(custom_watermark_strategy)

    elements: typing.List[UserStatistics] = list(
        define_workflow(flight_stream).execute_and_collect()
    )

    expected_1 = UserStatistics.merge(
        UserStatistics.from_flight(flight_data_1), UserStatistics.from_flight(flight_data_2)
    )
    expected_2 = UserStatistics.merge(expected_1, UserStatistics.from_flight(flight_data_3))

    assert len(elements) == 2
    for e in elements:
        if e.number_of_flights < 3:
            assert e.total_flight_duration == expected_1.total_flight_duration
        else:
            assert e.total_flight_duration == expected_2.total_flight_duration
