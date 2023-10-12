import datetime
import typing
import pytest

from pyflink.datastream import StreamExecutionEnvironment

from models import FlightData
from s05_data_gen import DataGenerator
from s16_merge import define_workflow


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


def test_define_workflow_should_convert_data_from_two_streams(env):
    data_gen = DataGenerator()
    # skyone
    skyone_flight = data_gen.generate_skyone_data()
    skyone_flight.flight_arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
    skyone_stream = env.from_collection(collection=[skyone_flight.to_row()])
    # sunset
    sunset_flight = data_gen.generate_sunset_data()
    sunset_flight.arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
    sunset_stream = env.from_collection(collection=[sunset_flight.to_row()])
    # collect from union on stream
    elements: typing.List[FlightData] = list(
        define_workflow(skyone_stream, sunset_stream).execute_and_collect()
    )
    # test
    assert len(elements) == 2
    for e in elements:
        if e.source == "skyone":
            assert skyone_flight.confirmation == e.confirmation
        else:
            assert sunset_flight.reference_number == e.confirmation


def test_define_workflow_should_filter_out_flights_in_the_past(env):
    data_gen = DataGenerator()
    # skyone
    new_skyone_flight = data_gen.generate_skyone_data()
    new_skyone_flight.flight_arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
    old_skyone_flight = data_gen.generate_skyone_data()
    old_skyone_flight.flight_arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=-1)
    skyone_stream = env.from_collection(
        collection=[
            new_skyone_flight.to_row(),
            old_skyone_flight.to_row(),
        ]
    )
    # sunset
    new_sunset_flight = data_gen.generate_sunset_data()
    new_sunset_flight.arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
    old_sunset_flight = data_gen.generate_sunset_data()
    old_sunset_flight.arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=-1)
    sunset_stream = env.from_collection(
        collection=[
            new_sunset_flight.to_row(),
            old_sunset_flight.to_row(),
        ]
    )
    # collect from union on stream
    elements: typing.List[FlightData] = list(
        define_workflow(skyone_stream, sunset_stream).execute_and_collect()
    )
    # test
    assert len(elements) == 2
    for e in elements:
        if e.source == "skyone":
            assert new_skyone_flight.confirmation == e.confirmation
        else:
            assert new_sunset_flight.reference_number == e.confirmation
