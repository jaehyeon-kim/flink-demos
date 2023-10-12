import datetime
import typing
import pytest

from pyflink.datastream import StreamExecutionEnvironment

from models import FlightData
from s05_data_gen import DataGenerator
from s12_transformation import define_workflow


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


def test_define_workflow_should_convert_data_from_one_stream(env: StreamExecutionEnvironment):
    data_gen = DataGenerator()
    source_flight = data_gen.generate_skyone_data()
    source_flight.flight_arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
    source_stream = env.from_collection(collection=[source_flight.to_row()])

    elements: typing.List[FlightData] = list(define_workflow(source_stream).execute_and_collect())
    assert len(elements) == 1
    assert source_flight.confirmation == next(iter(elements)).confirmation


def test_define_workflow_should_filter_out_flights_in_the_past(env):
    data_gen = DataGenerator()
    new_flight = data_gen.generate_skyone_data()
    new_flight.flight_arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
    old_flight = data_gen.generate_skyone_data()
    old_flight.flight_arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=-1)
    source_stream = env.from_collection(collection=[new_flight.to_row(), old_flight.to_row()])

    elements: typing.List[FlightData] = list(define_workflow(source_stream).execute_and_collect())
    assert len(elements) == 1
    assert new_flight.confirmation == next(iter(elements)).confirmation
