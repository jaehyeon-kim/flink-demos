import datetime
import json
import pytest

from pyflink.datastream import StreamExecutionEnvironment

from utils import serialize
from s05_data_gen import DataGenerator
from s12_transformation import define_workflow


@pytest.fixture(scope="module")
def env():
    env = StreamExecutionEnvironment.get_execution_environment()
    yield env


def test_define_workflow_should_convert_data_from_one_streams(env):
    data_gen = DataGenerator()
    source_flight = data_gen.generate_skyone_data()
    source_flight.flight_arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
    source_stream = env.from_collection(
        collection=[json.dumps(source_flight.asdict(), default=serialize)]
    )

    elements = define_workflow(source_stream).execute_and_collect()
    element, num = None, 0
    for e in elements:
        element = e
        num += 1
    assert num == 1
    assert source_flight.confirmation == element.confirmation


def test_define_workflow_should_filter_out_flights_in_the_past(env):
    data_gen = DataGenerator()
    new_flight = data_gen.generate_skyone_data()
    new_flight.flight_arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
    old_flight = data_gen.generate_skyone_data()
    old_flight.flight_arrival_time = datetime.datetime.now() + datetime.timedelta(minutes=-1)
    source_stream = env.from_collection(
        collection=[
            json.dumps(new_flight.asdict(), default=serialize),
            json.dumps(old_flight.asdict(), default=serialize),
        ]
    )

    elements = define_workflow(source_stream).execute_and_collect()
    element, num = None, 0
    for e in elements:
        element = e
        num += 1
    assert num == 1
    assert new_flight.confirmation == element.confirmation
