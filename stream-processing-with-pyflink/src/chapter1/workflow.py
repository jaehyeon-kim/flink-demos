import datetime
from typing import Iterable, Tuple

from pyflink.common import Row
from pyflink.datastream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import ProcessWindowFunction

from model import SensorReading


class AggreteProcessWindowFunction(ProcessWindowFunction):
    def process(
        self,
        key: int,
        context: ProcessWindowFunction.Context,
        elements: Iterable[Tuple[int, datetime.datetime]],
    ) -> Iterable[Row]:
        id, count, temperature = SensorReading.process_elements(elements)
        yield Row(
            id=id,
            timestamp=int(context.window().end),
            num_records=count,
            temperature=round(temperature / count, 2),
        )


def define_workflow(source_stream: DataStream):
    sensor_stream = (
        source_stream.key_by(lambda e: e[0])
        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        .process(AggreteProcessWindowFunction(), output_type=SensorReading.get_value_type())
    )
    return sensor_stream
