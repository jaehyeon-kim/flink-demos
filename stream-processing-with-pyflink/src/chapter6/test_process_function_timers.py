import datetime

from pyflink.datastream.state import ValueState
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.timerservice import TimerService

from utils.model import SensorReading
from process_function_timers import TempIncAlertFunc


class TestTimerService(TimerService):
    def __init__(self):
        self.curr_processing_time = None

    def current_processing_time(self):
        return int(datetime.datetime.now().strftime("%s")) * 1000

    def current_watermark(self):
        return super().current_watermark()

    def register_processing_time_timer(self, timestamp: int):
        self.curr_processing_time = timestamp

    def register_event_time_timer(self, timestamp: int):
        return super().register_event_time_timer(timestamp)

    def delete_processing_time_timer(self, timestamp: int):
        self.curr_processing_time = None

    def delete_event_time_timer(self, timestamp: int):
        return super().delete_event_time_timer(timestamp)


class TestContext(KeyedProcessFunction.Context):
    def __init__(self, key: int):
        self.state = {"key": f"sensor_{key}"}

    def get_current_key(self):
        return self.state["key"]

    def timer_service(self) -> TimerService:
        return TestTimerService()

    def timestamp(self) -> int:
        return None


class LastTemp(ValueState):
    def __init__(self):
        self.vs = None

    def value(self):
        return self.vs

    def update(self, value):
        self.vs = value

    def clear(self):
        self.vs = None


class CurrentTimer(ValueState):
    def __init__(self):
        self.vs = None

    def value(self):
        return self.vs

    def update(self, value):
        self.vs = value

    def clear(self):
        self.vs = None


def test_temp_inc_alert_func_should_create_timer_if_temp_increasing():
    reading_1 = SensorReading.from_tuple((1, 0, datetime.datetime.now()))
    reading_2 = SensorReading.from_tuple((1, 50, datetime.datetime.now()))
    reading_3 = SensorReading.from_tuple((1, 100, datetime.datetime.now()))

    temp_alert = TempIncAlertFunc(last_temp=LastTemp(), current_timer=CurrentTimer())
    ctx = TestContext(1)
    temp_alert.process_element(reading_1, ctx)
    assert temp_alert.last_temp.value() == 65
    assert temp_alert.current_timer.value() == None

    temp_alert.process_element(reading_2, ctx)
    timer_ts_2 = temp_alert.current_timer.value()
    assert temp_alert.last_temp.value() == 75
    assert timer_ts_2 != None

    temp_alert.process_element(reading_3, ctx)
    assert temp_alert.last_temp.value() == 85
    assert temp_alert.current_timer.value() == timer_ts_2


def test_temp_inc_alert_func_should_not_create_timer_if_temp_decreasing():
    reading_1 = SensorReading.from_tuple((1, 100, datetime.datetime.now()))
    reading_2 = SensorReading.from_tuple((1, 50, datetime.datetime.now()))
    reading_3 = SensorReading.from_tuple((1, 0, datetime.datetime.now()))

    temp_alert = TempIncAlertFunc(last_temp=LastTemp(), current_timer=CurrentTimer())
    ctx = TestContext(1)
    temp_alert.process_element(reading_1, ctx)
    assert temp_alert.last_temp.value() == 85
    assert temp_alert.current_timer.value() == None

    temp_alert.process_element(reading_2, ctx)
    assert temp_alert.last_temp.value() == 75
    assert temp_alert.current_timer.value() == None

    temp_alert.process_element(reading_3, ctx)
    assert temp_alert.last_temp.value() == 65
    assert temp_alert.current_timer.value() == None


def test_temp_inc_alert_func_should_not_create_timer_if_fluctuating():
    reading_1 = SensorReading.from_tuple((1, 0, datetime.datetime.now()))
    reading_2 = SensorReading.from_tuple((1, 100, datetime.datetime.now()))
    reading_3 = SensorReading.from_tuple((1, 50, datetime.datetime.now()))

    temp_alert = TempIncAlertFunc(last_temp=LastTemp(), current_timer=CurrentTimer())
    ctx = TestContext(1)
    temp_alert.process_element(reading_1, ctx)
    assert temp_alert.last_temp.value() == 65
    assert temp_alert.current_timer.value() == None

    temp_alert.process_element(reading_2, ctx)
    assert temp_alert.last_temp.value() == 85
    assert temp_alert.current_timer.value() != None

    temp_alert.process_element(reading_3, ctx)
    assert temp_alert.last_temp.value() == 75
    assert temp_alert.current_timer.value() == None
