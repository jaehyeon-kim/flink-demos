import datetime
import json
import typing
import dataclasses


@dataclasses.dataclass
class FlightData:
    email_address: str
    departure_time: str
    departure_airport_code: str
    arrival_time: str
    arrival_airport_code: str
    flight_number: str
    confirmation: str
    source: str


@dataclasses.dataclass
class SkyoneData:
    email_address: str
    flight_departure_time: str
    iata_departure_code: str
    flight_arrival_time: str
    iata_arrival_code: str
    flight_number: str
    confirmation: str
    ticket_price: int
    aircraft: str
    booking_agency_email: str

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def fromstr(cls, msg: typing.Union[str, dict]):
        if isinstance(msg, str):
            msg = json.loads(msg)
        return cls(**msg)

    @staticmethod
    def parse(value: str):
        return SkyoneData.fromstr(json.loads(value))

    @staticmethod
    def to_flight_data(value: str) -> FlightData:
        data = SkyoneData.parse(value)
        return FlightData(
            data.email_address,
            data.flight_departure_time,
            data.iata_departure_code,
            data.flight_arrival_time,
            data.iata_arrival_code,
            data.flight_number,
            data.confirmation,
            "skyone",
        )


@dataclasses.dataclass
class SunsetData:
    customer_email_address: str
    departure_time: str
    departure_airport: str
    arrival_time: str
    arrival_airport: str
    flight_duration: int
    flight_id: str
    reference_number: str
    total_price: int
    aircraft_details: str

    def asdict(self):
        return dataclasses.asdict(self)

    @classmethod
    def fromstr(cls, msg: typing.Union[str, dict]):
        if isinstance(msg, str):
            msg = json.loads(msg)
        return cls(**msg)

    @staticmethod
    def parse(value: str):
        return SunsetData.fromstr(json.loads(value))

    @staticmethod
    def to_flight_data(value: str) -> FlightData:
        data = SunsetData.parse(value)
        return FlightData(
            data.customer_email_address,
            data.departure_time,
            data.departure_airport,
            data.arrival_time,
            data.arrival_airport,
            data.flight_id,
            data.reference_number,
            "sunset",
        )
