import datetime
import dataclasses

from pyflink.common import Types, Row

from utils import serialize


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

    def get_duration(self):
        return int(
            (
                datetime.datetime.fromisoformat(self.arrival_time)
                - datetime.datetime.fromisoformat(self.departure_time)
            ).seconds
            / 60
        )

    def to_row(self):
        return Row(
            email_address=self.email_address,
            departure_time=serialize(self.departure_time),
            departure_airport_code=self.departure_airport_code,
            arrival_time=serialize(self.arrival_time),
            arrival_airport_code=self.arrival_airport_code,
            flight_number=self.flight_number,
            confirmation=self.confirmation,
            source=self.source,
        )

    @classmethod
    def from_row(cls, row: Row):
        return cls(
            email_address=row.email_address,
            departure_time=row.departure_time,
            departure_airport_code=row.departure_airport_code,
            arrival_time=row.arrival_time,
            arrival_airport_code=row.arrival_airport_code,
            flight_number=row.flight_number,
            confirmation=row.confirmation,
            source=row.source,
        )

    @staticmethod
    def get_key_type_info():
        return Types.ROW_NAMED(
            field_names=[
                "confirmation",
            ],
            field_types=[
                Types.STRING(),
            ],
        )

    @staticmethod
    def get_value_type_info():
        return Types.ROW_NAMED(
            field_names=[
                "email_address",
                "departure_time",
                "departure_airport_code",
                "arrival_time",
                "arrival_airport_code",
                "flight_number",
                "confirmation",
                "source",
            ],
            field_types=[
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
            ],
        )

    @staticmethod
    def to_user_statistics_data(row: Row):
        data = FlightData.from_row(row)
        return UserStatistics(data.email_address, data.get_duration(), 1)


@dataclasses.dataclass
class BaseStatistics:
    email_address: str
    total_flight_duration: int
    number_of_flights: int


@dataclasses.dataclass
class UserStatistics(BaseStatistics):
    def to_row(self):
        return Row(
            email_address=self.email_address,
            total_flight_duration=self.total_flight_duration,
            number_of_flights=self.number_of_flights,
        )

    @staticmethod
    def merge(this: BaseStatistics, that: BaseStatistics):
        assert this.email_address == that.email_address
        return UserStatistics(
            email_address=this.email_address,
            total_flight_duration=this.total_flight_duration + that.total_flight_duration,
            number_of_flights=this.number_of_flights + that.number_of_flights,
        )

    @classmethod
    def from_flight(cls, data: FlightData):
        return cls(
            email_address=data.email_address,
            total_flight_duration=data.get_duration(),
            number_of_flights=1,
        )

    @classmethod
    def from_row(cls, row: Row):
        return cls(
            email_address=row.email_address,
            total_flight_duration=row.total_flight_duration,
            number_of_flights=row.number_of_flights,
        )

    @staticmethod
    def get_key_type_info():
        return Types.ROW_NAMED(
            field_names=[
                "email_address",
            ],
            field_types=[
                Types.STRING(),
            ],
        )

    @staticmethod
    def get_value_type_info():
        return Types.ROW_NAMED(
            field_names=[
                "email_address",
                "total_flight_duration",
                "number_of_flights",
            ],
            field_types=[
                Types.STRING(),
                Types.INT(),
                Types.INT(),
            ],
        )


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
    def from_row(cls, row: Row):
        return cls(
            email_address=row.email_address,
            flight_departure_time=row.flight_departure_time,
            iata_departure_code=row.iata_departure_code,
            flight_arrival_time=row.flight_arrival_time,
            iata_arrival_code=row.iata_arrival_code,
            flight_number=row.flight_number,
            confirmation=row.confirmation,
            ticket_price=row.ticket_price,
            aircraft=row.aircraft,
            booking_agency_email=row.booking_agency_email,
        )

    def to_row(self):
        return Row(
            email_address=self.email_address,
            flight_departure_time=serialize(self.flight_departure_time),
            iata_departure_code=self.iata_departure_code,
            flight_arrival_time=serialize(self.flight_arrival_time),
            iata_arrival_code=self.iata_arrival_code,
            flight_number=self.flight_number,
            confirmation=self.confirmation,
            ticket_price=self.ticket_price,
            aircraft=self.aircraft,
            booking_agency_email=self.booking_agency_email,
        )

    @staticmethod
    def get_value_type_info():
        return Types.ROW_NAMED(
            field_names=[
                "email_address",
                "flight_departure_time",
                "iata_departure_code",
                "flight_arrival_time",
                "iata_arrival_code",
                "flight_number",
                "confirmation",
                "ticket_price",
                "aircraft",
                "booking_agency_email",
            ],
            field_types=[
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
            ],
        )

    @staticmethod
    def to_flight_data(row: Row):
        data = SkyoneData.from_row(row)
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
    def from_row(cls, row: Row):
        return cls(
            customer_email_address=row.customer_email_address,
            departure_time=row.departure_time,
            departure_airport=row.departure_airport,
            arrival_time=row.arrival_time,
            arrival_airport=row.arrival_airport,
            flight_duration=row.flight_duration,
            flight_id=row.flight_id,
            reference_number=row.reference_number,
            total_price=row.total_price,
            aircraft_details=row.aircraft_details,
        )

    def to_row(self):
        return Row(
            customer_email_address=self.customer_email_address,
            departure_time=serialize(self.departure_time),
            departure_airport=self.departure_airport,
            arrival_time=serialize(self.arrival_time),
            arrival_airport=self.arrival_airport,
            flight_duration=self.flight_duration,
            flight_id=self.flight_id,
            reference_number=self.reference_number,
            total_price=self.total_price,
            aircraft_details=self.aircraft_details,
        )

    @staticmethod
    def get_value_type_info():
        return Types.ROW_NAMED(
            field_names=[
                "customer_email_address",
                "departure_time",
                "departure_airport",
                "arrival_time",
                "arrival_airport",
                "flight_duration",
                "flight_id",
                "reference_number",
                "total_price",
                "aircraft_details",
            ],
            field_types=[
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
            ],
        )

    @staticmethod
    def to_flight_data(row: Row):
        data = SunsetData.from_row(row)
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
