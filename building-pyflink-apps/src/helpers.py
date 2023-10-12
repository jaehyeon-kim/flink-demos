from s05_data_gen import DataGenerator
from models import SkyoneData, UserStatistics


def build_flight(email_address: str = None):
    data_gen = DataGenerator()
    skyone = data_gen.generate_skyone_data()
    flight = SkyoneData.to_flight_data(skyone.to_row())
    if email_address is not None:
        flight.email_address = email_address
    return flight


def build_user_statistics(email_address: str = None):
    flight = build_flight(email_address)
    return UserStatistics.from_flight(flight)
