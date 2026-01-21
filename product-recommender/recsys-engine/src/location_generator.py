import argparse
import ast
import logging
from pathlib import Path
from faker import Faker
from typing import List, Union, Dict

from src.utils import generate_from_csv
from src.logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)


class LocationGenerator:
    """
    A class to pre-calculate a location distribution and generate random locations from it efficiently.
    """

    def __init__(
        self,
        country: Union[str, Dict[str, float]] = "*",
        state: Union[str, Dict[str, float]] = "*",
        city: Union[str, Dict[str, float]] = "*",
        postal_code: Union[str, Dict[str, float]] = "*",
        location_data: List[Dict] = None,
        fake: Faker = Faker(),
    ):
        """Pre-calculates a filtered universe and weighted distribution for locations.

        This constructor performs the computationally expensive setup required to
        generate random locations. It filters the source data based on the provided
        rules and then calculates the final probability weights for each location
        in the resulting universe.

        This allows for subsequent calls to the `.get_one()` or `.get_many()`
        methods to be extremely fast, as they only need to perform the final
        random sampling.

        The filtering logic is sequential and cumulative (like an "AND" condition).
        The weighting logic prioritizes the most specific rule provided in the
        order: postal_code > city > state > country.

        Args:
            country (Union[str, Dict[str, float]]): Specifies the country.
                Can be a single string (e.g., 'United States'), a wildcard '*'
                to allow any, or a dictionary of proportions (e.g.,
                {'United States': 0.75, 'United Kingdom': 0.25}).
            state (Union[str, Dict[str, float]]): Specifies the state.
                Accepts the same formats as the country parameter.
            city (Union[str, Dict[str, float]]): Specifies the city.
                Accepts the same formats as the country parameter.
            postal_code (Union[str, Dict[str, float]]): Specifies the postal code.
                Accepts the same formats as the country parameter.
            location_data (List[Dict], optional): A list of dictionaries, where
                each dictionary represents a location. If None, it will be
                generated from the default source. Defaults to None.

        Raises:
            TypeError: If a filter parameter is provided with a type other than
                str or dict.
        """

        self.fake = fake

        def collect_filter_vals(col):
            if isinstance(col, str):
                return [col]
            elif isinstance(col, dict):
                return col.keys()
            else:
                raise TypeError("Location filter should be either string or dictionary")

        if location_data is None:
            location_data = generate_from_csv(
                Path(__file__).resolve().parent.joinpath("data", "world_pop.csv")
            )

        # 1. Filtering Stage
        universe = location_data
        filter_map = {
            "country": country,
            "state": state,
            "city": city,
            "postal_code": postal_code,
        }
        for column, spec in filter_map.items():
            if spec != "*":
                allowed = collect_filter_vals(spec)
                universe = [row for row in universe if row[column] in allowed]

        if not universe:
            universe = location_data

        # 2. Weighting Stage
        total_pop = sum(int(loc["population"]) for loc in universe)

        for loc in universe:
            original_pop = int(loc["population"])
            loc["population"] = original_pop

            if isinstance(postal_code, dict) and loc["postal_code"] in postal_code:
                loc["population"] = postal_code[loc["postal_code"]] * total_pop
            elif isinstance(city, dict) and loc["city"] in city:
                city_pop = sum(
                    int(loc2["population"])
                    for loc2 in universe
                    if loc2["city"] == loc["city"]
                )
                if city_pop > 0:
                    loc["population"] = (
                        city[loc["city"]] * (original_pop / city_pop)
                    ) * total_pop
            elif isinstance(state, dict) and loc["state"] in state:
                state_pop = sum(
                    int(loc2["population"])
                    for loc2 in universe
                    if loc2["state"] == loc["state"]
                )
                if state_pop > 0:
                    loc["population"] = (
                        state[loc["state"]] * (original_pop / state_pop)
                    ) * total_pop
            elif isinstance(country, dict) and loc["country"] in country:
                country_pop = sum(
                    int(loc2["population"])
                    for loc2 in universe
                    if loc2["country"] == loc["country"]
                )
                if country_pop > 0:
                    loc["population"] = (
                        country[loc["country"]] * (original_pop / country_pop)
                    ) * total_pop

        # 3. Pre-calculation of final weights
        current_total_pop = sum(loc["population"] for loc in universe)

        # Store the pre-calculated universe and weights for later use
        self.universe = universe
        if current_total_pop == 0:
            self.weights = [1] * len(self.universe)
        else:
            self.weights = [
                loc["population"] / current_total_pop for loc in self.universe
            ]

    def get_one(self) -> dict:
        """
        Returns a single random location. This method is very fast.
        """
        if not self.universe:
            return {}

        selected_loc = self.fake.random.choices(
            self.universe, weights=self.weights, k=1
        )[0]

        return {
            "city": selected_loc["city"],
            "state": selected_loc["state"],
            "postal_code": selected_loc["postal_code"],
            "country": selected_loc["country"],
            "latitude": selected_loc["latitude"],
            "longitude": selected_loc["longitude"],
        }

    def get_many(self, num_locations: int) -> List[dict]:
        """
        Returns a list of random locations. Even more efficient for bulk generation.
        """
        if not self.universe:
            return []

        selected_locs = self.fake.random.choices(
            self.universe, weights=self.weights, k=num_locations
        )

        return [
            {
                "city": loc["city"],
                "state": loc["state"],
                "postal_code": loc["postal_code"],
                "country": loc["country"],
                "latitude": loc["latitude"],
                "longitude": loc["longitude"],
            }
            for loc in selected_locs
        ]

    @staticmethod
    def loc_prop(value):
        """Parses string, wildcard, or dictionary string."""
        value = value.strip()
        # Check if it looks like a dictionary
        if value.startswith("{") and value.endswith("}"):
            try:
                return ast.literal_eval(value)  # Safely evaluates string to dict
            except (ValueError, SyntaxError):
                raise argparse.ArgumentTypeError(f"Invalid dictionary format: {value}")
        return value
