import logging
from pathlib import Path
from datetime import datetime, timedelta

from faker import Faker
import pandas as pd
import numpy as np

from src.logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)


class TimeContextGenerator:
    """
    Generates time-based context features.
    Used by both the Offline Simulator and Live Recommender.
    """

    def __init__(self, fake: Faker):
        self.fake = fake

    def get_context(self, dt: datetime = None, end_date_str: str = None):
        """
        If dt is provided (Live Mode), uses that.
        If end_date_str is provided (Offline Mode), generates random time in last 90 days.
        """
        if dt is None and end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            dt = self.fake.date_time_between(
                start_date=end_date - timedelta(days=90), end_date=end_date
            )
        elif dt is None:
            dt = datetime.now()

        hour = dt.hour
        weekday = dt.weekday()  # 0=Mon, 6=Sun
        is_weekend = 1 if weekday >= 5 else 0

        return {
            "is_morning": 1 if 6 <= hour < 12 else 0,
            "is_afternoon": 1 if 12 <= hour < 18 else 0,
            "is_evening": 1 if 18 <= hour < 24 else 0,
            "is_weekend": is_weekend,
            "is_weekday": 1 - is_weekend,
        }


class GroundTruth:
    """
    The HIDDEN FORMULA (Ground Truth).
    Determines if a user clicks based on context.
    """

    @staticmethod
    def calculate_probability(user_ctx: dict, item_ctx: dict) -> float:
        score = -2.5  # Base Logit

        # Rule 1: Morning Coffee
        if (
            user_ctx.get("is_morning") == 1
            and item_ctx.get("cat_Drinks & Desserts") == 1
        ):
            score += 2.5

        # Rule 2: Weekend Comfort Food
        if user_ctx.get("is_weekend") == 1:
            if (
                item_ctx.get("cat_Pizzas") == 1
                or item_ctx.get("cat_Burgers & Sandwiches") == 1
            ):
                score += 1.8

        # Rule 3: Budget Constraint
        user_age = user_ctx.get("age", 0.5)
        item_price = item_ctx.get("price", 0.5)
        if user_age < 0.25 and item_price > 0.8:
            score -= 3.0

        # Rule 4: Traffic Bias
        if user_ctx.get("traffic_source_Search") == 1:
            score += 0.5

        return 1 / (1 + np.exp(-score))

    def will_click(self, user_ctx: dict, item_ctx: dict, fake: Faker) -> int:
        """Simulates a Bernoulli trial (1 or 0)."""
        prob = self.calculate_probability(user_ctx, item_ctx)
        return 1 if fake.random.random() < prob else 0


class BanditSimulator:
    def __init__(self, data_path: Path, fake: Faker):
        self.fake = fake
        self.data_path = data_path

        # Init helpers
        self.time_gen = TimeContextGenerator(fake)
        self.ground_truth = GroundTruth()

        # Load data
        self.users = pd.read_csv(data_path / "user_features.csv").to_dict("records")
        self.products = pd.read_csv(data_path / "product_features.csv").to_dict(
            "records"
        )
        logger.info(
            f"Loaded {len(self.users)} users and {len(self.products)} products."
        )

    def generate_history(self, num_events: int, end_date_str: str):
        logger.info(f"Generating {num_events} events...")

        event_rows = []
        interaction_rows = []

        for i in range(num_events):
            event_id = i + 1

            # Random Entities
            user = self.fake.random_element(elements=self.users)
            item = self.fake.random_element(elements=self.products)

            # Time Context
            time_ctx = self.time_gen.get_context(end_date_str=end_date_str)

            # Combine Context
            context_row = {
                "event_id": event_id,
                **{k: v for k, v in user.items() if k != "user_id"},
                **time_ctx,
            }

            # Make Response
            response = self.ground_truth.will_click(context_row, item, self.fake)

            # Append
            event_rows.append(context_row)
            interaction_rows.append(
                {
                    "event_id": event_id,
                    "product_id": item["product_id"],
                    "response": response,
                }
            )

        # Save
        df_events = pd.DataFrame(event_rows)
        df_interactions = pd.DataFrame(interaction_rows)

        df_events.to_csv(self.data_path / "event_features.csv", index=False)
        df_interactions.to_csv(self.data_path / "interaction.csv", index=False)

        logger.info(f"Done. Saved to {self.data_path}/")
        logger.info(f"Avg Click Rate: {df_interactions['response'].mean():.2%}")
